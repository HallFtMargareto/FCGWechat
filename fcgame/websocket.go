package fcgame

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// GetDefaultConfig 获取默认配置
func GetDefaultConfig() ClientConfig {
	return ClientConfig{
		ServerHost:    WSServer,
		ServerPort:    WSPort,
		Path:          WSPath,
		Scheme:        WSScheme,
		Token:         DefaultToken,
		RequireAuth:   RequireAuth,
		Version:       ClientVersion,
		UserAgent:     "FCGameServer-WebSocket-Client/" + ClientVersion,
		Origin:        "http://localhost",
		Subprotocols:  []string{},
		Timeout:       ConnectTimeout,
		Heartbeat:     HeartbeatInterval,
		Reconnect:     true,
		MaxReconnect:  MaxReconnectCount,
		ReconnectWait: ReconnectInterval,
	}
}

type WebSocketClient struct {
	conn            *websocket.Conn
	msgID           int
	config          ClientConfig
	isConnected     bool
	isShutdown      bool
	reconnectCnt    int
	heartbeatStop   chan bool
	messageStop     chan bool
	errorChan       chan error
	logger          *zap.Logger
	writeMtx        sync.Mutex  // 用来保护写操作
	sendChan        chan []byte // 写协程使用的消息队列
	pendingMessages [][]byte    // 缓存未发送的消息
	pendingMtx      sync.Mutex  // 保护pendingMessages的并发访问
}

// 创建新的WebSocket客户端
func NewWebSocketClient(config ClientConfig, log *zap.Logger) *WebSocketClient {
	return &WebSocketClient{
		config:          config,
		msgID:           1,
		isConnected:     false,
		isShutdown:      false,
		reconnectCnt:    0,
		heartbeatStop:   make(chan bool, 1),
		messageStop:     make(chan bool, 1),
		errorChan:       make(chan error, 10),
		logger:          log,
		sendChan:        make(chan []byte, MaxMessageQueue), // 缓冲区可调
		pendingMessages: make([][]byte, 0),                  // 初始化消息缓存
	}
}

func (client *WebSocketClient) Run() {
	for !client.isShutdown {
		// 1. 尝试连接
		if err := client.Connect(); err != nil {
			fmt.Println("Connection failed. Retrying...", err)
			// 使用ConnectWithRetry的逻辑进行等待
			if client.config.Reconnect && !client.isShutdown {
				time.Sleep(client.config.ReconnectWait)
				continue // 继续下一次循环尝试连接
			} else {
				break // 如果不允许重连，则退出
			}
		}

		// 2. 连接成功，启动所有协程
		// 使用一个 channel 来监听任意协程退出
		goroutineDone := make(chan bool, 2)

		SafeRun(func() {
			client.StartWriter()
			fmt.Println("StartWriter Coroutine Exit。")
			goroutineDone <- true
		})

		SafeRun(func() {
			client.ListenMessages()
			fmt.Println("ListenMessages Coroutine Exit。")
			goroutineDone <- true
		})

		// go func() {
		// 	client.StartHeartbeat()
		// 	client.logger.Info("StartHeartbeat 协程已退出。")
		// 	goroutineDone <- true
		// }()
		//client.logger.Info("客户端已连接，读、写、心跳协程已启动。")

		// 3. 等待任意一个协程退出（意味着连接断开）
		<-goroutineDone
		fmt.Println("Coroutine Exit, Retrying...")

		// 4. 清理旧的连接和协程
		client.isConnected = false // 标记连接为断开状态

		// 保存未发送的消息
		client.saveUnsentMessages()

		// 关闭连接，这会导致其他协程也退出
		if client.conn != nil {
			client.conn.Close()
			client.conn = nil
		}

		// 关闭sendChan来停止写协程（如果还没有停止的话）
		close(client.sendChan)

		// 等待一小段时间让其他协程有机会退出
		time.Sleep(100 * time.Millisecond)

		// 5. 准备下一次重连
		if client.config.Reconnect && !client.isShutdown {
			pendingCount := client.getPendingMessageCount()
			if pendingCount > 0 {
				fmt.Printf("waiting retrying. has %d message on queue...\n", pendingCount)
			}

			// 创建新的sendChan用于下一次连接
			client.sendChan = make(chan []byte, MaxMessageQueue)

			fmt.Println("retrying...")
			time.Sleep(client.config.ReconnectWait)
		}
	}
	client.logger.Info("client for end.")
}

// 连接到WebSocket服务器
func (client *WebSocketClient) Connect() error {
	if client.isShutdown {
		return fmt.Errorf("client has close")
	}

	// 构建连接URL
	fullHost := client.config.ServerHost
	if client.config.ServerPort != "" {
		fullHost = fullHost + ":" + client.config.ServerPort
	}

	u := url.URL{
		Scheme: client.config.Scheme,
		Host:   fullHost,
		Path:   client.config.Path,
	}

	client.logger.Info("正在连接到WebSocket服务器: " + u.String())

	// 设置请求头
	headers := http.Header{}
	headers.Set("User-Agent", client.config.UserAgent)
	headers.Set("Origin", client.config.Origin)

	// 如果需要认证，添加JWT Token
	if client.config.RequireAuth && client.config.Token != "" {
		headers.Set("Authorization", "Bearer "+client.config.Token)
	}

	// 设置子协议
	if len(client.config.Subprotocols) > 0 {
		headers.Set("Sec-WebSocket-Protocol", strings.Join(client.config.Subprotocols, ", "))
	}

	// 创建连接
	dialer := websocket.Dialer{
		HandshakeTimeout: client.config.Timeout,
		Subprotocols:     client.config.Subprotocols,
	}

	conn, resp, err := dialer.Dial(u.String(), headers)
	if err != nil {
		if resp != nil {
			client.logger.Error("Socket连接失败", zap.Int("resp.StatusCode", resp.StatusCode), zap.Int("resp.StatusCode", resp.StatusCode))
			switch resp.StatusCode {
			case 401:
				return fmt.Errorf("认证失败: 请检查JWT Token是否正确")
			case 403:
				return fmt.Errorf("IP被禁止或权限不足")
			case 429:
				return fmt.Errorf("请求过于频繁，被限流")
			}
		}
		return fmt.Errorf("连接失败: %v", err)
	}

	// 关闭响应体
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	client.conn = conn
	client.isConnected = true
	client.reconnectCnt = 0
	fmt.Println("✅ Socket Conn Success!", u)

	// 设置连接参数
	client.conn.SetReadLimit(int64(MaxMessageSize))
	return nil
}

// 自动重连机制
func (client *WebSocketClient) ConnectWithRetry() error {
	for client.reconnectCnt < client.config.MaxReconnect && !client.isShutdown {
		err := client.Connect()
		if err == nil {
			return nil
		}

		client.reconnectCnt++
		fmt.Printf("❗ connection fail: (%d/%d): %v", client.reconnectCnt, client.config.MaxReconnect, err)

		if client.reconnectCnt < client.config.MaxReconnect && !client.isShutdown {
			fmt.Printf("🔄 %v Second. Retrying...", client.config.ReconnectWait)
			time.Sleep(client.config.ReconnectWait)
		}
	}

	return fmt.Errorf("Max Retrying (%d) Connection Fail", client.config.MaxReconnect)
}

// 检查连接状态
func (client *WebSocketClient) IsConnected() bool {
	return client.isConnected && client.conn != nil && !client.isShutdown
}

// 获取连接信息
func (client *WebSocketClient) GetConnectionInfo() string {
	if !client.IsConnected() {
		return "未连接"
	}
	return fmt.Sprintf("%s://%s:%s%s",
		client.config.Scheme,
		client.config.ServerHost,
		client.config.ServerPort,
		client.config.Path)
}

// 统一的写方法（带写锁 & 超时控制）
func (client *WebSocketClient) write(msgType int, data []byte) error {
	client.writeMtx.Lock()
	defer client.writeMtx.Unlock()

	if !client.isConnected {
		return fmt.Errorf("client not connection")
	}

	// 设置写超时
	client.conn.SetWriteDeadline(time.Now().Add(SendTimeout))
	err := client.conn.WriteMessage(msgType, data)
	client.conn.SetWriteDeadline(time.Time{}) // 清除超时

	if err != nil {
		client.isConnected = false
		return fmt.Errorf("write message error: %v", err)
	}
	return nil
}

// 发送消息到服务器
func (client *WebSocketClient) SendMessage(path string, data interface{}) error {
	// 使用互斥锁保护Snowflake的并发访问
	snowflakeMutex.Lock()
	messageId := Snowflake.Generate().Int64()
	snowflakeMutex.Unlock()

	request := Request{
		Id:        messageId,
		Ver:       client.config.Version,
		Path:      path,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}
	client.msgID++

	msgBytes, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %v", err)
	}

	// 将消息添加到待发送队列
	client.addPendingMessage(msgBytes)

	// 如果连接正常，直接发送
	if client.IsConnected() {
		client.sendChan <- msgBytes
	}
	return nil
}

func (client *WebSocketClient) SendContact(contact FcgContact) error {
	return client.SendMessage("fccontact", contact)
}
func (client *WebSocketClient) SendFcgMessage(message FcgMessage) error {
	return client.SendMessage("fcmessage", message)
}
func (client *WebSocketClient) SendClientLog(log any) error {
	return client.SendMessage("clientlog", log)
}

// 发送心跳（Ping）
func (client *WebSocketClient) SendHeartbeat() error {
	// 心跳消息不需要缓存，因为它是周期性的
	if client.IsConnected() {
		client.sendChan <- websocket.FormatCloseMessage(websocket.PingMessage, "")
	}
	return nil
}

// 发送文本消息
func (client *WebSocketClient) SendTextMessage(text []byte) error {
	// 将消息添加到待发送队列
	client.addPendingMessage(text)

	// 如果连接正常，直接发送
	if client.IsConnected() {
		client.sendChan <- text
	}
	return nil
}

// 监听服务器消息
func (client *WebSocketClient) ListenMessages() {

	// 设置 PongHandler
	client.conn.SetPongHandler(func(appData string) error {
		fmt.Println("接收服务器PING包:", time.Now().Unix())
		client.conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		return nil
	})

	for {
		if !client.IsConnected() {
			return
		}

		// 设置读取超时
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			client.logger.Error("Read Message Error:", zap.Error(err))

			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Println("WebSocket Unexpected shutdown: ", err)
			} else if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				fmt.Println("WebSocket Unexpected shutdown")
			} else {
				fmt.Println("Read Message Error: ", err)
			}
			client.isConnected = false
			return
			// 如果需要自动重连
			// if client.config.Reconnect && !client.isShutdown {
			// 	err = client.ConnectWithRetry()
			// 	if err != nil {
			// 		fmt.Println("自动重连失败: ", err)
			// 		return
			// 	}
			// } else {
			// 	return
			// }
		}

		// 解析响应消息
		// var response Response
		// err = json.Unmarshal(message, &response)
		// if err != nil {
		// 	client.logger.Error("parsing response message error: ", zap.Error(err), zap.Any("response", response))
		// 	fmt.Println("parsing response message error: ", message)
		// } else {
		// 	if response.Data != nil {
		// 		dataBytes, _ := json.MarshalIndent(response.Data, "", "  ")
		// 		fmt.Println("接收数据: ", string(dataBytes))
		// 	}
		// }
		fmt.Println(string(message))
	}
}

// 启动心跳检测
func (client *WebSocketClient) StartHeartbeat() {
	if client.isShutdown {
		return
	}

	ticker := time.NewTicker(client.config.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !client.IsConnected() {
				return
			}
			err := client.SendHeartbeat()
			if err != nil {
				client.logger.Error("❗ 心跳失败: ", zap.Error(err))
				return
			}
		case <-client.heartbeatStop:
			return
		}
	}
}

// 停止心跳
func (client *WebSocketClient) StopHeartbeat() {
	select {
	case client.heartbeatStop <- true:
	default:
	}
}

// 停止消息监听
func (client *WebSocketClient) StopListening() {
	select {
	case client.messageStop <- true:
	default:
	}
}

// 获取统计信息
func (client *WebSocketClient) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"connected":             client.IsConnected(),
		"connection_info":       client.GetConnectionInfo(),
		"message_id":            client.msgID,
		"reconnect_count":       client.reconnectCnt,
		"is_shutdown":           client.isShutdown,
		"pending_message_count": client.getPendingMessageCount(),
	}
}

// 关闭连接
func (client *WebSocketClient) Close() {
	client.isShutdown = true
	client.isConnected = false

	// 保存未发送的消息
	client.saveUnsentMessages()

	// 停止心跳和消息监听
	client.StopHeartbeat()
	client.StopListening()

	if client.conn != nil {
		// 发送关闭消息
		client.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "客户端主动关闭"))
		client.conn.Close()
		fmt.Println("🔌 WebSocket连接已关闭")
	}

	// 安全地关闭通道，避免重复关闭
	select {
	case <-client.heartbeatStop:
		// 通道已关闭
	default:
		close(client.heartbeatStop)
	}

	select {
	case <-client.messageStop:
		// 通道已关闭
	default:
		close(client.messageStop)
	}

	select {
	case <-client.errorChan:
		// 通道已关闭
	default:
		close(client.errorChan)
	}

	// 关闭sendChan，停止写协程
	select {
	case <-client.sendChan:
		// 通道已关闭
	default:
		close(client.sendChan)
	}

	pendingCount := client.getPendingMessageCount()
	if pendingCount > 0 {
		fmt.Printf("客户端关闭，仍有 %d 条消息未发送\n", pendingCount)
	}
}

// 写协程: 顺序写出所有消息
func (client *WebSocketClient) StartWriter() {
	// 连接成功后，先发送缓存的消息
	if client.IsConnected() {
		client.sendPendingMessages()
	}

	for msg := range client.sendChan {
		if len(msg) == 0 {
			fmt.Println("空消息")
			continue
		}
		if !client.IsConnected() {
			client.logger.Error("连接已断开")
			client.isConnected = false
			return
		}
		client.conn.SetWriteDeadline(time.Now().Add(SendTimeout))
		err := client.conn.WriteMessage(websocket.TextMessage, msg)
		client.conn.SetWriteDeadline(time.Time{}) // 清除超时

		if err != nil {
			client.logger.Error("发送消息失败:", zap.Error(err))
			client.isConnected = false
			return
		}

		// 消息发送成功后，从缓存中移除
		client.removePendingMessage(msg)
	}
}

// addPendingMessage 添加消息到待发送缓存
func (client *WebSocketClient) addPendingMessage(msg []byte) {
	client.pendingMtx.Lock()
	defer client.pendingMtx.Unlock()

	// 避免重复添加相同的消息
	for _, pending := range client.pendingMessages {
		if string(pending) == string(msg) {
			return
		}
	}

	client.pendingMessages = append(client.pendingMessages, msg)
	client.logger.Debug("消息已添加到待发送缓存", zap.Int("缓存消息数", len(client.pendingMessages)))
}

// removePendingMessage 从待发送缓存中移除消息
func (client *WebSocketClient) removePendingMessage(msg []byte) {
	client.pendingMtx.Lock()
	defer client.pendingMtx.Unlock()

	for i, pending := range client.pendingMessages {
		if string(pending) == string(msg) {
			// 移除该消息
			client.pendingMessages = append(client.pendingMessages[:i], client.pendingMessages[i+1:]...)
			client.logger.Debug("消息已从待发送缓存中移除", zap.Int("剩余缓存消息数", len(client.pendingMessages)))
			break
		}
	}
}

// sendPendingMessages 发送所有缓存的消息
func (client *WebSocketClient) sendPendingMessages() {
	client.pendingMtx.Lock()
	pendingCount := len(client.pendingMessages)
	client.pendingMtx.Unlock()

	if pendingCount == 0 {
		return
	}

	client.logger.Info("开始发送缓存的消息", zap.Int("缓存消息数", pendingCount))

	// 复制一份待发送的消息，避免长时间锁定
	client.pendingMtx.Lock()
	messagesToSend := make([][]byte, len(client.pendingMessages))
	copy(messagesToSend, client.pendingMessages)
	client.pendingMtx.Unlock()

	// 逐个发送缓存的消息
	for _, msg := range messagesToSend {
		if !client.IsConnected() {
			client.logger.Error("连接已断开，停止发送缓存消息")
			return
		}

		select {
		case client.sendChan <- msg:
			client.logger.Debug("缓存消息已重新加入发送队列")
		default:
			// 如果发送队列满了，等待一段时间后重试
			client.logger.Warn("发送队列已满，等待后重试发送缓存消息")
			time.Sleep(100 * time.Millisecond)
			select {
			case client.sendChan <- msg:
				client.logger.Debug("缓存消息已重新加入发送队列（重试成功）")
			default:
				client.logger.Error("发送队列仍然满，缓存消息发送失败: " + string(msg))
				return
			}
		}
	}

	client.logger.Info("所有缓存消息已重新加入发送队列")
}

// saveUnsentMessages 保存未发送的消息（在连接断开时调用）
func (client *WebSocketClient) saveUnsentMessages() {
	client.pendingMtx.Lock()
	defer client.pendingMtx.Unlock()

	// 将sendChan中的消息转移到pendingMessages
	for {
		select {
		case msg := <-client.sendChan:
			if len(msg) > 0 {
				// 避免重复添加
				found := false
				for _, pending := range client.pendingMessages {
					if string(pending) == string(msg) {
						found = true
						break
					}
				}
				if !found {
					client.pendingMessages = append(client.pendingMessages, msg)
				}
			}
		default:
			// sendChan已空，退出循环
			return
		}
	}
}

// getPendingMessageCount 获取待发送消息数量
func (client *WebSocketClient) getPendingMessageCount() int {
	client.pendingMtx.Lock()
	defer client.pendingMtx.Unlock()
	return len(client.pendingMessages)
}
