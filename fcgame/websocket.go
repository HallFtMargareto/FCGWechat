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
	conn          *websocket.Conn
	msgID         int
	config        ClientConfig
	isConnected   bool
	isShutdown    bool
	reconnectCnt  int
	heartbeatStop chan bool
	messageStop   chan bool
	errorChan     chan error
	logger        *zap.Logger
	writeMtx      sync.Mutex  // 新增: 用来保护写操作
	sendChan      chan []byte // 新增: 写协程使用的消息队列
}

// 创建新的WebSocket客户端
func NewWebSocketClient(config ClientConfig, log *zap.Logger) *WebSocketClient {
	return &WebSocketClient{
		config:        config,
		msgID:         1,
		isConnected:   false,
		isShutdown:    false,
		reconnectCnt:  0,
		heartbeatStop: make(chan bool, 1),
		messageStop:   make(chan bool, 1),
		errorChan:     make(chan error, 10),
		logger:        log,
		sendChan:      make(chan []byte, MaxMessageQueue), // 缓冲区可调
	}
}

// 连接到WebSocket服务器
func (client *WebSocketClient) Connect() error {
	if client.isShutdown {
		return fmt.Errorf("客户端已关闭")
	}

	// 构建连接URL
	u := url.URL{
		Scheme: client.config.Scheme,
		Host:   client.config.ServerHost + ":" + client.config.ServerPort,
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
	fmt.Println("✅ WebSocket连接建立成功!")

	// 设置连接参数
	client.conn.SetReadLimit(MaxMessageSize)
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
		fmt.Printf("❗ 连接失败 (%d/%d): %v", client.reconnectCnt, client.config.MaxReconnect, err)

		if client.reconnectCnt < client.config.MaxReconnect && !client.isShutdown {
			fmt.Printf("🔄 %v 后尝试重连...", client.config.ReconnectWait)
			time.Sleep(client.config.ReconnectWait)
		}
	}

	return fmt.Errorf("达到最大重连次数 (%d)，连接失败, 请联系管理员处理。", client.config.MaxReconnect)
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
		return fmt.Errorf("连接未建立")
	}

	// 设置写超时
	client.conn.SetWriteDeadline(time.Now().Add(SendTimeout))
	err := client.conn.WriteMessage(msgType, data)
	client.conn.SetWriteDeadline(time.Time{}) // 清除超时

	if err != nil {
		client.isConnected = false
		return fmt.Errorf("写入消息失败: %v", err)
	}
	return nil
}

// 发送消息到服务器
func (client *WebSocketClient) SendMessage(path string, data interface{}) error {
	request := Request{
		Id:        Snowflake.Generate().Int64(),
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

	// return client.write(websocket.TextMessage, msgBytes)
	client.sendChan <- msgBytes
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
	// return client.write(websocket.PingMessage, nil)
	client.sendChan <- websocket.FormatCloseMessage(websocket.PingMessage, "")
	return nil
}

// 发送文本消息
func (client *WebSocketClient) SendTextMessage(text []byte) error {
	// return client.write(websocket.TextMessage, []byte(text))
	client.sendChan <- text
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

	for client.IsConnected() && !client.isShutdown {
		// 设置读取超时
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			client.logger.Error("读取消息错误:", zap.Error(err))

			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Println("WebSocket意外关闭: ", err)
			} else if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				fmt.Println("WebSocket正常关闭")
			} else {
				fmt.Println("读取消息失败: ", err)
			}
			client.isConnected = false

			// 如果需要自动重连
			if client.config.Reconnect && !client.isShutdown {
				err = client.ConnectWithRetry()
				if err != nil {
					fmt.Println("自动重连失败: ", err)
					return
				}
			} else {
				return
			}
		}

		// 解析响应消息
		var response Response
		err = json.Unmarshal(message, &response)
		if err != nil {
			client.logger.Error("parsing response message error: ", zap.Error(err), zap.Any("response", response))
			fmt.Println("parsing response message error: ", message)
		} else {
			if response.Data != nil {
				dataBytes, _ := json.MarshalIndent(response.Data, "", "  ")
				fmt.Println("接收数据: ", string(dataBytes))
			}
		}
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
		"connected":       client.IsConnected(),
		"connection_info": client.GetConnectionInfo(),
		"message_id":      client.msgID,
		"reconnect_count": client.reconnectCnt,
		"is_shutdown":     client.isShutdown,
	}
}

// 关闭连接
func (client *WebSocketClient) Close() {
	// client.isShutdown = true
	// client.isConnected = false

	// // 停止心跳和消息监听
	// client.StopHeartbeat()
	// client.StopListening()

	// if client.conn != nil {
	// 	// 发送关闭消息
	// 	client.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "客户端主动关闭"))
	// 	client.conn.Close()
	// 	fmt.Println("🔌 WebSocket连接已关闭")
	// }

	// 关闭通道
	close(client.heartbeatStop)
	close(client.messageStop)
	close(client.errorChan)

	client.isShutdown = true
	client.isConnected = false

	client.StopHeartbeat()
	client.StopListening()

	if client.conn != nil {
		client.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "客户端主动关闭"))
		client.conn.Close()
		fmt.Println("🔌 WebSocket连接已关闭")
	}

	close(client.sendChan) // 新增: 停止写协程
	close(client.heartbeatStop)
	close(client.messageStop)
	close(client.errorChan)
}

// 写协程: 顺序写出所有消息
func (client *WebSocketClient) StartWriter() {
	for msg := range client.sendChan {
		if !client.IsConnected() {
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
	}
}
