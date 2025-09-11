package fcgame

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
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
	logger        *log.Logger
}

// 创建新的WebSocket客户端
func NewWebSocketClient(config ...ClientConfig) *WebSocketClient {
	var cfg ClientConfig
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = GetDefaultConfig()
	}

	return &WebSocketClient{
		config:        cfg,
		msgID:         1,
		isConnected:   false,
		isShutdown:    false,
		reconnectCnt:  0,
		heartbeatStop: make(chan bool, 1),
		messageStop:   make(chan bool, 1),
		errorChan:     make(chan error, 10),
		logger:        log.New(os.Stdout, "[WS-Client] ", log.LstdFlags|log.Lshortfile),
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

	client.logger.Printf("正在连接到WebSocket服务器: %s", u.String())

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
			client.logger.Printf("连接失败: HTTP %d %s", resp.StatusCode, resp.Status)
			if resp.StatusCode == 401 {
				return fmt.Errorf("认证失败: 请检查JWT Token是否正确")
			} else if resp.StatusCode == 403 {
				return fmt.Errorf("IP被禁止或权限不足")
			} else if resp.StatusCode == 429 {
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
	client.logger.Println("✅ WebSocket连接建立成功!")

	// 设置连接参数
	client.conn.SetReadLimit(MaxMessageSize)
	client.conn.SetPongHandler(func(appData string) error {
		client.logger.Println("💗 收到心跳响应: pong")
		return client.conn.SetReadDeadline(time.Now().Add(ReadTimeout))
	})

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
		client.logger.Printf("❗ 连接失败 (%d/%d): %v", client.reconnectCnt, client.config.MaxReconnect, err)

		if client.reconnectCnt < client.config.MaxReconnect && !client.isShutdown {
			client.logger.Printf("🔄 %v 后尝试重连...", client.config.ReconnectWait)
			time.Sleep(client.config.ReconnectWait)
		}
	}

	return fmt.Errorf("达到最大重连次数 (%d)，连接失败", client.config.MaxReconnect)
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

// 发送消息到服务器
func (client *WebSocketClient) SendMessage(path string, data interface{}) error {
	if !client.IsConnected() {
		return fmt.Errorf("连接未建立")
	}

	request := Request{
		Id:        client.msgID,
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

	client.logger.Printf("📤 发送消息 [ID:%d]: %s", request.Id, string(msgBytes))

	// 设置发送超时
	client.conn.SetWriteDeadline(time.Now().Add(SendTimeout))
	err = client.conn.WriteMessage(websocket.TextMessage, msgBytes)
	if err != nil {
		client.isConnected = false
		return fmt.Errorf("发送消息失败: %v", err)
	}

	// 清除发送超时
	client.conn.SetWriteDeadline(time.Time{})
	return nil
}

// 发送联系人数据
func (client *WebSocketClient) SendContact(contact FcgContact) error {
	// client.logger.Printf("📬 发送联系人数据: %s (%s)", contact.Username, contact.NickName)
	return client.SendMessage("fccontact", contact)
}

// 发送消息数据
func (client *WebSocketClient) SendFcgMessage(message FcgMessage) error {
	// client.logger.Printf("💬 发送消息数据: %s - %s", message.UserName, message.MessageContent)
	return client.SendMessage("fcmessage", message)
}

// 发送心跳
func (client *WebSocketClient) SendHeartbeat() error {
	if !client.IsConnected() {
		return fmt.Errorf("连接未建立")
	}

	client.logger.Println("💗 发送心跳")
	client.conn.SetWriteDeadline(time.Now().Add(SendTimeout))
	err := client.conn.WriteMessage(websocket.PingMessage, []byte("ping"))
	if err != nil {
		client.isConnected = false
		return fmt.Errorf("发送心跳失败: %v", err)
	}
	client.conn.SetWriteDeadline(time.Time{})
	return nil
}

// 监听服务器消息
func (client *WebSocketClient) ListenMessages() {
	defer func() {
		if r := recover(); r != nil {
			client.logger.Printf("❗ 消息监听出现panic: %v", r)
		}
		client.isConnected = false
	}()

	for client.IsConnected() && !client.isShutdown {
		// 设置读取超时
		client.conn.SetReadDeadline(time.Now().Add(ReadTimeout))

		_, message, err := client.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				client.logger.Printf("❗ WebSocket意外关闭: %v", err)
			} else if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				client.logger.Println("🔌 WebSocket正常关闭")
			} else {
				client.logger.Printf("❗ 读取消息失败: %v", err)
			}
			client.isConnected = false

			// 如果需要自动重连
			if client.config.Reconnect && !client.isShutdown {
				go client.handleReconnect()
			}
			return
		}

		// 处理pong消息
		if string(message) == "pong" {
			// client.logger.Println("💗 收到心跳响应: pong")
			continue
		}

		// 解析响应消息
		var response Response
		err = json.Unmarshal(message, &response)
		if err != nil {
			// client.logger.Printf("📥 收到原始消息: %s", string(message))
		} else {
			// client.logger.Printf("📥 收到服务器响应 [ID:%d Code:%d]: %s", response.Id, response.Code, response.Msg)
			if response.Data != nil {
				dataBytes, _ := json.MarshalIndent(response.Data, "", "  ")
				client.logger.Printf("   数据: %s", string(dataBytes))
			}
		}
	}
}

// 处理重连逻辑
func (client *WebSocketClient) handleReconnect() {
	if client.isShutdown {
		return
	}

	client.logger.Println("🔄 尝试重连...")
	time.Sleep(client.config.ReconnectWait)

	err := client.ConnectWithRetry()
	if err != nil {
		client.logger.Printf("❗ 重连失败: %v", err)
		client.errorChan <- err
		return
	}

	client.logger.Println("✅ 重连成功！")
	// 重新启动消息监听
	go client.ListenMessages()
	// 重新启动心跳
	go client.StartHeartbeat()
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
				client.logger.Printf("❗ 心跳失败: %v", err)
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

// 发送文本消息
func (client *WebSocketClient) SendTextMessage(text string) error {
	if !client.IsConnected() {
		return fmt.Errorf("连接未建立")
	}

	client.logger.Printf("📝 发送文本消息: %s", text)
	client.conn.SetWriteDeadline(time.Now().Add(SendTimeout))
	err := client.conn.WriteMessage(websocket.TextMessage, []byte(text))
	if err != nil {
		client.isConnected = false
		return fmt.Errorf("发送文本消息失败: %v", err)
	}
	client.conn.SetWriteDeadline(time.Time{})
	return nil
}

// 更新配置
func (client *WebSocketClient) UpdateConfig(config ClientConfig) {
	client.config = config
	client.logger.Printf("⚙️ 配置已更新")
}

// 获取配置
func (client *WebSocketClient) GetConfig() ClientConfig {
	return client.config
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
	client.isShutdown = true
	client.isConnected = false

	// 停止心跳和消息监听
	client.StopHeartbeat()
	client.StopListening()

	if client.conn != nil {
		// 发送关闭消息
		client.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "客户端主动关闭"))
		client.conn.Close()
		client.logger.Println("🔌 WebSocket连接已关闭")
	}

	// 关闭通道
	close(client.heartbeatStop)
	close(client.messageStop)
	close(client.errorChan)
}
