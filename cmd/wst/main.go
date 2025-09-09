package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// 配置常量 - 静态变量，方便修改调整
const (
	// WebSocket服务器配置
	WSServer = "192.168.31.87" // WebSocket服务器地址
	WSPort   = "888"           // WebSocket服务器端口
	WSPath   = "/websocket"    // WebSocket路径
	WSScheme = "ws"            // WebSocket协议 (ws 或 wss)

	// 认证配置
	RequireAuth  = false // 是否需要JWT认证
	DefaultToken = ""    // 默认JWT Token (如果需要认证)

	// 客户端配置
	ClientVersion     = "1.0.0"          // 客户端版本
	HeartbeatInterval = 30 * time.Second // 心跳间隔
	ReconnectInterval = 5 * time.Second  // 重连间隔
	MaxReconnectCount = 10               // 最大重连次数
	ConnectTimeout    = 10 * time.Second // 连接超时时间

	// 消息配置
	MaxMessageSize = 5012             // 最大消息大小
	SendTimeout    = 10 * time.Second // 发送超时时间
	ReadTimeout    = 60 * time.Second // 读取超时时间

	// 测试数据配置
	ContactSendInterval = 1 * time.Second // 联系人发送间隔
	MessageSendInterval = 2 * time.Second // 消息发送间隔
	TestDataDelay       = 2 * time.Second // 测试数据发送延迟
)

// ClientConfig WebSocket客户端配置
type ClientConfig struct {
	ServerHost    string        `json:"server_host"`    // 服务器地址
	ServerPort    string        `json:"server_port"`    // 服务器端口
	Path          string        `json:"path"`           // WebSocket路径
	Scheme        string        `json:"scheme"`         // 协议 (ws/wss)
	Token         string        `json:"token"`          // JWT Token
	RequireAuth   bool          `json:"require_auth"`   // 是否需要认证
	Version       string        `json:"version"`        // 客户端版本
	UserAgent     string        `json:"user_agent"`     // User-Agent
	Origin        string        `json:"origin"`         // Origin头
	Subprotocols  []string      `json:"subprotocols"`   // 子协议
	Timeout       time.Duration `json:"timeout"`        // 连接超时
	Heartbeat     time.Duration `json:"heartbeat"`      // 心跳间隔
	Reconnect     bool          `json:"reconnect"`      // 是否自动重连
	MaxReconnect  int           `json:"max_reconnect"`  // 最大重连次数
	ReconnectWait time.Duration `json:"reconnect_wait"` // 重连等待时间
}

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

// WebSocket消息结构体
type Request struct {
	Id        int         `json:"id"`        // 消息ID
	Ver       string      `json:"ver"`       // 版本号
	Path      string      `json:"path"`      // 请求命令字
	Data      interface{} `json:"data"`      // 数据 JSON
	Timestamp int64       `json:"timestamp"` // 请求时间戳
}

// WebSocket响应结构体
type Response struct {
	Id        int         `json:"id"`        // 消息ID
	Msg       string      `json:"msg"`       // 返回信息
	Code      int         `json:"code"`      // 返回错误码
	Data      interface{} `json:"data"`      // 返回数据JSON
	Timestamp int64       `json:"timestamp"` // 响应时间戳
	Duration  float64     `json:"duration"`  // 处理请求所花费时间
}

// FcgContact 联系人结构体
type FcgContact struct {
	TenantId      uint   `json:"tenant_id"`
	Username      string `json:"username"`
	NickName      string `json:"nick_name"`
	Alias         string `json:"alias"`
	LocalType     uint   `json:"local_type"`
	PinYinInitial string `json:"pin_yin_initial"`
	QuanPin       string `json:"quan_pin"`
	BigHeadUrl    string `json:"big_head_url"`
	SmallHeadUrl  string `json:"small_head_url"`
	Remark        string `json:"remark"`
	Description   string `json:"description"`
	Hash          string `json:"hash"`
}

// FcgMessage 消息结构体
type FcgMessage struct {
	TenantId          uint   `json:"tenant_id"`
	UserName          string `json:"user_name"`
	NickName          string `json:"nick_name"`
	LocalId           uint64 `json:"local_id"`
	SortSeq           uint64 `json:"sort_seq"`
	ServerId          uint64 `json:"server_id"`
	LocalType         uint   `json:"local_type"`
	CreateTime        uint64 `json:"create_time"`
	RealSenderId      uint64 `json:"real_sender_id"`
	MessageContent    string `json:"message_content"`
	Status            uint   `json:"status"`
	RecognitionStatus bool   `json:"recognition_status"`
	MessageNo         string `json:"message_no"`
	TaskList          string `json:"task_list"`
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
		client.logger.Printf("使用JWT Token认证: %s...", client.config.Token[:min(len(client.config.Token), 20)])
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

// min 返回两个整数中的最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

	//client.logger.Printf("📤 发送消息 [ID:%d]: %s", request.Id, string(msgBytes))

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
	//client.logger.Printf("📬 发送联系人数据: %s (%s)", contact.Username, contact.NickName)
	return client.SendMessage("/fccontact", contact)
}

// 发送消息数据
func (client *WebSocketClient) SendFcgMessage(message FcgMessage) error {
	//client.logger.Printf("💬 发送消息数据: %s - %s", message.UserName, message.MessageContent)
	return client.SendMessage("/fcmessage", message)
}

// 发送心跳
func (client *WebSocketClient) SendHeartbeat() error {
	if !client.IsConnected() {
		return fmt.Errorf("连接未建立")
	}

	//client.logger.Println("💗 发送心跳")
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
			//client.logger.Println("💗 收到心跳响应: pong")
			continue
		}

		// 解析响应消息
		var response Response
		err = json.Unmarshal(message, &response)
		if err != nil {
			client.logger.Printf("📥 收到原始消息: %s", string(message))
		} else {
			client.logger.Printf("📥 收到服务器响应 [ID:%d Code:%d]: %s", response.Id, response.Code, response.Msg)
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

// 从命令行加载配置
func loadConfigFromArgs() ClientConfig {
	config := GetDefaultConfig()

	// 简单的命令行参数解析
	for i, arg := range os.Args[1:] {
		switch arg {
		case "-h", "--host":
			if i+1 < len(os.Args[1:]) {
				config.ServerHost = os.Args[i+2]
			}
		case "-p", "--port":
			if i+1 < len(os.Args[1:]) {
				config.ServerPort = os.Args[i+2]
			}
		case "-t", "--token":
			if i+1 < len(os.Args[1:]) {
				config.Token = os.Args[i+2]
				config.RequireAuth = true
			}
		case "--auth":
			config.RequireAuth = true
		case "--no-auth":
			config.RequireAuth = false
		case "--wss":
			config.Scheme = "wss"
		case "--help":
			printUsage()
			os.Exit(0)
		}
	}

	return config
}

// 从配置文件加载配置
func loadConfigFromFile(filename string) (ClientConfig, error) {
	config := GetDefaultConfig()

	file, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return config, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return config, nil
}

// 保存配置到文件
func saveConfigToFile(config ClientConfig, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(config)
}

// 打印使用说明
func printUsage() {
	fmt.Println("FCGameServer WebSocket客户端使用说明:")
	fmt.Println("")
	fmt.Println("参数:")
	fmt.Println("  -h, --host <host>    WebSocket服务器地址 (默认: localhost)")
	fmt.Println("  -p, --port <port>    WebSocket服务器端口 (默认: 888)")
	fmt.Println("  -t, --token <token>  JWT Token (启用认证)")
	fmt.Println("  --auth               强制启用认证")
	fmt.Println("  --no-auth            禁用认证")
	fmt.Println("  --wss                使用WSS(加密)协议")
	fmt.Println("  --help               显示帮助信息")
	fmt.Println("")
	fmt.Println("使用示例:")
	fmt.Println("  ./skclient -h localhost -p 888")
	fmt.Println("  ./skclient -t your_jwt_token --auth")
	fmt.Println("  ./skclient --wss -h example.com -p 443")
}

// 交互式配置
func interactiveConfig() ClientConfig {
	config := GetDefaultConfig()
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("🚀 交互式配置 WebSocket 客户端")
	fmt.Println("=====================================\n")

	// 服务器地址
	fmt.Printf("服务器地址 [默认: %s]: ", config.ServerHost)
	if input, _ := reader.ReadString('\n'); strings.TrimSpace(input) != "" {
		config.ServerHost = strings.TrimSpace(input)
	}

	// 服务器端口
	fmt.Printf("服务器端口 [默认: %s]: ", config.ServerPort)
	if input, _ := reader.ReadString('\n'); strings.TrimSpace(input) != "" {
		config.ServerPort = strings.TrimSpace(input)
	}

	// 协议选择
	fmt.Printf("使用加密协议 (WSS)? [y/N]: ")
	if input, _ := reader.ReadString('\n'); strings.ToLower(strings.TrimSpace(input)) == "y" {
		config.Scheme = "wss"
	}

	// 认证配置
	fmt.Printf("需要JWT认证? [y/N]: ")
	if input, _ := reader.ReadString('\n'); strings.ToLower(strings.TrimSpace(input)) == "y" {
		config.RequireAuth = true
		fmt.Print("请输入JWT Token: ")
		if token, _ := reader.ReadString('\n'); strings.TrimSpace(token) != "" {
			config.Token = strings.TrimSpace(token)
		}
	}

	fmt.Println("\n✅ 配置完成！")
	return config
}
func generateSampleContacts() []FcgContact {
	return []FcgContact{
		{
			TenantId:      1,
			Username:      "user001",
			NickName:      "张三",
			Alias:         "小张",
			LocalType:     1,
			PinYinInitial: "ZS",
			QuanPin:       "zhangsan",
			BigHeadUrl:    "https://example.com/avatar/big/001.jpg",
			SmallHeadUrl:  "https://example.com/avatar/small/001.jpg",
			Remark:        "好朋友",
			Description:   "我的好朋友张三",
			Hash:          "hash001",
		},
		{
			TenantId:      1,
			Username:      "user002",
			NickName:      "李四",
			Alias:         "小李",
			LocalType:     1,
			PinYinInitial: "LS",
			QuanPin:       "lisi",
			BigHeadUrl:    "https://example.com/avatar/big/002.jpg",
			SmallHeadUrl:  "https://example.com/avatar/small/002.jpg",
			Remark:        "同事",
			Description:   "公司同事李四",
			Hash:          "hash002",
		},
		{
			TenantId:      1,
			Username:      "user003",
			NickName:      "王五",
			Alias:         "小王",
			LocalType:     1,
			PinYinInitial: "WW",
			QuanPin:       "wangwu",
			BigHeadUrl:    "https://example.com/avatar/big/003.jpg",
			SmallHeadUrl:  "https://example.com/avatar/small/003.jpg",
			Remark:        "邻居",
			Description:   "隔壁邻居王五",
			Hash:          "hash003",
		},
	}
}

// 生成示例消息数据
func generateSampleMessages() []FcgMessage {
	now := uint64(time.Now().Unix())
	return []FcgMessage{
		{
			TenantId:          1,
			UserName:          "user001",
			NickName:          "张三",
			LocalId:           1001,
			SortSeq:           1,
			ServerId:          0, // 服务器会自动分配
			LocalType:         1,
			CreateTime:        now,
			RealSenderId:      1001,
			MessageContent:    "大家好，我是张三！这是我的第一条消息。",
			Status:            1,
			RecognitionStatus: false,
			MessageNo:         "MSG_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_001",
			TaskList:          "",
		},
		{
			TenantId:          1,
			UserName:          "user002",
			NickName:          "李四",
			LocalId:           1002,
			SortSeq:           2,
			ServerId:          0,
			LocalType:         1,
			CreateTime:        now + 1,
			RealSenderId:      1002,
			MessageContent:    "你好张三！我是李四，很高兴认识你。",
			Status:            1,
			RecognitionStatus: false,
			MessageNo:         "MSG_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_002",
			TaskList:          "",
		},
		{
			TenantId:          1,
			UserName:          "user003",
			NickName:          "王五",
			LocalId:           1003,
			SortSeq:           3,
			ServerId:          0,
			LocalType:         1,
			CreateTime:        now + 2,
			RealSenderId:      1003,
			MessageContent:    "大家好！我是王五，欢迎来到我们的聊天群。",
			Status:            1,
			RecognitionStatus: false,
			MessageNo:         "MSG_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_003",
			TaskList:          "",
		},
		{
			TenantId:          1,
			UserName:          "user001",
			NickName:          "张三",
			LocalId:           1004,
			SortSeq:           4,
			ServerId:          0,
			LocalType:         1,
			CreateTime:        now + 3,
			RealSenderId:      1001,
			MessageContent:    "今天天气真不错，大家有什么计划吗？",
			Status:            1,
			RecognitionStatus: false,
			MessageNo:         "MSG_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_004",
			TaskList:          "",
		},
		{
			TenantId:          1,
			UserName:          "user002",
			NickName:          "李四",
			LocalId:           1005,
			SortSeq:           5,
			ServerId:          0,
			LocalType:         1,
			CreateTime:        now + 4,
			RealSenderId:      1002,
			MessageContent:    "我计划去公园散步，有人一起吗？",
			Status:            1,
			RecognitionStatus: false,
			MessageNo:         "MSG_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_005",
			TaskList:          "",
		},
	}
}

func main() {
	fmt.Println("🚀 FCGameServer WebSocket 客户端启动")
	fmt.Println("=====================================\n")

	// 加载配置
	var config ClientConfig
	var err error

	// 检查是否有配置文件
	configFile := "client-config.json"
	if _, err := os.Stat(configFile); err == nil {
		fmt.Printf("📄 从配置文件加载: %s\n", configFile)
		config, err = loadConfigFromFile(configFile)
		if err != nil {
			fmt.Printf("⚠️ 读取配置文件失败: %v\n", err)
			config = GetDefaultConfig()
		}
	} else {
		// 从命令行加载配置
		config = loadConfigFromArgs()
	}

	// 显示配置信息
	fmt.Printf("🔧 客户端配置:\n")
	fmt.Printf("   服务器: %s://%s:%s%s\n", config.Scheme, config.ServerHost, config.ServerPort, config.Path)
	fmt.Printf("   认证: %t\n", config.RequireAuth)
	if config.RequireAuth && config.Token != "" {
		fmt.Printf("   Token: %s...\n", config.Token[:min(len(config.Token), 20)])
	}
	fmt.Printf("   自动重连: %t\n", config.Reconnect)
	fmt.Printf("   心跳间隔: %v\n\n", config.Heartbeat)

	// 保存配置到文件（供下次使用）
	if err := saveConfigToFile(config, configFile); err != nil {
		fmt.Printf("⚠️ 保存配置文件失败: %v\n", err)
	}

	// 创建 WebSocket 客户端
	client := NewWebSocketClient(config)

	// 连接到服务器（带重连机制）
	err = client.ConnectWithRetry()
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	// 启动消息监听
	go client.ListenMessages()

	// 启动心跳
	go client.StartHeartbeat()

	// 设置优雅退出
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// 监听错误
	go func() {
		for err := range client.errorChan {
			client.logger.Printf("❗ 客户端错误: %v", err)
		}
	}()

	// 发送测试数据
	go func() {
		time.Sleep(TestDataDelay) // 等待连接稳定

		fmt.Println("\n📋 开始发送联系人数据...")
		fmt.Println("----------------------------")

		// 发送联系人数据
		contacts := generateSampleContacts()
		for i, contact := range contacts {
			if !client.IsConnected() {
				client.logger.Println("⚠️ 连接已断开，停止发送联系人数据")
				break
			}
			err := client.SendContact(contact)
			if err != nil {
				client.logger.Printf("发送联系人失败: %v", err)
				continue
			}
			time.Sleep(ContactSendInterval)
			fmt.Printf("✅ 联系人 %d/%d 发送完成\n", i+1, len(contacts))
		}

		time.Sleep(TestDataDelay)

		fmt.Println("\n💬 开始发送消息数据...")
		fmt.Println("----------------------------")

		// 发送消息数据
		messages := generateSampleMessages()
		for i, message := range messages {
			if !client.IsConnected() {
				client.logger.Println("⚠️ 连接已断开，停止发送消息数据")
				break
			}
			err := client.SendFcgMessage(message)
			if err != nil {
				client.logger.Printf("发送消息失败: %v", err)
				continue
			}
			time.Sleep(MessageSendInterval)
			fmt.Printf("✅ 消息 %d/%d 发送完成\n", i+1, len(messages))
		}

		fmt.Println("\n🎉 所有测试数据发送完成！")
		fmt.Println("按 Ctrl+C 退出程序...")

		// 定期打印统计信息
		statsTicker := time.NewTicker(30 * time.Second)
		defer statsTicker.Stop()

		for {
			select {
			case <-statsTicker.C:
				stats := client.GetStats()
				fmt.Printf("\n📊 客户端统计信息: %+v\n", stats)
			case <-interrupt:
				return
			}
		}
	}()

	// 等待中断信号
	<-interrupt
	fmt.Println("\n👋 正在关闭WebSocket客户端...")

	// 显示最终统计信息
	stats := client.GetStats()
	fmt.Printf("📊 最终统计信息: %+v\n", stats)
}
