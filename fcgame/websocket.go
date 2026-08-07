package fcgame

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
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
		ProxyEnabled:  ProxyEnabled,
		ProxyHost:     ProxyHost,
		ProxyPort:     ProxyPort,
		ProxyUsername: ProxyUsername,
		ProxyPassword: ProxyPassword,
	}
}

type WebSocketClient struct {
	conn            *websocket.Conn
	msgID           int
	config          ClientConfig
	isConnected     atomic.Bool
	isShutdown      atomic.Bool
	reconnectCnt    int
	heartbeatStop   chan bool
	messageStop     chan bool
	errorChan       chan error
	logger          *zap.Logger
	writeMtx        sync.Mutex  // 用来保护写操作
	sendChan        chan []byte // 写协程使用的消息队列
	sendChanMu      sync.Mutex  // 保护 sendChan 的关闭操作
	sendChanClosed  bool        // 标记 sendChan 是否已关闭
	pendingMessages [][]byte    // 缓存未发送的消息
	pendingMtx      sync.Mutex  // 保护pendingMessages的并发访问
}

// 创建新的WebSocket客户端
func NewWebSocketClient(config ClientConfig, log *zap.Logger) *WebSocketClient {
	return &WebSocketClient{
		config:          config,
		msgID:           1,
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
	for !client.isShutdown.Load() {
		// 1. 尝试连接
		if err := client.Connect(); err != nil {
			fmt.Println("Connection failed. ", err)
			// 使用ConnectWithRetry的逻辑进行等待
			if client.config.Reconnect && !client.isShutdown.Load() {
				time.Sleep(client.config.ReconnectWait)
				continue // 继续下一次循环尝试连接
			} else {
				break // 如果不允许重连，则退出
			}
		}

		// 2. 连接成功，启动所有协程，使用 WaitGroup 等待全部退出
		var wg sync.WaitGroup
		anyDone := make(chan struct{}, 4)

		runGoroutine := func(name string, fn func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						client.logger.Error(name+" panic recovered",
							zap.Any("error", r))
					}
					anyDone <- struct{}{}
				}()
				fn()
				fmt.Println(name + " Process Exit。")
			}()
		}

		runGoroutine("StartWriter", client.StartWriter)
		runGoroutine("StartHeartbeat", client.StartHeartbeat)
		runGoroutine("ListenMessages", client.ListenMessages)
		runGoroutine("ResendFailedMessages", client.ResendFailedMessages)

		// 3. 等待任意一个协程退出（意味着连接断开）
		<-anyDone
		fmt.Println("Coroutine Exit, Retrying...")

		// 4. 通知所有协程停止
		client.isConnected.Store(false)
		client.StopHeartbeat()
		client.StopListening()

		// 关闭连接，这会导致其他协程也退出
		if client.conn != nil {
			client.conn.Close()
			client.conn = nil
		}

		// 关闭 sendChan 来停止写协程（StartWriter 会从 range 退出）
		client.safeCloseSendChan()

		// 等待所有协程退出
		wg.Wait()

		// 保存未发送的消息（在关闭sendChan之后，此时可以安全读取）
		client.saveUnsentMessages()

		// 5. 准备下一次重连
		if client.config.Reconnect && !client.isShutdown.Load() {
			pendingCount := client.getPendingMessageCount()
			if pendingCount > 0 {
				fmt.Printf("waiting retrying. has %d message on queue...\n", pendingCount)
			}

			// 创建新的sendChan用于下一次连接
			client.sendChanMu.Lock()
			client.sendChan = make(chan []byte, MaxMessageQueue)
			client.sendChanClosed = false
			client.sendChanMu.Unlock()

			fmt.Println("retrying...")
			time.Sleep(client.config.ReconnectWait)
		}
	}
	client.logger.Info("client for end.")
}

// safeCloseSendChan 安全关闭 sendChan，防止重复关闭导致 panic
func (client *WebSocketClient) safeCloseSendChan() {
	client.sendChanMu.Lock()
	defer client.sendChanMu.Unlock()
	if !client.sendChanClosed {
		close(client.sendChan)
		client.sendChanClosed = true
	}
}

// safeSendSendChan 安全地向 sendChan 发送消息，防止向已关闭的 channel 发送导致 panic
func (client *WebSocketClient) safeSendSendChan(msg []byte) (sent bool) {
	defer func() {
		if r := recover(); r != nil {
			sent = false
		}
	}()
	client.sendChan <- msg
	return true
}

// 连接到WebSocket服务器
func (client *WebSocketClient) Connect() error {
	if client.isShutdown.Load() {
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
	// 天机回调参数
	q := u.Query()
	q.Set("resp", "true")
	u.RawQuery = q.Encode()

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

	// 如果启用了代理，配置代理
	if client.config.ProxyEnabled && client.config.ProxyHost != "" && client.config.ProxyPort != "" {
		// 构建代理URL
		var proxyURL *url.URL
		var err error

		if client.config.ProxyUsername != "" && client.config.ProxyPassword != "" {
			// 带认证的代理URL
			proxyURL, err = url.Parse(fmt.Sprintf("http://%s:%s@%s:%s",
				url.QueryEscape(client.config.ProxyUsername),
				url.QueryEscape(client.config.ProxyPassword),
				client.config.ProxyHost,
				client.config.ProxyPort))
		} else {
			// 不带认证的代理URL
			proxyURL, err = url.Parse(fmt.Sprintf("http://%s:%s",
				client.config.ProxyHost,
				client.config.ProxyPort))
		}

		if err != nil {
			return fmt.Errorf("解析代理URL失败: %v", err)
		}

		// 创建HTTP代理
		proxyFunc := http.ProxyURL(proxyURL)

		// 创建自定义的Transport
		transport := &http.Transport{
			Proxy: proxyFunc,
		}

		// 设置WebSocket Dialer的代理
		dialer.Proxy = proxyFunc

		// 设置自定义的NetDialContext
		dialer.NetDialContext = transport.DialContext
		client.logger.Info("使用代理连接WebSocket", zap.String("proxy", proxyURL.String()))
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
		return err
	}

	// 关闭响应体
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	client.conn = conn
	client.isConnected.Store(true)
	client.reconnectCnt = 0
	fmt.Println("✅ Socket Conn Success!")

	// 设置连接参数
	client.conn.SetReadLimit(int64(MaxMessageSize))
	return nil
}

// 自动重连机制
func (client *WebSocketClient) ConnectWithRetry() error {
	for client.reconnectCnt < client.config.MaxReconnect && !client.isShutdown.Load() {
		err := client.Connect()
		if err == nil {
			return nil
		}

		client.reconnectCnt++
		fmt.Printf("❗ connection fail: (%d/%d): %v", client.reconnectCnt, client.config.MaxReconnect, err)

		if client.reconnectCnt < client.config.MaxReconnect && !client.isShutdown.Load() {
			fmt.Printf("🔄 %v Second. Retrying...", client.config.ReconnectWait)
			time.Sleep(client.config.ReconnectWait)
		}
	}

	return fmt.Errorf("max retrying (%d). connection fail", client.config.MaxReconnect)
}

// 检查连接状态
func (client *WebSocketClient) IsConnected() bool {
	return client.isConnected.Load() && client.conn != nil && !client.isShutdown.Load()
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

	conn := client.conn
	if conn == nil || !client.isConnected.Load() {
		return fmt.Errorf("client not connection")
	}

	// 设置写超时
	conn.SetWriteDeadline(time.Now().Add(SendTimeout))
	err := conn.WriteMessage(msgType, data)
	conn.SetWriteDeadline(time.Time{}) // 清除超时

	if err != nil {
		client.isConnected.Store(false)
		return fmt.Errorf("write message error: %v", err)
	}
	return nil
}

// 发送消息到服务器
func (client *WebSocketClient) SendMessage(path string, data interface{}) error {
	if !DomainRZ {
		return nil
	}

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

	// 检查连接状态
	if client.IsConnected() {
		// 如果连接正常，直接发送
		if !client.safeSendSendChan(msgBytes) {
			return fmt.Errorf("客户端发送通道已关闭")
		}
		return nil
	}

	return fmt.Errorf("客户端未连接")
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

// SendHeartbeat 发送心跳（Ping）
func (client *WebSocketClient) SendHeartbeat() error {
	// 心跳消息不需要缓存，因为它是周期性的
	if client.IsConnected() {
		if err := client.write(websocket.PingMessage, nil); err != nil {
			return err
		}
	}
	return nil
}

// 发送文本消息
func (client *WebSocketClient) SendTextMessage(text []byte) error {
	// 将消息添加到待发送队列
	client.addPendingMessage(text)

	// 如果连接正常，直接发送
	if client.IsConnected() {
		client.safeSendSendChan(text)
	}
	return nil
}

// 监听服务器消息
func (client *WebSocketClient) ListenMessages() {

	// 设置初始读取超时，防止死连接导致 ReadMessage 永久阻塞
	// PongHandler 会在收到 Pong 后刷新此超时
	client.conn.SetReadDeadline(time.Now().Add(ReadTimeout))

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

		// 读取消息
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
			client.isConnected.Store(false)
			return
		}

		// 收到数据后刷新读取超时，防止活跃连接因超时被断开
		client.conn.SetReadDeadline(time.Now().Add(ReadTimeout))

		// 解析响应消息
		var response Response
		err = json.Unmarshal(message, &response)
		if err != nil {
			client.logger.Error("parsing response message error: ", zap.Error(err), zap.String("message", string(message)))
			fmt.Println("parsing response message error: ", string(message))
		} else {
			if response.Data != nil {
				// 处理消息发送成功的回复
				dataMap, ok := response.Data.(map[string]interface{})
				if ok {
					hash, hashOk := dataMap["hash"].(string)
					localIdFloat, idOk := dataMap["local_id"].(float64) // json解析数字默认是float64
					localIdType, _ := dataMap["local_type"].(float64)   // json解析数字默认是float64

					if hashOk && idOk {
						localId := uint64(localIdFloat)
						localIdTypeID := uint64(localIdType)
						// 异步更新数据库状态
						go func(h string, id uint64, lidType uint64) {
							defer func() {
								if r := recover(); r != nil {
									client.logger.Error("更新消息发送状态panic",
										zap.Any("error", r),
										zap.String("stack", string(debug.Stack())))
								}
							}()

							messageDB, err := GetMessageGormDB()
							if err != nil {
								client.logger.Error("更新消息发送状态获取数据库失败",
									zap.String("hash", h), zap.Uint64("local_id", id), zap.Error(err))
								return
							}

							var updateErr error
							if lidType == 10000 {
								updateErr = messageDB.Model(&FcgMessageLike{}).
									Where("hash = ? AND local_id = ?", h, id).
									Update("send_status", 1).Error
							} else {
								updateErr = messageDB.Model(&FcgMessageModel{}).
									Where("hash = ? AND local_id = ?", h, id).
									Update("send_status", 1).Error
							}
							if updateErr != nil {
								client.logger.Error("更新消息发送状态失败",
									zap.String("hash", h), zap.Uint64("local_id", id), zap.Error(updateErr))
							}
						}(hash, localId, localIdTypeID)
					}
				}
			}
		}
	}
}

// 启动心跳检测
func (client *WebSocketClient) StartHeartbeat() {
	if client.isShutdown.Load() {
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

// 重新发送未发送成功的消息
func (client *WebSocketClient) ResendFailedMessages() {
	if client.isShutdown.Load() {
		return
	}

	now := time.Now()
	today := time.Date(
		now.Year(), now.Month(), now.Day(),
		0, 0, 0, 0, now.Location(),
	)
	currentSortSeq := today.Unix() * 1000

	// 定时器：每隔1分钟检查一次

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !client.IsConnected() {
				return
			}

			cdb, err := GetMessageGormDB()
			if err != nil {
				client.logger.Error("重发协程获取数据库连接失败", zap.Error(err))
				continue
			}
			oneMinuteAgo := time.Now().Add(-1 * time.Minute)

			// ======================================正常消息重复================================================
			// 查询发送状态为0，并且创建时间在1分钟之前的记录
			// 这样做是为了给正常的发送->服务器回复流程留出足够的时间，避免刚刚插入还没来得及收到回复的消息被错误重发
			var failedMessages []FcgMessageModel
			err = cdb.Where("send_status = ? AND sort_seq >= ? AND db_create_at < ?", 0, currentSortSeq, oneMinuteAgo).
				Limit(100). // 每次最多重发100条，避免突发大量消息
				Find(&failedMessages).
				Error

			if len(failedMessages) > 0 {
				client.logger.Info("开始重发未发送成功的消息", zap.Int("count", len(failedMessages)))

				for _, msgModel := range failedMessages {
					if !client.IsConnected() {
						return
					}

					if msgModel.SendRetryCount >= 5 {
						err = cdb.Exec("UPDATE fcg_message SET send_status = 99 WHERE id = ? AND send_status = 0", msgModel.ID).Error
						if err != nil {
							client.logger.Error("更新消息为不再重发失败", zap.Uint("id", msgModel.ID), zap.Error(err))
						}
						continue
					}

					err = cdb.Exec("UPDATE fcg_message SET send_retry_count = send_retry_count + 1 WHERE id = ? AND send_status = 0 AND send_retry_count < 5", msgModel.ID).Error
					if err != nil {
						client.logger.Error("更新消息重发次数失败", zap.Uint("id", msgModel.ID), zap.Error(err))
						continue
					}

					plainContent, derr := DecryptMessageContent(msgModel.MessageContent)
					if derr != nil {
						plainContent = msgModel.MessageContent
					}
					// 组装要重发的消息
					message := FcgMessage{
						TenantId:          msgModel.TenantId,
						UserName:          msgModel.UserName,
						NickName:          msgModel.NickName,
						LocalId:           msgModel.LocalId,
						SortSeq:           msgModel.SortSeq,
						ServerId:          msgModel.ServerId,
						LocalType:         msgModel.LocalType,
						CreateTime:        msgModel.CreateTime,
						RealSenderId:      msgModel.RealSenderId,
						MessageContent:    plainContent,
						Status:            msgModel.Status,
						RecognitionStatus: msgModel.RecognitionStatus,
						MessageNo:         msgModel.MessageNo,
						TaskList:          msgModel.TaskList,
						Owner:             msgModel.Owner,
						Hash:              msgModel.Hash,
					}

					err = client.SendFcgMessage(message)
					if err != nil {
						client.logger.Error("重发消息失败", zap.String("hash", message.Hash), zap.Uint64("local_id", message.LocalId), zap.Error(err))
					} else {
						// 为了避免发送太快，稍微休眠一下
						time.Sleep(50 * time.Millisecond)
					}
				}
			}

			// ======================================Like 消息重发==============================================
			var likeMessages []FcgMessageLike
			err = cdb.Where("send_status = ? AND sort_seq >= ? AND db_create_at < ?", 0, currentSortSeq, oneMinuteAgo).
				Limit(100). // 每次最多重发100条，避免突发大量消息
				Find(&likeMessages).
				Error

			if len(likeMessages) > 0 {
				client.logger.Info("Reced Send Like Msg", zap.Int("count", len(likeMessages)))

				for _, msgModel := range likeMessages {
					if !client.IsConnected() {
						return
					}

					if msgModel.SendRetryCount >= 5 {
						err = cdb.Exec("UPDATE fcg_message_like SET send_status = 99 WHERE id = ? AND send_status = 0", msgModel.ID).Error
						if err != nil {
							client.logger.Error("更新消息为不再重发失败", zap.Uint("id", msgModel.ID), zap.Error(err))
						}
						continue
					}

					err = cdb.Exec("UPDATE fcg_message_like SET send_retry_count = send_retry_count + 1 WHERE id = ? AND send_status = 0 AND send_retry_count < 5", msgModel.ID).Error
					if err != nil {
						client.logger.Error("更新消息重发次数失败", zap.Uint("id", msgModel.ID), zap.Error(err))
						continue
					}

					plainContent, derr := DecryptMessageContent(msgModel.MessageContent)
					if derr != nil {
						plainContent = msgModel.MessageContent
					}
					// 组装要重发的消息
					message := FcgMessage{
						TenantId:          msgModel.TenantId,
						UserName:          msgModel.UserName,
						NickName:          msgModel.NickName,
						LocalId:           msgModel.LocalId,
						SortSeq:           msgModel.SortSeq,
						ServerId:          msgModel.ServerId,
						LocalType:         msgModel.LocalType,
						CreateTime:        msgModel.CreateTime,
						RealSenderId:      msgModel.RealSenderId,
						MessageContent:    plainContent,
						Status:            msgModel.Status,
						RecognitionStatus: msgModel.RecognitionStatus,
						MessageNo:         msgModel.MessageNo,
						TaskList:          msgModel.TaskList,
						Owner:             msgModel.Owner,
						Hash:              msgModel.Hash,
					}

					err = client.SendFcgMessage(message)
					if err != nil {
						client.logger.Error("重发LIKE消息失败", zap.String("hash", message.Hash), zap.Uint64("local_id", message.LocalId), zap.Error(err))
					} else {
						// 为了避免发送太快，稍微休眠一下
						time.Sleep(50 * time.Millisecond)
					}
				}
			}
		case <-client.messageStop: // 复用 messageStop 或新建一个 stop channel
			return
		}
	}
}

// 获取统计信息
func (client *WebSocketClient) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"connected":             client.IsConnected(),
		"connection_info":       client.GetConnectionInfo(),
		"message_id":            client.msgID,
		"reconnect_count":       client.reconnectCnt,
		"is_shutdown":           client.isShutdown.Load(),
		"pending_message_count": client.getPendingMessageCount(),
	}
}

// 关闭连接
func (client *WebSocketClient) Close() {
	client.isShutdown.Store(true)
	client.isConnected.Store(false)

	// 停止心跳和消息监听
	client.StopHeartbeat()
	client.StopListening()

	// 关闭连接
	if client.conn != nil {
		// 发送关闭消息
		client.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "客户端主动关闭"))
		client.conn.Close()
		fmt.Println("🔌 WebSocket连接已关闭")
	}

	// 安全关闭 sendChan，防止重复关闭导致 panic
	client.safeCloseSendChan()

	// 等待一小段时间让其他协程有机会退出
	time.Sleep(100 * time.Millisecond)

	// 保存未发送的消息（在关闭sendChan之后，此时可以安全读取）
	client.saveUnsentMessages()

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
		// 使用统一的 write() 方法（带锁保护）
		err := client.write(websocket.TextMessage, msg)
		if err != nil {
			client.logger.Error("发送消息失败:", zap.Error(err))
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
	// client.logger.Debug("消息已添加到待发送缓存", zap.Int("缓存消息数", len(client.pendingMessages)))
}

// removePendingMessage 从待发送缓存中移除消息
func (client *WebSocketClient) removePendingMessage(msg []byte) {
	client.pendingMtx.Lock()
	defer client.pendingMtx.Unlock()

	for i, pending := range client.pendingMessages {
		if string(pending) == string(msg) {
			// 移除该消息
			client.pendingMessages = append(client.pendingMessages[:i], client.pendingMessages[i+1:]...)
			// client.logger.Debug("消息已从待发送缓存中移除", zap.Int("剩余缓存消息数", len(client.pendingMessages)))
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

		if !client.safeSendSendChan(msg) {
			client.logger.Error("发送通道已关闭，停止发送缓存消息")
			return
		}
	}

	client.logger.Info("所有缓存消息已重新加入发送队列")
}

// saveUnsentMessages 保存未发送的消息（在连接断开时调用）
func (client *WebSocketClient) saveUnsentMessages() {
	// 先收集所有未发送的消息，不持有锁的时候读取 channel
	var unsentMessages [][]byte
	for {
		select {
		case msg, ok := <-client.sendChan:
			if !ok {
				// channel 已关闭，退出读取循环
				goto saveMessages
			}
			if len(msg) > 0 {
				unsentMessages = append(unsentMessages, msg)
			}
		default:
			// sendChan 暂时为空，退出读取循环
			goto saveMessages
		}
	}

saveMessages:
	// 现在持有锁，保存收集到的消息
	if len(unsentMessages) > 0 {
		client.pendingMtx.Lock()
		defer client.pendingMtx.Unlock()

		for _, msg := range unsentMessages {
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
	}
}

// getPendingMessageCount 获取待发送消息数量
func (client *WebSocketClient) getPendingMessageCount() int {
	client.pendingMtx.Lock()
	defer client.pendingMtx.Unlock()
	return len(client.pendingMessages)
}
