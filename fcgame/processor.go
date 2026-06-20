package fcgame

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
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
	processSem    chan struct{} // 限制并发任务数的信号量
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
		processSem:    make(chan struct{}, 2), // 最多允许2个并发任务
		rbblot:        rb,
		wechatManager: NewWechatManager(),
		logger:        logger,
	}
}

// InitializeWebSocket 初始化WebSocket连接
func (dp *DataProcessor) InitializeWebSocket() error {
	config := GetDefaultConfig()
	dp.wsClient = NewWebSocketClient(config, dp.logger)

	// 设置日志系统的WebSocket客户端
	SetWebSocketClient(dp.wsClient)

	SafeRun(dp.wsClient.Run)

	// 等待连接成功
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	fmt.Println("等待WebSocket连接建立...")

	for {
		select {
		case <-ticker.C:
			if dp.wsClient.IsConnected() {
				fmt.Println("✅ WebSocket连接已建立成功")
				return nil
			}
		case <-timeout:
			return fmt.Errorf("WebSocket连接超时，30秒内未连接成功")
		}
	}
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

		dp.dbState = DatabaseState{
			ContactLastID:   0,
			MessageTableMap: make(map[string]int64),
			FileStates:      make(map[string]FileState),
		}
		return
	}

	// 每次打开程序，强制改为0(默认取本日消息数据)
	for k := range dp.dbState.MessageTableMap {
		dp.dbState.MessageTableMap[k] = 0
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
}

// StartPeriodicProcessing 启动定期处理
func (dp *DataProcessor) StartPeriodicProcessing() {
	dp.logger.Info("定时任务开启，", zap.Int("account_count", len(dp.accounts)))

	// 定时器，每30秒执行一次
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 每30秒启动一个新任务，使用信号量控制并发
			go dp.runTask()
		}
	}
}

// runTask 执行单次任务
func (dp *DataProcessor) runTask() {
	defer func() {
		if err := recover(); err != nil {
			dp.logger.Error(
				"runTask panic recovered",
				zap.String("stack", string(debug.Stack())),
				zap.Any("error", err),
			)
		}
	}()

	// 尝试获取信号量，非阻塞
	select {
	case dp.processSem <- struct{}{}:
		// 获取成功，继续执行
	default:
		// 获取失败，并发数已达上限，跳过本次任务
		// dp.logger.Warn("并发任务数已达上限，跳过本次任务")
		return
	}

	defer func() {
		// 释放信号量
		<-dp.processSem
	}()

	startTime := time.Now()
	// dp.logger.Debug("任务开始")

	for _, account := range dp.accounts {
		dp.processAccountData(account)
	}

	// 保存数据库状态
	dp.SaveDatabaseState()

	elapsed := time.Since(startTime)
	dp.logger.Info("sync finish", zap.Duration("elapsed", elapsed))
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
	defer func() {
		if err := recover(); err != nil {
			dp.logger.Error(
				"processAccountData panic recovered",
				zap.String("account", account.Name),
				zap.String("stack", string(debug.Stack())),
				zap.Any("error", err),
			)
		}
	}()

	dp.logger.Info("start sync account.", zap.String("account", account.Name))

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
			dp.logger.Error("读取CTT-DB失败", zap.String("account", account.Name), zap.Error(err))
		}
	}

	// 处理消息数据库文件
	if len(messageFiles) > 0 {
		for _, file := range messageFiles {
			dp.ProcessMessageDatabase(decryptor, file, account)
		}
	}
	// dp.logger.Info("sync finish", zap.String("account", account.Name))
}

// ProcessContactDatabase 处理联系人数据库
func (dp *DataProcessor) ProcessContactDatabase(decryptor decrypt.Decryptor, dbFile string, account AccountInfo) error {
	// 检查文件是否需要更新
	if !dp.needsUpdate(dbFile) {
		dp.logger.Info("contact no update required", zap.String("file", filepath.Base(dbFile)))
		return nil
	}

	dp.logger.Info("process contact", zap.String("file", filepath.Base(dbFile)))

	// 解密到临时文件
	file, err := dp.wechatManager.DecryptToTempFile(decryptor, dbFile, account.Key, true)
	if err != nil {
		dp.logger.Error("process contact fail", zap.Error(err))
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
	// if !dp.needsUpdate(dbFile) {
	// 	dp.logger.Debug("消息数据库无更新", zap.String("file", filepath.Base(dbFile)))
	// 	return
	// }

	// dp.logger.Debug("process message", zap.String("file", filepath.Base(dbFile)))

	// 解密到临时文件
	tempDBFile, err := dp.wechatManager.DecryptToTempFile(decryptor, dbFile, account.Key, false)
	if err != nil {
		dp.logger.Debug("Failed to decrypt message database", zap.Error(err))
		return
	}
	// 确保删除临时文件
	defer func() {
		if err := os.Remove(tempDBFile); err != nil {
			dp.logger.Info("remove temp data error", zap.String("file", tempDBFile), zap.Error(err))
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
		dp.logger.Error("get find info error", zap.String("file", filePath), zap.Error(err))
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
