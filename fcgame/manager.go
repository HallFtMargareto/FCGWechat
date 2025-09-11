package fcgame

import (
	"github.com/sjzar/chatlog/pkg/logger"
	"github.com/sjzar/chatlog/pkg/rbblot"
	"go.uber.org/zap"
)

// Manager 应用程序管理器
type Manager struct {
	processor *DataProcessor
	rbblot    *rbblot.RBblotCore
}

// NewManager 创建新的管理器
func NewManager() *Manager {
	// 初始化RBblot数据库
	rb := rbblot.NewBblot(10) // 10MB初始内存

	return &Manager{
		processor: NewDataProcessor(rb),
		rbblot:    rb,
	}
}

// Initialize 初始化管理器
func (m *Manager) Initialize() error {
	logger.Info("debug模式启动")

	// 初始化WebSocket连接
	if err := m.processor.InitializeWebSocket(); err != nil {
		logger.Warn("WebSocket连接失败", zap.Error(err))
		// 这里不返回错误，允许程序继续运行
	} else {
		logger.Info("WebSocket连接建立成功")
	}

	// 加载账户信息
	if err := m.processor.LoadAccounts(); err != nil {
		logger.Error("加载账户失败", zap.Error(err))
		return err
	}

	// 从缓存加载数据库状态
	m.processor.LoadDatabaseState()

	return nil
}

// Run 运行管理器
func (m *Manager) Run() {
	// 处理所有账户
	m.processor.ProcessAllAccounts()

	// 启动定期处理
	m.processor.StartPeriodicProcessing()
}

// Close 关闭管理器
func (m *Manager) Close() {
	if m.processor != nil {
		m.processor.Close()
	}
	if m.rbblot != nil {
		m.rbblot.Close()
	}
}
