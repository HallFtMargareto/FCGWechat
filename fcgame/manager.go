package fcgame

import (
	"fmt"
	"os"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/sjzar/chatlog/pkg/rbblot"
	"github.com/xuri/excelize/v2"
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
	node, err := snowflake.NewNode(1)
	if err != nil {
		return err
	}
	Snowflake = node

	// 初始化WebSocket连接
	if err := m.processor.InitializeWebSocket(); err != nil {
		fmt.Println("WebSocket连接失败", zap.Error(err))
		return err
	}

	// 加载账户信息
	if err := m.processor.LoadAccounts(); err != nil {
		fmt.Println("加载账户失败", zap.Error(err))
		return err
	}

	// 从缓存加载数据库状态
	m.processor.LoadDatabaseState()

	return nil
}

// Run 运行管理器
func (m *Manager) Run() {
	SafeRun(m.readexcel)

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

// Run 运行管理器
func (m *Manager) Test() {
	m.processor.Test()
}

func (m *Manager) readexcel() {
	for {
		time.Sleep(time.Second * 10)

		// 检查文件是否存在
		if _, err := os.Stat("chatlog.xlsx"); os.IsNotExist(err) {
			fmt.Println("文件 chatlog.xlsx 不存在")
			return
		}

		// 打开Excel文件
		f, err := excelize.OpenFile("chatlog.xlsx")
		if err != nil {
			fmt.Printf("无法打开Excel文件: %v\n", err)
			return
		}
		defer func() {
			if err := f.Close(); err != nil {
				fmt.Printf("关闭文件时出错: %v\n", err)
			}
		}()

		// 获取所有工作表名称
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			fmt.Println("Excel文件中没有工作表")
			return
		}

		// 读取第一个工作表
		sheetName := sheets[0]
		fmt.Printf("读取工作表: %s\n", sheetName)
		fmt.Println("文件内容:")

		// 逐行读取并打印
		rows, err := f.GetRows(sheetName)
		if err != nil {
			fmt.Printf("读取行数据时出错: %v\n", err)
			return
		}

		st := 0
		// 打印每一行
		for _, row := range rows {
			// fmt.Printf("第%d行: ", i+1)
			for _, colCell := range row {
				if colCell == "" {
					continue
				}

				message := FcgMessage{
					TenantId:          1,
					LocalId:           1,
					UserName:          "xuesencai",
					NickName:          "Sen",
					SortSeq:           1,
					ServerId:          uint64(Snowflake.Generate().Int64()),
					LocalType:         1,
					CreateTime:        uint64(time.Now().Unix()),
					RealSenderId:      1,
					MessageContent:    colCell,
					Status:            1,
					RecognitionStatus: false,
					MessageNo:         Snowflake.Generate().String(),
					TaskList:          "",
					Owner:             "wxid_7t9azqe51qyg22",
					Hash:              "2fe4fa04bd7faa46c1d2be9ceb07e98d",
				}

				fmt.Println(message.ServerId, colCell)
				err = m.processor.wsClient.SendFcgMessage(message)
				if err != nil {
					m.processor.logger.Error("发送消息数据失败", zap.Error(err))
				}
				st++
				if st == 50 {
					time.Sleep(time.Second * 5)
					st = 0
				}
			}
		}
	}
}
