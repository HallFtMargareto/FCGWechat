package fcgame

import (
	"fmt"
	"os"
	"path/filepath"
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
		fmt.Println("Socket Connection Fail:", err.Error())
		return err
	}

	// 加载账户信息
	if err := m.processor.LoadAccounts(); err != nil {
		fmt.Println("Load Account Fail:", err.Error())
		return err
	}

	// 从缓存加载数据库状态
	m.processor.LoadDatabaseState()

	return nil
}

// Run 运行管理器568 569 589 689各50组3500
func (m *Manager) Run() {

	// SafeRun(m.readexcel)

	// 处理所有账户
	SafeRun(m.processor.ProcessAllAccounts)
}

// Close 关闭管理器
func (m *Manager) Close() {
	if m.processor != nil {
		m.processor.Close()
	}
	CloseMessageGormDB()
	if m.rbblot != nil {
		m.rbblot.Close()
	}
}

// Run 运行管理器
func (m *Manager) Test() {
	m.processor.Test()
}

func (m *Manager) readexcel() {
	time.Sleep(time.Second * 5)

	// 读取当前目录下所有Excel文件
	entries, err := os.ReadDir("./excel")
	if err != nil {
		fmt.Printf("读取目录失败: %v\n", err)
		return
	}

	excelFiles := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// 检查是否为Excel文件
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".xlsx" {
			excelFiles = append(excelFiles, name)
		}
	}

	if len(excelFiles) == 0 {
		fmt.Println("当前目录下没有Excel文件")
		return
	}

	fmt.Printf("找到 %d 个Excel文件\n", len(excelFiles))

	// 处理每个Excel文件
	for _, fileName := range excelFiles {
		fmt.Printf("处理文件: %s\n", fileName)

		// 打开Excel文件
		f, err := excelize.OpenFile("./excel/" + fileName)
		if err != nil {
			fmt.Printf("无法打开Excel文件 %s: %v\n", fileName, err)
			continue
		}

		// 获取所有工作表名称
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			fmt.Printf("Excel文件 %s 中没有工作表\n", fileName)
			f.Close()
			continue
		}

		// 处理每个工作表
		for _, sheetName := range sheets {
			fmt.Printf("读取工作表: %s\n", sheetName)

			// 逐行读取并打印
			rows, err := f.GetRows(sheetName)
			if err != nil {
				fmt.Printf("读取行数据时出错: %v\n", err)
				continue
			}

			st := 0
			// 打印每一行
			for _, row := range rows {
				// fmt.Printf("第%d行: ", i+1)
				for _, colCell := range row {
					if colCell == "" {
						continue
					}

					// 使用互斥锁保护Snowflake的并发访问
					snowflakeMutex.Lock()
					serverID := Snowflake.Generate()
					messageNo := Snowflake.Generate().String()
					snowflakeMutex.Unlock()

					message := FcgMessage{
						TenantId:          1,
						LocalId:           1,
						UserName:          "wxid_7t9azqe51qyg22",
						NickName:          "[FM]",
						SortSeq:           1,
						ServerId:          uint64(serverID.Int64()),
						LocalType:         1,
						CreateTime:        uint64(time.Now().Unix()),
						RealSenderId:      1,
						MessageContent:    colCell,
						Status:            1,
						RecognitionStatus: 0,
						MessageNo:         messageNo,
						TaskList:          "",
						Owner:             "wxid_7t9azqe51qyg22",
						Hash:              "77adf5664e5d0761dca3be6a134d1de3",
					}

					fmt.Println(message.ServerId, colCell)
					err = m.processor.wsClient.SendFcgMessage(message)
					if err != nil {
						m.processor.logger.Error("发送消息数据失败", zap.Error(err))
					}

					st++
					//time.Sleep(time.Second * 5)
					// if st == 5 {
					// 	time.Sleep(time.Second * 2)
					// 	st = 0
					// }
				}
			}
		}

		fmt.Println(fileName, "已全部发送")
		time.Sleep(time.Second * 5)

		f.Close()
	}
}

func (m *Manager) Send(msg string) {
	// 使用互斥锁保护Snowflake的并发访问
	snowflakeMutex.Lock()
	serverID := Snowflake.Generate()
	messageNo := Snowflake.Generate().String()
	snowflakeMutex.Unlock()

	message := FcgMessage{
		TenantId:          1,
		LocalId:           1,
		UserName:          "wxid_7t9azqe51qyg22",
		NickName:          "[FM]",
		SortSeq:           uint64(time.Now().UnixMicro()),
		ServerId:          uint64(serverID.Int64()),
		LocalType:         1,
		CreateTime:        uint64(time.Now().Unix()),
		RealSenderId:      1,
		MessageContent:    msg,
		Status:            1,
		RecognitionStatus: 0,
		MessageNo:         messageNo,
		Owner:             "wxid_7t9azqe51qyg22",
		Hash:              "77adf5664e5d0761dca3be6a134d1de3",
	}

	err := m.processor.wsClient.SendFcgMessage(message)
	if err != nil {
		fmt.Println("发送失败：", err.Error())
		return
	}

	fmt.Println("发送成功：", time.Now().Format(time.DateTime), msg)
}
