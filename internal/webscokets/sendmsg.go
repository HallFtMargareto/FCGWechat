package webscokets

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sjzar/chatlog/pkg/logger"
	"go.uber.org/zap"
)

// 全局处理器接口
type GlobalProcessor interface {
	GetMutex() *sync.Mutex
	GetDBState() *DatabaseState
	GetWSClient() *WebSocketClient
	UpdateContactLastID(id int64)
	UpdateMessageTableLastID(tableName string, localID int64)
}

// DatabaseState 数据库处理状态
type DatabaseState struct {
	ContactLastID   int64            `json:"contact_last_id"`
	MessageTableMap map[string]int64 `json:"message_table_map"` // 表名 -> 最后处理的local_id
}

// 全局处理器实例
var globalProcessor GlobalProcessor

// SetGlobalProcessor 设置全局处理器
func SetGlobalProcessor(processor GlobalProcessor) {
	globalProcessor = processor
}

// ProcessContactData 处理联系人数据
func ProcessContactData(db *sql.DB, account interface{}) {
	// 查询 SQL
	query := `SELECT id, username, local_type, alias, remark, nick_name, pin_yin_initial, quan_pin, big_head_url, small_head_url FROM contact`

	// 获取上次处理的最后 ID
	lastID := getContactLastID()
	if lastID > 0 {
		query += fmt.Sprintf(" WHERE id > %d", lastID)
	}
	query += " ORDER BY id ASC"

	// 执行查询
	rows, err := db.Query(query)
	if err != nil {
		logger.Error("查询联系人数据失败", zap.Error(err))
		return
	}
	defer rows.Close()

	var maxID int64
	count := 0

	// 遍历结果
	for rows.Next() {
		var contact FcgContact
		var id int64

		err := rows.Scan(
			&id,
			&contact.Username,
			&contact.LocalType,
			&contact.Alias,
			&contact.Remark,
			&contact.NickName,
			&contact.PinYinInitial,
			&contact.QuanPin,
			&contact.BigHeadUrl,
			&contact.SmallHeadUrl,
		)

		if err != nil {
			logger.Error("扫描联系人数据失败", zap.Error(err))
			continue
		}

		// 设置 TenantId（可以根据实际情况调整）
		contact.TenantId = 1

		// 通过 WebSocket 发送
		if globalProcessor != nil {
			wsClient := globalProcessor.GetWSClient()
			if wsClient != nil && wsClient.IsConnected() {
				err = wsClient.SendContact(contact)
				if err != nil {
					logger.Error("发送联系人数据失败", zap.Error(err))
					continue
				}
			}
		}

		if id > maxID {
			maxID = id
		}
		count++
	}

	// 更新最后处理的 ID
	if maxID > lastID {
		updateContactLastID(maxID)
	}

	if count > 0 {
		logger.Info("处理联系人数据完成", zap.Int("count", count))
	}
}

// ProcessMessageData 处理消息数据
func ProcessMessageData(db *sql.DB, account interface{}, dbFile string) {
	// 获取所有消息表
	tables := getMessageTables(db)
	if len(tables) == 0 {
		logger.Warn("未找到消息表")
		return
	}

	totalCount := 0

	// 处理每个消息表
	for _, table := range tables {
		count := processMessageTable(db, table, dbFile)
		totalCount += count
	}

	if totalCount > 0 {
		logger.Info("处理消息数据完成",
			zap.String("dbFile", dbFile),
			zap.Int("total_count", totalCount))
	}
}

// getMessageTables 获取所有消息表
func getMessageTables(db *sql.DB) []string {
	query := `SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'Msg_%' OR name LIKE 'msg_%' OR name LIKE 'MSG%')`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("查询消息表失败", zap.Error(err))
		return nil
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}

	return tables
}

// processMessageTable 处理单个消息表
func processMessageTable(db *sql.DB, tableName, dbFile string) int {
	// 获取上次处理的最后 local_id
	lastID := getMessageTableLastID(tableName)

	// 构建查询 SQL
	query := fmt.Sprintf(`
		SELECT n.user_name, m.local_id, m.sort_seq, m.server_id, m.local_type, 
		       m.create_time, m.real_sender_id, m.message_content, m.status  
		FROM %s m  
		LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
	`, tableName)

	if lastID > 0 {
		query += fmt.Sprintf(" WHERE m.local_id > %d", lastID)
	}
	query += " ORDER BY m.create_time ASC"

	// 执行查询
	rows, err := db.Query(query)
	if err != nil {
		logger.Error("查询消息表失败", zap.String("table", tableName), zap.Error(err))
		return 0
	}
	defer rows.Close()

	var maxLocalID int64
	count := 0

	// 遍历结果
	for rows.Next() {
		var message FcgMessage
		var userName sql.NullString

		err := rows.Scan(
			&userName,
			&message.LocalId,
			&message.SortSeq,
			&message.ServerId,
			&message.LocalType,
			&message.CreateTime,
			&message.RealSenderId,
			&message.MessageContent,
			&message.Status,
		)

		if err != nil {
			logger.Error("扫描消息数据失败", zap.Error(err))
			continue
		}

		// 设置默认值
		message.TenantId = 1
		if userName.Valid {
			message.UserName = userName.String
		}
		message.NickName = message.UserName // 可以根据实际情况调整
		message.RecognitionStatus = false
		message.MessageNo = fmt.Sprintf("MSG_%d_%d", time.Now().UnixNano(), message.LocalId)
		message.TaskList = ""

		// 通过 WebSocket 发送
		if globalProcessor != nil {
			wsClient := globalProcessor.GetWSClient()
			if wsClient != nil && wsClient.IsConnected() {
				err = wsClient.SendFcgMessage(message)
				if err != nil {
					logger.Error("发送消息数据失败", zap.Error(err))
					continue
				}
			}
		}

		if int64(message.LocalId) > maxLocalID {
			maxLocalID = int64(message.LocalId)
		}
		count++
	}

	// 更新最后处理的 local_id
	if maxLocalID > lastID {
		updateMessageTableLastID(tableName, maxLocalID)
	}

	return count
}

// 状态管理函数

// getContactLastID 获取联系人最后处理的 ID
func getContactLastID() int64 {
	if globalProcessor == nil {
		return 0
	}
	mutex := globalProcessor.GetMutex()
	dbState := globalProcessor.GetDBState()
	if mutex == nil || dbState == nil {
		return 0
	}
	mutex.Lock()
	defer mutex.Unlock()
	return dbState.ContactLastID
}

// updateContactLastID 更新联系人最后处理的 ID
func updateContactLastID(id int64) {
	if globalProcessor == nil {
		return
	}
	globalProcessor.UpdateContactLastID(id)
}

// getMessageTableLastID 获取消息表最后处理的 local_id
func getMessageTableLastID(tableName string) int64 {
	if globalProcessor == nil {
		return 0
	}
	mutex := globalProcessor.GetMutex()
	dbState := globalProcessor.GetDBState()
	if mutex == nil || dbState == nil {
		return 0
	}
	mutex.Lock()
	defer mutex.Unlock()

	if lastID, exists := dbState.MessageTableMap[tableName]; exists {
		return lastID
	}
	return 0
}

// updateMessageTableLastID 更新消息表最后处理的 local_id
func updateMessageTableLastID(tableName string, localID int64) {
	if globalProcessor == nil {
		return
	}
	globalProcessor.UpdateMessageTableLastID(tableName, localID)
}
