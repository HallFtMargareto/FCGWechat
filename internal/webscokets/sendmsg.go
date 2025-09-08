package webscokets

import (
	"fmt"
	"strings"
	"sync"
	"time"

	//_ "github.com/mattn/go-sqlite3"
	"database/sql"

	"github.com/sjzar/chatlog/pkg/logger"
	"go.uber.org/zap"
	_ "modernc.org/sqlite" // 替换 import "github.com/mattn/go-sqlite3"
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

func ProcessContactData(tempDBFile string, account string) {
	// 打开临时数据库文件
	db, err := sql.Open("sqlite", tempDBFile)
	if err != nil {
		logger.Error("打开临时数据库失败", zap.Error(err))
		return
	}
	defer db.Close()

	// 分批处理联系人数据，每次最多500条
	const batchSize = 500
	totalCount := 0

	for {
		// 获取上次处理的最后ID
		lastID := getContactLastID()

		// 查询 SQL - 添加 LIMIT 限制
		query := `SELECT id, username, local_type, alias, remark, nick_name, pin_yin_initial, quan_pin, big_head_url, small_head_url FROM contact`

		if lastID > 0 {
			query += fmt.Sprintf(" WHERE id > %d", lastID)
		}
		query += fmt.Sprintf(" ORDER BY id ASC LIMIT %d", batchSize)

		// 执行查询
		rows, err := db.Query(query)
		if err != nil {
			logger.Error("查询联系人数据失败", zap.Error(err))
			return
		}

		var maxID int64
		batchCount := 0

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
			contact.Owner = account

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
			batchCount++
		}
		rows.Close()

		// 更新最后处理的 ID
		if maxID > lastID {
			updateContactLastID(maxID)
		}

		totalCount += batchCount

		// 如果这批数据不足batchSize，说明已经处理完所有数据
		if batchCount < batchSize {
			break
		}

		logger.Debug("处理联系人数据批次完成",
			zap.Int("batch_count", batchCount),
			zap.Int64("max_id", maxID))
	}

	if totalCount > 0 {
		logger.Info("处理联系人数据完成", zap.Int("total_count", totalCount))
	}
}

// ProcessMessageData 处理消息数据
func ProcessMessageData(tempDBFile string, account string, dbFile string, minCreateTime int64) {
	// 打开临时数据库文件
	db, err := sql.Open("sqlite", tempDBFile)
	if err != nil {
		logger.Error("打开临时数据库失败", zap.Error(err))
		return
	}
	defer db.Close()

	// 获取所有消息表
	tables := getMessageTables(db)
	if len(tables) == 0 {
		logger.Warn("未找到消息表")
		return
	}

	totalCount := 0

	// 处理每个消息表
	for _, table := range tables {
		count := processMessageTable(db, table, dbFile, account, minCreateTime)
		totalCount += count
	}

	if totalCount > 0 {
		logger.Info("处理消息数据完成",
			zap.String("dbFile", dbFile),
			zap.Int("total_count", totalCount),
			zap.Int64("min_create_time", minCreateTime))
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
func processMessageTable(db *sql.DB, tableName, dbFile string, account string, minCreateTime int64) int {
	// 分批处理消息数据，每次最多500条
	const batchSize = 500
	totalCount := 0

	for {
		// 获取上次处理的最后 local_id
		lastID := getMessageTableLastID(tableName)

		// 构建查询 SQL
		query := fmt.Sprintf(`
			SELECT n.user_name, m.local_id, m.sort_seq, m.server_id, m.local_type, 
			       m.create_time, m.real_sender_id, m.message_content,m.packed_info_data, m.status
			FROM %s m  
			LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
		`, tableName)

		// 添加条件
		var conditions []string

		// 添加 local_id 条件
		if lastID > 0 {
			conditions = append(conditions, fmt.Sprintf("m.local_id > %d", lastID))
		}

		// 添加 create_time 条件
		if minCreateTime > 0 {
			conditions = append(conditions, fmt.Sprintf("m.create_time > %d", minCreateTime))
		}

		// 组合条件
		if len(conditions) > 0 {
			query += " WHERE " + fmt.Sprintf("(%s)", strings.Join(conditions, " AND "))
		}

		query += fmt.Sprintf(" ORDER BY m.local_id ASC LIMIT %d", batchSize)

		// 执行查询
		rows, err := db.Query(query)
		if err != nil {
			logger.Error("查询消息表失败", zap.String("table", tableName), zap.Error(err))
			return totalCount
		}

		var maxLocalID int64
		batchCount := 0

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

			// 设置默认值
			message.TenantId = 1
			if userName.Valid {
				message.UserName = userName.String
			}
			//message.NickName = message.UserName // 可以根据实际情况调整
			message.RecognitionStatus = false
			message.MessageNo = fmt.Sprintf("MSG_%d_%d", time.Now().UnixNano(), message.LocalId)
			message.TaskList = ""
			message.Owner = account
			message.Hash = strings.TrimPrefix(tableName, "Msg_")

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
			batchCount++
		}
		rows.Close()

		// 更新最后处理的 local_id
		if maxLocalID > lastID {
			updateMessageTableLastID(tableName, maxLocalID)
		}

		totalCount += batchCount

		// 如果这批数据不足batchSize，说明已经处理完所有数据
		if batchCount < batchSize {
			break
		}

		logger.Debug("处理消息表批次完成",
			zap.String("table", tableName),
			zap.Int("batch_count", batchCount),
			zap.Int64("max_local_id", maxLocalID),
			zap.Int64("min_create_time", minCreateTime))
	}

	if totalCount > 0 {
		logger.Debug("处理消息表完成",
			zap.String("table", tableName),
			zap.Int("total_count", totalCount),
			zap.Int64("min_create_time", minCreateTime))
	}

	return totalCount
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
