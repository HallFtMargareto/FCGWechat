package fcgame

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ProcessContactData 处理联系人数据
func (dp *DataProcessor) ProcessContactData(tempDBFile string, account string) (err error) {
	// 打开数据库连接
	db, err := GetGormDB(tempDBFile)
	if err != nil {
		dp.logger.Error("打开临时数据库失败", zap.Error(err))
		return
	}

	// 获取原始数据库连接以便关闭
	sqlDB, err := db.DB()
	if err != nil {
		dp.logger.Error("获取原始数据库连接失败", zap.Error(err))
		return
	}
	defer sqlDB.Close()

	// 分批处理联系人数据，每次最多500条
	const batchSize = 500
	totalCount := 0

	for {
		// 获取上次处理的最后ID
		lastID := dp.dbState.ContactLastID

		// 使用GORM查询联系人数据
		var contacts []Contact
		query := db.Select("id, username, local_type, alias, remark, nick_name, pin_yin_initial, quan_pin, big_head_url, small_head_url")

		if lastID > 0 {
			query = query.Where("id > ?", lastID)
		}

		err = query.Order("id ASC").Limit(batchSize).Find(&contacts).Error
		if err != nil {
			dp.logger.Error("查询联系人数据失败", zap.Error(err))
			return
		}

		batchCount := len(contacts)
		if batchCount == 0 {
			break
		}

		// 处理每个联系人
		for _, contact := range contacts {
			// 转换为FcgContact格式
			fcgContact := FcgContact{
				TenantId:      1,
				Username:      contact.Username,
				LocalType:     contact.LocalType,
				Alias:         contact.Alias,
				Remark:        contact.Remark,
				NickName:      contact.NickName,
				PinYinInitial: contact.PinYinInitial,
				QuanPin:       contact.QuanPin,
				BigHeadUrl:    contact.BigHeadUrl,
				SmallHeadUrl:  contact.SmallHeadUrl,
				Owner:         account,
			}

			// 通过 WebSocket 发送
			err = dp.wsClient.SendContact(fcgContact)
			if err != nil {
				dp.logger.Error("发送联系人数据失败", zap.Error(err))
				continue
			}

			// 更新最后处理的ID
			dp.dbState.ContactLastID = contact.ID
		}

		totalCount += batchCount

		// 如果这批数据不足batchSize，说明已经处理完所有数据
		if batchCount < batchSize {
			break
		}
	}

	if totalCount > 0 {
		fmt.Println("发送新群聊数据  ", totalCount, " 条")
		dp.logger.Debug("process contact batch success",
			zap.Int("totalCount", totalCount),
			zap.Int64("last_id", dp.dbState.ContactLastID))
	}

	return err
}

// ProcessMessageData 处理消息数据
func (dp *DataProcessor) ProcessMessageData(tempDBFile string, account string, dbFile string) {
	// 打开数据库连接
	db, err := GetGormDB(tempDBFile)
	if err != nil {
		dp.logger.Error("打开临时数据库失败", zap.Error(err))
		return
	}

	// 获取原始数据库连接以便关闭
	sqlDB, err := db.DB()
	if err != nil {
		dp.logger.Error("获取原始数据库连接失败", zap.Error(err))
		return
	}
	defer sqlDB.Close()

	// 获取所有消息表
	tables := dp.getMessageTablesWithGORM(db)
	if len(tables) == 0 {
		dp.logger.Warn("未找到消息表")
		return
	}

	totalCount := 0

	// 处理每个消息表
	for _, table := range tables {
		count := dp.processMessageTableWithGORM(db, table, dbFile, account)
		totalCount += count
	}
}

// processMessageTableWithGORM 使用GORM处理单个消息表
func (dp *DataProcessor) processMessageTableWithGORM(db *gorm.DB, tableName, dbFile string, account string) int {
	// 分批处理消息数据，每次最多500条
	const batchSize = 500
	totalCount := 0

	var lastID int64
	exists := false
	if lastID, exists = dp.dbState.MessageTableMap[tableName]; !exists {
		lastID = 0
	}

	for {
		var messages []Message

		// 构建查询 - 使用原始SQL以支持动态表名和JOIN
		query := fmt.Sprintf(`
			SELECT n.user_name, m.local_id, m.sort_seq, m.server_id, m.local_type, 
			       m.create_time, m.real_sender_id, m.message_content, m.status 
			FROM %s m 
			LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
		`, tableName)

		// 添加条件
		var conditions []string
		var args []interface{}

		// 添加 local_id 条件（防重复查询）
		if lastID > 0 {
			conditions = append(conditions, "m.local_id > ?")
			args = append(args, lastID)
		}

		// 组合条件
		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}

		query += " ORDER BY m.local_id ASC LIMIT ?"
		args = append(args, batchSize)

		// 执行GORM原始SQL查询
		err := db.Raw(query, args...).Scan(&messages).Error
		if err != nil {
			dp.logger.Error("查询消息表失败", zap.String("table", tableName), zap.Error(err))
			return totalCount
		}

		batchCount := len(messages)
		if batchCount == 0 {
			break
		}

		// 处理每个消息
		for _, msgResult := range messages {
			// 转换为FcgMessage格式
			message := FcgMessage{
				TenantId:          1,
				LocalId:           uint64(msgResult.LocalId),
				UserName:          msgResult.UserName,
				SortSeq:           msgResult.SortSeq,
				ServerId:          msgResult.ServerId,
				LocalType:         msgResult.LocalType,
				CreateTime:        msgResult.CreateTime,
				RealSenderId:      msgResult.RealSenderId,
				MessageContent:    msgResult.MessageContent,
				Status:            msgResult.Status,
				RecognitionStatus: false,
				MessageNo:         fmt.Sprintf("MSG_%d_%d", time.Now().UnixNano(), msgResult.LocalId),
				TaskList:          "",
				Owner:             account,
				Hash:              strings.TrimPrefix(tableName, "Msg_"),
			}

			// 发送消息
			err = dp.wsClient.SendFcgMessage(message)
			if err != nil {
				dp.logger.Error("发送消息数据失败", zap.Error(err))
				return 0
			}

			// 更新最后处理的ID
			lastID = msgResult.LocalId
		}

		// 保存进度
		dp.dbState.MessageTableMap[tableName] = lastID
		totalCount += batchCount

		// 如果这批数据不足batchSize，说明已经处理完所有数据
		if batchCount < batchSize {
			break
		}
	}

	if totalCount > 0 {
		fmt.Println("已发送 ", totalCount, " 条新消息")
		dp.logger.Debug("process message batch success",
			zap.String("table", tableName),
			zap.Int("totalCount", totalCount),
			zap.Int64("max_local_id", lastID),
		)
	}

	return totalCount
}

// getMessageTablesWithGORM 使用GORM获取所有消息表
func (dp *DataProcessor) getMessageTablesWithGORM(db *gorm.DB) []string {
	type TableName struct {
		Name string `gorm:"column:name"`
	}

	var tables []TableName
	err := db.Raw("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'Msg_%' OR name LIKE 'msg_%' OR name LIKE 'MSG%')").Scan(&tables).Error
	if err != nil {
		dp.logger.Error("查询消息表失败", zap.Error(err))
		return nil
	}

	var tableNames []string
	for _, table := range tables {
		tableNames = append(tableNames, table.Name)
	}

	return tableNames
}
