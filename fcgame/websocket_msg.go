package fcgame

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/sjzar/chatlog/internal/model"
	"github.com/sjzar/chatlog/pkg/util/zstd"
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
			dp.logger.Error("查询Ctt数据失败", zap.Error(err))
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
				TenantId:      uint(TenantId),
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
				dp.logger.Error("发送CTT数据失败", zap.Error(err))
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
		fmt.Println("update session success:  ", totalCount)
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

	// 初始化并连接联系人数据库
	contactDB, err := GetGormDB(CONTACT_DB)
	if err != nil {
		dp.logger.Error("打开联系人数据库失败", zap.Error(err))
		return
	}
	contactDBConn, err := contactDB.DB()
	if err != nil {
		dp.logger.Error("获取联系人数据库连接失败", zap.Error(err))
		return
	}
	defer contactDBConn.Close()

	messageDB, err := GetMessageGormDB()
	if err != nil {
		dp.logger.Error("初始化消息数据库失败", zap.Error(err))
		return
	}
	messageDBConn, err := messageDB.DB()
	if err != nil {
		dp.logger.Error("获取消息数据库连接失败", zap.Error(err))
		return
	}
	defer messageDBConn.Close()

	// 获取所有消息表
	tables := dp.getMessageTablesWithGORM(db)
	if len(tables) == 0 {
		dp.logger.Warn("未找到消息表")
		return
	}

	totalCount := 0

	// 处理每个消息表
	for _, table := range tables {
		func() {
			defer func() {
				if r := recover(); r != nil {
					dp.logger.Error("处理消息发生panic", zap.String("table", table), zap.Any("panic", r), zap.String("stack", string(debug.Stack())))
				}
			}()
			// 处理Like消息
			dp.processMessageTableWithGORMCH(db, contactDB, messageDB, table, dbFile, account)
			// 处理普通消息
			count := dp.processMessageTableWithGORM(db, contactDB, messageDB, table, dbFile, account)
			totalCount += count
		}()
	}
}

// processMessageTableWithGORM 使用GORM处理单个消息表
func (dp *DataProcessor) processMessageTableWithGORM(db *gorm.DB, contactDB *gorm.DB, messageDB *gorm.DB, tableName, dbFile string, account string) int {

	// 分批处理消息数据，每次最多500条
	const batchSize = 500
	totalCount := 0

	// 第一次启动（本地没有记录），以当天零点作为起始查询条件
	// 获取当天零点
	now := time.Now()
	today := time.Date(
		now.Year(), now.Month(), now.Day(),
		0, 0, 0, 0, now.Location(),
	)
	todayStart := today.Unix() * 1000

	// 尝试从持久化状态中获取最后处理的 sort_seq
	var currentSortSeq int64
	exists := false
	if currentSortSeq, exists = dp.dbState.MessageTableMap[tableName]; !exists {
		currentSortSeq = todayStart
	} else {
		tenMinutesMillis := int64(2 * 60 * 1000) // 2分钟前的时间
		if currentSortSeq > tenMinutesMillis {
			currentSortSeq = currentSortSeq - tenMinutesMillis

			if currentSortSeq < todayStart {
				currentSortSeq = todayStart
			}

		} else {
			currentSortSeq = todayStart
		}
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

		// 只获取文本记录
		conditions = append(conditions, "m.local_type = ?")
		args = append(args, model.MessageTypeText)

		// 过滤出在 currentSortSeq 之后的消息
		conditions = append(conditions, "m.sort_seq >= ?")
		args = append(args, currentSortSeq)

		// 组合条件
		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}

		query += " ORDER BY m.sort_seq ASC LIMIT ?"
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

		subTable := strings.TrimPrefix(tableName, "Msg_")

		var sendErr error
		// 处理每个消息
		for _, msgResult := range messages {
			content := ""
			if bytes.HasPrefix(msgResult.MessageContent, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
				if b, err := zstd.Decompress(msgResult.MessageContent); err == nil {
					content = string(b)
				}
			} else {
				content = string(msgResult.MessageContent)
			}

			// 转换为FcgMessage格式
			message := FcgMessage{
				TenantId:          0, //会根据所属会话确定tenant_id
				LocalId:           uint64(msgResult.LocalId),
				UserName:          msgResult.UserName,
				SortSeq:           msgResult.SortSeq,
				ServerId:          msgResult.ServerId,
				LocalType:         msgResult.LocalType,
				CreateTime:        msgResult.CreateTime,
				RealSenderId:      msgResult.RealSenderId,
				MessageContent:    content,
				Status:            msgResult.Status,
				RecognitionStatus: 0,
				MessageNo:         fmt.Sprintf("MSG_%s_%d", subTable, msgResult.LocalId),
				TaskList:          "",
				Owner:             account,
				Hash:              subTable,
			}

			var contact Contact
			err = contactDB.Raw("select * from contact where username = ?", message.UserName).Scan(&contact).Error
			if err != nil || contact.ID == 0 {
				message.NickName = "未知用户"
			} else {
				message.NickName = contact.NickName
			}

			encContent, encErr := EncryptMessageContent(message.MessageContent)
			if encErr != nil {
				encContent = message.MessageContent
			}
			// 插入本地消息表
			msgModel := FcgMessageModel{
				TenantId:          message.TenantId,
				UserName:          message.UserName,
				NickName:          message.NickName,
				LocalId:           message.LocalId,
				SortSeq:           message.SortSeq,
				ServerId:          message.ServerId,
				LocalType:         message.LocalType,
				CreateTime:        message.CreateTime,
				RealSenderId:      message.RealSenderId,
				MessageContent:    encContent,
				Status:            message.Status,
				RecognitionStatus: message.RecognitionStatus,
				MessageNo:         message.MessageNo,
				TaskList:          message.TaskList,
				Owner:             message.Owner,
				Hash:              message.Hash,
				SendStatus:        0, // 初始发送状态为0
				SendRetryCount:    0,
			}

			// 先查询是否已存在相同的记录
			var existingMsg FcgMessageModel
			messageDB.Where("local_id = ? AND sort_seq = ?  AND server_id = ?",
				msgModel.LocalId, msgModel.SortSeq, msgModel.ServerId).First(&existingMsg)

			// 存在则跳过所有处理
			if existingMsg.ID > 0 {
				continue
			}

			// 不存在，则插入
			insertResult := messageDB.Create(&msgModel)
			if insertResult.Error != nil {
				dp.logger.Error("保存消息到本地失败", zap.Error(insertResult.Error))
				sendErr = insertResult.Error
				break
			}

			// 发送消息
			err = dp.wsClient.SendFcgMessage(message)
			if err != nil {
				dp.logger.Error("发送消息数据失败", zap.Error(err))
				sendErr = err
				break
			}

			// 更新最后处理的 sort_seq
			currentSortSeq = int64(msgResult.SortSeq)
			totalCount += 1
		}

		if sendErr != nil {
			return totalCount
		}

		// 如果这批数据不足batchSize，说明已经处理完所有数据
		if batchCount < batchSize {
			break
		}
	}

	// 跑完一次后，将更新后的 currentSortSeq 保存回持久化状态。
	// 下一次定时任务执行时，由于会取 Min(tenMinsAgoMs, currentSortSeq)，
	// 从而实现了“每次执行都用10分钟的时间窗口滑动作为查询条件，且不会漏掉停机期间的消息”
	dp.dbState.MessageTableMap[tableName] = currentSortSeq

	if totalCount > 0 {
		fmt.Println("update message: ", totalCount)
		// dp.logger.Debug("process message batch success",
		// 	zap.String("table", tableName),
		// 	zap.Int("totalCount", totalCount),
		// )
	}

	return totalCount
}

// processMessageTableWithGORM 使用GORM处理单个消息表Like
func (dp *DataProcessor) processMessageTableWithGORMCH(db *gorm.DB, contactDB *gorm.DB, messageDB *gorm.DB, tableName, dbFile string, account string) int {

	// 分批处理消息数据，每次最多500条
	const batchSize = 500
	totalCount := 0

	// 第一次启动（本地没有记录），以当天零点作为起始查询条件
	// 获取当天零点
	now := time.Now()
	today := time.Date(
		now.Year(), now.Month(), now.Day(),
		0, 0, 0, 0, now.Location(),
	)
	currentSortSeq := today.Unix() * 1000

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

		// 只获取文本记录
		conditions = append(conditions, "m.local_type = ?")
		args = append(args, model.MessageTypeSystem)
		conditions = append(conditions, "m.status = ?")
		args = append(args, 4)

		// 过滤出在 currentSortSeq 之后的消息
		conditions = append(conditions, "m.sort_seq >= ?")
		args = append(args, currentSortSeq)

		// 组合条件
		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}

		query += " ORDER BY m.sort_seq ASC LIMIT ?"
		args = append(args, batchSize)

		// 执行GORM原始SQL查询
		err := db.Raw(query, args...).Scan(&messages).Error
		if err != nil {
			dp.logger.Error("查询CH消息表失败", zap.String("table", tableName), zap.Error(err))
			return totalCount
		}

		batchCount := len(messages)
		if batchCount == 0 {
			break
		}

		subTable := strings.TrimPrefix(tableName, "Msg_")

		var sendErr error
		// 处理每个消息
		for _, msgResult := range messages {
			content := ""
			if bytes.HasPrefix(msgResult.MessageContent, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
				if b, err := zstd.Decompress(msgResult.MessageContent); err == nil {
					content = string(b)
				}
			} else {
				content = string(msgResult.MessageContent)
			}

			// 转换为FcgMessage格式
			message := FcgMessage{
				TenantId:          0, //会根据所属会话确定tenant_id
				LocalId:           uint64(msgResult.LocalId),
				UserName:          msgResult.UserName,
				SortSeq:           msgResult.SortSeq,
				ServerId:          msgResult.ServerId,
				LocalType:         msgResult.LocalType,
				CreateTime:        msgResult.CreateTime,
				RealSenderId:      msgResult.RealSenderId,
				MessageContent:    content,
				Status:            msgResult.Status,
				RecognitionStatus: 0,
				MessageNo:         fmt.Sprintf("MSG_%s_%d", subTable, msgResult.LocalId),
				TaskList:          "",
				Owner:             account,
				Hash:              subTable,
			}

			var contact Contact
			err = contactDB.Raw("select * from contact where username = ?", message.UserName).Scan(&contact).Error
			if err != nil || contact.ID == 0 {
				message.NickName = "未知用户"
			} else {
				message.NickName = contact.NickName
			}

			// 插入本地消息表
			msgModel := FcgMessageLike{
				TenantId:          message.TenantId,
				UserName:          message.UserName,
				NickName:          message.NickName,
				LocalId:           message.LocalId,
				SortSeq:           message.SortSeq,
				ServerId:          message.ServerId,
				LocalType:         message.LocalType,
				CreateTime:        message.CreateTime,
				RealSenderId:      message.RealSenderId,
				MessageContent:    "",
				Status:            message.Status,
				RecognitionStatus: message.RecognitionStatus,
				MessageNo:         message.MessageNo,
				TaskList:          message.TaskList,
				Owner:             message.Owner,
				Hash:              message.Hash,
				SendStatus:        0, // 初始发送状态为0
				SendRetryCount:    0,
			}

			// 先查询是否已存在相同的记录
			var existingMsg FcgMessageLike
			messageDB.Where("local_id = ? AND sort_seq = ?  AND server_id = ?",
				msgModel.LocalId, msgModel.SortSeq, msgModel.ServerId).First(&existingMsg)

			// 存在则跳过所有处理
			if existingMsg.ID > 0 {
				continue
			}

			// 不存在，则插入
			insertResult := messageDB.Create(&msgModel)
			if insertResult.Error != nil {
				dp.logger.Error("保存消息到本地失败", zap.Error(insertResult.Error))
				sendErr = insertResult.Error
				break
			}

			// 发送消息
			err = dp.wsClient.SendFcgMessage(message)
			if err != nil {
				dp.logger.Error("发送CH消息数据失败", zap.Error(err))
				sendErr = err
				break
			}

			// 更新最后处理的 sort_seq
			currentSortSeq = int64(msgResult.SortSeq)
			totalCount += 1
		}

		if sendErr != nil {
			return totalCount
		}

		// 如果这批数据不足batchSize，说明已经处理完所有数据
		if batchCount < batchSize {
			break
		}
	}

	if totalCount > 0 {
		fmt.Println("update message: ", totalCount)
		// dp.logger.Debug("process message batch success",
		// 	zap.String("table", tableName),
		// 	zap.Int("totalCount", totalCount),
		// )
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
