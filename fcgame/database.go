package fcgame

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gorm.io/gorm/logger"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	_ "modernc.org/sqlite"
)

const MessageDBFileName = "fcgame_um.dat"

// Contact GORM模型 - 联系人表
type Contact struct {
	ID            int64  `gorm:"column:id;primaryKey"`
	Username      string `gorm:"column:username"`
	LocalType     uint   `gorm:"column:local_type"`
	Alias         string `gorm:"column:alias"`
	Remark        string `gorm:"column:remark"`
	NickName      string `gorm:"column:nick_name"`
	PinYinInitial string `gorm:"column:pin_yin_initial"`
	QuanPin       string `gorm:"column:quan_pin"`
	BigHeadUrl    string `gorm:"column:big_head_url"`
	SmallHeadUrl  string `gorm:"column:small_head_url"`
}

// TableName 设置表名
func (Contact) TableName() string {
	return "contact"
}

// Message GORM模型 - 消息表（动态表名）
type Message struct {
	UserName       string `gorm:"column:user_name"`
	LocalId        int64  `gorm:"column:local_id;primaryKey"`
	SortSeq        uint64 `gorm:"column:sort_seq"`
	ServerId       uint64 `gorm:"column:server_id"`
	LocalType      uint   `gorm:"column:local_type"`
	CreateTime     uint64 `gorm:"column:create_time"`
	RealSenderId   uint64 `gorm:"column:real_sender_id"`
	MessageContent []byte `gorm:"column:message_content"`
	Status         uint   `gorm:"column:status"`
}

// Name2Id GORM模型 - 名字映射表
type Name2Id struct {
	RowId    int64  `gorm:"column:rowid;primaryKey"`
	UserName string `gorm:"column:user_name"`
}

// TableName 设置表名
func (Name2Id) TableName() string {
	return "Name2Id"
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	MinCreateTime int64 `json:"min_create_time"` // 最小创建时间戳过滤
}

// ValidateSQLiteDB 校验SQLite数据库完整性，返回true表示数据库完整可用
func ValidateSQLiteDB(db *gorm.DB) bool {
	var result string
	row := db.Raw("PRAGMA integrity_check").Row()
	if err := row.Scan(&result); err != nil {
		return false
	}
	return result == "ok"
}

// GetGormDB 获取GORM数据库连接
func GetGormDB(dbPath string) (*gorm.DB, error) {
	// 配置GORM使用静默日志模式，避免输出到控制台
	config := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	}

	db, err := gorm.Open(sqlite.Open(dbPath), config)
	if err != nil {
		return nil, err
	}

	return db, nil
}

var (
	messageDBInstance *gorm.DB
	messageDBOnce     sync.Once
	messageDBErr      error
)

func GetMessageGormDB() (*gorm.DB, error) {
	messageDBOnce.Do(func() {
		currentDir, err := os.Getwd()
		if err != nil {
			messageDBErr = fmt.Errorf("获取当前目录失败: %w", err)
			return
		}

		dbPath := filepath.Join(currentDir, MessageDBFileName)
		if _, err := os.Stat(dbPath); err != nil {
			if !os.IsNotExist(err) {
				messageDBErr = fmt.Errorf("检查消息数据库文件失败: %w", err)
				return
			}

			f, createErr := os.Create(dbPath)
			if createErr != nil {
				messageDBErr = fmt.Errorf("创建消息数据库文件失败: %w", createErr)
				return
			}
			closeErr := f.Close()
			if closeErr != nil {
				messageDBErr = fmt.Errorf("关闭消息数据库文件失败: %w", closeErr)
				return
			}
		}

		dsn := dbPath + "?_journal_mode=WAL&_busy_timeout=5000"
		config := &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		}

		db, err := gorm.Open(sqlite.Open(dsn), config)
		if err != nil {
			messageDBErr = fmt.Errorf("打开消息数据库失败: %w", err)
			return
		}

		sqlDB, err := db.DB()
		if err != nil {
			messageDBErr = fmt.Errorf("获取底层数据库连接失败: %w", err)
			return
		}
		sqlDB.SetMaxOpenConns(1)

		err = db.AutoMigrate(&FcgMessageModel{})
		if err != nil {
			messageDBErr = fmt.Errorf("自动创建本地消息表失败: %w", err)
			return
		}
		err = db.AutoMigrate(&FcgMessageLike{})
		if err != nil {
			messageDBErr = fmt.Errorf("自动创建本地消息Like表失败: %w", err)
			return
		}

		messageDBInstance = db
	})
	return messageDBInstance, messageDBErr
}

func CloseMessageGormDB() {
	if messageDBInstance != nil {
		if sqlDB, err := messageDBInstance.DB(); err == nil {
			sqlDB.Close()
		}
	}
}
