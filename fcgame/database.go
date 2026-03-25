package fcgame

import (
	"fmt"
	"os"
	"path/filepath"

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

func GetMessageGormDB() (*gorm.DB, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("获取当前目录失败: %w", err)
	}

	dbPath := filepath.Join(currentDir, MessageDBFileName)
	if _, err := os.Stat(dbPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("检查消息数据库文件失败: %w", err)
		}

		f, createErr := os.Create(dbPath)
		if createErr != nil {
			return nil, fmt.Errorf("创建消息数据库文件失败: %w", createErr)
		}
		closeErr := f.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("关闭消息数据库文件失败: %w", closeErr)
		}
	}

	db, err := GetGormDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("打开消息数据库失败: %w", err)
	}

	err = db.AutoMigrate(&FcgMessageModel{})
	if err != nil {
		return nil, fmt.Errorf("自动创建本地消息表失败: %w", err)
	}

	return db, nil
}
