package fcgame

import (
	"time"

	"github.com/bwmarrin/snowflake"
)

var Snowflake *snowflake.Node

// 基础配置
const (
	// 默认最小时间戳
	MinCreateTime = 1757370778

	// 默认处理最新的20个数据库
	MaxMessageDBCount = 20

	TimeLayout = "2006-01-02 15:04:05"

	CONTACT_DB = "fcgame_lxr.dat"

	// 发送最大chan数量
	MaxMessageQueue = 1000
)

// WEBSOCKET配置
const (
	// WebSocket服务器配置
	// WSServer = "127.0.0.1" // WebSocket服务器地址
	WSServer = "115.190.130.54" // WebSocket服务器地址
	WSPort   = "888"            // WebSocket服务器端口
	WSPath   = "/websocket"     // WebSocket路径
	WSScheme = "ws"             // WebSocket协议 (ws 或 wss)

	// 认证配置
	// 是否需要JWT认证
	RequireAuth = true
	// 默认JWT Token (如果需要认证)
	// DefaultToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MSwiYWNjb3VudCI6IiIsIm5hbWUiOiJob3N0IiwibW9kdWxlIjoxLCJleHAiOjE3NjE1MDQyMzUsImlzcyI6ImZjZ2FtZSJ9.xaRC2M1cjsNXrf-8c8e_i5pW-htVJJW5aQGoKluAGHw"
	DefaultToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MCwiYWNjb3VudCI6IiIsIm5hbWUiOiJob3N0IiwibW9kdWxlIjoxLCJleHAiOjE3NjIwNzU3MzgsImlzcyI6ImZjZ2FtZSJ9.W2bfc9nOpx5nNGctH_cVokxNY0QQPNJIsAR-GTwDG1I"

	// 客户端配置
	ClientVersion     = "1.0.0"           // 客户端版本
	HeartbeatInterval = 30 * time.Second  // 心跳间隔
	ReconnectInterval = 3 * time.Second   // 重连间隔
	MaxReconnectCount = 100               // 最大重连次数
	ConnectTimeout    = 100 * time.Second // 连接超时时间

	// 消息配置
	MaxMessageSize = 50120             // 最大消息大小
	SendTimeout    = 100 * time.Second // 发送超时时间
	ReadTimeout    = 120 * time.Second // 读取超时时间
)

// ClientConfig WebSocket客户端配置
type ClientConfig struct {
	ServerHost    string        `json:"server_host"`    // 服务器地址
	ServerPort    string        `json:"server_port"`    // 服务器端口
	Path          string        `json:"path"`           // WebSocket路径
	Scheme        string        `json:"scheme"`         // 协议 (ws/wss)
	Token         string        `json:"token"`          // JWT Token
	RequireAuth   bool          `json:"require_auth"`   // 是否需要认证
	Version       string        `json:"version"`        // 客户端版本
	UserAgent     string        `json:"user_agent"`     // User-Agent
	Origin        string        `json:"origin"`         // Origin头
	Subprotocols  []string      `json:"subprotocols"`   // 子协议
	Timeout       time.Duration `json:"timeout"`        // 连接超时
	Heartbeat     time.Duration `json:"heartbeat"`      // 心跳间隔
	Reconnect     bool          `json:"reconnect"`      // 是否自动重连
	MaxReconnect  int           `json:"max_reconnect"`  // 最大重连次数
	ReconnectWait time.Duration `json:"reconnect_wait"` // 重连等待时间
}

type DatabaseState struct {
	ContactLastID   int64                `json:"contact_last_id"`
	MessageTableMap map[string]int64     `json:"message_table_map"` // 表名 -> 最后处理的local_id
	FileStates      map[string]FileState `json:"file_states"`       // 文件路径 -> 文件状态
}

// FileState 文件状态跟踪
type FileState struct {
	Path         string    `json:"path"`
	LastModTime  time.Time `json:"last_mod_time"`
	LastProcTime time.Time `json:"last_proc_time"`
	Size         int64     `json:"size"`
}

// WebSocket消息结构体
type Request struct {
	Id        int64       `json:"id"`        // 消息ID
	Ver       string      `json:"ver"`       // 版本号
	Path      string      `json:"path"`      // 请求命令字
	Data      interface{} `json:"data"`      // 数据 JSON
	Timestamp int64       `json:"timestamp"` // 请求时间戳
}

// WebSocket响应结构体
type Response struct {
	Id        int         `json:"id"`        // 消息ID
	Msg       string      `json:"msg"`       // 返回信息
	Code      int         `json:"code"`      // 返回错误码
	Data      interface{} `json:"data"`      // 返回数据JSON
	Timestamp int64       `json:"timestamp"` // 响应时间戳
	Duration  float64     `json:"duration"`  // 处理请求所花费时间
}

// FcgContact 联系人结构体
type FcgContact struct {
	TenantId      uint   `json:"tenant_id"`
	Username      string `json:"user_name"`
	NickName      string `json:"nick_name"`
	Alias         string `json:"alias"`
	LocalType     uint   `json:"local_type"`
	PinYinInitial string `json:"pin_yin_initial"`
	QuanPin       string `json:"quan_pin"`
	BigHeadUrl    string `json:"big_head_url"`
	SmallHeadUrl  string `json:"small_head_url"`
	Remark        string `json:"remark"`
	Description   string `json:"description"`
	Hash          string `json:"hash"`
	Owner         string `json:"owner"`
}

// FcgMessage 消息结构体
type FcgMessage struct {
	TenantId          uint   `json:"tenant_id"`
	UserName          string `json:"user_name"`
	NickName          string `json:"nick_name"`
	LocalId           uint64 `json:"local_id"`
	SortSeq           uint64 `json:"sort_seq"`
	ServerId          uint64 `json:"server_id"`
	LocalType         uint   `json:"local_type"`
	CreateTime        uint64 `json:"create_time"`
	RealSenderId      uint64 `json:"real_sender_id"`
	MessageContent    string `json:"message_content"`
	Status            uint   `json:"status"`
	RecognitionStatus bool   `json:"recognition_status"`
	MessageNo         string `json:"message_no"`
	TaskList          string `json:"task_list"`
	Owner             string `json:"owner"`
	Hash              string `json:"hash"`
}
