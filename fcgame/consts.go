package fcgame

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
)

// Config 应用程序配置结构体
type Config struct {
	// WebSocket服务器配置
	WebSocket struct {
		Server string `json:"server"` // WebSocket服务器地址
		Port   string `json:"port"`   // WebSocket服务器端口
		Path   string `json:"path"`   // WebSocket路径
		Scheme string `json:"scheme"` // WebSocket协议 (ws 或 wss)
	} `json:"websocket"`

	// 认证配置
	Auth struct {
		RequireAuth bool   `json:"require_auth"` // 是否需要JWT认证
		Token       string `json:"token"`        // 默认JWT Token
	} `json:"auth"`

	// 客户端配置
	Client struct {
		Version           string `json:"version"`             // 客户端版本
		HeartbeatInterval int    `json:"heartbeat_interval"`  // 心跳间隔(秒)
		ReconnectInterval int    `json:"reconnect_interval"`  // 重连间隔(秒)
		MaxReconnectCount int    `json:"max_reconnect_count"` // 最大重连次数
		ConnectTimeout    int    `json:"connect_timeout"`     // 连接超时时间(秒)
	} `json:"client"`

	// 消息配置
	Message struct {
		MaxMessageSize int `json:"max_message_size"` // 最大消息大小
		SendTimeout    int `json:"send_timeout"`     // 发送超时时间(秒)
		ReadTimeout    int `json:"read_timeout"`     // 读取超时时间(秒)
		MaxQueue       int `json:"max_queue"`        // 发送最大chan数量
	} `json:"message"`

	// 基础配置
	Basic struct {
		MinCreateTime     int    `json:"min_create_time"`      // 默认最小时间戳
		MaxMessageDBCount int    `json:"max_message_db_count"` // 默认处理最新的20个数据库
		TimeLayout        string `json:"time_layout"`          // 时间格式
		ContactDB         string `json:"contact_db"`           // 联系人数据库文件名
	} `json:"basic"`

	// 账户配置
	Accounts []AccountInfo `json:"accounts"`
}

var (
	// Snowflake 全局ID生成器实例
	Snowflake *snowflake.Node
	// snowflakeMutex 保护Snowflake的并发访问
	snowflakeMutex sync.Mutex

	// 全局配置实例
	AppConfig *Config
)

// LoadConfig 从配置文件加载配置
func LoadConfig(configPath string) error {
	// 设置默认配置
	AppConfig = &Config{}
	setDefaultConfig()

	// 如果配置文件存在，则加载配置文件
	if _, err := os.Stat(configPath); err == nil {
		file, err := os.Open(configPath)
		if err != nil {
			return err
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		if err := decoder.Decode(AppConfig); err != nil {
			return err
		}
	}

	// 应用配置到全局变量
	applyConfig()

	return nil
}

// setDefaultConfig 设置默认配置
func setDefaultConfig() {
	// WebSocket配置
	AppConfig.WebSocket.Server = "192.168.1.209"
	AppConfig.WebSocket.Port = "9050"
	AppConfig.WebSocket.Path = "/websocket"
	AppConfig.WebSocket.Scheme = "ws"

	// 认证配置
	AppConfig.Auth.RequireAuth = true
	AppConfig.Auth.Token = ""

	// 客户端配置
	AppConfig.Client.Version = "1.0.0"
	AppConfig.Client.HeartbeatInterval = 30
	AppConfig.Client.ReconnectInterval = 3
	AppConfig.Client.MaxReconnectCount = 100
	AppConfig.Client.ConnectTimeout = 100

	// 消息配置
	AppConfig.Message.MaxMessageSize = 50120
	AppConfig.Message.SendTimeout = 100
	AppConfig.Message.ReadTimeout = 120
	AppConfig.Message.MaxQueue = 1000

	// 基础配置
	AppConfig.Basic.MinCreateTime = 1757370778
	AppConfig.Basic.MaxMessageDBCount = 20
	AppConfig.Basic.TimeLayout = "2006-01-02 15:04:05"
	AppConfig.Basic.ContactDB = "fcgame_lxr.dat"
}

// applyConfig 将配置应用到全局变量
func applyConfig() {
	WSServer = AppConfig.WebSocket.Server
	WSPort = AppConfig.WebSocket.Port
	WSPath = AppConfig.WebSocket.Path
	WSScheme = AppConfig.WebSocket.Scheme

	RequireAuth = AppConfig.Auth.RequireAuth
	DefaultToken = AppConfig.Auth.Token

	ClientVersion = AppConfig.Client.Version
	HeartbeatInterval = time.Duration(AppConfig.Client.HeartbeatInterval) * time.Second
	ReconnectInterval = time.Duration(AppConfig.Client.ReconnectInterval) * time.Second
	MaxReconnectCount = AppConfig.Client.MaxReconnectCount
	ConnectTimeout = time.Duration(AppConfig.Client.ConnectTimeout) * time.Second

	MaxMessageSize = AppConfig.Message.MaxMessageSize
	SendTimeout = time.Duration(AppConfig.Message.SendTimeout) * time.Second
	ReadTimeout = time.Duration(AppConfig.Message.ReadTimeout) * time.Second
	MaxMessageQueue = AppConfig.Message.MaxQueue

	MinCreateTime = AppConfig.Basic.MinCreateTime
	MaxMessageDBCount = AppConfig.Basic.MaxMessageDBCount
	TimeLayout = AppConfig.Basic.TimeLayout
	CONTACT_DB = AppConfig.Basic.ContactDB
}

// 全局配置变量
var (
	// WebSocket服务器配置
	WSServer string // WebSocket服务器地址
	WSPort   string // WebSocket服务器端口
	WSPath   string // WebSocket路径
	WSScheme string // WebSocket协议 (ws 或 wss)

	// 认证配置
	RequireAuth  bool   // 是否需要JWT认证
	DefaultToken string // 默认JWT Token

	// 客户端配置
	ClientVersion     string        // 客户端版本
	HeartbeatInterval time.Duration // 心跳间隔
	ReconnectInterval time.Duration // 重连间隔
	MaxReconnectCount int           // 最大重连次数
	ConnectTimeout    time.Duration // 连接超时时间

	// 消息配置
	MaxMessageSize  int           // 最大消息大小
	SendTimeout     time.Duration // 发送超时时间
	ReadTimeout     time.Duration // 读取超时时间
	MaxMessageQueue int           // 发送最大chan数量

	// 基础配置
	MinCreateTime     int    // 默认最小时间戳
	MaxMessageDBCount int    // 默认处理最新的20个数据库
	TimeLayout        string // 时间格式
	CONTACT_DB        string // 联系人数据库文件名
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
	RecognitionStatus int    `json:"recognition_status"`
	MessageNo         string `json:"message_no"`
	TaskList          string `json:"task_list"`
	Owner             string `json:"owner"`
	Hash              string `json:"hash"`
}
