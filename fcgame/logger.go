package fcgame

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	Logger     *zap.Logger
	wsClient   *WebSocketClient // WebSocket客户端实例
	wsClientMu sync.RWMutex     // 保护WebSocket客户端的读写锁
)

// LogData 日志数据结构
type LogData struct {
	Level    string                 `json:"level"`
	Time     time.Time              `json:"time"`
	Caller   string                 `json:"caller"`
	Message  string                 `json:"message"`
	Fields   map[string]interface{} `json:"fields,omitempty"`
	Host     string                 `json:"host"`
	App      string                 `json:"app"`
	TenantId int                    `json:"tenant_id"`
}

// WebSocketWriter 自定义的WebSocket写入器
type WebSocketWriter struct {
	originalWriter zapcore.WriteSyncer // 原始文件写入器
}

// NewWebSocketWriter 创建WebSocket写入器
func NewWebSocketWriter(originalWriter zapcore.WriteSyncer) *WebSocketWriter {
	return &WebSocketWriter{
		originalWriter: originalWriter,
	}
}

// Write 实现io.Writer接口
func (w *WebSocketWriter) Write(p []byte) (n int, err error) {
	processed := p

	// 尝试解析日志，只在有 stacktrace 时才处理
	var logEntry map[string]interface{}
	if jsonErr := json.Unmarshal(p, &logEntry); jsonErr == nil {
		// 检查是否有 stacktrace 字段
		if stacktrace, ok := logEntry["stacktrace"].(string); ok {
			// 清理 stacktrace
			logEntry["stacktrace"] = cleanStacktrace(stacktrace)
			var marshalErr error
			processed, marshalErr = json.Marshal(logEntry)
			if marshalErr != nil {
				// 如果重新序列化失败，使用原始数据
				processed = p
			}
		}
	}

	// 写入处理后的日志，确保即使处理失败也能写入原始数据
	n, err = w.originalWriter.Write(processed)
	if err != nil {
		return n, err
	}

	// 解析日志用于 WebSocket 发送
	if jsonErr := json.Unmarshal(processed, &logEntry); jsonErr == nil {
		// 检查是否是错误级别的日志
		if level, ok := logEntry["level"].(string); ok && level == "error" {
			// 构建日志数据
			logData := LogData{
				Level:    level,
				Time:     time.Now(),
				Message:  fmt.Sprintf("%v", logEntry["msg"]),
				Host:     getHostName(),
				App:      "fcgame-20260807",
				TenantId: TenantId,
			}

			if caller, ok := logEntry["caller"]; ok {
				logData.Caller = fmt.Sprintf("%v", caller)
			}

			// 提取其他字段
			fields := make(map[string]interface{})
			for k, v := range logEntry {
				if k != "level" && k != "time" && k != "msg" && k != "caller" {
					fields[k] = v
				}
			}
			if len(fields) > 0 {
				logData.Fields = fields
			}

			// 异步发送到WebSocket服务器
			go sendErrorLogToServer(logData)
		}
	}

	return n, nil
}

// cleanStacktrace 清理 stacktrace 中的绝对路径，只保留文件名
func cleanStacktrace(stacktrace string) string {
	lines := strings.Split(stacktrace, "\n")
	result := make([]string, 0, len(lines))

	for _, line := range lines {
		if idx := strings.Index(line, "\t"); idx != -1 {
			pathPart := line[idx+1:]
			if colonIdx := strings.LastIndex(pathPart, ":"); colonIdx != -1 {
				filePath := pathPart[:colonIdx]
				lineNum := pathPart[colonIdx:]
				fileName := filepath.Base(filePath)
				result = append(result, line[:idx+1]+fileName+lineNum)
			} else {
				result = append(result, line)
			}
		} else {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

// Sync 实现zapcore.WriteSyncer接口
func (w *WebSocketWriter) Sync() error {
	return w.originalWriter.Sync()
}

// getHostName 获取主机名
func getHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// sendErrorLogToServer 发送错误日志到服务器
func sendErrorLogToServer(logData LogData) {
	wsClientMu.RLock()
	client := wsClient
	wsClientMu.RUnlock()

	if client == nil || !client.IsConnected() {
		return // WebSocket未连接，跳过发送
	}

	err := client.SendClientLog(logData)
	if err != nil {
		// 注意：这里不能调用Logger.Error，否则会造成循环调用
		fmt.Printf("处理错误日志失败: %v\n", err)
		fmt.Printf("data: %+v\n", logData)
	}
}

// InitLogger 初始化日志记录器
func InitLogger() (*zap.Logger, error) {
	// 获取当前程序执行目录
	execPath, err := os.Executable()
	if err != nil {
		return nil, err
	}
	execDir := filepath.Dir(execPath)

	// 创建日志目录
	logDir := filepath.Join(execDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	// 创建按天分割的日志文件路径
	now := time.Now()
	dateStr := now.Format("20060102")
	logFile := filepath.Join(logDir, fmt.Sprintf("fcgame_%s.log", dateStr))

	// 配置lumberjack按天轮转
	lumberjackLogger := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    10,   // 最大文件大小(MB)
		MaxBackups: 7,    // 最多保留30个备份文件
		MaxAge:     7,    // 最多保留30天
		Compress:   true, // 压缩旧文件
		LocalTime:  true, // 使用本地时间
	}

	// 配置日志编码器
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 创建WebSocket写入器包装原始文件写入器
	originalWriter := zapcore.AddSync(lumberjackLogger)
	webSocketWriter := NewWebSocketWriter(originalWriter)

	// 创建核心组件 - 只输出到文件
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		webSocketWriter,
		zapcore.DebugLevel,
	)

	// 创建日志记录器
	Logger = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return Logger, nil
}

// SetWebSocketClient 设置WebSocket客户端实例
func SetWebSocketClient(client *WebSocketClient) {
	wsClientMu.Lock()
	defer wsClientMu.Unlock()
	wsClient = client
}

// ClearWebSocketClient 清除WebSocket客户端实例
func ClearWebSocketClient() {
	wsClientMu.Lock()
	defer wsClientMu.Unlock()
	wsClient = nil
}

// Sync 刷新日志缓冲区
func Sync() {
	if Logger != nil {
		Logger.Sync()
	}
}

// 便捷方法
func Info(msg string, fields ...zap.Field) {
	Logger.Info(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	Logger.Error(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	Logger.Debug(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	Logger.Warn(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	Logger.Fatal(msg, fields...)
}
