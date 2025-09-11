package fcgame

import (
	"github.com/sjzar/chatlog/pkg/logger"
	"go.uber.org/zap"
)

// LogManager 日志管理器
type LogManager struct{}

// NewLogManager 创建新的日志管理器
func NewLogManager() *LogManager {
	return &LogManager{}
}

// LogInfo 记录信息日志
func (lm *LogManager) LogInfo(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

// LogError 记录错误日志
func (lm *LogManager) LogError(msg string, fields ...zap.Field) {
	logger.Error(msg, fields...)
}

// LogWarn 记录警告日志
func (lm *LogManager) LogWarn(msg string, fields ...zap.Field) {
	logger.Warn(msg, fields...)
}

// LogDebug 记录调试日志
func (lm *LogManager) LogDebug(msg string, fields ...zap.Field) {
	logger.Debug(msg, fields...)
}
