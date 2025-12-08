package fcgame

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestReconnectWithPendingMessages 测试重连时保存未发送消息的功能
func TestReconnectWithPendingMessages(t *testing.T) {
	// 创建测试日志
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// 创建测试配置
	config := GetDefaultConfig()
	config.Reconnect = true
	config.MaxReconnect = 3
	config.ReconnectWait = 1 * time.Second

	// 创建WebSocket客户端
	client := NewWebSocketClient(config, logger)

	// 测试添加待发送消息
	testMessage1 := []byte(`{"id":1,"path":"test","data":"message1"}`)
	testMessage2 := []byte(`{"id":2,"path":"test","data":"message2"}`)

	// 添加消息到缓存
	client.addPendingMessage(testMessage1)
	client.addPendingMessage(testMessage2)

	// 验证消息已添加到缓存
	if count := client.getPendingMessageCount(); count != 2 {
		t.Errorf("期望缓存消息数量为2，实际为%d", count)
	}

	// 测试移除消息
	client.removePendingMessage(testMessage1)
	if count := client.getPendingMessageCount(); count != 1 {
		t.Errorf("移除一条消息后，期望缓存消息数量为1，实际为%d", count)
	}

	// 测试重复添加相同消息
	client.addPendingMessage(testMessage2) // 应该不会重复添加
	if count := client.getPendingMessageCount(); count != 1 {
		t.Errorf("重复添加相同消息后，期望缓存消息数量仍为1，实际为%d", count)
	}

	t.Log("消息缓存机制测试通过")
}

// TestSaveUnsentMessages 测试保存未发送消息的功能
func TestSaveUnsentMessages(t *testing.T) {
	// 创建测试日志
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// 创建测试配置
	config := GetDefaultConfig()

	// 创建WebSocket客户端
	client := NewWebSocketClient(config, logger)

	// 向sendChan添加一些消息
	testMessage1 := []byte(`{"id":1,"path":"test","data":"message1"}`)
	testMessage2 := []byte(`{"id":2,"path":"test","data":"message2"}`)

	client.sendChan <- testMessage1
	client.sendChan <- testMessage2

	// 调用saveUnsentMessages
	client.saveUnsentMessages()

	// 验证消息已从sendChan转移到pendingMessages
	if count := client.getPendingMessageCount(); count != 2 {
		t.Errorf("期望保存2条未发送消息，实际保存了%d条", count)
	}

	t.Log("保存未发送消息测试通过")
}

// TestGetStats 测试获取统计信息时包含待发送消息数量
func TestGetStats(t *testing.T) {
	// 创建测试日志
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// 创建测试配置
	config := GetDefaultConfig()

	// 创建WebSocket客户端
	client := NewWebSocketClient(config, logger)

	// 添加一些测试消息
	testMessage := []byte(`{"id":1,"path":"test","data":"message1"}`)
	client.addPendingMessage(testMessage)

	// 获取统计信息
	stats := client.GetStats()

	// 验证统计信息中包含待发送消息数量
	pendingCount, ok := stats["pending_message_count"].(int)
	if !ok {
		t.Error("统计信息中缺少pending_message_count字段")
	}

	if pendingCount != 1 {
		t.Errorf("期望待发送消息数量为1，实际为%d", pendingCount)
	}

	t.Log("统计信息测试通过")
}
