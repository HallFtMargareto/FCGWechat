package newdec

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sjzar/chatlog/internal/wechat"
	"github.com/sjzar/chatlog/internal/wechat/decrypt"
	"github.com/sjzar/chatlog/pkg/filemonitor"
)

func Run() {
	// 定义命令行参数
	getKey := flag.Bool("getkey", false, "获取密钥")
	flag.Parse()

	// 加载微信实例
	if err := wechat.Load(); err != nil {
		fmt.Printf("Loca Wx Error: %v\n", err)
		return
	}

	accounts := wechat.GetAccounts()
	if len(accounts) == 0 {
		fmt.Println("Cant Find Wx Object.")
		return
	}

	var account *wechat.Account
	// 选择第一个在线的账号
	for _, acc := range accounts {
		if acc.Status == "online" {
			account = acc
			break
		}
	}

	if account == nil {
		fmt.Println("未找到在线的微信账号")
		return
	}

	// 如果需要获取密钥
	var key, imgKey string
	if *getKey {
		fmt.Println("正在获取密钥...")
		var err error
		key, imgKey, err = account.GetKey(context.Background())
		if err != nil {
			fmt.Printf("获取密钥失败: %v\n", err)
			return
		}
		fmt.Printf("数据密钥: %s\n", key)
		fmt.Printf("图片密钥: %s\n", imgKey)
	}

	// 获取当前工作目录作为解密文件的输出目录
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("获取当前目录失败: %v\n", err)
		return
	}

	// 如果没有通过-getkey参数获取密钥，则尝试使用账号中已有的密钥
	if key == "" {
		key = account.Key
	}

	if key == "" {
		fmt.Println("未获取到数据密钥，无法解密文件")
		return
	}

	// 解密数据库文件
	fmt.Println("开始解密数据库文件...")
	if err := decryptDBFiles(account, key, currentDir); err != nil {
		fmt.Printf("解密文件失败: %v\n", err)
		return
	}

	fmt.Println("解密完成，文件已保存到当前目录")
}

func decryptDBFiles(account *wechat.Account, key, outputDir string) error {
	// 创建文件监控组来查找数据库文件
	var dbPattern string
	switch {
	case account.Platform == "windows" && account.Version == 3:
		dbPattern = `Msg\\.*\.db$`
	case account.Platform == "windows" && account.Version == 4:
		dbPattern = `db_storage\\.*\.db$`
	case account.Platform == "darwin" && account.Version == 3:
		dbPattern = `Message/.*\.db$`
	case account.Platform == "darwin" && account.Version == 4:
		dbPattern = `db_storage/.*\.db$`
	default:
		return fmt.Errorf("不支持的平台或版本: %s v%d", account.Platform, account.Version)
	}

	// 创建文件组
	dbGroup, err := filemonitor.NewFileGroup("wechat", account.DataDir, dbPattern, []string{"fts"})
	if err != nil {
		return err
	}

	// 获取文件列表
	dbFiles, err := dbGroup.List()
	if err != nil {
		return err
	}

	// 创建解密器
	decryptor, err := decrypt.NewDecryptor(account.Platform, account.Version)
	if err != nil {
		return err
	}

	// 解密每个文件
	for _, dbFile := range dbFiles {
		if err := decryptFile(decryptor, dbFile, key, outputDir); err != nil {
			fmt.Printf("解密文件 %s 失败: %v\n", dbFile, err)
			continue
		}
		fmt.Printf("已解密: %s\n", filepath.Base(dbFile))
	}

	return nil
}

func decryptFile(decryptor decrypt.Decryptor, dbFile, key, outputDir string) error {
	// 确定输出文件路径
	relPath, err := filepath.Rel(filepath.Dir(dbFile), dbFile)
	if err != nil {
		relPath = filepath.Base(dbFile)
	}

	outputFile := filepath.Join(outputDir, relPath)

	// 确保输出目录存在
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return err
	}

	// 创建临时输出文件
	tempOutputFile := outputFile + ".tmp"
	outFile, err := os.Create(tempOutputFile)
	if err != nil {
		return err
	}
	defer func() {

		outFile.Close()
		// 清理临时文件
		if _, err := os.Stat(outputFile); err == nil {
			os.Remove(tempOutputFile)
		} else {
			os.Rename(tempOutputFile, outputFile)
		}
	}()

	// 解密文件
	if err := decryptor.Decrypt(context.Background(), dbFile, key, outFile); err != nil {
		return err
	}

	return nil
}
