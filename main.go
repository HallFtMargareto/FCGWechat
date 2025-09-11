package main

import (
	"flag"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sjzar/chatlog/cmd/newdec"
	"github.com/sjzar/chatlog/fcgame"
	"github.com/sjzar/chatlog/pkg/logger"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 初始化日志系统
	if err := logger.InitLogger(); err != nil {
		log.Fatalf("初始化日志系统失败: %v", err)
	}
	defer logger.Sync()

	logger.Info("程序启动")

	// 解析命令行参数
	flag.Parse()

	// 检查是否是newdec命令
	if len(os.Args) > 1 && os.Args[1] == "newdec" {
		newdec.Run()
		return
	}

	manager := fcgame.NewManager()
	defer manager.Close()

	// 初始化管理器
	if err := manager.Initialize(); err != nil {
		logger.Error("初始化管理器失败")
		return
	}

	// 运行管理器
	manager.Run()
}
