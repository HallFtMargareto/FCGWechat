package main

import (
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sjzar/chatlog/fcgame"
)

func main() {
	manager := fcgame.NewManager()
	// defer manager.Close()

	// 初始化管理器
	if err := manager.Initialize(); err != nil {
		fmt.Println("初始化失败:", err)
		return
	}

	// 运行管理器
	manager.Run()
}
