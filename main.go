package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sjzar/chatlog/fcgame"
	"go.uber.org/zap"
)

func main() {
	manager := fcgame.NewManager()
	// defer manager.Close()

	// 初始化管理器
	if err := manager.Initialize(); err != nil {
		fcgame.Logger.Error("初始化失败", zap.Error(err))
		fmt.Println("初始化失败:", err)
		return
	}

	// 运行管理器
	manager.Run()

	Wait()
}

// 程序挂起
func Wait() {
	exit := make(chan os.Signal, 10)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM) //notify方法用来监听收到的信号
	sig := <-exit
	fmt.Printf("receive signal: %s, submit program exit.", sig.String())
}
