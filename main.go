package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sjzar/chatlog/fcgame"
	"go.uber.org/zap"
)

func main() {
	// 加载配置文件
	if err := fcgame.LoadConfig("config.json"); err != nil {
		fmt.Printf("加载配置文件失败: %v\n", err)
		return
	}
	fmt.Println("配置文件加载成功")

	manager := fcgame.NewManager()
	defer manager.Close()

	// 初始化管理器
	if err := manager.Initialize(); err != nil {
		fcgame.Logger.Error("初始化失败", zap.Error(err))
		fmt.Println("初始化失败:", err)
		return
	}

	// 运行解密发送任务
	manager.Run()

	// fcgame.CreateApp(manager)

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("请输入口令 (exit退出): ")
		if !scanner.Scan() { // 检查是否有输入
			break
		}
		text := strings.TrimSpace(scanner.Text())

		if text == "" {
			continue
		}
		if text == "exit" {
			fmt.Println("程序结束")
			break
		}
		manager.Send(text)
	}
}

// 程序挂起
func Wait() {
	exit := make(chan os.Signal, 10)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM) //notify方法用来监听收到的信号
	sig := <-exit
	fmt.Printf("receive signal: %s, submit program exit.", sig.String())
}
