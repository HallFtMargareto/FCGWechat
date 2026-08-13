package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/minio/selfupdate"
	"github.com/sjzar/chatlog/fcgame"
	"go.uber.org/zap"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			// 使用 log.Fatal 或者将错误写入文件、发送到监控平台
			log.Printf("!!! FATAL: Program crashed with panic: %v\n", err)
			log.Printf("!!! Stack Trace:\n%s\n", debug.Stack())
			// 可以选择退出，也可以尝试恢复
		}
	}()

	// 加载配置文件
	if err := fcgame.LoadConfig("config.json"); err != nil {
		fmt.Printf("加载配置文件失败: %v\n", err)
		return
	}

	manager := fcgame.NewManager()
	defer manager.Close()

	// 初始化管理器
	if err := manager.Initialize(); err != nil {
		fcgame.Logger.Error("初始化失败", zap.Error(err))
		fmt.Println("初始化失败:", err)
		return
	}

	//检查新版本
	fcgame.RefreshServerInfo()
	doUpdate()

	// 运行解密发送任务
	manager.Run()

	time.Sleep(time.Second)
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

func doUpdate() error {
	if fcgame.SInfo.Version == fcgame.Version {
		return nil
	}
	fmt.Println("正在更新客户端,请勿关闭程序...")
	resp, err := http.Get(strings.TrimSpace(fcgame.SInfo.Download))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	fmt.Println("Status-Code:", resp.StatusCode)
	fmt.Println("Content-Length:", resp.Header.Get("Content-Length"))
	fmt.Println("Content-Type:", resp.Header.Get("Content-Type"))

	if resp.StatusCode != 200 {
		fcgame.Logger.Error("更新失败", zap.Any("info", fcgame.SInfo))
		fmt.Println("更新失败")
		return nil
	}

	err = selfupdate.Apply(resp.Body, selfupdate.Options{})
	if err != nil {
		fcgame.Logger.Error("update fail", zap.Error(err))
		fmt.Println("update fail: ", err)
		return err
	}

	// fmt.Println("已更新版本：", fcgame.Version)
	fmt.Println("更新成功，请重新打开程序")
	return nil
}
