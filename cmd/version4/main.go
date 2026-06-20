package main

import (
	"fmt"

	"github.com/sjzar/chatlog/fcgame"
	"github.com/sjzar/chatlog/internal/wechat/decrypt"
)

/*
{ Name: "wxid_7t9azqe51qyg22_0210", SortName: "", Platform: "windows", Version: 4, FullVersion: "4.0.3.36", DataDir: "C:\\Users\\oliver\\Documents\\xwechat_files\\wxid_7t9azqe51qyg22_0210", Key: "3520aff0dafe4f83b6d255c073e3f7afc8b6d316123d44c1b587b3f07d30099c", ImgKey: "34303365613865303735336430636332", PID: 4052, ExePath: "C:\\Program Files\\Tencent\\Weixin\\Weixin.exe", Status: "online", SavedAt: 1757837104
}
*/

// var key = "29a447e0cedb4771b1b5076bb80f297d0a198abfabe5497e89333c1bd51d4dfa"
var key = "2abdce50839242268b5a1229b3aecde4d6edb782cb394803857521353461ce79"

func main() {
	// 创建解密器
	decryptor, err := decrypt.NewDecryptor("windows", 4)
	if err != nil {
		fmt.Println(err)
		return
	}

	manager := fcgame.NewManager()
	defer manager.Close()

	wechatManager := fcgame.NewWechatManager()
	tempDBFile, err := wechatManager.DecryptToTempFile(decryptor, "D:\\CryptDrive\\chatlog-main\\cmd\\version4\\message_0-5.db", key, false)

	// tempDBFile, err := wechatManager.DecryptToTempFile(decryptor, "D:\\CryptDrive\\chatlog-main\\cmd\\version4\\list\\wxid_0f32w7u1lmox22_3011\\db_storage\\contact\\contact.db", key, false)

	// tempDBFile, err := wechatManager.DecryptToTempFile(decryptor, "D:\\CryptDrive\\chatlog-main\\cmd\\version4\\list\\wxid_0f32w7u1lmox22_3011\\db_storage\\message\\message_0.db", key, false)
	fmt.Println(tempDBFile, err)
}
