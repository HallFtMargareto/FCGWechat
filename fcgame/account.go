package fcgame

import (
	"encoding/json"
	"os"
)

/*
{Name: "wxid_7t9azqe51qyg22_0210", SortName: "", Platform: "windows", Version: 4, FullVersion: "4.0.3.36", DataDir: "C:\\Users\\oliver\\Documents\\xwechat_files\\wxid_7t9azqe51qyg22_0210", Key: "3520aff0dafe4f83b6d255c073e3f7afc8b6d316123d44c1b587b3f07d30099c", ImgKey: "34303365613865303735336430636332", PID: 4052, ExePath: "C:\\Program Files\\Tencent\\Weixin\\Weixin.exe", Status: "online", SavedAt: 1757837104}
*/

func FetchAccount() []AccountInfo {
	// 尝试从配置文件加载账户信息
	if accounts := loadAccountsFromConfig(); len(accounts) > 0 {
		return accounts
	}

	// 如果配置文件中没有账户信息，返回空列表
	return []AccountInfo{}
}

// loadAccountsFromConfig 从配置文件加载账户信息
func loadAccountsFromConfig() []AccountInfo {
	// 使用与主程序相同的配置文件路径
	configPath := "config.json"

	// 检查配置文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil
	}

	// 读取配置文件
	file, err := os.Open(configPath)
	if err != nil {
		return nil
	}
	defer file.Close()

	// 解析配置文件
	var config struct {
		Accounts []AccountInfo `json:"accounts"`
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil
	}

	return config.Accounts
}

/*
小博士
[]fcgame.AccountInfo len: 1, cap: 1, [{Name: "wxid_j1xusivt756j12_708c", SortName: "", Platform: "windows", Version: 4, FullVersion: "4.0.3.36", DataDir: "C:\\Users\\mouren\\Documents\\xwechat_files\\wxid_j1xusivt756j12_708c", Key: "9375adfbf2774ec2a6cae1ae50b5be2ca0023b4f80ce4cb5bbbc37a949a9823f", ImgKey: "39653935333562653962303138623331", PID: 2288, ExePath: "C:\\Program Files\\Tencent\\Weixin\\Weixin.exe", Status: "online", SavedAt: 1761408482}]
*/

// 小绵羊
// account := AccountInfo{
// 		Name:        "wxid_0f32w7u1lmox22_3011",
// 		SortName:    "",
// 		Platform:    "windows",
// 		Version:     4,
// 		FullVersion: "4.1.2.0",
// 		DataDir:     "C:\\Users\\51722\\Documents\\xwechat_files\\wxid_0f32w7u1lmox22_3011",
// 		Key:         "2abdce50839242268b5a1229b3aecde4d6edb782cb394803857521353461ce79",
// 		ImgKey:      "34303365613865303735336430636332",
// 		PID:         1000,
// 		ExePath:     "C:\\Program Files\\Tencent\\Weixin\\Weixin.exe",
// 		Status:      "online",
// 		SavedAt:     1761637918,
// 	}
