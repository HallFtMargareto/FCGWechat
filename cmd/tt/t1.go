package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sjzar/chatlog/fcgame"
	_ "modernc.org/sqlite" // 替换 import "github.com/mattn/go-sqlite3"
)

func main() {
	// tempDBFile := "C:\\Users\\xusc\\AppData\\Local\\Temp\\log_decrypt_1113100117.db"
	tempDBFile := filepath.Join(os.TempDir(), "chatlog_decrypt_1113100117.db")

	db, err := sql.Open("sqlite", tempDBFile)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	// 查询 SQL
	query := `SELECT id, username, local_type, alias, remark, nick_name, pin_yin_initial, quan_pin, big_head_url, small_head_url FROM contact`

	// 获取上次处理的最后 ID
	query += " ORDER BY id ASC"

	// 执行查询
	rows, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()

	var maxID int64
	count := 0

	// 遍历结果
	for rows.Next() {
		var contact fcgame.FcgContact
		var id int64

		err := rows.Scan(
			&id,
			&contact.Username,
			&contact.LocalType,
			&contact.Alias,
			&contact.Remark,
			&contact.NickName,
			&contact.PinYinInitial,
			&contact.QuanPin,
			&contact.BigHeadUrl,
			&contact.SmallHeadUrl,
		)

		if err != nil {
			fmt.Println(err)
			continue
		}

		// 设置 TenantId（可以根据实际情况调整）
		contact.TenantId = 1

		if id > maxID {
			maxID = id
		}
		count++
	}
}
