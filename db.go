/*
 * @Author: your name
 * @Date: 2021-11-23 16:19:24
 * @LastEditTime: 2021-11-23 16:19:24
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \新建文件夹\db.go
 */
/*
 * @Author: your name
 * @Date: 2021-07-12 16:30:01
 * @LastEditTime: 2021-11-23 11:43:46
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \demo\db\sqlite.go
 */
package hls

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

var DB *sql.DB

var err error

func OpenDB() {
	DB, err = sql.Open("sqlite3", "./m3u8.db")
	//数据库打开异常，程序没必要继续进行
	CheckErr(err)
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

func CloseDB() {
	if DB != nil {
		DB.Close()
	}
}

func CreateTsInfoTable() error {

	sql := `create table if not exists "tsinfo" (
		  "id" integer primary key autoincrement,
		  "meeting_id" text not null,
		  "user_id" text not null,
		  "duration" real not null,
		  "title" text not null,
		  "created_at" TIMESTAMP default (datetime('now', 'localtime'))
	  )`
	_, err := DB.Exec(sql)
	return err
}
