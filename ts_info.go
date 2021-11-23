/*
 * @Author: your name
 * @Date: 2021-11-23 16:19:11
 * @LastEditTime: 2021-11-23 16:19:11
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \新建文件夹\ts_info.go
 */
/*
 * @Author: your name
 * @Date: 2021-11-23 10:22:54
 * @LastEditTime: 2021-11-23 14:52:31
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \demo\model\ts_info.go
 */
package hls

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

type TsInfo struct {
	ID        uint64    //用户ID，主键
	MeetingID string    //会议ID
	UserID    string    //用户ID
	Duration  float64   //时长
	Title     string    //标题
	CreatedAt time.Time //创建时间
}

func (u *TsInfo) Add() bool {
	//插入数据
	stmt, err := DB.Prepare("INSERT INTO tsinfo(meeting_id, user_id, duration,title,created_at) values(?,?,?,?,?)")
	CheckErr(err)
	//此处可以使用结构体的数据
	_, err = stmt.Exec(u.MeetingID, u.UserID, u.Duration, u.Title, u.CreatedAt)

	if err != nil {
		log.Println("insert error")
		return false
	}
	return true

}

func (u *TsInfo) DeleteData() {
	//删除数据
	stmt, err := DB.Prepare("delete from tsinfo where id=?")
	CheckErr(err)
	res, err := stmt.Exec(u.ID)
	CheckErr(err)
	affect, err := res.RowsAffected()
	CheckErr(err)
	fmt.Println(affect)

}

func ClearAllData() {
	//删除数据
	stmt, err := DB.Prepare("delete from tsinfo")
	CheckErr(err)
	res, err := stmt.Exec()
	CheckErr(err)
	affect, err := res.RowsAffected()
	CheckErr(err)
	fmt.Println(affect)

}

func GetTsSet() []TsInfo {
	//查询数据
	rows, err := DB.Query("SELECT * FROM tsinfo")
	CheckErr(err)

	infos := make([]TsInfo, 0)
	for rows.Next() {
		var user TsInfo
		rows.Scan(&user.ID, &user.MeetingID, &user.UserID, &user.Duration, &user.Title, &user.CreatedAt)
		infos = append(infos, user)
	}
	return infos
}

//返回满足条件的所有对象，数据库中存储的
func GetTsSetWithCondition(infos []TsInfo, from int64, to int64) []TsInfo {

	meetConditionList := make([]TsInfo, 0)
	for _, v := range infos {
		name := strings.TrimSuffix(v.Title, ".ts")
		ts_name_unix, _ := strconv.ParseInt(name, 10, 64)
		if ts_name_unix > from && ts_name_unix < to {
			meetConditionList = append(meetConditionList, v)
		}
	}
	return meetConditionList
}
