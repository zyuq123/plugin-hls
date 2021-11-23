/*
 * @Author: your name
 * @Date: 2021-11-23 16:42:25
 * @LastEditTime: 2021-11-23 16:46:28
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \plugin-hls\tool.go
 */
package hls

import (
	"fmt"
	"io/ioutil"
)

func GetAllDirectory(pathname string) ([]string, error) {
	rd, _ := ioutil.ReadDir(pathname)
	if err != nil {
		fmt.Println("read dir fail:", err)
		return nil, err
	}

	var names []string
	for _, fi := range rd {
		if fi.IsDir() {
			names = append(names, fi.Name())
		}
	}
	return names, nil
}

type JsonResult struct {
	Code       int      `json:"code"`
	Msg        string   `json:"msg"`
	AllDirName []string `json:"allDir"`
}
