package hls

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Monibuca/utils/v3/codec/mpegts"

	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/plugin-ts/v3"
	. "github.com/Monibuca/utils/v3"
	"github.com/quangngotan95/go-m3u8/m3u8"
)

var collection sync.Map
var config struct {
	Fragment     int64
	Window       int
	EnableWrite  bool   //启动HLS写文件
	EnableMemory bool   // 启动内存直播模式
	Path         string //存放路径
	SliceNum     int64  //切割数量
}

func init() {
	config.Fragment = 10
	config.Window = 2
	//实例化数据局
	OpenDB()
	CreateTsInfoTable()

	InstallPlugin(&PluginConfig{
		Name:   "HLS",
		Config: &config,
		Run: func() {
			//os.MkdirAll(config.Path, 0666)
			if config.EnableWrite || config.EnableMemory {
				AddHook(HOOK_PUBLISH, writeHLS)
			}
		},
	})
	http.HandleFunc("/api/hls/list", func(w http.ResponseWriter, r *http.Request) {
		CORS(w, r)
		sse := NewSSE(w, r.Context())
		var err error
		for tick := time.NewTicker(time.Second); err == nil; <-tick.C {
			var info []*HLS
			collection.Range(func(key, value interface{}) bool {
				info = append(info, value.(*HLS))
				return true
			})
			err = sse.WriteJSON(info)
		}
	})
	http.HandleFunc("/api/hls/save", func(w http.ResponseWriter, r *http.Request) {
		CORS(w, r)
		streamPath := r.URL.Query().Get("streamPath")
		if data, ok := collection.Load(streamPath); ok {
			hls := data.(*HLS)
			hls.SaveContext = r.Context()
			<-hls.SaveContext.Done()
		}
	})
	http.HandleFunc("/api/hls/pull", func(w http.ResponseWriter, r *http.Request) {
		CORS(w, r)
		targetURL := r.URL.Query().Get("target")
		streamPath := r.URL.Query().Get("streamPath")
		p := new(HLS)
		var err error
		p.Video.Req, err = http.NewRequest("GET", targetURL, nil)
		if err == nil {
			p.Publish(streamPath)
			w.Write([]byte(`{"code":0}`))
		} else {
			w.Write([]byte(fmt.Sprintf(`{"code":1,"msg":"%s"}`, err.Error())))
		}
	})
	http.HandleFunc("/hls/", func(w http.ResponseWriter, r *http.Request) {
		CORS(w, r)
		if strings.HasSuffix(r.URL.Path, ".m3u8") {
			if f, err := os.Open(filepath.Join(config.Path, strings.TrimPrefix(r.URL.Path, "/hls/"))); err == nil {
				io.Copy(w, f)
				err = f.Close()
			} else {
				w.WriteHeader(404)
			}
		} else if strings.HasSuffix(r.URL.Path, ".ts") {
			tsPath := filepath.Join(config.Path, strings.TrimPrefix(r.URL.Path, "/hls/"))
			log.Println("ts request coming", tsPath)
			//如何区分直播还是录播文件
			mode := strings.Contains(tsPath, "_")
			//分为两种模式，一种是直播模式，一种是录播模式，使用相同的接口，区别就在于
			//请求的是不是包含_
			if config.EnableMemory && mode {
				if tsData, ok := memoryTs.Load(tsPath); ok {
					w.Write(mpegts.DefaultPATPacket)
					w.Write(mpegts.DefaultPMTPacket)
					w.Write(tsData.([]byte))
				} else {
					w.WriteHeader(404)
				}
			} else {
				if f, err := os.Open(tsPath); err == nil {
					io.Copy(w, f)
					f.Close()
				} else {
					w.WriteHeader(404)
				}
			}
		}
	})
	//获取录播文件的m3u8
	//此处有个疑问:请求指定时间段的m3u8的时候，返回的ts文件路径并没有IP信息，需要查看m3u8文件的URL生成规则
	//经过深入的研究，原来是请求m3u8的网址将文件替换，继续请求ts文件，抓包分析的
	//请求m3u8的URL：http://127.0.0.1:8080/getVod/getM3u8?streamPath=live/main&startTime=1637138192&endTime=1637138232
	http.HandleFunc("/getVod/", func(w http.ResponseWriter, r *http.Request) {
		CORS(w, r)
		if strings.HasSuffix(r.URL.Path, ".ts") {

			tsPath := filepath.Join(config.Path, strings.TrimPrefix(r.URL.Path, "/getVod/"))
			log.Println("ts vod request coming", tsPath)

			if f, err := os.Open(tsPath); err == nil {
				io.Copy(w, f)
				f.Close()
			} else {
				w.WriteHeader(404)
			}

		} else {
			streamPath := r.URL.Query().Get("streamPath")
			startTime := r.URL.Query().Get("startTime")
			endTime := r.URL.Query().Get("endTime")
			if len(streamPath)*len(startTime)*len(endTime) == 0 {
				log.Println("参数设置不全")
				return
			}
			log.Println(streamPath, startTime, endTime)
			//参数校验，时间最大跨度不能超过24H
			startUnix, _ := strconv.ParseInt(startTime, 10, 64)
			endUnix, _ := strconv.ParseInt(endTime, 10, 64)
			if endUnix-startUnix > 60*60*24 {
				//返回时间跨度不能超过24H
				log.Println("时间跨度不能超过24H")
				return
			}

			//1:遍历streamPath路径下所有的文件，筛选满足条件的先存储起来
			//tsDir := filepath.Join(config.Path, streamPath)

			//返回数据库存储的满足条件的m3u8
			users := GetTsSet()
			selectedUsers := GetTsSetWithCondition(users, startUnix, endUnix)

			allInfoMap := make([]*PlaylistInf, 0)
			for _, v := range selectedUsers {
				prefixPath := filepath.Join(streamPath, v.Title)
				currentInf := &PlaylistInf{
					Title:    strings.Replace(prefixPath, "\\", "/", -1),
					Duration: v.Duration,
				}
				allInfoMap = append(allInfoMap, currentInf)
			}

			/*
				for _, v := range selectedUsers {
					name := strings.TrimSuffix(v.Title, ".ts")
					ts_name_unix, _ := strconv.ParseInt(name, 10, 64)
					if ts_name_unix > startUnix && ts_name_unix < endUnix {
						meetConditionList = append(meetConditionList, filepath.Join(tsDir, v.Title))
					}
				}
				fmt.Println(len(meetConditionList))
			*/

			//2:满足条件的文件名称以及时长获取
			//问题一：如何不去获取之前m3u8,直接知道ts文件的时长
			// exec.Command("ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 ")

			//for _, v := range meetConditionList {
			/*
				cmd := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", v)
				output, err := cmd.Output()

				if err == nil {
					v2, _ := strconv.ParseFloat(compressStr(string(output)), 64)
					currentInf := &PlaylistInf{
						Title:    strings.Replace(strings.TrimPrefix(v, "resource\\"), "\\", "/", -1),
						Duration: v2,
					}
					allInfoMap = append(allInfoMap, currentInf)
				}
			*/
			//}
			//3:开始组装m3u8，返回给客户端即可，无需写入文件
			plHeader := Playlist{
				Version:        3,
				Sequence:       0,
				Targetduration: int(config.Fragment * 1000 / 666),
			}
			writeContent := fmt.Sprintf("#EXTM3U\n"+
				"#EXT-X-VERSION:%d\n"+
				"#EXT-X-PLAYLIST-TYPE:VOD\n"+
				"#EXT-X-TARGETDURATION:%d\n", plHeader.Version, plHeader.Targetduration)
			for _, v := range allInfoMap {
				ss := fmt.Sprintf("#EXTINF:%.3f,\n"+
					"%s\n", v.Duration, v.Title)
				writeContent += ss
			}
			writeContent += "#EXT-X-ENDLIST"
			w.Write([]byte(writeContent))
		}

	})

	//列举出当前录制的文件源
	//列举出当前录制的文件源
	http.HandleFunc("/vod/list", func(w http.ResponseWriter, r *http.Request) {
		//暂时无需采用sse的方式，请求的时候直接调用即可
		uidPath := r.URL.Query().Get("meetingId")
		concretePath := filepath.Join(config.Path, uidPath)
		allDirs, _ := GetAllDirectory(concretePath)

		if len(allDirs) == 0 {
			msg, _ := json.Marshal(JsonResult{Code: 400, Msg: "目录不存在"})
			w.Write(msg)
		} else {
			//返回Json数据
			data, _ := json.Marshal(JsonResult{Code: 200, Msg: "获取成功", AllDirName: allDirs})
			w.Write(data)
		}
	})
}

// HLS 发布者
type HLS struct {
	TS
	Video       M3u8Info
	Audio       M3u8Info
	TsHead      http.Header     `json:"-"` //用于提供cookie等特殊身份的http头
	SaveContext context.Context `json:"-"` //用来保存ts文件到服务器
}

// M3u8Info m3u8文件的信息，用于拉取m3u8文件，和提供查询
type M3u8Info struct {
	Req       *http.Request `json:"-"`
	M3U8Count int           //一共拉取的m3u8文件数量
	TSCount   int           //一共拉取的ts文件数量
	LastM3u8  string        //最后一个m3u8文件内容
	M3u8Info  []TSCost      //每一个ts文件的消耗
}

// TSCost ts文件拉取的消耗信息
type TSCost struct {
	DownloadCost int
	DecodeCost   int
	BufferLength int
}

func readM3U8(res *http.Response) (playlist *m3u8.Playlist, err error) {
	var reader io.Reader = res.Body
	if res.Header.Get("Content-Encoding") == "gzip" {
		reader, err = gzip.NewReader(reader)
	}
	if err == nil {
		playlist, err = m3u8.Read(reader)
	}
	if err != nil {
		log.Printf("readM3U8 error:%s", err.Error())
	}
	return
}
func (p *HLS) run(info *M3u8Info) {
	//请求失败自动退出
	req := info.Req.WithContext(p)
	client := http.Client{Timeout: time.Second * 5}
	sequence := -1
	lastTs := make(map[string]bool)
	resp, err := client.Do(req)
	defer func() {
		log.Printf("hls %s exit:%v", p.StreamPath, err)
		p.Close()
	}()
	errcount := 0
	for ; err == nil; resp, err = client.Do(req) {
		if playlist, err := readM3U8(resp); err == nil {
			errcount = 0
			info.LastM3u8 = playlist.String()
			//if !playlist.Live {
			//	log.Println(p.LastM3u8)
			//	return
			//}
			if playlist.Sequence <= sequence {
				log.Printf("same sequence:%d,max:%d", playlist.Sequence, sequence)
				time.Sleep(time.Second)
				continue
			}
			info.M3U8Count++
			sequence = playlist.Sequence
			thisTs := make(map[string]bool)
			tsItems := make([]*m3u8.SegmentItem, 0)
			discontinuity := false
			for _, item := range playlist.Items {
				switch v := item.(type) {
				case *m3u8.DiscontinuityItem:
					discontinuity = true
				case *m3u8.SegmentItem:
					thisTs[v.Segment] = true
					if _, ok := lastTs[v.Segment]; ok && !discontinuity {
						continue
					}
					tsItems = append(tsItems, v)
				}
			}
			lastTs = thisTs
			if len(tsItems) > 3 {
				tsItems = tsItems[len(tsItems)-3:]
			}
			info.M3u8Info = nil
			for _, v := range tsItems {
				tsCost := TSCost{}
				tsUrl, _ := info.Req.URL.Parse(v.Segment)
				tsReq, _ := http.NewRequestWithContext(p, "GET", tsUrl.String(), nil)
				tsReq.Header = p.TsHead
				t1 := time.Now()
				if tsRes, err := client.Do(tsReq); err == nil {
					info.TSCount++
					if body, err := ioutil.ReadAll(tsRes.Body); err == nil {
						tsCost.DownloadCost = int(time.Since(t1) / time.Millisecond)
						if p.SaveContext != nil && p.SaveContext.Err() == nil {
							os.MkdirAll(filepath.Join(config.Path, p.StreamPath), 0666)
							err = ioutil.WriteFile(filepath.Join(config.Path, p.StreamPath, filepath.Base(tsUrl.Path)), body, 0666)
						}
						t1 = time.Now()
						beginLen := len(p.TsPesPktChan)
						if err = p.Feed(bytes.NewReader(body)); err != nil {
							//close(p.TsPesPktChan)
						} else {
							tsCost.DecodeCost = int(time.Since(t1) / time.Millisecond)
							tsCost.BufferLength = len(p.TsPesPktChan)
							p.PesCount = tsCost.BufferLength - beginLen
						}
					} else if err != nil {
						log.Printf("%s readTs:%v", p.StreamPath, err)
					}
				} else if err != nil {
					log.Printf("%s reqTs:%v", p.StreamPath, err)
				}
				info.M3u8Info = append(info.M3u8Info, tsCost)
			}

			time.Sleep(time.Second * time.Duration(playlist.Target) * 2)
		} else {
			log.Printf("%s readM3u8:%v", p.StreamPath, err)
			errcount++
			if errcount > 10 {
				return
			}
			//return
		}
	}
}

func (p *HLS) Publish(streamName string) (result bool) {
	if result = p.TS.Publish(streamName); result {
		p.Type = "HLS"
		collection.Store(streamName, p)
		go func() {
			p.run(&p.Video)
			collection.Delete(streamName)
		}()
		if p.Audio.Req != nil {
			go p.run(&p.Audio)
		}
	}
	return
}
