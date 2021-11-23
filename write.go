/*
 * @Author: your name
 * @Date: 2021-11-23 16:18:57
 * @LastEditTime: 2021-11-23 16:41:18
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \新建文件夹\write.go
 */
package hls

import (
	"bytes"
	"container/ring"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/Monibuca/engine/v3"
	"github.com/Monibuca/utils/v3"
	"github.com/Monibuca/utils/v3/codec/mpegts"
)

var memoryTs sync.Map

func writeHLS(r *Stream) {
	var err error
	var hls_fragment int64       // hls fragment
	var hls_segment_count uint32 // hls segment count
	var sliceNum int64 = config.SliceNum
	//限制数量最大为10，因为平均每一秒有一个IDR
	if sliceNum > 10 {
		sliceNum = 10
		config.SliceNum = 10
	}
	var vwrite_time uint32
	//var video_cc, audio_cc uint16
	var video_cc uint16
	var subs = Subscriber{ID: "HLSWriter", Type: "HLS"}

	// 初始化一个环形列表，长度为size+1
	var ring = ring.New(int(sliceNum) + 1)

	//将订阅者与stream进行关联，其实就是将订阅者写入到Stream之中的流数组中保存
	//订阅者内部有一个Subscribe函数，首先根据path找到对应的Stream，然后Stream内部又有一个Subscribe函数，才是真正将
	//订阅者这个对象添加到流内部的切片数组之中。此处也是让人容易混淆的地方
	if err = subs.Subscribe(r.StreamPath); err != nil {
		utils.Println(err)
		return
	}
	//此处逻辑：Subscriber订阅者订阅之后流之后，订阅者内部就保存了Stream的引用，就可以调用流的对应的方法
	//问题：h264与h265写入的区别需要仔细阅读源码
	//经过测试h264以及aac的没有任何问题，h265的形成的文件无法播放
	//订阅者去取订阅者所订阅的Stream中的对应codec编码的流
	//同一个Stream可以多种编码共存的，内部存储的时候有区分，主要是为了方便各种格式的转换
	vt := subs.WaitVideoTrack("h264", "h265")
	//7是h264  12是h265，根据codec id区分，之后进行解析

	if vt == nil {
		log.Println("get stream failed，request time out")
		return
	}
	codecID := vt.CodecID
	/*
		  at := outStream.WaitAudioTrack("aac")
		  if at == nil {
			  log.Println("get aac failed")
			  return
		  }
		  if err != nil {
			  return
		  }
		  var asc codec.AudioSpecificConfig
		  if at != nil {
			  asc, err = decodeAudioSpecificConfig(at.ExtraData)
		  }
		  if err != nil {
			  return
		  }
	*/
	//默认单位是毫秒
	if config.Fragment > 0 {
		hls_fragment = config.Fragment * 1000
	} else {
		hls_fragment = 10000
	}

	hls_playlist := Playlist{
		Version:  3,
		Sequence: 0,
		//	Targetduration: int(hls_fragment / 666), // hlsFragment * 1.5 / 1000
		Targetduration: int((hls_fragment / config.SliceNum) / 666), // hlsFragment * 1.5 / 1000
	}

	hls_vod_playlist := Playlist{
		Version:        3,
		Sequence:       0,
		Targetduration: int(hls_fragment / 666), // hlsFragment * 1.5 / 1000
		//Targetduration: int((hls_fragment / config.SliceNum) / 666), // hlsFragment * 1.5 / 1000
	}

	//直播m3u8文件初始化
	hls_path := filepath.Join(config.Path, r.StreamPath)
	hls_m3u8_name := hls_path + ".m3u8"
	log.Println("hls_m3u8_name = ", hls_m3u8_name)
	//如果文件夹存在，则不做任何事情
	os.MkdirAll(hls_path, 0755)
	if err = hls_playlist.Init(hls_m3u8_name); err != nil {
		log.Println(err)
		return
	}
	//录播m3u8文件初始化
	hls_m3u8_vod_name := hls_path + "_vod" + ".m3u8"
	if err = hls_vod_playlist.InitVodFile(hls_m3u8_vod_name); err != nil {
		log.Println(err)
		return
	}

	hls_segment_data := &bytes.Buffer{}

	var seekIndex int = 0
	var oldSeekIndex int = 0

	var currentIndex int64 = 1
	//统一字符串
	var fileNameFormat string
	//时间差
	var minusDuration float64

	subs.OnVideo = func(ts uint32, pack *VideoPack) {
		var packet mpegts.MpegTsPESPacket
		var err error
		if codecID == 7 {
			//H264 == AVC(Advanced Video Coding)
			packet, err = VideoPacketToPES(ts, pack.NALUs, vt.ExtraData.NALUs[0], vt.ExtraData.NALUs[1])
		} else if codecID == 12 {
			//H265 == HEVC (High Effiency Video Coding)
			packet, err = VideoPacketToPES(ts, pack.NALUs, vt.ExtraData.NALUs[0], vt.ExtraData.NALUs[1])
		} else {
			log.Println("have no fixed codec,please check your setting")
			return
		}

		if err != nil {
			return
		}
		/*
		  修改如下：TS文件还是按照设置的时间进行产生，但是返回给客户端的数据不再是文件返回的，而是在内存中产生的time-1.ts，time-2.ts
		  一直到time-5.ts，相当于把时间缩短了n份，同时在内存里面，加快了访问速度
		*/

		if pack.IDR {
			//判断当前所处的阶段，如果所处的阶段和当前下标相等，执行一次操作
			//scope := JugdeIntIndex(int64(ts-vwrite_time), hls_fragment/sliceNum)
			scope := JugdeIndex(int64(ts-vwrite_time), hls_fragment/sliceNum)
			//log.Println("scope = ", scope)
			if scope == currentIndex && scope < sliceNum {
				//时间差记录
				minusDuration = float64(ts-vwrite_time)/1000.0 - minusDuration
				if scope == 1 {
					fileNameFormat = strconv.FormatInt(time.Now().Unix(), 10)
				}

				//产生新的文件名称
				tsFilename := fileNameFormat + "_" + strconv.FormatInt(currentIndex, 10) + ".ts"
				tsFilePath := filepath.Join(hls_path, tsFilename)

				tsData := hls_segment_data.Bytes()
				//当前数据的长度就是其下标
				seekIndex = len(tsData)

				//内存模式
				if config.EnableMemory {
					ring.Value = tsFilePath
					//sliceElement := make([]byte, len(tsData[oldSeekIndex:seekIndex-1])) //使用copy函数必须复制切片的结构必须和源数据结构一致
					//copy(sliceElement, tsData[oldSeekIndex:])
					memoryTs.Store(tsFilePath, tsData[oldSeekIndex:seekIndex-1])
					//log.Println("store to ringbuffer ", tsFilePath, len(tsData[oldSeekIndex:seekIndex-1]))

					if ring = ring.Next(); ring.Value != nil && len(ring.Value.(string)) > 0 {
						memoryTs.Delete(ring.Value)
					}
				}

				inf := PlaylistInf{
					Duration: minusDuration,
					Title:    filepath.Base(hls_path) + "/" + tsFilename,
				}

				//如果窗口seg数量大于指定的数量了，滑动窗口
				//在不大于窗口之前，就是一直写入。等于window的时候，就该进行替换操作了
				if hls_segment_count >= uint32(config.Window) {
					if err = hls_playlist.UpdateInf(hls_m3u8_name, hls_m3u8_name+".tmp", inf); err != nil {
						return
					}
				} else {
					if err = hls_playlist.WriteInf(hls_m3u8_name, inf); err != nil {
						return
					}
				}
				currentIndex++
				hls_segment_count++
				oldSeekIndex = seekIndex
				minusDuration = float64(ts-vwrite_time) / 1000.0

			} else if scope >= sliceNum {
				minusDuration = float64(ts-vwrite_time)/1000.0 - minusDuration
				tsFilename := fileNameFormat + ".ts"
				tsPartFilename := fileNameFormat + "_" + strconv.FormatInt(sliceNum, 10) + ".ts"

				tsData := hls_segment_data.Bytes()
				//当前数据的长度就是其下标
				seekIndex = len(tsData)

				//整体写入文件
				tsFilePath := filepath.Join(hls_path, tsFilename)
				if config.EnableWrite {
					//更新VOD的m3u8文件
					vodInf := PlaylistInf{
						Duration: float64(ts-vwrite_time) / 1000.0,
						Title:    filepath.Base(hls_path) + "/" + tsFilename,
					}
					hls_vod_playlist.WriteVODInf(hls_m3u8_vod_name, hls_m3u8_vod_name+".tmp", vodInf)
					//将生成的记录写入数据库

					//初始化一条记录并且插入数据库
					currentTs := TsInfo{
						MeetingID: strings.Split(r.StreamPath, "/")[0],
						UserID:    strings.Split(r.StreamPath, "/")[1],
						Duration:  float64(ts-vwrite_time) / 1000.0,
						Title:     tsFilename,
						CreatedAt: time.Now(),
					}
					currentTs.Add()
					//将写入操作写入到协程里面，节省时间
					go func() {
						//data里面不包含PAT和PMT，写入的时候再次重新组装即可
						if err = writeHlsTsSegmentFile(tsFilePath, tsData); err != nil {
							log.Println("err occur", err.Error())
							return
						}
					}()
				}
				tsPartFilePath := filepath.Join(hls_path, tsPartFilename)

				//内存模式
				if config.EnableMemory {
					ring.Value = tsPartFilePath
					// sliceElement := make([]byte, len(tsData[oldSeekIndex:])) //使用copy函数必须复制切片的结构必须和源数据结构一致
					// copy(sliceElement, tsData[oldSeekIndex:])
					memoryTs.Store(tsPartFilePath, tsData[oldSeekIndex:])
					//log.Println("store to ringbuffer ", tsPartFilePath, len(tsData[oldSeekIndex:]))
					if ring = ring.Next(); ring.Value != nil && len(ring.Value.(string)) > 0 {
						memoryTs.Delete(ring.Value)
					}
				}

				inf := PlaylistInf{
					Duration: minusDuration,
					Title:    filepath.Base(hls_path) + "/" + tsPartFilename,
				}
				//如果窗口seg数量大于指定的数量了，滑动窗口
				//在不大于窗口之前，就是一直写入。等于window的时候，就该进行替换操作了
				if hls_segment_count >= uint32(config.Window) {
					if err = hls_playlist.UpdateInf(hls_m3u8_name, hls_m3u8_name+".tmp", inf); err != nil {
						return
					}
				} else {
					if err = hls_playlist.WriteInf(hls_m3u8_name, inf); err != nil {
						return
					}
				}

				hls_segment_count++
				vwrite_time = ts
				hls_segment_data.Reset()
				seekIndex = 0
				oldSeekIndex = 0
				currentIndex = 1
				fileNameFormat = ""
				minusDuration = 0.0
			}
		}
		frame := new(mpegts.MpegtsPESFrame)
		frame.Pid = 0x101
		frame.IsKeyFrame = pack.IDR
		frame.ContinuityCounter = byte(video_cc % 16)
		frame.ProgramClockReferenceBase = uint64(ts) * 90
		// 将pes转换为ts数据写入buffer
		if err = mpegts.WritePESPacket(hls_segment_data, frame, packet); err != nil {
			return
		}
		video_cc = uint16(frame.ContinuityCounter)
	}

	/*
		  outStream.OnAudio = func(ts uint32, pack *AudioPack) {
			  var packet mpegts.MpegTsPESPacket
			  if packet, err = AudioPacketToPES(ts, pack.Raw, asc); err != nil {
				  return
			  }

			  frame := new(mpegts.MpegtsPESFrame)
			  frame.Pid = 0x102
			  frame.IsKeyFrame = false
			  frame.ContinuityCounter = byte(audio_cc % 16)
			  //frame.ProgramClockReferenceBase = 0
			  if err = mpegts.WritePESPacket(hls_segment_data, frame, packet); err != nil {
				  return
			  }
			  audio_cc = uint16(frame.ContinuityCounter)
		  }
	*/
	//订阅者触发play动作
	subs.Play(nil, vt)

	//清空RingBuffer
	if config.EnableMemory {
		ring.Do(memoryTs.Delete)
	}
}

//判断当前所处的阶段
//这里需要注意，因为源代码中，IDR一秒产生一次，所以说500ms时间会有问题，呈现偶数增长
func JugdeIndex(timeDuration int64, everyDuration int64) int64 {

	num := float64(timeDuration) / float64(everyDuration)
	//log.Printf("timeDuration = %d, everyDuration=%d,num = %v", timeDuration, everyDuration, int64(num))
	return int64(num)

}

func JugdeIntIndex(timeDuration int64, everyDuration int64) int64 {

	num := timeDuration / everyDuration

	return num

}
