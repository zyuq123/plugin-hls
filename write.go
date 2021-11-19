package hls

import (
	"bytes"
	"container/ring"
	"log"
	"os"
	"path/filepath"
	"strconv"
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
	var vwrite_time uint32
	var video_cc, audio_cc uint16
	var outStream = Subscriber{ID: "HLSWriter", Type: "HLS"}

	//目前限制数量最大为10
	var sliceNum int64 = config.SliceNum
	if sliceNum > 10 {
		sliceNum = 10
		config.SliceNum = 10
	}
	// 初始化一个环形列表，长度为size+1
	var ring = ring.New(int(sliceNum) + 1)

	//新增加变量
	//Buffer当前下标
	var seekIndex int = 0
	//Buffer上次下标
	var oldSeekIndex int = 0
	// 当前内存所处片段
	var currentIndex int64 = 1
	//文件名称时间戳
	var fileNameFormat string
	//时间差
	var minusDuration float64

	if err = outStream.Subscribe(r.StreamPath); err != nil {
		utils.Println(err)
		return
	}
	vt := outStream.WaitVideoTrack("h264")
	if vt == nil {
		log.Println("illegal format ")
		return
	}
	/*
		at := outStream.WaitAudioTrack("aac")
		var asc codec.AudioSpecificConfig
		if at != nil {
			asc, err = decodeAudioSpecificConfig(at.ExtraData)
		}
		if err != nil {
			return
		}
	*/
	if config.Fragment > 0 {
		hls_fragment = config.Fragment * 1000
	} else {
		hls_fragment = 10000
	}

	hls_playlist := Playlist{
		Version:  3,
		Sequence: 0,
		//Targetduration: int(hls_fragment / 666), // hlsFragment * 1.5 / 1000
		Targetduration: int((hls_fragment / config.SliceNum) / 666),
	}

	hls_path := filepath.Join(config.Path, r.StreamPath)
	hls_m3u8_name := hls_path + ".m3u8"
	os.MkdirAll(hls_path, 0755)
	if err = hls_playlist.Init(hls_m3u8_name); err != nil {
		log.Println(err)
		return
	}

	hls_segment_data := &bytes.Buffer{}
	outStream.OnVideo = func(ts uint32, pack *VideoPack) {
		packet, err := VideoPacketToPES(ts, pack.NALUs, vt.ExtraData.NALUs[0], vt.ExtraData.NALUs[1])
		if err != nil {
			return
		}
		if pack.IDR {
			//判断当前所处的阶段，如果所处的阶段和当前下标相等，等待即可，知道时间进入下一个阶段
			scope := JugdeIndex(int64(ts-vwrite_time), hls_fragment/sliceNum)
			log.Println("scope = ", scope)
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
					memoryTs.Store(tsFilePath, tsData[oldSeekIndex:seekIndex-1])

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
					memoryTs.Store(tsPartFilePath, tsData[oldSeekIndex:])
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
		if err = mpegts.WritePESPacket(hls_segment_data, frame, packet); err != nil {
			return
		}

		video_cc = uint16(frame.ContinuityCounter)
	}
	//音频按照视频类似的方式直接处理即可
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
	outStream.Play(nil, vt)
	if config.EnableMemory {
		ring.Do(memoryTs.Delete)
	}
}

func JugdeIndex(timeDuration int64, everyDuration int64) int64 {

	num := float64(timeDuration) / float64(everyDuration)
	//log.Printf("timeDuration = %d, everyDuration=%d,num = %v", timeDuration, everyDuration, int64(num))
	return int64(num)

}
