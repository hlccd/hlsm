package hlsm

import (
	"encoding/binary"
	"github.com/hlccd/hlsm/kv"
	"log"
)

func (lsm *HLsm) Log(key string, value any, deleted bool) {
	// 长度
	data, _ := kv.NewValue(key, value, deleted).Encode()
	err := binary.Write(lsm.cacheFile, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Println("插入kv数据时候写入长度失败")
		panic(err)
	}
	// 实际数据
	err = binary.Write(lsm.cacheFile, binary.LittleEndian, data)
	if err != nil {
		log.Println("插入kv数据时候写入数据失败")
		panic(err)
	}
}
