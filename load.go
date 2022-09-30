package hlsm

import (
	"github.com/hlccd/hlsm/kv"
	"io/ioutil"
	"log"
	"os"
	"path"
)

const (
	cacheName = "cache.hlsm"
)

func (lsm *HLsm) loadCache() *os.File {
	file, err := os.OpenFile(path.Join(lsm.dir, cacheName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("缓存文件创建失败")
		panic(err)
	}
	info, _ := os.Stat(path.Join(lsm.dir, cacheName))
	if info.Size() == 0 {
		return file
	}
	// 将文件内容全部读取到内存
	data := make([]byte, info.Size())
	_, err = file.Seek(0, 0)
	if err != nil {
		log.Println("无法回到 cache 文件起始位置")
		panic(err)
	}
	_, err = file.Read(data)
	if err != nil {
		log.Println("无法打开缓冲文件")
		panic(err)
	}
	lsm.cache.Put(kv.GetValue(data, info.Size()))
	return file
}
func (lsm *HLsm) loadSSTable() {
	infos, err := ioutil.ReadDir(lsm.dir)
	if err != nil {
		log.Println("读取数据库文件失败")
		panic(err)
	}
	for _, info := range infos {
		// 如果是 SSTable 文件
		lsm.tree.LoadDB(info.Name())
	}
}
