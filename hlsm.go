package hlsm

import (
	"github.com/hlccd/hlsm/cache"
	"github.com/hlccd/hlsm/ssTable"
	"os"
	"path"
	"sync"
)

const (
	B  = int64(1)
	KB = B * 1024
	MB = KB * 1024
	GB = MB * 1024
)

type HLsm struct {
	dir       string             // 数据目录
	capMin    int64              // 最小区块容量,也可以当作缓存容量
	capMax    int64              // 最大区块容量,超过后进行持久化存储,不再进行合并
	cacheFile *os.File           // 缓冲区的文件句柄
	cache     cache.Cache        // 缓存区
	tree      *ssTable.TableTree // 区块树,用于区块合并,缓存超过容量上限后会成为一个新区块
	sf        *singleFlight      // 单次请求
	//dur *durability.Durability
	sync.RWMutex
}

func NewHLsm(dir string, capMin, capMax int64) *HLsm {
	if dir == "" {
		dir = "."
	}
	lsm := &HLsm{
		dir:    dir,
		capMin: capMin,
		capMax: capMax,
		cache:  cache.NewLRU(capMin),
		tree:   ssTable.NewTableTree(dir, capMin, capMax),
		sf:     newSingleFlight(),
	}
	// 从磁盘中加载缓存内容和非顶级区块的key
	lsm.cacheFile = lsm.loadCache()
	lsm.loadSSTable()
	return lsm
}
func (lsm *HLsm) compaction() {
	lsm.tree.Insert(lsm.cache.ClearAndGainSorted(), 0)
	lsm.tree.Compaction(0)
	lsm.cacheFileReset()
}
func (lsm *HLsm) cacheFileReset() {
	err := lsm.cacheFile.Close()
	if err != nil {
		panic(err)
	}
	lsm.cacheFile = nil
	err = os.Remove(path.Join(lsm.dir, cacheName))
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(path.Join(lsm.dir, cacheName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	lsm.cacheFile = f
}
