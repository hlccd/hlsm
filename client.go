package hlsm

import (
	"errors"
	"log"
)

func (lsm *HLsm) Insert(key string, value any) bool {
	lsm.Lock()
	defer lsm.Unlock()
	lsm.Log(key, value, false)
	if lsm.cache.Insert(key, value) {
		return true
	}
	// 区块压缩
	lsm.compaction()
	return lsm.cache.Insert(key, value)
}
func (lsm *HLsm) Erase(key string) bool {
	lsm.Lock()
	defer lsm.Unlock()
	lsm.Log(key, nil, true)
	if lsm.cache.Erase(key) {
		return true
	}
	// 区块压缩
	lsm.compaction()
	return lsm.cache.Erase(key)
}
func (lsm *HLsm) Get(key string) (any, bool) {
	if val, ok := lsm.cache.Get(key); ok {
		log.Println("命中缓存")
		return val, true
	}
	v, err := lsm.sf.Do(key, func() (any, error) {
		// 从 level 树中查找
		if val, ok := lsm.tree.Get(key); ok {
			log.Println("命中 level 树")
			return val, nil
		}

		// 从为载入内存的顶级区块中查找
		if val, ok := lsm.tree.GetFromStorage(key); ok {
			log.Println("命中顶级区块")
			return val, nil
		}
		return nil, errors.New("未能找到")
	})
	if err != nil {
		lsm.Erase(key)
		return nil, false
	}
	lsm.Insert(key, v)
	return v, true
}
