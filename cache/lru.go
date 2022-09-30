package cache

import (
	"container/list"
	"github.com/hlccd/hlsm/kv"
	"sort"
	"sync"
)

type lru struct {
	len          int64                    // 当前容量
	cap          int64                    // 容量上限,增删时会判断是否到达容量上限,到达后会增删失败,此时应当将缓存设为区块进行持久化
	ll           *list.List               // 用于存储的链表
	cache        map[string]*list.Element // 链表元素与key的映射表
	sync.RWMutex                          // 并发控制锁
}

func NewLRU(cap int64) *lru {
	return &lru{
		len:   0,
		cap:   cap,
		ll:    list.New(),
		cache: make(map[string]*list.Element),
	}
}
func (l *lru) Size() int64 {
	if l == nil {
		return 0
	}
	l.RLock()
	defer l.RUnlock()
	return l.len
}

// Insert 将对应的key进行插入,若已有进行替换即可
func (l *lru) Insert(key string, value any) bool {
	if l == nil {
		return false
	}
	l.Lock()
	defer l.Unlock()
	return l.insert(key, value, false)
}

// Erase 将对应的key标记为删除,若不存在则新建
func (l *lru) Erase(key string) bool {
	if l == nil {
		return false
	}
	l.Lock()
	defer l.Unlock()
	return l.insert(key, nil, true)
}

// 向缓存中插入数据
func (l *lru) insert(key string, value any, deleted bool) bool {
	if l == nil {
		return false
	}
	//利用map从已存的元素中寻找
	if ele, ok := l.cache[key]; ok {
		//该key已存在,直接替换即可
		l.ll.MoveToFront(ele)
		v := ele.Value.(*kv.Value)
		v.Deleted = deleted
		//此处是一个替换,即将cache中的value替换为新的value,同时根据实际存储量修改其当前存储的实际大小
		if l.cap >= l.len+size(value)-size(v.Value) {
			// 仍有空间进行插入
			l.len += size(value) - size(v.Value)
			v.Value = value
			return true
		}
	} else {
		//此处是一个增加操作,即原本不存在,所以直接插入即可,同时在当前数值范围内增加对应的占用空间
		if l.cap >= l.len+size(key)+size(value) {
			// 仍有空间进行插入
			l.len += size(key) + size(value)
			//该key不存在,需要进行插入
			l.cache[key] = l.ll.PushFront(kv.NewValue(key, value, deleted))
			return true
		}
	}
	return false
}
func (l *lru) Put(values []*kv.Value) {
	if l == nil {
		return
	}
	for _, v := range values {
		l.insert(v.Key, v.Value, v.Deleted)
	}
}
func (l *lru) Get(key string) (value any, ok bool) {
	if l == nil {
		return nil, false
	}
	l.RLock()
	defer l.RUnlock()
	if ele, ok := l.cache[key]; ok {
		//找到了value,将其移到链表首部
		l.ll.MoveToFront(ele)
		return ele.Value.(*kv.Value).Value, true
	}
	return nil, false
}

func (l *lru) ClearAndGainSorted() []*kv.Value {
	if l == nil {
		return nil
	}
	l.Lock()
	defer l.Unlock()
	ss := make([]string, 0, len(l.cache))
	values := make([]*kv.Value, 0, len(l.cache))
	for k, _ := range l.cache {
		ss = append(ss, k)
	}
	sort.Strings(ss)
	for _, k := range ss {
		if e, ok := l.cache[k]; ok {
			values = append(values, e.Value.(*kv.Value))
			l.ll.Remove(e)
			delete(l.cache, k)
		}
	}
	l.len = 0
	return values
}
