package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"github.com/hlccd/hlsm/kv"
	"log"
	"os"
	"sort"
	"sync"
)

// SSTable 表，存储在磁盘文件中
type SSTable struct {
	// 文件句柄，要注意，操作系统的文件句柄是有限的
	f        *os.File
	filePath string
	// 元数据
	tableMetaInfo MetaInfo
	// 文件的稀疏索引列表
	sparseIndex map[string]Position
	// 排序后的 key 列表
	sortIndex []string
	// SSTable 只能使排他锁
	sync.Mutex
	/*
		sortIndex 是有序的，便于 CPU 缓存等，还可以使用布隆过滤器，有助于快速查找。
		sortIndex 找到后，使用 sparseIndex 快速定位
	*/
}

func NewSSTable(meta MetaInfo, positions map[string]Position, keys []string) *SSTable {
	return &SSTable{
		tableMetaInfo: meta,
		sparseIndex:   positions,
		sortIndex:     keys,
	}
}
func NewSSTableFormLoad(path string) *SSTable {
	// 以只读的形式打开文件
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		log.Println("打开文件失败: ", path)
		panic(err)
	}
	ss := &SSTable{
		filePath: path,
		f:        f,
	}

	// 加载文件句柄的同时，加载表的元数据
	ss.loadMetaInfo()
	ss.loadSparseIndex()
	return ss
}

// 加载 SSTable 文件的元数据，从 SSTable 磁盘文件中读取出 TableMetaInfo
func (ss *SSTable) loadMetaInfo() {
	f := ss.f
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Println("打开文件失败", ss.filePath)
		panic(err)
	}
	info, _ := f.Stat()
	_, err = f.Seek(info.Size()-8*5, 0)
	if err != nil {
		log.Println("读取元数据失败", ss.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &ss.tableMetaInfo.version)

	_, err = f.Seek(info.Size()-8*4, 0)
	if err != nil {
		log.Println("读取元数据失败", ss.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &ss.tableMetaInfo.dataStart)

	_, err = f.Seek(info.Size()-8*3, 0)
	if err != nil {
		log.Println("Error reading metadata ", ss.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &ss.tableMetaInfo.dataLen)

	_, err = f.Seek(info.Size()-8*2, 0)
	if err != nil {
		log.Println("读取元数据失败", ss.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &ss.tableMetaInfo.indexStart)

	_, err = f.Seek(info.Size()-8*1, 0)
	if err != nil {
		log.Println("读取元数据失败", ss.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &ss.tableMetaInfo.indexLen)
}

// 加载稀疏索引区到内存
func (ss *SSTable) loadSparseIndex() {
	// 加载稀疏索引区
	bytes := make([]byte, ss.tableMetaInfo.indexLen)
	if _, err := ss.f.Seek(ss.tableMetaInfo.indexStart, 0); err != nil {
		log.Println("打开文件失败", ss.filePath)
		panic(err)
	}
	if _, err := ss.f.Read(bytes); err != nil {
		log.Println("打开文件失败", ss.filePath)
		panic(err)
	}

	// 反序列化到内存
	ss.sparseIndex = make(map[string]Position)
	err := json.Unmarshal(bytes, &ss.sparseIndex)
	if err != nil {
		log.Println("打开文件失败", ss.filePath)
		panic(err)
	}
	_, _ = ss.f.Seek(0, 0)

	// 先排序
	keys := make([]string, 0, len(ss.sparseIndex))
	for k := range ss.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ss.sortIndex = keys
}

// Get 查找元素，
// 先使用二分查找法从内存中的 keys 列表查找 Key，如果存在，找到 Position ，再通过从数据区加载
func (ss *SSTable) Get(key string) (any, bool) {
	ss.Lock()
	defer ss.Unlock()

	// 元素定位
	var position = Position{
		Start: -1,
	}
	l := 0
	r := len(ss.sortIndex) - 1

	// 二分查找法，查找 key 是否存在
	for l <= r {
		mid := (l + r) / 2
		if ss.sortIndex[mid] == key {
			// 获取元素定位
			position = ss.sparseIndex[key]
			// 如果元素已被删除，则返回
			if position.Deleted {
				return nil, true
			}
			break
		} else if ss.sortIndex[mid] < key {
			l = mid + 1
		} else if ss.sortIndex[mid] > key {
			r = mid - 1
		}
	}

	if position.Start == -1 {
		return nil, false
	}

	// Todo：如果读取失败，需要增加错误处理过程
	// 从磁盘文件中查找
	bytes := make([]byte, position.Len)
	if _, err := ss.f.Seek(position.Start, 0); err != nil {
		log.Println(err)
		return nil, false
	}
	if _, err := ss.f.Read(bytes); err != nil {
		log.Println(err)
		return nil, false
	}

	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println(err)
		return nil, false
	}
	return value.Value, true
}
