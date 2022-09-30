package ssTable

import (
	"encoding/json"
	"fmt"
	"github.com/hlccd/hlsm/kv"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	capDisparity = 4
	partSize     = 10
	dbSuffix     = "db"
	topBlockPre  = "hlsm"
)

type TableTree struct {
	dir          string
	cap          int64
	levelSize    int
	levelMaxSize []int
	levels       []*Table
	topBlockNum  int
	sync.RWMutex
}

func NewTableTree(dir string, capMin, capMax int64) *TableTree {
	levelSize := 0
	for c := capMin; c <= capMax; c *= capDisparity {
		levelSize++
	}
	levelMaxSize := make([]int, levelSize)
	levelMaxSize[0] = 10
	for i := 1; i < levelSize; i++ {
		levelMaxSize[i] = levelMaxSize[i-1] * 10
	}
	return &TableTree{
		dir:          dir,
		cap:          capMin,
		levelSize:    levelSize,
		levelMaxSize: levelMaxSize,
		levels:       make([]*Table, levelSize),
		topBlockNum:  0,
	}
}

func (tree *TableTree) LoadDB(name string) {
	if strings.HasSuffix(name, dbSuffix) {
		if strings.HasPrefix(name, topBlockPre) {
			// 属于顶级区块,不载入内存
			tree.topBlockNum++
		} else {
			// 属于 db 文件且并非顶级区块
			tree.LoadDbFile(path.Join(tree.dir, name))
		}
	}
}

// LoadDbFile 加载一个 db 文件到 TableTree 中
func (tree *TableTree) LoadDbFile(path string) {
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("加载数据库文件", path, ",耗时: ", elapse)
	}()

	level, index, err := getLevel(filepath.Base(path))
	if err != nil {
		return
	}
	table := NewSSTableFormLoad(path)
	newNode := NewTable(index, table)

	currentNode := tree.levels[level]
	// 该层不存在节点
	if currentNode == nil {
		tree.levels[level] = newNode
		return
	}
	// 该节点应当置于首位
	if newNode.index < currentNode.index {
		newNode.next = currentNode
		tree.levels[level] = newNode
		return
	}

	// 将 SSTable 插入到合适的位置
	for currentNode != nil {
		if currentNode.next == nil || newNode.index < currentNode.next.index {
			newNode.next = currentNode.next
			currentNode.next = newNode
			break
		}
		currentNode = currentNode.next
	}
}

func (tree *TableTree) Get(key string) (any, bool) {
	tree.RLock()
	defer tree.RUnlock()

	// 遍历每一层的 SSTable
	for _, node := range tree.levels {
		// 整理 SSTable 列表
		tables := make([]*SSTable, 0)
		for node != nil {
			tables = append(tables, node.table)
			node = node.next
		}
		// 查找的时候要从最后一个 SSTable 开始查找
		for i := len(tables) - 1; i >= 0; i-- {
			if value, ok := tables[i].Get(key); ok {
				return value, true
			}
		}
	}
	return nil, false
}

func (tree *TableTree) GetFromStorage(key string) (any, bool) {
	tree.RLock()
	num := tree.topBlockNum
	dir := tree.dir
	tree.RUnlock()
	for index := num; index > 0; index-- {
		log.Printf("正在从顶级区块 %d 中查找", index)
		p := dir + "/" + topBlockPre + "." + strconv.Itoa(index) + "." + dbSuffix
		table := NewSSTableFormLoad(p)
		if value, ok := table.Get(key); ok {
			return value, true
		}
	}
	return nil, false
}

// Insert 创建新的 SSTable，插入到合适的层
func (tree *TableTree) Insert(values []*kv.Value, level int) *SSTable {
	// 生成数据区
	keys := make([]string, 0, len(values))
	positions := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, value := range values {
		data, err := value.Encode()
		if err != nil {
			log.Println("key 插入失败:", value.Key, err)
			continue
		}
		keys = append(keys, value.Key)
		// 文件定位记录
		positions[value.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: value.Deleted,
		}
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	// 生成稀疏索引区
	// map[string]Position to json
	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal("ssTable 文件创建失败,", err)
	}

	// 生成 MetaInfo
	meta := NewMetaInfo(int64(len(dataArea)), int64(len(dataArea)), int64(len(indexArea)))
	ss := NewSSTable(meta, positions, keys)

	index := tree.insert(ss, level)

	log.Printf("创建了一个新区块,level: %d ,index: %d\r\n", level, index)
	ss.filePath = tree.dir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + "." + dbSuffix

	// 持久化保存
	writeDataToFile(ss.filePath, dataArea, indexArea, meta)
	// 以只读的形式打开文件
	ss.f, err = os.OpenFile(ss.filePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Println("打开文件失败", ss.filePath)
		panic(err)
	}

	return ss
}

// 插入一个 SSTable 到指定层
func (tree *TableTree) insert(table *SSTable, level int) (index int) {
	tree.Lock()
	defer tree.Unlock()

	// 每次插入的，都出现在最后面
	node := tree.levels[level]
	newNode := &Table{
		table: table,
		next:  nil,
		index: 0,
	}

	if node == nil {
		tree.levels[level] = newNode
	} else {
		for node != nil {
			if node.next == nil {
				newNode.index = node.index + 1
				node.next = newNode
				break
			} else {
				node = node.next
			}
		}
	}
	return newNode.index
}

// Storage 生成顶级区块,持久化保存
func (tree *TableTree) Storage(values []*kv.Value) {
	// 生成数据区
	keys := make([]string, 0, len(values))
	positions := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, value := range values {
		data, err := value.Encode()
		if err != nil {
			log.Println("key 插入失败:", value.Key, err)
			continue
		}
		keys = append(keys, value.Key)
		// 文件定位记录
		positions[value.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: value.Deleted,
		}
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	// 生成稀疏索引区
	// map[string]Position to json
	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal("顶级 ssTable 文件创建失败,", err)
	}

	// 生成 MetaInfo
	meta := NewMetaInfo(int64(len(dataArea)), int64(len(dataArea)), int64(len(indexArea)))
	ss := NewSSTable(meta, positions, keys)

	log.Printf("创建了一个顶级区块: %d\n")
	tree.topBlockNum++
	ss.filePath = tree.dir + "/" + topBlockPre + "." + strconv.Itoa(tree.topBlockNum) + "." + dbSuffix
	// 持久化保存
	writeDataToFile(ss.filePath, dataArea, indexArea, meta)
}

// 获取该层有多少个 SSTable
func (tree *TableTree) getCount(level int) int {
	node := tree.levels[level]
	count := 0
	for node != nil {
		count++
		node = node.next
	}
	return count
}

// 获取一个 db 文件所代表的 SSTable 的所在层数和索引
func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d."+dbSuffix, &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("解析 db 文件出现错误: %q", name)
	}
	return level, index, nil
}
