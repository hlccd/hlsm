package ssTable

import (
	"github.com/hlccd/hlsm/cache"
	"github.com/hlccd/hlsm/kv"
	"log"
	"os"
	"time"
)

// Compaction 检查是否需要压缩 SSTable
func (tree *TableTree) Compaction(level int) {
	if level >= tree.levelSize {
		// 超过上限,结束
		return
	}
	tableSize := int(tree.GetLevelSize(level) / 1024 / 1024) // 转为 MB
	// 当前层 SSTable 数量是否已经到达阈值
	// 当前层的 SSTable 总大小已经到底阈值
	if tree.getCount(level) < partSize && tableSize < tree.levelMaxSize[level] {
		return
	}

	// 压完本层后继续压下一层
	defer tree.Compaction(level + 1)
	log.Println("正在压实第", level, "层的内容")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Printf("压实第%d层耗时:%d\n", level, elapse)
	}()
	// 用于加载 一个 SSTable 的数据区到缓存中
	tableCache := make([]byte, tree.levelMaxSize[level])
	currentNode := tree.levels[level]

	// 将当前层的 SSTable 合并到缓存中
	c := cache.NewLRU(tree.cap)

	tree.Lock()
	// 遍历该层所有区块,从硬盘中读取所有信息进行构建有序集合
	for currentNode != nil {
		table := currentNode.table
		// 将 SSTable 的数据区加载到 tableCache 内存中
		if int64(len(tableCache)) < table.tableMetaInfo.dataLen {
			tableCache = make([]byte, table.tableMetaInfo.dataLen)
		}
		newSlice := tableCache[0:table.tableMetaInfo.dataLen]
		// 读取 SSTable 的数据区
		if _, err := table.f.Seek(0, 0); err != nil {
			log.Println("打开 db 文件失败", table.filePath)
			panic(err)
		}
		if _, err := table.f.Read(newSlice); err != nil {
			log.Println("读取 db 文件失败", table.filePath)
			panic(err)
		}
		// 读取每一个元素
		for k, position := range table.sparseIndex {
			if position.Deleted == false {
				value, err := kv.Decode(newSlice[position.Start:(position.Start + position.Len)])
				if err != nil {
					log.Fatal(err)
				}
				c.Insert(k, value.Value)
			} else {
				c.Erase(k)
			}
		}
		currentNode = currentNode.next
	}
	tree.Unlock()

	// 将 SortTree 压缩合并成一个 SSTable
	values := c.ClearAndGainSorted()

	if level+1 >= tree.levelSize {
		// 超过层级上限,应当设为顶级区块
		tree.Storage(values)
	} else {
		// 创建新的 SSTable
		tree.Insert(values, level+1)
	}
	// 清理并重置该层文件
	tree.clearLevel(tree.levels[level])
	tree.levels[level] = nil
}

func (tree *TableTree) clearLevel(oldNode *Table) {
	tree.Lock()
	defer tree.Unlock()
	// 清理当前层的每个的 SSTable
	for oldNode != nil {
		err := oldNode.table.f.Close()
		if err != nil {
			log.Println("关闭文件失败:", oldNode.table.filePath)
			panic(err)
		}
		err = os.Remove(oldNode.table.filePath)
		if err != nil {
			log.Println("删除文件失败:", oldNode.table.filePath)
			panic(err)
		}
		oldNode.table.f = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
