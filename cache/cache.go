package cache

import (
	"fmt"
	"github.com/hlccd/hlsm/kv"
)

type Cache interface {
	Size() int64
	Insert(key string, value any) bool
	Erase(key string) bool
	Put(values []*kv.Value)
	Get(key string) (value any, ok bool)
	ClearAndGainSorted() []*kv.Value
}

func size(e any) int64 {
	return int64(len(fmt.Sprintf("%v", e)))
}
