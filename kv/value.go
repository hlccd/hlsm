package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

const (
	indexBits = 8
)

type Value struct {
	Key     string
	Value   any
	Deleted bool
}

func NewValue(key string, value any, delete bool) *Value {
	return &Value{
		Key:     key,
		Value:   value,
		Deleted: delete,
	}
}

// Decode 二进制数据反序列化为 Value
func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}

// Encode 将 Value 序列化为二进制
func (v Value) Encode() ([]byte, error) {
	return json.Marshal(v)
}

func GetValue(data []byte, size int64) []*Value {
	values := make([]*Value, 0, 0)
	dataLen := int64(0) // 元素的字节数量
	index := int64(0)   // 当前索引
	for index < size {
		// 前面的 8 个字节表示元素的长度
		indexData := data[index:(index + indexBits)]
		// 获取元素的字节长度
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			panic(err)
		}
		// 将元素的所有字节读取出来，并还原为 kv.Value
		index += indexBits
		dataArea := data[index:(index + dataLen)]
		var value *Value
		err = json.Unmarshal(dataArea, &value)
		if err != nil {
			panic(err)
		}
		// 读取下一个元素
		index = index + dataLen
		values = append(values, value)
	}
	return values
}
