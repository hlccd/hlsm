package main

import (
	"fmt"
	"github.com/hlccd/hlsm"
)

func main() {
	lsm := hlsm.NewHLsm("./db", 4*hlsm.KB, 16*hlsm.KB)
	//for i := 0; i < 100000; i++ {
	//	s := fmt.Sprintf("%d", i)
	//	lsm.Insert(s, "hlccd")
	//}
	//lsm.Insert("hlccd", "test")
	v, ok := lsm.Get("2")
	fmt.Println(v, ok)
}
