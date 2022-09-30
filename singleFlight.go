package hlsm

import "sync"

//呼叫请求结构体
type call struct {
	sync.WaitGroup       //可重入锁
	val            any   //请求结果
	err            error //错误反馈
}

type singleFlight struct {
	m          map[any]*call //一类请求与同一类呼叫的映射表
	sync.Mutex               //并发控制锁,保证线程安全
}

func newSingleFlight() *singleFlight {
	return &singleFlight{
		m: make(map[any]*call),
	}
}

func (sf *singleFlight) Do(key any, fn func() (any, error)) (v any, err error) {
	sf.Lock()
	if sf.m == nil {
		sf.m = make(map[any]*call)
	}
	//判断以key为关键词的该类请求是否存在
	if c, ok := sf.m[key]; ok {
		sf.Unlock()
		// 如果请求正在进行中，则等待
		c.Wait()
		return c.val, c.err
	}
	//该类请求不存在,创建个请求
	c := new(call)
	// 发起请求前加锁,并将请求添加到请求组内以表示该类请求正在处理
	c.Add(1)
	sf.m[key] = c
	sf.Unlock()
	//调用请求函数获取内容
	c.val, c.err = fn()
	//请求结束
	c.Done()
	sf.Lock()
	//从请求组中删除该呼叫请求
	delete(sf.m, key)
	sf.Unlock()
	return c.val, c.err
}

func (sf *singleFlight) DoChan(key any, fn func() (any, error)) (ch chan any) {
	ch = make(chan any, 1)
	sf.Lock()
	if sf.m == nil {
		sf.m = make(map[any]*call)
	}
	if _, ok := sf.m[key]; ok {
		sf.Unlock()
		return ch
	}
	c := new(call)
	c.Add(1)      // 发起请求前加锁
	sf.m[key] = c // 添加到 g.m，表明 key 已经有对应的请求在处理
	sf.Unlock()
	go func() {
		c.val, c.err = fn() // 调用 fn，发起请求
		c.Done()            // 请求结束
		sf.Lock()
		delete(sf.m, key) // 更新 g.m
		ch <- c.val
		sf.Unlock()
	}()
	return ch
}
func (sf *singleFlight) ForgetUnshared(key any) {
	sf.Lock()
	_, ok := sf.m[key]
	if ok {
		delete(sf.m, key)
	}
	sf.Unlock()
}
