package redisPipe

import (
	"fmt"
	"sync"
	"testing"
)

var wg sync.WaitGroup

// go test -run TestNewRedisPipe
func TestNewRedisPipe(t *testing.T) {
	// pipe := NewRedisPipe("dspRedis123@175.6.1.222:6379", 1000000)
	pipe := NewRedisPipe("hk@127.0.0.1:6379", 10000000)

	/// 单个测试
	// 声明接受变量
	v1 := &RtnStruct{Type: "string"}
	v2 := &RtnStruct{Type: "int"}
	v3 := &RtnStruct{Type: "bool"}
	// 助手对象
	pipeHelper := NewRedisPipeHelper()
	/// 添加指令
	// 字符串
	pipeHelper.Add(nil, "SET", "keyString", "abc")
	pipeHelper.Add(v1, "GET", "keyString")
	// 数字
	pipeHelper.Add(nil, "SET", "keyInt", 123)
	pipeHelper.Add(v2, "GET", "keyInt")
	// Bool
	pipeHelper.Add(nil, "SET", "keyBool", true)
	pipeHelper.Add(v3, "GET", "keyBool")
	// 发送指令
	pipe.Send(pipeHelper.Make())
	// 等待完成
	pipeHelper.Wait()
	// 返回值校验
	if v1.String != "abc" {
		t.Error("abc", v1.String)
	}
	if v2.Int != 123 {
		t.Error(123, v2.Int)
	}
	if v3.Bool != true {
		t.Error(true, v3.Bool)
	}

	/// 批量测试
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go send(pipe, 1000, t)
	}

	wg.Wait()
}

func send(pipe *RedisPipe, size int, t *testing.T) {
	pipeHelper := NewRedisPipeHelper()
	res := make([]*RtnStruct, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("k-%v-%v", size, i)
		val := fmt.Sprintf("v-%v-%v", size, i)
		pipeHelper.Add(nil, "SET", key, val)
	}
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("k-%v-%v", size, i)
		res[i] = &RtnStruct{Type: "string"}
		pipeHelper.Add(res[i], "GET", key)
	}
	pipe.Send(pipeHelper.Make())
	pipeHelper.Wait()
	for k, v := range res {
		if v.String != fmt.Sprintf("v-%v-%v", size, k) {
			t.Error(fmt.Sprintf("v-%v-%v", size, k), v.String)
		}
	}
	wg.Done()
}
