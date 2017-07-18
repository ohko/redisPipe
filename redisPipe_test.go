package redisPipe

import (
	"fmt"
	"sync"
	"testing"

	"runtime"

	"time"

	"github.com/garyburd/redigo/redis"
)

var wg sync.WaitGroup

// go test -run TestNewRedisPipe
func Test1(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	pipe := NewRedisPipe("hk@127.0.0.1:6379", 10000000)

	{ // INFO
		t1 := time.Now()
		a, ae := pipe.Do("INFO")
		if _, e := redis.String(a, ae); e != nil {
			// t.Fatal(e, a, ae)
		}
		fmt.Println("INFO:", time.Now().Sub(t1).Seconds()*1000, "ms")
	}

	{ // SET/GET
		t1 := time.Now()
		pipe.Send("SET", "a0", 0)
		pipe.Send("SET", "a1", 1)
		a, ae := pipe.Do("GET", "a1")
		if i, e := redis.Int(a, ae); e != nil || i != 1 {
			t.Fatal(a, ae)
		}
		fmt.Println("SET/GET:", time.Now().Sub(t1).Seconds()*1000, "ms")
	}

	{ // MGET
		t1 := time.Now()
		pipe.Send("SET", "a2", 2)
		pipe.Send("SET", "a3", 3)
		a, ae := pipe.Do("MGET", redis.Args{}.Add("a2").Add("a3")...)
		if i, e := redis.Ints(a, ae); e != nil || i[0] != 2 || i[1] != 3 {
			t.Fatal(a, ae)
		}
		fmt.Println("SET/MGET:", time.Now().Sub(t1).Seconds()*1000, "ms")
	}

	{ // SCAN
		// t1 := time.Now()
		// var ks []string
		// pipe.Send("SET", "keys/a2", 2)
		// pipe.Send("SET", "keys/a3", 3)
		// next := "0"
		// for {
		// 	a, ae := pipe.Do("SCAN", next, "MATCH", "keys/*", "COUNT", "1000000000000000000")
		// 	pipe.Wait()
		// 	if ae != nil {
		// 		t.Fatal(a, ae)
		// 	}
		// 	next, _ = redis.String(a.([]interface{})[0], ae)
		// 	_v, _ := redis.Strings(a.([]interface{})[1], ae)
		// 	ks = append(ks, _v[:]...)
		// 	if next == "0" {
		// 		break
		// 	}
		// }
		// if len(ks) != 2 {
		// 	t.Fatal(ks)
		// }
		// fmt.Println("SET/SCAN:", time.Now().Sub(t1).Seconds()*1000, "ms")
	}

	{ // KEYS
		// t1 := time.Now()
		// pipe.Send("SET", "keys/a2", 2)
		// pipe.Send("SET", "keys/a3", 3)
		// a, ae := pipe.Do("KEYS", "keys/*")
		// if i, e := redis.Strings(a, ae); e != nil || len(i) != 2 {
		// 	t.Fatal(a, ae)
		// }
		// fmt.Println("SET/KEYS:", time.Now().Sub(t1).Seconds()*1000, "ms")
	}

	{ /// 多协程测试 1
		t1 := time.Now()
		c1 := pipe.runCount
		for i := 1; i <= 100; i++ {
			wg.Add(1)
			go send(pipe, i, 100, t)
		}
		wg.Wait()
		fmt.Println("go runtinue1 time:", time.Now().Sub(t1).Seconds()*1000, "ms")
		fmt.Println("go runtinue1 count:", pipe.runCount-c1)
	}

	{ /// 多协程测试 2
		t1 := time.Now()
		c1 := pipe.runCount
		for i := 1; i <= 100; i++ {
			wg.Add(1)
			go send2(pipe, i, 100, t)
		}
		wg.Wait()
		fmt.Println("go runtinue2 time:", time.Now().Sub(t1).Seconds()*1000, "ms")
		fmt.Println("go runtinue2 count:", pipe.runCount-c1)
	}

	fmt.Println("runCount:", pipe.runCount)
}

func send(pipe *RedisPipe, j, size int, t *testing.T) {
	r := make([]interface{}, size)
	e := make([]error, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("k-%v-%v", j, i)
		val := fmt.Sprintf("v-%v-%v", j, i)
		pipe.Send("SET", key, val)
	}
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("k-%v-%v", j, i)
		r[i], e[i] = pipe.Do("GET", key)
	}
	for i := 0; i < size; i++ {
		val := fmt.Sprintf("v-%v-%v", j, i)
		_r, _e := redis.String(r[i], e[i])
		if _e != nil || _r != val {
			t.Error(i, _r, _e)
		}
	}
	// fmt.Println("send over.")
	wg.Done()
}

func send2(pipe *RedisPipe, j, size int, t *testing.T) {
	r := make([]interface{}, size)
	e := make([]error, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("k-%v-%v", j, i)
		val := fmt.Sprintf("v-%v-%v", j, i)
		pipe.Send("SET", key, val)
	}
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("k-%v-%v", j, i)
		pipe.Do2(&r[i], &e[i], "GET", key)
	}
	pipe.Wait()
	for i := 0; i < size; i++ {
		val := fmt.Sprintf("v-%v-%v", j, i)
		_r, _e := redis.String(r[i], e[i])
		if _e != nil || _r != val {
			t.Error(i, _r, _e)
		}
	}
	// fmt.Println("send over.")
	wg.Done()
}
