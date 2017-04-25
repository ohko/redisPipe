package redisPipe

import (
	"log"
	"runtime"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisPipe Redis批量指令
type RedisPipe struct {
	pool    *redis.Pool     // redis 连接池
	send    chan *sendQueue // 发送channel
	bufSize int             // 发送channel缓冲大小
}

// 指令结构
type sendQueue struct {
	ch   chan []*RecvData
	cmds []*SendCmd
}

// SendCmd ...
type SendCmd struct {
	cmd  string
	args redis.Args
}

// RecvData ...
type RecvData struct {
	reply interface{}
	err   *error
}

// NewRedisPipe ...
func NewRedisPipe(rawurl string, bufSize int) *RedisPipe {
	o := &RedisPipe{}
	o.bufSize = bufSize
	o.send = make(chan *sendQueue, bufSize)
	o.pool = &redis.Pool{
		Wait:      true, // 连接池满了，等待其它使用者归还
		MaxIdle:   50,   // 最大空闲连接
		MaxActive: 50,   // 最大连接数
		// IdleTimeout: 3 * time.Second,
		// 连接方法
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://x:" + rawurl)
			if err != nil {
				log.Println("redis链接错误:", err)
				c.Close()
				return nil, err
			}
			return c, err
		},
		// 保持连接方法
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			if err != nil {
				log.Println("redis ping错误:", err)
			}
			return err
		},
	}
	go o.do()
	return o
}

// GetPool 获取pool
func (o *RedisPipe) GetPool() *redis.Pool {
	return o.pool
}

// Send 发送指令
func (o *RedisPipe) Send(ch chan []*RecvData, cmds []*SendCmd) {
	db := o.pool.Get()
	defer db.Close()

	o.send <- &sendQueue{ch: ch, cmds: cmds}
}

func (o *RedisPipe) do() {
	var lastDoTime time.Time                  // 最后检查时间
	var sendPackCount, cmdCount int           // 发送指令数量
	recvCache := make([]*RecvData, o.bufSize) // 接受指令缓冲
	var chIndex int                           // 返回chan的数量
	chMap := make([]struct {                  // 存储返回chan与数量的关系
		ch    chan []*RecvData
		count int
	}, o.bufSize)

	db := o.pool.Get()
	defer db.Close()
	for {
		runtime.Gosched()
		now := time.Now()
		// 间隔时间
		_sub := now.Sub(lastDoTime)
		if _sub < 2000000 {
			time.Sleep(2000000 - _sub)
			continue
		}
		lastDoTime = now

		// 没指令
		sendPackCount = len(o.send)
		if sendPackCount == 0 {
			continue
		}

		// 重制计数
		chIndex = 0
		cmdCount = 0

		// 压入指令
		// log.Println("压入指令")
		for i := 0; i < sendPackCount; i++ {
			s := <-o.send
			if chMap[chIndex].ch == nil {
				chMap[chIndex].ch = s.ch
			} else if chMap[chIndex].ch != s.ch {
				chIndex++
				chMap[chIndex].ch = s.ch
			}
			for _, v := range s.cmds {
				db.Send(v.cmd, v.args...)
				chMap[chIndex].count++
				cmdCount++
			}
		}

		// 执行指令
		// log.Println("执行指令")
		db.Flush()

		// 接受数据
		// log.Println("接受数据")
		for i := 0; i < cmdCount; i++ {
			r, e := db.Receive()
			recvCache[i] = &RecvData{r, &e}
		}

		// 返回数据
		// log.Println("返回数据")
		index := 0
		for i := 0; i <= chIndex; i++ {
			v := &chMap[i]
			d := make([]*RecvData, v.count)
			for i := 0; i < v.count; i++ {
				d[i] = recvCache[index]
				index++
				if i == v.count-1 {
					v.ch <- d
					break
				}
			}
			v.ch = nil
			v.count = 0
		}
		// log.Println("Next")
	}
}

// NewRedisPipeHelper ...
func NewRedisPipeHelper() *RedisPipeHelper {
	oo := new(RedisPipeHelper)
	oo.cmds = make([]*SendCmd, 0)
	oo.rtnsv = make(map[string]*RecvData, 0)
	return oo
}

// RtnStruct 结果结构
type RtnStruct struct {
	Type      string // string/int/bool
	String    string
	Int       int
	Bool      bool
	Interface interface{}
	Err       error
}

// RedisPipeHelper ...
type RedisPipeHelper struct {
	chRecv chan []*RecvData
	cmds   []*SendCmd
	rtns   []*RtnStruct
	rtnsv  map[string]*RecvData
}

// Add 添加指令
func (o *RedisPipeHelper) Add(addr *RtnStruct, cmd string, args ...interface{}) *RedisPipeHelper {
	var as redis.Args
	for _, v := range args {
		as = as.Add(v)
	}
	o.rtns = append(o.rtns, addr)
	o.cmds = append(o.cmds, &SendCmd{cmd: cmd, args: as})
	return o
}

// Make 生产指令集
func (o *RedisPipeHelper) Make() (chan []*RecvData, []*SendCmd) {
	if o.chRecv == nil {
		o.chRecv = make(chan []*RecvData, len(o.cmds))
	}
	return o.chRecv, o.cmds
}

// Wait 等待完成，并会写数据
func (o *RedisPipeHelper) Wait() {
	for k, vv := range <-o.chRecv {
		if o.rtns[k] == nil {
			continue
		}
		switch o.rtns[k].Type {
		case "string":
			o.rtns[k].String, o.rtns[k].Err = redis.String(vv.reply, *vv.err)
		case "int":
			o.rtns[k].Int, o.rtns[k].Err = redis.Int(vv.reply, *vv.err)
		case "bool":
			o.rtns[k].Bool, o.rtns[k].Err = redis.Bool(vv.reply, *vv.err)
		default:
			o.rtns[k].Interface, o.rtns[k].Err = vv.reply, *vv.err
		}
	}
}
