package redisPipe

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisPipe Redis批量指令
type RedisPipe struct {
	pool      *redis.Pool   // redis 连接池
	queue     chan *command // 发送队列
	queueSize int           // 发送队列缓冲大小
	runCount  int64         // 执行次数
}

// 指令结构
type command struct {
	ch    chan bool    // Wait，等待返回
	reply *interface{} // 返回值
	err   *error       // 返回错误
	cmd   string       // 指令
	args  redis.Args   // 值
}

// NewRedisPipe ...
func NewRedisPipe(rawurl string, bufSize int) *RedisPipe {
	o := new(RedisPipe)
	o.queueSize = bufSize
	o.queue = make(chan *command, bufSize)
	o.pool = &redis.Pool{
		Wait:      true, // 连接池满了，等待其它使用者归还
		MaxIdle:   50,   // 最大空闲连接
		MaxActive: 50,   // 最大连接数
		// IdleTimeout: 3 * time.Second,
		// 连接方法
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://:" + rawurl)
			if err != nil {
				log.Println("redis链接错误:", err)
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

// Send 提交执行指令，无返回信息
func (o *RedisPipe) Send(cmd string, args ...interface{}) {
	o.queue <- &command{cmd: cmd, args: args}
}

// Do 执行指令，同步返回
func (o *RedisPipe) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	ch := make(chan bool)
	o.queue <- &command{reply: &reply, err: &err, cmd: cmd, args: args, ch: ch}
	<-ch
	return
}

// Do2 执行指令，一批指令后需要调用Wait等待返回
func (o *RedisPipe) Do2(reply *interface{}, err *error, cmd string, args ...interface{}) {
	o.queue <- &command{reply: reply, err: err, cmd: cmd, args: args}
}

// Wait 配合Do2使用
func (o *RedisPipe) Wait() {
	ch := make(chan bool)
	o.queue <- &command{ch: ch}
	<-ch
}

// CacheRemaining 返回发送队列大小
func (o *RedisPipe) CacheRemaining() int {
	return len(o.queue)
}

func (o *RedisPipe) do() {
	var cmdCount int   // 指令发送数量
	var chLen int      // 等待的指令数量
	var rtn []*command // 接受返回数据
	var c *command

	for {
		func() {

			// 等待执行，避免死循环造成CPU100%
			chLen = len(o.queue)
			if chLen == 0 { // 没有指令，休息一下
				time.Sleep(time.Millisecond)
				return
			}

			db := o.pool.Get()
			defer db.Close()
			cmdCount = 0

			// 把缓冲拿空
			rtn = make([]*command, chLen) // 生成指定大小的返回数据结构
			for i := 0; i < chLen; i++ {
				c = <-o.queue
				rtn[i] = c
				cmdCount++
				if c.cmd != "" {
					db.Send(c.cmd, c.args...)
				}
			}
			// o.runCount++
			db.Flush()

			for i := 0; i < cmdCount; i++ {
				c = rtn[i]
				if c.reply != nil && c.err != nil { // Do 需要先返回值，再通知
					*(c.reply), *(c.err) = db.Receive()
					if c.ch != nil {
						c.ch <- true
					}
				} else {
					if c.ch != nil { // Do2 通知Wait
						c.ch <- true
					} else {
						db.Receive() // 读取不需要返回的数据
					}
				}
			}
		}()
	}
}
