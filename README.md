# 介绍
1. 将多协程对Redis的读写操作通过Channel转化为单线程处理。
2. 在应用高并发情况下有效的降低了对Redis造成的高负载。

# 安装
```
go get -u -v github.com/ohko/redisPipe
```

# 使用
```
# 创建对象，提供Redis链接地址和预设缓冲大小，缓冲大小根据实际情况设置
pipe := NewRedisPipe("hk@127.0.0.1:6379", 10000000)

# 与Redis.Send相同，不关心返回值，异步投递执行
pipe.Send("SET", "key", "value")

# 与Redis.Do相同，需要即时获取返回值，同步执行
# 与Redis的实际交互在100~200次（10万并发测试数据）
r, e := pipe.Do("GET", "key")

# Do2 + Wait 在并发很高，数据量很大，同时有需要即时获取返回时使用
# 性能比上面的Do提高30%左右，与Redis的实际交互在2～5次（10万并发测试数据）
var r1, r2 interface{}
var e1, e2 error
pipe.Do2(&r1, &e1, "GET", key)
pipe.Do2(&r2, &e2, "GET", key)
pipe.Wait()
```
