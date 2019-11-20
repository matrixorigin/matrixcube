package redis

const (
	// write cmd
	set              uint64 = 1
	incrBy           uint64 = 2
	incr             uint64 = 3
	decrby           uint64 = 4
	decr             uint64 = 5
	getset           uint64 = 6
	append           uint64 = 7
	setnx            uint64 = 8
	linsert          uint64 = 9
	lpop             uint64 = 10
	lpush            uint64 = 11
	lpushx           uint64 = 12
	lrem             uint64 = 13
	lset             uint64 = 14
	ltrim            uint64 = 15
	rpop             uint64 = 16
	rpush            uint64 = 17
	rpushx           uint64 = 18
	sadd             uint64 = 19
	srem             uint64 = 20
	scard            uint64 = 21
	spop             uint64 = 22
	zadd             uint64 = 23
	zcard            uint64 = 24
	zincrby          uint64 = 25
	zrem             uint64 = 26
	zremrangebylex   uint64 = 27
	zremrangebyrank  uint64 = 28
	zremrangebyscore uint64 = 29
	zscore           uint64 = 30
	hset             uint64 = 31
	hdel             uint64 = 32
	hmset            uint64 = 33
	hsetnx           uint64 = 34
	hincrby          uint64 = 35

	// read cmd
	get           uint64 = 10000
	strlen        uint64 = 10001
	lindex        uint64 = 10002
	llen          uint64 = 10003
	lrange        uint64 = 10004
	smembers      uint64 = 10005
	sismember     uint64 = 10006
	zcount        uint64 = 10007
	zlexcount     uint64 = 10008
	zrange        uint64 = 10009
	zrangebylex   uint64 = 10010
	zrangebyscore uint64 = 10011
	zrank         uint64 = 10012
	hget          uint64 = 10013
	hexists       uint64 = 10014
	hkeys         uint64 = 10015
	hvals         uint64 = 10016
	hgetall       uint64 = 10017
	hscanget      uint64 = 10018
	hlen          uint64 = 10019
	hmget         uint64 = 10020
	hstrlen       uint64 = 10021

	// CustomBeginWith custom command type begin from this value
	CustomBeginWith uint64 = 20000
)
