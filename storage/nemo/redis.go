package nemo

// RedisKV redis kv
type RedisKV interface {
	// Set redis set command
	Set(key, value []byte, ttl int) error
	// MSet redis mset command
	MSet(keys [][]byte, values [][]byte) error
	// Get redis get command
	Get(key []byte) ([]byte, error)
	// IncrBy redis incrby command
	IncrBy(key []byte, incrment int64) (int64, error)
	// DecrBy redis decrby command
	DecrBy(key []byte, incrment int64) (int64, error)
	// GetSet redis getset command
	GetSet(key, value []byte) ([]byte, error)
	// Append redis append command
	Append(key, value []byte) (int64, error)
	// SetNX redis setnx command
	SetNX(key, value []byte) (int64, error)
	// StrLen redis setlen command
	StrLen(key []byte) (int64, error)
}

// RedisHash redis hash
type RedisHash interface {
	// HSet redis hset command
	HSet(key, field, value []byte) (int64, error)
	// HGet redis HGet command
	HGet(key, field []byte) ([]byte, error)
	// HDel redis HDel command
	HDel(key []byte, fields ...[]byte) (int64, error)
	// HExists redis HExists command
	HExists(key, field []byte) (bool, error)
	// HKeys redis HKeys command
	HKeys(key []byte) ([][]byte, error)
	// HVals redis HVals command
	HVals(key []byte) ([][]byte, error)
	// HGetAll redis HGetAll command
	HGetAll(key []byte) ([][]byte, error)
	// HScanGet redis HScanGet command extend
	HScanGet(key, start []byte, count int) ([][]byte, error)
	// HLen redis HLen command extend
	HLen(key []byte) (int64, error)
	// HMGet redis HMGet command extend
	HMGet(key []byte, fields ...[]byte) ([][]byte, []error)
	// HMSet redis HMSet command extend
	HMSet(key []byte, fields, values [][]byte) error
	// HSetNX redis HSetNX command extend
	HSetNX(key, field, value []byte) (int64, error)
	// HStrLen redis HStrLen command extend
	HStrLen(key, field []byte) (int64, error)
	// HIncrBy redis HIncrBy command extend
	HIncrBy(key, field []byte, incrment int64) ([]byte, error)
}

// RedisSet redis set
type RedisSet interface {
	// SAdd redis SAdd command extend
	SAdd(key []byte, members ...[]byte) (int64, error)
	// SRem redis SRem command extend
	SRem(key []byte, members ...[]byte) (int64, error)
	// SCard redis SCard command extend
	SCard(key []byte) (int64, error)
	// SMembers redis SMembers command extend
	SMembers(key []byte) ([][]byte, error)
	// SIsMember redis SIsMember command extend
	SIsMember(key []byte, member []byte) (int64, error)
	// SPop redis SPop command extend
	SPop(key []byte) ([]byte, error)
}

// RedisZSet redis zset
type RedisZSet interface {
	// ZAdd redis ZAdd command extend
	ZAdd(key []byte, score float64, member []byte) (int64, error)
	// ZCard redis ZCard command extend
	ZCard(key []byte) (int64, error)
	// ZCount redis ZCount command extend
	ZCount(key []byte, min []byte, max []byte) (int64, error)
	// ZIncrBy redis ZIncrBy command extend
	ZIncrBy(key []byte, member []byte, by float64) ([]byte, error)
	// ZLexCount redis ZLexCount command extend
	ZLexCount(key []byte, min []byte, max []byte) (int64, error)
	// ZRange redis ZRange command extend
	ZRange(key []byte, start int64, stop int64) ([][]byte, error)
	// ZRangeByLex redis ZRangeByLex command extend
	ZRangeByLex(key []byte, min []byte, max []byte) ([][]byte, error)
	// ZRangeByScore redis ZRangeByScore command extend
	ZRangeByScore(key []byte, min []byte, max []byte) ([][]byte, error)
	// ZRank redis ZRank command extend
	ZRank(key []byte, member []byte) (int64, error)
	// ZRem redis ZRem command extend
	ZRem(key []byte, members ...[]byte) (int64, error)
	// ZRemRangeByLex redis ZRemRangeByLex command extend
	ZRemRangeByLex(key []byte, min []byte, max []byte) (int64, error)
	// ZRemRangeByRank redis ZRemRangeByRank command extend
	ZRemRangeByRank(key []byte, start int64, stop int64) (int64, error)
	// ZRemRangeByScore redis ZRemRangeByScore command extend
	ZRemRangeByScore(key []byte, min []byte, max []byte) (int64, error)
	// ZScore redis ZScore command extend
	ZScore(key []byte, member []byte) ([]byte, error)
}

// RedisList redis list
type RedisList interface {
	// LIndex redis LIndex command extend
	LIndex(key []byte, index int64) ([]byte, error)
	// LInsert redis LInsert command extend
	LInsert(key []byte, pos int, pivot []byte, value []byte) (int64, error)
	// LLen redis LLen command extend
	LLen(key []byte) (int64, error)
	// LPop redis LPop command extend
	LPop(key []byte) ([]byte, error)
	// LPush redis LPush command extend
	LPush(key []byte, values ...[]byte) (int64, error)
	// LPushX redis LPushX command extend
	LPushX(key []byte, value []byte) (int64, error)
	// LRange redis LRange command extend
	LRange(key []byte, begin int64, end int64) ([][]byte, error)
	// LRem redis LRem command extend
	LRem(key []byte, count int64, value []byte) (int64, error)
	// LSet redis LSet command extend
	LSet(key []byte, index int64, value []byte) error
	// LTrim redis LTrim command extend
	LTrim(key []byte, begin int64, end int64) error
	// RPop redis RPop command extend
	RPop(key []byte) ([]byte, error)
	// RPush redis RPush command extend
	RPush(key []byte, values ...[]byte) (int64, error)
	// RPushX redis RPushX command extend
	RPushX(key []byte, value []byte) (int64, error)
}
