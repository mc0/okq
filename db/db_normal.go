package db

import (
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

var normalPool *pool.Pool

func normalInit() {
	log.L.Printf("connecting to redis at %s", config.RedisAddr)
	Cmd = normalCmd
	Pipe = normalPipe
	Scan = normalScan
	Lua = normalLua
	GetAddr = normalGetAddr

	var err error
	normalPool, err = pool.NewPool("tcp", config.RedisAddr, 200)
	if err != nil {
		log.L.Fatal(err)
	}
}

func normalCmd(cmd string, args ...interface{}) *redis.Resp {
	c, err := normalPool.Get()
	if err != nil {
		return redis.NewResp(err)
	}

	r := c.Cmd(cmd, args...)
	normalPool.Put(c)
	return r
}

func normalPipe(p ...*PipePart) ([]*redis.Resp, error) {
	c, err := normalPool.Get()
	if err != nil {
		return nil, err
	}
	defer normalPool.Put(c)

	for i := range p {
		c.PipeAppend(p[i].cmd, p[i].args...)
	}

	rs := make([]*redis.Resp, len(p))
	for i := range rs {
		rs[i] = c.PipeResp()
		if err = rs[i].Err; err != nil {
			return nil, err
		}
	}

	return rs, nil
}

func normalScan(pattern string) <-chan string {
	retCh := make(chan string)
	go func() {
		defer close(retCh)

		redisClient, err := normalPool.Get()
		if err != nil {
			log.L.Printf("normalScan(%s) Get(): %s", pattern, err)
			return
		}
		defer normalPool.Put(redisClient)

		if err = scanHelper(redisClient, pattern, retCh); err != nil {
			log.L.Printf("normalScan(%s) scanHelper: %s", pattern, err)
			return
		}
	}()

	return retCh
}

func normalLua(cmd string, numKeys int, args ...interface{}) *redis.Resp {
	c, err := normalPool.Get()
	if err != nil {
		return redis.NewResp(err)
	}

	r := luaHelper(c, cmd, numKeys, args...)
	normalPool.Put(c)
	return r
}

func normalGetAddr() string {
	return config.RedisAddr
}
