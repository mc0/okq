package db

import (
	"github.com/fzzy/radix/extra/pool"
	"github.com/fzzy/radix/redis"
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
)

var normalPool *pool.Pool

func normalInit() {
	log.L.Printf("connecting to redis at %s", config.RedisAddr)
	Cmd = normalCmd
	Pipe = normalPipe
	Scan = normalScan

	var err error
	normalPool, err = pool.NewPool("tcp", config.RedisAddr, 200)
	if err != nil {
		log.L.Fatal(err)
	}
}

func normalCmd(cmd string, args ...interface{}) *redis.Reply {
	c, err := normalPool.Get()
	if err != nil {
		return &redis.Reply{Type: redis.ErrorReply, Err: err}
	}

	r := c.Cmd(cmd, args...)

	normalPool.CarefullyPut(c, &r.Err)
	return r
}

func normalPipe(p ...*PipePart) ([]*redis.Reply, error) {
	c, err := normalPool.Get()
	if err != nil {
		return nil, err
	}
	defer normalPool.CarefullyPut(c, &err)

	for i := range p {
		c.Append(p[i].cmd, p[i].args...)
	}

	rs := make([]*redis.Reply, len(p))
	for i := range rs {
		rs[i] = c.GetReply()
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

		var err error
		defer func() {
			if err != nil {
				log.L.Printf("normalScan(%s): %s", pattern, err)
			}
		}()

		redisClient, err := normalPool.Get()
		if err != nil {
			return
		}
		defer normalPool.CarefullyPut(redisClient, &err)

		cursor := "0"
		for {
			r := redisClient.Cmd("SCAN", cursor, "MATCH", pattern)
			if err = r.Err; err != nil {
				return
			}

			var results []string
			results, err = r.Elems[1].List()
			if err != nil {
				return
			}

			for i := range results {
				retCh <- results[i]
			}

			if cursor, err = r.Elems[0].Str(); err != nil {
				return
			} else if cursor == "0" {
				return
			}
		}
	}()

	return retCh
}
