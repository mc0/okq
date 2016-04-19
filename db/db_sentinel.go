package db

import (
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/sentinel"
)

type sentinelDB struct {
	Clients []*sentinel.Client
}

func newSentinelDB() (DBer, error) {
	clients := make([]*sentinel.Client, len(config.RedisSentinels))
	for i, server := range config.RedisSentinels {
		log.L.Printf("connecting to redis sentinel at %s", server)
		c, err := sentinel.NewClient("tcp", server, 10, config.RedisSentinelGroup)
		if err != nil {
			log.L.Fatal(err)
		}
		clients[i] = c
	}

	return &sentinelDB{clients}, nil
}

func (d *sentinelDB) getSentinelAndMaster() (*sentinel.Client, *redis.Client, error) {
	var err error
	for _, sentinel := range d.Clients {
		var c *redis.Client
		c, err = sentinel.GetMaster(config.RedisSentinelGroup)
		if err == nil {
			return sentinel, c, nil
		}
	}

	return nil, nil, err
}

func (d *sentinelDB) Cmd(cmd string, args ...interface{}) *redis.Resp {
	sentinel, conn, err := d.getSentinelAndMaster()
	if err != nil {
		return redis.NewResp(err)
	}
	defer sentinel.PutMaster(config.RedisSentinelGroup, conn)

	return conn.Cmd(cmd, args...)
}

func (d sentinelDB) Pipe(p ...*PipePart) ([]*redis.Resp, error) {
	sentinel, c, err := d.getSentinelAndMaster()
	if err != nil {
		return nil, err
	}
	defer sentinel.PutMaster(config.RedisSentinelGroup, c)

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

func (d sentinelDB) Scan(pattern string) <-chan string {
	retCh := make(chan string)
	go func() {
		defer close(retCh)

		sentinel, redisClient, err := d.getSentinelAndMaster()
		if err != nil {
			log.L.Printf("sentinelScan(%s) getSentinelAndMaster(): %s", pattern, err)
			return
		}
		defer sentinel.PutMaster(config.RedisSentinelGroup, redisClient)

		if err = scanHelper(redisClient, pattern, retCh); err != nil {
			log.L.Printf("sentinelScan(%s) scanHelper: %s", pattern, err)
			return
		}
	}()

	return retCh
}

func (d sentinelDB) Lua(cmd string, numKeys int, args ...interface{}) *redis.Resp {
	sentinel, c, err := d.getSentinelAndMaster()
	if err != nil {
		return redis.NewResp(err)
	}
	defer sentinel.PutMaster(config.RedisSentinelGroup, c)

	r := luaHelper(c, cmd, numKeys, args...)
	return r
}

func (d sentinelDB) GetAddr() (string, error) {
	sentinel, c, err := d.getSentinelAndMaster()
	if err != nil {
		return "", err
	}
	defer sentinel.PutMaster(config.RedisSentinelGroup, c)

	return c.Addr, nil
}
