package db

import (
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
)

var clusterInst *cluster.Cluster

func clusterInit() {
	log.L.Printf("connection to redis cluster at %s", config.RedisAddr)
	Cmd = clusterCmd
	Pipe = clusterPipe
	Scan = clusterScan
	Lua = clusterLua

	var err error
	clusterInst, err = cluster.New(config.RedisAddr)
	if err != nil {
		log.L.Fatal(err)
	}
}

func clusterCmd(cmd string, args ...interface{}) *redis.Resp {
	return clusterInst.Cmd(cmd, args...)
}

func clusterPipe(p ...*PipePart) ([]*redis.Resp, error) {
	// We can't really pipe with cluster, just do Cmd in a loop
	ret := make([]*redis.Resp, 0, len(p))
	for i := range p {
		r := clusterInst.Cmd(p[i].cmd, p[i].args...)
		if r.Err != nil {
			return nil, r.Err
		}
		ret = append(ret, r)
	}
	return ret, nil
}

func clusterScan(pattern string) <-chan string {
	retCh := make(chan string)
	go func() {
		defer close(retCh)

		redisClients, err := clusterInst.GetEvery()
		if err != nil {
			log.L.Printf("clusterScan(%s) ClientPerMaster(): %s", pattern, err)
			return
		}

		// Make sure we return all clients no matter what
		for i := range redisClients {
			defer clusterInst.Put(redisClients[i])
		}

		for _, redisClient := range redisClients {
			if err := scanHelper(redisClient, pattern, retCh); err != nil {
				log.L.Printf("clusterScan(%s) scanHelper(): %s", pattern, err)
				return
			}
		}
	}()
	return retCh
}

func clusterLua(cmd string, numKeys int, args ...interface{}) *redis.Resp {
	key, err := cluster.KeyFromArgs(args)
	if err != nil {
		return redis.NewResp(err)
	}

	c, err := clusterInst.GetForKey(key)
	if err != nil {
		return redis.NewResp(err)
	}
	defer clusterInst.Put(c)

	return luaHelper(c, cmd, numKeys, args...)
}
