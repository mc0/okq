package db

import (
	"github.com/fzzy/radix/extra/cluster"
	"github.com/fzzy/radix/redis"
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
)

var clusterInst *cluster.Cluster

func clusterInit() {
	log.L.Printf("connection to redis cluster at %s", config.RedisAddr)
	Cmd = clusterCmd
	Pipe = clusterPipe
	Scan = clusterScan

	var err error
	clusterInst, err = cluster.NewCluster(config.RedisAddr)
	if err != nil {
		log.L.Fatal(err)
	}
}

func clusterCmd(cmd string, args ...interface{}) *redis.Reply {
	return clusterInst.Cmd(cmd, args...)
}

func clusterPipe(p ...*PipePart) ([]*redis.Reply, error) {
	// We can't really pipe with cluster, just do Cmd in a loop
	ret := make([]*redis.Reply, 0, len(p))
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

		redisClients, err := clusterInst.ClientPerMaster()
		if err != nil {
			log.L.Printf("clusterScan(%s) ClientPerMaster(): %s", pattern, err)
			return
		}

		// Make sure we close all the given clients no matter what
		for i := range redisClients {
			defer redisClients[i].Close()
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
