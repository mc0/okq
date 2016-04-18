package db

import (
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
)

type clusterDB struct {
	*cluster.Cluster
}

func newClusterDB() (DBer, error) {
	log.L.Printf("connecting to redis cluster at %s", config.RedisAddr)
	c, err := cluster.New(config.RedisAddr)
	if err != nil {
		log.L.Fatal(err)
	}
	return &clusterDB{c}, err
}

func (d *clusterDB) Cmd(cmd string, args ...interface{}) *redis.Resp {
	return d.Cluster.Cmd(cmd, args...)
}

func (d *clusterDB) Pipe(p ...*PipePart) ([]*redis.Resp, error) {
	// We can't really pipe with cluster, just do Cmd in a loop
	ret := make([]*redis.Resp, 0, len(p))
	for i := range p {
		r := d.Cmd(p[i].cmd, p[i].args...)
		if r.Err != nil {
			return nil, r.Err
		}
		ret = append(ret, r)
	}
	return ret, nil
}

func (d *clusterDB) Scan(pattern string) <-chan string {
	retCh := make(chan string)
	go func() {
		defer close(retCh)

		redisClients, err := d.GetEvery()
		if err != nil {
			log.L.Printf("clusterScan(%s) ClientPerMaster(): %s", pattern, err)
			return
		}

		// Make sure we return all clients no matter what
		for i := range redisClients {
			defer d.Put(redisClients[i])
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

func (d *clusterDB) Lua(cmd string, numKeys int, args ...interface{}) *redis.Resp {
	key, err := redis.KeyFromArgs(args)
	if err != nil {
		return redis.NewResp(err)
	}

	c, err := d.GetForKey(key)
	if err != nil {
		return redis.NewResp(err)
	}
	defer d.Put(c)

	return luaHelper(c, cmd, numKeys, args...)
}

func (d *clusterDB) GetAddr() (string, error) {
	return d.GetAddrForKey(""), nil
}
