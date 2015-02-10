// Package config reads configuration parameters off of either the command-line
// or a configuration file and makes them available to all packages
package config

import (
	"github.com/mediocregopher/lever"
)

// Variables populated by the flag parsing process at runtime
var (
	ListenAddr     string
	RedisAddr      string
	RedisCluster   bool
	Debug          bool
	BGPushPoolSize int
)

func init() {
	l := lever.New("okq", nil)
	l.Add(lever.Param{
		Name:        "--listen-addr",
		Description: "Address to listen for client connections on",
		Default:     ":4777",
	})
	l.Add(lever.Param{
		Name:        "--redis-addr",
		Description: "Address redis is listening on",
		Default:     "127.0.0.1:6379",
	})
	l.Add(lever.Param{
		Name:        "--redis-cluster",
		Description: "Whether or not to treat the redis address as a node in a larger cluster",
		Flag:        true,
	})
	l.Add(lever.Param{
		Name:        "--debug",
		Aliases:     []string{"-d"},
		Description: "Turn on debug logging",
		Flag:        true,
	})
	l.Add(lever.Param{
		Name:        "--bg-push-pool-size",
		Description: "Number of goroutines to have processing NOBLOCK Q*PUSH commands",
		Default:     "128",
	})
	l.Parse()

	ListenAddr, _ = l.ParamStr("--listen-addr")
	RedisAddr, _ = l.ParamStr("--redis-addr")
	RedisCluster = l.ParamFlag("--redis-cluster")
	Debug = l.ParamFlag("debug")
	BGPushPoolSize, _ = l.ParamInt("--bg-push-pool-size")
}
