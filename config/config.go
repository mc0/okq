// Package config reads configuration parameters off of either the command-line
// or a configuration file and makes them available to all packages
package config

import (
	"github.com/mediocregopher/lever"
)

// Variables populated by the flag parsing process at runtime
var (
	ListenAddr         string
	RedisAddr          string
	RedisCluster       bool
	RedisSentinel      bool
	RedisSentinels     []string
	RedisSentinelGroup string
	RedisPoolSize      int
	Debug              bool
	BGPushPoolSize     int
	CPUProfile         string
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
		Name:        "--redis-sentinel-addr",
		Description: "A sentinel address to connect to through to the client - overrides other options",
	})
	l.Add(lever.Param{
		Name:        "--redis-cluster",
		Description: "Whether or not to treat the redis address as a node in a larger cluster",
		Flag:        true,
	})
	l.Add(lever.Param{
		Name:        "--redis-sentinel-group",
		Description: "A redis sentinel group name for selecting which redis masters to connect",
		Default:     "master",
	})
	l.Add(lever.Param{
		Name:        "--redis-pool-size",
		Description: "Number of connections to make to redis. If cluster or sentinel is being used this many connections will be made to *each* instance involved",
		Default:     "50",
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
	l.Add(lever.Param{
		Name:        "--cpu-profile",
		Description: "Name of a file to write a cpu profile out to. If set the cpu profile will be written until okq is closed",
	})
	l.Parse()

	ListenAddr, _ = l.ParamStr("--listen-addr")
	RedisAddr, _ = l.ParamStr("--redis-addr")
	RedisCluster = l.ParamFlag("--redis-cluster")
	RedisSentinels, RedisSentinel = l.ParamStrs("--redis-sentinel-addr")
	RedisSentinelGroup, _ = l.ParamStr("--redis-sentinel-group")
	RedisPoolSize, _ = l.ParamInt("--redis-pool-size")
	Debug = l.ParamFlag("--debug")
	BGPushPoolSize, _ = l.ParamInt("--bg-push-pool-size")
	CPUProfile, _ = l.ParamStr("--cpu-profile")
}
