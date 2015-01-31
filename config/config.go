// Package config reads configuration parameters off of either the command-line
// or a configuration file and makes them available to all packages
package config

import (
	"log" // we don't use our log because it imports this package

	"github.com/mediocregopher/flagconfig"
)

// Variables populated by the flag parsing process at runtime
var (
	ListenAddr   string
	RedisAddr    string
	RedisCluster bool
	Debug        bool
)

func init() {
	fc := flagconfig.New("redeqeue")

	fc.StrParam("listen-addr", "Address to listen for client connections on", ":4777")
	fc.StrParam("redis-addr", "Address redis is listening on", "127.0.0.1:6379")
	fc.FlagParam("redis-cluster", "Whether or not to treat the redis address as a node in a larger cluster", false)
	fc.FlagParam("debug", "Turn on debug logging", false)

	if err := fc.Parse(); err != nil {
		log.Fatal(err)
	}

	ListenAddr = fc.GetStr("listen-addr")
	RedisAddr = fc.GetStr("redis-addr")
	RedisCluster = fc.GetFlag("redis-cluster")
	Debug = fc.GetFlag("debug")
}
