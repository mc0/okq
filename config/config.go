// Reads configuration parameters off of either the command-line or a
// configuration file and makes them available to all packages
package config

import (
	"log" // we don't use our log because it imports this package

	"github.com/mediocregopher/flagconfig"
)

var (
	ListenAddr string
	RedisAddr  string
	Debug      bool
)

func init() {
	fc := flagconfig.New("redeqeue")

	fc.StrParam("listen-addr", "Address to listen for client connections on", ":4777")
	fc.StrParam("redis-addr", "Address redis is listening on", "127.0.0.1:6379")
	fc.FlagParam("debug", "Turn on debug logging", false)

	if err := fc.Parse(); err != nil {
		log.Fatal(err)
	}

	ListenAddr = fc.GetStr("listen-addr")
	RedisAddr = fc.GetStr("redis-addr")
	Debug = fc.GetFlag("debug")
}
