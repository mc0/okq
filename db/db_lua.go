package db

import (
	"fmt"
	"strings"

	"github.com/fzzy/radix/redis"
)

type lua struct {
	hash, script string
}

var luaScripts = map[string]*lua{
	"LREMRPUSH": {script: `local n = redis.call("LREM", KEYS[1], 0, ARGV[1])
		if n > 0 then
		    redis.call("RPUSH", KEYS[2], ARGV[1])
			end
		return n`,
	},
}

func initLuaScripts() error {
	for cmd, l := range luaScripts {
		hash, err := Cmd("SCRIPT", "LOAD", l.script).Str()
		if err != nil {
			return fmt.Errorf("loading %s: %s", cmd, err)
		}
		l.hash = hash
	}

	return nil
}

// Lua performs one of the preloaded Lua scripts that have been built-in. It's
// *possible* that the script wasn't loaded in initLuaScripts() for some strange
// reason, this tries to handle that case as well.
//
// Example:
//
//	db.Lua(redisClient, "LREMRPUSH", 2, "foo", "bar", "value")
func Lua(cmd string, numKeys int, args ...interface{}) *redis.Reply {

	cmd = strings.ToUpper(cmd)
	l, ok := luaScripts[cmd]
	if !ok {
		return &redis.Reply{
			Type: redis.ErrorReply,
			Err:  fmt.Errorf("unknown lua script: %s", cmd),
		}
	}

	realArgs := make([]interface{}, 0, len(args)+2)
	realArgs = append(realArgs, l.hash, numKeys)
	realArgs = append(realArgs, args...)

	r, notLoaded := luaEvalSha(realArgs)
	if !notLoaded {
		return r
	}

	if err := Cmd("SCRIPT", "LOAD", l.script).Err; err != nil {
		return r
	}

	r, _ = luaEvalSha(realArgs)
	return r
}

// Performs and EVALSHA with the given args, returning the reply and whether or
// not that reply is due to the script for that sha not being loaded yet
func luaEvalSha(args []interface{}) (*redis.Reply, bool) {
	r := Cmd("EVALSHA", args...)
	if r.Err != nil {
		if cerr, ok := r.Err.(*redis.CmdError); ok {
			return r, strings.HasPrefix(cerr.Error(), "NOSCRIPT")
		}
	}
	return r, false
}
