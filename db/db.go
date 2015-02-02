// Package db is used for retrieving redis connections, and has helper methods
// for interacting with the data held within redis
package db

import (
	"errors"
	"strings"

	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
	"github.com/mediocregopher/radix.v2/redis"
)

// These functions are filled in dynamically at runtime
var (
	// Cmd is a function which will perform the given cmd/args in redis and
	// returns the resp. It automatically handles using redis cluster, if that
	// is enabled
	Cmd func(string, ...interface{}) *redis.Resp

	// Pipe runs a set of commands (given by p) one after the other. It is *not*
	// guaranteed that all the commands will be run on the same client. If any
	// commands return an error the pipeline will stop and return that error.
	// Otherwise the Resp from each command is returned in a slice
	//
	//	r, err := db.Pipe(
	//		db.PP("SET", "foo", "bar"),
	//		db.PP("GET", "foo"),
	//	)
	Pipe func(...*PipePart) ([]*redis.Resp, error)

	// Scan is a function which returns a channel to which keys matching the
	// given pattern are written to. The channel must be read from until it is
	// closed, which occurs when there are no more keys or when an error has
	// occured (this error will be logged)
	//
	// This should not be used in any critical paths
	Scan func(string) <-chan string

	// Lua performs one of the preloaded Lua scripts that have been built-in.
	// It's *possible* that the script wasn't loaded in initLuaScripts() for
	// some strange reason, this tries to handle that case as well. The integer
	// passed in is the number of keys the command takes in
	//
	// Example:
	//
	//	db.Lua(redisClient, "LREMRPUSH", 2, "foo", "bar", "value")
	Lua func(string, int, ...interface{}) *redis.Resp
)

// PipePart is a single command to be run in a pipe. See Pipe for an example on
// usage
type PipePart struct {
	cmd  string
	args []interface{}
}

// PP should be called NewPipePart, but that's pretty verbose. It simple returns
// a new PipePart, to be used in a call to Pipe
func PP(cmd string, args ...interface{}) *PipePart {
	return &PipePart{cmd, args}
}

func init() {
	if config.RedisCluster {
		clusterInit()
	} else {
		normalInit()
	}
	if err := initLuaScripts(); err != nil {
		log.L.Fatal(err)
	}
}

func queueKey(queueName string, parts ...string) string {
	fullParts := make([]string, 0, len(parts)+2)
	fullParts = append(fullParts, "queue", "{"+queueName+"}")
	fullParts = append(fullParts, parts...)
	return strings.Join(fullParts, ":")
}

// AllQueueNames returns a list of all currently active queues
func AllQueueNames() []string {
	var queueNames []string
	for queueKey := range Scan(ItemsKey("*")) {
		keyParts := strings.Split(queueKey, ":")
		queueName := keyParts[1]
		queueNames = append(queueNames, queueName[1:len(queueName)-1])
	}

	return queueNames
}

// UnclaimedKey returns the key for the list of unclaimed eventIDs in a queue
func UnclaimedKey(queueName string) string {
	return queueKey(queueName)
}

// ClaimedKey returns the key for the list of claimed (QRPOP'd) eventIDs in a
// queue
func ClaimedKey(queueName string) string {
	return queueKey(queueName, "claimed")
}

// ConsumersKey returns the key for the sorted set of consumer clientIDs
// QREGISTER'd to consume from a quueue
func ConsumersKey(queueName string) string {
	return queueKey(queueName, "consumers")
}

// ItemsKey returns the key for the hash of eventID -> event for a queue
func ItemsKey(queueName string) string {
	return queueKey(queueName, "items")
}

// ItemLockKey returns the key which is used as a lock when a consumer QRPOPs an
// event off the unclaimed list. When the lock expires the item is put back in
// unclaimed if it hasn't been QACK'd already
func ItemLockKey(queueName, eventID string) string {
	return queueKey(queueName, "lock", eventID)
}

// ItemRestoreKey returns the key which is used as a lock when restoring an
// eventID from the claimed to unclaimed queues, so that other running okq
// processes don't try to do the same
func ItemRestoreKey(queueName, eventID string) string {
	return queueKey(queueName, "restore", eventID)
}

// QueueChannelNameKey returns the name of the pubsub channel used to broadcast
// events for the given queue
func QueueChannelNameKey(queueName string) string {
	return queueKey(queueName)
}

// GetQueueNameFromKey takes in any of the above keys produced by this package
// and returns the queue name used to generate the key
func GetQueueNameFromKey(key string) (string, error) {
	parts := strings.Split(key, ":")
	if len(parts) < 2 {
		return "", errors.New("not enough string parts")
	}
	if len(parts[1]) < 3 {
		return "", errors.New("key invalid due to 2nd part")
	}

	queueNamePart := parts[1]
	queueName := queueNamePart[1 : len(queueNamePart)-1]

	return queueName, nil
}

func scanHelper(redisClient *redis.Client, pattern string, retCh chan string) error {
	cursor := "0"
	for {
		r := redisClient.Cmd("SCAN", cursor, "MATCH", pattern)
		if r.Err != nil {
			return r.Err
		}
		elems, err := r.Array()
		if err != nil {
			return err
		}

		results, err := elems[1].List()
		if err != nil {
			return err
		}

		for i := range results {
			retCh <- results[i]
		}

		if cursor, err = elems[0].Str(); err != nil {
			return err
		} else if cursor == "0" {
			return nil
		}
	}
}
