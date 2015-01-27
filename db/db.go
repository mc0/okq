// Package db is used for retrieving redis connections, and has helper methods
// for interacting with the data held within redis
package db

import (
	"errors"
	"strings"

	"github.com/fzzy/radix/extra/pool"

	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
)

// A pool of redis connections which can be read from asynchronously by anyone
var RedisPool *pool.Pool

func init() {
	var err error
	log.L.Printf("connecting to redis at %s", config.RedisAddr)
	RedisPool, err = pool.NewPool("tcp", config.RedisAddr, 200)
	if err != nil {
		log.L.Fatal(err)
	}
	if err = initLuaScripts(); err != nil {
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
	for queueKey := range ScanWrapped(ItemsKey("*")) {
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
