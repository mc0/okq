// Package for retrieving redis connections and helper methods for interacting
// with the data held within redis
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

// Returns a list of all currently active queues
func AllQueueNames() []string {
	var queueNames []string
	for queueKey := range ScanWrapped(ItemsKey("*")) {
		keyParts := strings.Split(queueKey, ":")
		queueName := keyParts[1]
		queueNames = append(queueNames, queueName[1:len(queueName)-1])
	}

	return queueNames
}

func UnclaimedKey(queueName string) string {
	return queueKey(queueName)
}

func ClaimedKey(queueName string) string {
	return queueKey(queueName, "claimed")
}

func ConsumersKey(queueName string) string {
	return queueKey(queueName, "consumers")
}

func ItemsKey(queueName string) string {
	return queueKey(queueName, "items")
}

func ItemLockKey(queueName, eventID string) string {
	return queueKey(queueName, "lock", eventID)
}

func ItemRestoreKey(queueName, eventID string) string {
	return queueKey(queueName, "restore", eventID)
}

func QueueChannelNameKey(queueName string) string {
	return queueKey(queueName)
}

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
