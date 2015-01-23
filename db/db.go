// Package for retrieving redis connections and helper methods for interacting
// with the data held within redis
package db

import (
	"errors"
	"strings"

	"github.com/fzzy/radix/extra/pool"
	"github.com/fzzy/radix/redis"

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
		panic(err)
	}
}

func queueKey(queueName string, parts ...string) string {
	fullParts := make([]string, 0, len(parts)+2)
	fullParts = append(fullParts, "queue", "{"+queueName+"}")
	fullParts = append(fullParts, parts...)
	return strings.Join(fullParts, ":")
}

// Returns a list of all currently active queues
// TODO use scan
func AllQueueNames(redisClient *redis.Client) ([]string, error) {
	queueKeys, err := redisClient.Cmd("KEYS", ItemsKey("*")).List()
	if err != nil {
		return nil, err
	}

	var queueNames []string
	for i := range queueKeys {
		keyParts := strings.Split(queueKeys[i], ":")
		queueName := keyParts[1]
		queueNames = append(queueNames, queueName[1:len(queueName)-1])
	}

	return queueNames, nil
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
