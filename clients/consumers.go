package clients

import (
	"time"

	"github.com/mc0/redeque/db"
	"github.com/mc0/redeque/log"
)

// consumers are clients which have qregistered themselves as consuming some set
// of queues. A sorted set of consumers per queue is kept in redis. In this file
// we periodically update the timestamps of the clients in that sorted set which
// are still alive, and remove any clients which have timed out

const (
	STALE_CONSUMER_TIMEOUT = 30 * time.Second
)

// Only need a buffer of size one. If two things are writing to the buffer, they
// both want consumers updated
var updateConsumersCh = make(chan struct{}, 1)

func consumersUpdater() {
	tick := time.Tick(10 * time.Second)
	for {
		select {
		case <-tick:
		case <-updateConsumersCh:
		}

		log.L.Debug("updating consumers")
		if err := updateConsumers(); err != nil {
			log.L.Printf("updating consumers: %s", err)
		}

		// We sleep as a form of rate-limiting
		time.Sleep(5 * time.Second)
	}
}

func ForceUpdateConsumers() {
	select {
	case updateConsumersCh <- struct{}{}:
	default:
	}
}

func updateConsumers() error {
	respChan := make(chan error)
	callCh <- func() {
		// A list of args to pass into ZADD for each consumer
		consumersArgs := make(map[string][]interface{})

		ts := time.Now().Unix()
		staleTS := time.Now().Add(STALE_CONSUMER_TIMEOUT * -1).Unix()
		for _, client := range activeClients {
			for _, queueName := range client.queues {
				args, ok := consumersArgs[queueName]
				if !ok {
					args = make([]interface{}, 0, 2)
					args = append(args, db.ConsumersKey(queueName))
					consumersArgs[queueName] = args
				}

				consumersArgs[queueName] = append(args, ts, client.ClientId)
			}
		}

		redisClient, err := db.RedisPool.Get()
		if err != nil {
			respChan <- err
			return
		}

		for queueName, args := range consumersArgs {
			if err := redisClient.Cmd("ZADD", args...).Err; err != nil {
				respChan <- err
				return
			}

			consumersKey := db.ConsumersKey(queueName)
			r := redisClient.Cmd("ZREMRANGEBYSCORE", consumersKey, "-inf", staleTS)
			if err := r.Err; err != nil {
				respChan <- err
				return
			}
		}
		db.RedisPool.Put(redisClient)
		respChan <- nil
	}
	return <-respChan
}

// Clients can have UpdateQueues called to update the queues in redis that have
// this client labeled as a consumer for them
func (client *Client) UpdateQueues(queues []string) error {
	removed := stringSliceSub(client.queues, queues)
	respChan := make(chan error)
	callCh <- func() {
		client.queues = queues
		redisClient, err := db.RedisPool.Get()
		if err != nil {
			respChan <- err
			return
		}

		for _, queueName := range removed {
			consumersKey := db.ConsumersKey(queueName)
			err := redisClient.Cmd("ZREM", consumersKey, client.ClientId).Err
			if err != nil {
				respChan <- err
				return
			}
		}
		db.RedisPool.Put(redisClient)
		respChan <- nil
	}

	// This will write all additions to redis
	ForceUpdateConsumers()

	return <-respChan
}

// Returns all strings that are in s1 but not in s2 (i.e. subtracts s2 from s1)
func stringSliceSub(s1, s2 []string) []string {
	ret := make([]string, 0, len(s1))
outer:
	for _, s1Val := range s1 {
		for _, s2Val := range s2 {
			if s1Val == s2Val {
				continue outer
			}
		}
		ret = append(ret, s1Val)
	}
	return ret
}
