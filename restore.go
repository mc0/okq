package main

import (
	"github.com/fzzy/radix/redis"
	"time"
)

func setupRestoringTimedOutEvents() {
	go restoreTimedOutEvents()
}

func restoreTimedOutEvents() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for _ = range ticker.C {
		validateClaimedEvents()
	}
}

func validateClaimedEvents() {
	redisClient, err := redisPool.Get()
	if err != nil {
		logger.Printf("ERR failed to get redis conn %q", err)
		return
	}
	defer redisPool.Put(redisClient)

	queueNames, err := getAllQueueNames(redisClient)

	for i := range queueNames {
		queueName := queueNames[i]
		// get the presumably oldest 50 items
		reply := redisClient.Cmd("LRANGE", "queue:claimed:"+queueName, -50, -1)
		if reply.Err != nil {
			logger.Printf("ERR rpoplpush redis replied %q", reply.Err)
			continue
		}
		if reply.Type == redis.NilReply {
			continue
		}

		eventIDs, err := reply.List()
		if err != nil {
			continue
		}

		if len(eventIDs) == 0 {
			continue
		}

		var locks []interface{}
		for i := range eventIDs {
			locks = append(locks, interface{}("queue:lock:"+queueName+":"+eventIDs[i]))
		}

		reply = redisClient.Cmd("MGET", locks...)
		if reply.Err != nil {
			logger.Printf("ERR rpoplpush redis replied %q", reply.Err)
			continue
		}
		if reply.Type == redis.NilReply {
			continue
		}

		locksList, err := reply.ListBytes()
		if err != nil {
			continue
		}

		for i := range locksList {
			if locksList[i] == nil {
				restoreEventToQueue(redisClient, queueName, eventIDs[i])
			}
		}
	}
}

func restoreEventToQueue(redisClient *redis.Client, queueName string, eventID string) {
	// Set a lock for restoring
	reply := redisClient.Cmd("SET", "queue:restore:"+queueName+":"+eventID, 1, "EX", 10, "NX")
	if reply.Err != nil {
		logger.Printf("set failed for restoring %q", reply.Err)
		return
	}
	if reply.Type == redis.NilReply {
		logger.Print("set returned nil reply; must be restored already")
		return
	}

	reply = redisClient.Cmd("MULTI")
	if reply.Err != nil {
		logger.Printf("multi failed for restoring %q", reply.Err)
		return
	}

	// Push on the right so it gets action right away
	reply = redisClient.Cmd("RPUSH", "queue:"+queueName, eventID)
	if reply.Err != nil {
		logger.Printf("lpush failed for restoring %q", reply.Err)
		return
	}

	// Remove the claimed item
	reply = redisClient.Cmd("LREM", "queue:claimed:"+queueName, 1, eventID)
	if reply.Err != nil {
		logger.Printf("lpush failed for restoring %q", reply.Err)
		return
	}

	reply = redisClient.Cmd("EXEC")
	if reply.Err != nil {
		logger.Printf("exec failed for restoring %q", reply.Err)
		return
	}
}
