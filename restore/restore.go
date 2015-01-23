// Periodically runs through all the queues and finds jobs which are in the
// claimed queue but have been abandoned and puts them back in the unclaimed
// queue
package restore

import (
	"time"

	"github.com/fzzy/radix/redis"

	"github.com/mc0/okq/db"
	"github.com/mc0/okq/log"
)

func init() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for _ = range ticker.C {
			validateClaimedEvents()
		}
	}()
}

func validateClaimedEvents() {
	log.L.Debug("validating claimed events")

	redisClient, err := db.RedisPool.Get()
	if err != nil {
		log.L.Printf("ERR failed to get redis conn %q", err)
		return
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	queueNames, err := db.AllQueueNames(redisClient)
	if err != nil {
		log.L.Printf("ERR failed getting all queue names: %s", err)
		return
	}

	for i := range queueNames {
		queueName := queueNames[i]
		claimedKey := db.ClaimedKey(queueName)

		// get the presumably oldest 50 items
		var eventIDs []string
		eventIDs, err = redisClient.Cmd("LRANGE", claimedKey, -50, -1).List()
		if err != nil {
			log.L.Printf("ERR lrange redis replied %q", err)
			return
		} else if len(eventIDs) == 0 {
			continue
		}

		var locks []interface{}
		for i := range eventIDs {
			lockKey := db.ItemLockKey(queueName, eventIDs[i])
			locks = append(locks, lockKey)
		}

		var locksList [][]byte
		locksList, err = redisClient.Cmd("MGET", locks...).ListBytes()
		if err != nil {
			log.L.Printf("ERR mget redis replied %q", err)
			return
		}

		for i := range locksList {
			if locksList[i] == nil {
				err = restoreEventToQueue(redisClient, queueName, eventIDs[i])
				if err != nil {
					return
				}
			}
		}
	}
}

func restoreEventToQueue(redisClient *redis.Client, queueName string, eventID string) error {
	// Set a lock for restoring
	restoreKey := db.ItemRestoreKey(queueName, eventID)
	reply := redisClient.Cmd("SET", restoreKey, 1, "EX", 10, "NX")
	if reply.Err != nil {
		log.L.Printf("set failed for restoring %q", reply.Err)
		return reply.Err
	}
	if reply.Type == redis.NilReply {
		log.L.Debug("%s restored already", eventID)
		return nil
	}

	unclaimedKey := db.UnclaimedKey(queueName)
	claimedKey := db.ClaimedKey(queueName)
	redisClient.Append("MULTI")
	redisClient.Append("RPUSH", unclaimedKey, eventID)
	redisClient.Append("LREM", claimedKey, 1, eventID)
	redisClient.Append("EXEC")

	for {
		reply = redisClient.GetReply()
		if reply.Err == redis.PipelineQueueEmptyError {
			return nil
		} else if reply.Err != nil {
			log.L.Printf("restore transaction for %s failed: %s", eventID, reply.Err)
			return reply.Err
		}
	}
}
