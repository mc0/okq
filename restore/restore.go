// Periodically runs through all the queues and finds jobs which are in the
// claimed queue but have been abandoned and puts them back in the unclaimed
// queue
package restore

import (
	"github.com/fzzy/radix/redis"
	"time"

	"github.com/mc0/redeque/db"
	"github.com/mc0/redeque/log"
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
	// TODO don't defer this
	defer db.RedisPool.Put(redisClient)

	queueNames, err := db.AllQueueNames(redisClient)

	for i := range queueNames {
		queueName := queueNames[i]
		claimedKey := db.ClaimedKey(queueName)
		// get the presumably oldest 50 items
		reply := redisClient.Cmd("LRANGE", claimedKey, -50, -1)
		if reply.Err != nil {
			log.L.Printf("ERR rpoplpush redis replied %q", reply.Err)
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
			lockKey := db.ItemLockKey(queueName, eventIDs[i])
			locks = append(locks, interface{}(lockKey))
		}

		reply = redisClient.Cmd("MGET", locks...)
		if reply.Err != nil {
			log.L.Printf("ERR rpoplpush redis replied %q", reply.Err)
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

// TODO pipeline commands in here. Also, what's the point of setting the restore
// key?
func restoreEventToQueue(redisClient *redis.Client, queueName string, eventID string) {
	// Set a lock for restoring
	restoreKey := db.ItemRestoreKey(queueName, eventID)
	reply := redisClient.Cmd("SET", restoreKey, 1, "EX", 10, "NX")
	if reply.Err != nil {
		log.L.Printf("set failed for restoring %q", reply.Err)
		return
	}
	if reply.Type == redis.NilReply {
		log.L.Print("set returned nil reply; must be restored already")
		return
	}

	reply = redisClient.Cmd("MULTI")
	if reply.Err != nil {
		log.L.Printf("multi failed for restoring %q", reply.Err)
		return
	}

	// Push on the right so it gets action right away
	unclaimedKey := db.UnclaimedKey(queueName)
	reply = redisClient.Cmd("RPUSH", unclaimedKey, eventID)
	if reply.Err != nil {
		log.L.Printf("lpush failed for restoring %q", reply.Err)
		return
	}

	// Remove the claimed item
	claimedKey := db.ClaimedKey(queueName)
	reply = redisClient.Cmd("LREM", claimedKey, 1, eventID)
	if reply.Err != nil {
		log.L.Printf("lpush failed for restoring %q", reply.Err)
		return
	}

	reply = redisClient.Cmd("EXEC")
	if reply.Err != nil {
		log.L.Printf("exec failed for restoring %q", reply.Err)
		return
	}
}
