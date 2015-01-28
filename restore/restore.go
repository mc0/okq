// Package restore periodically runs through all the queues and finds events
// which are in the claimed queue but have been abandoned and puts them back in
// the unclaimed queue
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

		for range ticker.C {
			validateClaimedEvents()
		}
	}()
}

func validateClaimedEvents() {
	log.L.Debug("validating claimed events")

	queueNames := db.AllQueueNames()

	for i := range queueNames {
		queueName := queueNames[i]
		claimedKey := db.ClaimedKey(queueName)

		// get the presumably oldest 50 items
		var eventIDs []string
		eventIDs, err := db.Cmd("LRANGE", claimedKey, -50, -1).List()
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
		locksList, err = db.Cmd("MGET", locks...).ListBytes()
		if err != nil {
			log.L.Printf("ERR mget redis replied %q", err)
			return
		}

		for i := range locksList {
			if locksList[i] == nil {
				err = restoreEventToQueue(queueName, eventIDs[i])
				if err != nil {
					return
				}
			}
		}
	}
}

func restoreEventToQueue(queueName string, eventID string) error {
	// Set a lock for restoring
	restoreKey := db.ItemRestoreKey(queueName, eventID)
	reply := db.Cmd("SET", restoreKey, 1, "EX", 10, "NX")
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
	r := db.Lua("LREMRPUSH", 2, claimedKey, unclaimedKey, eventID)
	return r.Err
}
