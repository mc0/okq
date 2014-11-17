package main

import (
	"strconv"
	"time"
)

var (
	lastConsumerDataWrite  time.Time
	throttledWriteTimer    *time.Timer
	consumerWriteChannel   chan time.Duration
	consumerCleanupChannel chan Client
)

func setupConsumerChecking() {
	// make our channel on the primary goroutine
	consumerWriteChannel = make(chan time.Duration)

	consumerCleanupChannel = make(chan Client)

	go keepConsumersAlive()
	go cleanupConsumers()

	consumerWriteChannel <- 0 * time.Second
}

func keepConsumersAlive() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var durationRequiredToWrite time.Duration
	lastConsumerDataWrite = time.Now()

	checkDuration := func(durationRequiredToWrite time.Duration) {
		now := time.Now()
		nowDiff := now.Sub(lastConsumerDataWrite)
		if nowDiff < durationRequiredToWrite {
			// for now assume this means it's the ticker
			if durationRequiredToWrite > 5*time.Second {
				return
			}
			// only write every 5s at most
			nextRun := 5*time.Second - nowDiff
			updated := false
			if throttledWriteTimer != nil {
				updated = throttledWriteTimer.Reset(nextRun)
			}
			if !updated {
				throttledWriteTimer = time.AfterFunc(nextRun, writeAllConsumers)
			}
			return
		}

		writeAllConsumers()
	}

	for {
		select {
		case durationRequiredToWrite = <-consumerWriteChannel:
			checkDuration(durationRequiredToWrite)
		case <-ticker.C:
			checkDuration(20 * time.Second)
		}
	}
}

func cleanupConsumers() {
	for {
		client := <-consumerCleanupChannel

		redisClient, err := redisPool.Get()
		if err != nil {
			logger.Printf("failed to get redis conn %q", err)
			return
		}

		if &client.queues == nil {
			continue
		}
		queues := client.queues
		for j := range queues {
			queue := queues[j]

			reply := redisClient.Cmd("ZREM", "queue:consumers:"+queue, client.clientId)
			if reply.Err != nil {
				logger.Printf("zrem failed %q", reply.Err)
				continue
			}
		}

		redisPool.Put(redisClient)
	}
}

func throttledWriteAllConsumers() {
	consumerWriteChannel <- 5 * time.Second
}

func writeAllConsumers() {
	consumers := make(map[string][]string)
	timestamp := strconv.FormatInt(int64(time.Now().Unix()), 10)
	staleTimestamp := strconv.FormatInt(int64(time.Now().Add(STALE_CONSUMER_TIMEOUT*time.Second*-1).Unix()), 10)

	lastConsumerDataWrite = time.Now()

	for _, clientRef := range clients {
		client := *clientRef
		if &client.queues == nil {
			continue
		}
		queues := client.queues
		for j := range queues {
			queue := queues[j]
			_, ok := consumers[queue]
			if !ok {
				consumers[queue] = []string{}
			}
			consumers[queue] = append(consumers[queue], timestamp, client.clientId)
		}
	}

	redisClient, err := redisPool.Get()
	if err != nil {
		logger.Printf("failed to get redis conn %q", err)
		return
	}
	defer redisPool.Put(redisClient)

	for k, members := range consumers {
		var args []interface{}
		args = append(args, "queue:consumers:"+k)
		for i := range members {
			args = append(args, interface{}(members[i]))
		}
		reply := redisClient.Cmd("ZADD", args...)
		if reply.Err != nil {
			logger.Printf("zadd failed %q", reply.Err)
			continue
		}

		// TODO: it seems likely that this could not run in abnormal conditions, fix!
		// remove any stale consumers
		reply = redisClient.Cmd("ZREMRANGEBYSCORE", "queue:consumers:"+k, "-inf", staleTimestamp)
		if reply.Err != nil {
			logger.Printf("zrembyscore failed %q", reply.Err)
		}
	}
}
