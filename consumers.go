package main

import (
	"github.com/mc0/redeque/clients"
	"strconv"
	"time"
)

var (
	lastConsumerDataWrite time.Time
	consumerWriteChannel  chan time.Duration
)

func setupConsumers() {
	// make our channel on the primary goroutine
	consumerWriteChannel = make(chan time.Duration)

	go keepConsumersAlive()
	go cleanupQueues()

	consumerWriteChannel <- 0 * time.Second
}

func keepConsumersAlive() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

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
			time.Sleep(nextRun)
		}

		lastConsumerDataWrite = time.Now()
		respChan := make(chan map[string]*clients.Client)
		clients.CallCh <- func() {
			respChan <- clients.Active
		}
		writeAllConsumers(<-respChan)
	}

	for {
		select {
		case durationRequiredToWrite := <-consumerWriteChannel:
			checkDuration(durationRequiredToWrite)
		case <-ticker.C:
			checkDuration(20 * time.Second)
		}
	}
}

func throttledWriteAllConsumers() {
	consumerWriteChannel <- 5 * time.Second
}

func writeAllConsumers(clients map[string]*clients.Client) {
	consumers := make(map[string][]string)
	timestamp := strconv.FormatInt(int64(time.Now().Unix()), 10)
	staleTimestamp := strconv.FormatInt(int64(time.Now().Add(STALE_CONSUMER_TIMEOUT*time.Second*-1).Unix()), 10)

	for _, clientRef := range clients {
		client := *clientRef
		if &client.Queues == nil {
			continue
		}
		queues := client.Queues
		for j := range queues {
			queue := queues[j]
			_, ok := consumers[queue]
			if !ok {
				consumers[queue] = []string{}
			}
			consumers[queue] = append(consumers[queue], timestamp, client.ClientId)
		}
	}

	logger.Printf("writeAllConsumers %q", consumers)

	redisClient, err := redisPool.Get()
	if err != nil {
		logger.Printf("failed to get redis conn %q", err)
		return
	}
	defer redisPool.Put(redisClient)

	for k, members := range consumers {
		var args []interface{}
		args = append(args, queueKey(k, "consumers"))
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
		reply = redisClient.Cmd("ZREMRANGEBYSCORE", queueKey(k, "consumers"), "-inf", staleTimestamp)
		if reply.Err != nil {
			logger.Printf("zrembyscore failed %q", reply.Err)
		}
	}
}

func cleanupQueues() {
	for {
		request := <-clients.QueueCleanupCh
		queues := request.Queues
		clientId := request.ClientId

		if queues == nil {
			continue
		}

		redisClient, err := redisPool.Get()
		if err != nil {
			logger.Printf("failed to get redis conn %q", err)
			return
		}

		for j := range queues {
			queue := queues[j]

			reply := redisClient.Cmd("ZREM", queueKey(queue, "consumers"), clientId)
			if reply.Err != nil {
				logger.Printf("zrem failed %q", reply.Err)
				continue
			}
		}

		redisPool.Put(redisClient)
	}
}
