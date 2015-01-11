package clients

import (
	"github.com/fzzy/radix/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	. "testing"
	"time"

	"github.com/mc0/okq/db"
)

func TestUpdateQueues(t *T) {
	queues := []string{
		RandQueueName(),
		RandQueueName(),
		RandQueueName(),
	}

	redisClient, err := db.RedisPool.Get()
	require.Nil(t, err)

	client := NewClient(NewFakeClientConn())
	err = client.UpdateQueues(queues)
	require.Nil(t, err)

	// Make sure the clientId appears in the consumers set for those queues
	for i := range queues {
		key := db.ConsumersKey(queues[i])
		res := redisClient.Cmd("ZRANK", key, client.ClientId)
		assert.Equal(t, redis.IntegerReply, res.Type, "res: %s", res)
	}

	err = client.UpdateQueues(queues[1:])
	require.Nil(t, err)

	// Make sure the first queue had this clientId removed from it
	key := db.ConsumersKey(queues[0])
	res := redisClient.Cmd("ZRANK", key, client.ClientId)
	assert.Equal(t, redis.NilReply, res.Type, "res: %s", res)

	// Make sure the rest of the queues still have it
	for i := range queues[1:] {
		key := db.ConsumersKey(queues[1:][i])
		res := redisClient.Cmd("ZRANK", key, client.ClientId)
		assert.Equal(t, redis.IntegerReply, res.Type, "res: %s", res)
	}

	err = client.UpdateQueues([]string{})
	require.Nil(t, err)

	// Make sure the clientId appears nowhere
	for i := range queues {
		key := db.ConsumersKey(queues[i])
		res := redisClient.Cmd("ZRANK", key, client.ClientId)
		assert.Equal(t, redis.NilReply, res.Type, "res: %s", res)
	}

	db.RedisPool.Put(redisClient)
}

func TestStaleCleanup(t *T) {
	queue := RandQueueName()

	redisClient, err := db.RedisPool.Get()
	require.Nil(t, err)

	client := NewClient(NewFakeClientConn())
	err = client.UpdateQueues([]string{queue})
	require.Nil(t, err)

	// Make sure the queue has this clientId as a consumer
	key := db.ConsumersKey(queue)
	res := redisClient.Cmd("ZRANK", key, client.ClientId)
	assert.Equal(t, redis.IntegerReply, res.Type, "res: %s", res)

	// Remove all knowledge in the outside world about this client
	callCh <- func() {
		delete(activeClients, client.ClientId)
	}

	// Wait for the timeout period and force the consumer updater to run, plus
	// an extra second for good measure
	time.Sleep(STALE_CONSUMER_TIMEOUT)
	time.Sleep(1 * time.Second)

	// Make sure this client is no longer a consumer
	res = redisClient.Cmd("ZRANK", key, client.ClientId)
	assert.Equal(t, redis.NilReply, res.Type, "key: %s clientId: %s res: %s", key, client.ClientId, res)

}
