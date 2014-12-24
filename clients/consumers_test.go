package clients

import (
	. "testing"
	"github.com/fzzy/radix/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"time"
	
	"github.com/mc0/redeque/db"
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

	// TODO adding queues happens asyncronously, make this not be the case
	time.Sleep(1 * time.Second)

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

// BUG: This is currently broken, since ZREMRANGEBYSCORE doesn't run on a queue
// unless it has at least one active client. It should instead run on all
// existing consumer sets
func TestStaleCleanup(t *T) {
	queue := RandQueueName()

	redisClient, err := db.RedisPool.Get()
	require.Nil(t, err)

	client := NewClient(NewFakeClientConn())
	err = client.UpdateQueues([]string{queue})
	require.Nil(t, err)

	// TODO adding queues happens asyncronously, make this not be the case
	time.Sleep(1 * time.Second)

	// Make sure the queue has this clientId as a consumer
	key := db.ConsumersKey(queue)
	res := redisClient.Cmd("ZRANK", key, client.ClientId)
	assert.Equal(t, redis.IntegerReply, res.Type, "res: %s", res)

	// Remove all knowledge in the outside world about this client
	callCh <- func() {
		delete(activeClients, client.ClientId)
	}

	// Wait for the timeout period and force the consumer updater to run
	time.Sleep(STALE_CONSUMER_TIMEOUT)
	ForceUpdateConsumers()
	time.Sleep(1 * time.Second)

	// Make sure this client is no longer a consumer
	res = redisClient.Cmd("ZRANK", key, client.ClientId)
	assert.Equal(t, redis.NilReply, res.Type, "res: %s", res)

}
