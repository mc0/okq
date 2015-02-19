package consumers

import (
	. "testing"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mc0/okq/clients"
	"github.com/mc0/okq/db"
)

func TestUpdateQueues(t *T) {
	queues := []string{
		clients.RandQueueName(),
		clients.RandQueueName(),
		clients.RandQueueName(),
	}

	client := clients.NewClient(clients.NewFakeClientConn())
	err := UpdateQueues(client, queues)
	require.Nil(t, err)

	// Make sure the client.Id appears in the consumers set for those queues
	for i := range queues {
		key := db.ConsumersKey(queues[i])
		res := db.Inst.Cmd("ZRANK", key, client.ID)
		assert.Equal(t, true, res.IsType(redis.Int), "res: %s", res)
	}

	err = UpdateQueues(client, queues[1:])
	require.Nil(t, err)

	// Make sure the first queue had this clientId removed from it
	key := db.ConsumersKey(queues[0])
	res := db.Inst.Cmd("ZRANK", key, client.ID)
	assert.Equal(t, true, res.IsType(redis.Nil), "res: %s", res)

	// Make sure the rest of the queues still have it
	for i := range queues[1:] {
		key := db.ConsumersKey(queues[1:][i])
		res := db.Inst.Cmd("ZRANK", key, client.ID)
		assert.Equal(t, true, res.IsType(redis.Int), "res: %s", res)
	}

	err = UpdateQueues(client, []string{})
	require.Nil(t, err)

	// Make sure the clientId appears nowhere
	for i := range queues {
		key := db.ConsumersKey(queues[i])
		res := db.Inst.Cmd("ZRANK", key, client.ID)
		assert.Equal(t, true, res.IsType(redis.Nil), "res: %s", res)
	}
}

func TestStaleCleanup(t *T) {
	queue := clients.RandQueueName()

	client := clients.NewClient(clients.NewFakeClientConn())
	err := UpdateQueues(client, []string{queue})
	require.Nil(t, err)

	// Make sure the queue has this clientId as a consumer
	key := db.ConsumersKey(queue)
	res := db.Inst.Cmd("ZRANK", key, client.ID)
	assert.Equal(t, true, res.IsType(redis.Int), "res: %s", res)

	// Remove all knowledge about this client from the consumer state
	callCh <- func(s *state) {
		s.removeClientQueues(client, []string{queue})
	}

	// Wait a little bit and try to remove stale consumers manually
	time.Sleep(2 * time.Second)
	err = removeStaleConsumers(1 * time.Second)
	require.Nil(t, err)

	// Make sure this client is no longer a consumer
	res = db.Inst.Cmd("ZRANK", key, client.ID)
	assert.Equal(t, true, res.IsType(redis.Nil), "key: %s clientId: %s res: %s", key, client.ID, res)
}
