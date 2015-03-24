package commands

import (
	"fmt"
	"runtime/debug"
	"strings"
	. "testing"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mc0/okq/clients"
)

func read(t *T, client *clients.Client) *redis.Resp {
	rr := redis.NewRespReader(client.Conn)
	m := rr.Read()
	require.Nil(t, m.Err, "stack:\n%s", debug.Stack())
	return m
}

func readAndAssertStr(t *T, client *clients.Client, expected string) {
	rr := redis.NewRespReader(client.Conn)
	m := rr.Read()
	require.Nil(t, m.Err, "stack:\n%s", debug.Stack())
	s, err := m.Str()
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	assert.Equal(t, expected, s, "m: %v stack:\n%s", m, debug.Stack())
}

func readAndAssertInt(t *T, client *clients.Client, expected int64) {
	rr := redis.NewRespReader(client.Conn)
	m := rr.Read()
	require.Nil(t, m.Err, "stack:\n%s", debug.Stack())
	i, err := m.Int()
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	assert.Equal(t, expected, i, "m: %v stack:\n%s", m, debug.Stack())
}

func readAndAssertNil(t *T, client *clients.Client) {
	rr := redis.NewRespReader(client.Conn)
	m := rr.Read()
	require.Nil(t, m.Err, "stack:\n%s", debug.Stack())
	assert.Equal(t, true, m.IsType(redis.Nil), "m: %v stack:\n%s", m, debug.Stack())
}

func readAndAssertArr(t *T, client *clients.Client, expected []string) {
	rr := redis.NewRespReader(client.Conn)
	m := rr.Read()
	require.Nil(t, m.Err, "stack:\n%s", debug.Stack())

	arr, err := m.Array()
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	require.Equal(t, len(expected), len(arr), "stack:\n%s", debug.Stack())

	for i := range expected {
		s, err := arr[i].Str()
		require.Nil(t, err, "stack:\n%s", debug.Stack())
		assert.Equal(t, expected[i], s, "m: %v stack:\n%s", m, debug.Stack())
	}
}

func qstatusLine(queue string, totalCount, claimedCount, consumerCount int) string {
	return fmt.Sprintf(
		"%s total: %d processing: %d consumers: %d",
		queue, totalCount, claimedCount, consumerCount,
	)
}

func newClient() *clients.Client {
	return clients.NewClient(clients.NewFakeClientConn())
}

func TestPing(t *T) {
	client := newClient()
	Dispatch(client, "ping", []string{})
	readAndAssertStr(t, client, "PONG")
}

func TestQRegister(t *T) {
	client := newClient()
	queues := []string{
		clients.RandQueueName(),
		clients.RandQueueName(),
	}
	Dispatch(client, "qregister", queues)
	readAndAssertStr(t, client, "OK")
}

// Test adding events and removing them
func TestBasicFunctionality(t *T) {
	client := newClient()
	queue := clients.RandQueueName()
	events := []struct{ eventID, event string }{
		{"0", "foo"},
		{"1", "bar"},
		{"2", "baz"},
	}

	for i := range events {
		Dispatch(client, "qlpush", []string{queue, events[i].eventID, events[i].event})
		readAndAssertStr(t, client, "OK")
	}

	for i := range events {
		Dispatch(client, "qrpop", []string{queue})
		readAndAssertArr(t, client, []string{events[i].eventID, events[i].event})

		Dispatch(client, "qack", []string{queue, events[i].eventID})
		readAndAssertInt(t, client, 1)
	}
}

func TestQStatus(t *T) {
	client := newClient()
	queues := []string{
		clients.RandQueueName(),
		clients.RandQueueName(),
	}

	Dispatch(client, "qstatus", queues)
	readAndAssertArr(t, client, []string{
		qstatusLine(queues[0], 0, 0, 0),
		qstatusLine(queues[1], 0, 0, 0),
	})

	// Make sure that when a client is registered to a queue that it shows up in
	// the full list of queues, even if it doesn't have any items
	emptyQueue := clients.RandQueueName()
	Dispatch(client, "qregister", []string{emptyQueue})
	readAndAssertStr(t, client, "OK")
	Dispatch(client, "qstatus", []string{})
	statuses, err := read(t, client).Array()
	require.Nil(t, err)
	found := 0
	for _, statusM := range statuses {
		l, err := statusM.Str()
		require.Nil(t, err)
		parts := strings.Split(l, " ")
		require.True(t, len(parts) >= 1)
		if parts[0] == emptyQueue {
			found++
		}
	}
	assert.Equal(t, 1, found)
}

func TestPeeks(t *T) {
	client := newClient()
	queue := clients.RandQueueName()
	events := []struct{ eventID, event string }{
		{"0", "foo"},
		{"1", "bar"},
		{"2", "baz"},
	}
	eventFirst := events[0]
	eventLast := events[len(events)-1]

	Dispatch(client, "qrpeek", []string{queue})
	readAndAssertNil(t, client)

	Dispatch(client, "qlpeek", []string{queue})
	readAndAssertNil(t, client)

	for i := range events {
		Dispatch(client, "qlpush", []string{queue, events[i].eventID, events[i].event})
		readAndAssertStr(t, client, "OK")
	}

	Dispatch(client, "qrpeek", []string{queue})
	readAndAssertArr(t, client, []string{eventFirst.eventID, eventFirst.event})

	Dispatch(client, "qlpeek", []string{queue})
	readAndAssertArr(t, client, []string{eventLast.eventID, eventLast.event})

	// Make sure the actual status of the queue hasn't been affected
	Dispatch(client, "qstatus", []string{queue})
	readAndAssertArr(t, client, []string{qstatusLine(queue, len(events), 0, 0)})
}

func TestPush(t *T) {
	client := newClient()
	queue := clients.RandQueueName()

	Dispatch(client, "qlpush", []string{queue, "0", "foo"})
	readAndAssertStr(t, client, "OK")

	Dispatch(client, "qrpush", []string{queue, "1", "bar"})
	readAndAssertStr(t, client, "OK")

	Dispatch(client, "qrpeek", []string{queue})
	readAndAssertArr(t, client, []string{"1", "bar"})

	Dispatch(client, "qstatus", []string{queue})
	readAndAssertArr(t, client, []string{qstatusLine(queue, 2, 0, 0)})
}

func TestPushNoBlock(t *T) {
	client := newClient()
	queue := clients.RandQueueName()

	Dispatch(client, "qlpush", []string{queue, "0", "foo", "NOBLOCK"})
	readAndAssertStr(t, client, "OK")
	time.Sleep(50 * time.Millisecond)

	Dispatch(client, "qrpush", []string{queue, "1", "bar", "NOBLOCK"})
	readAndAssertStr(t, client, "OK")
	time.Sleep(50 * time.Millisecond)

	Dispatch(client, "qrpeek", []string{queue})
	readAndAssertArr(t, client, []string{"1", "bar"})

	Dispatch(client, "qstatus", []string{queue})
	readAndAssertArr(t, client, []string{qstatusLine(queue, 2, 0, 0)})
}

func TestQNotify(t *T) {
	client := newClient()
	queue := clients.RandQueueName()

	Dispatch(client, "qregister", []string{queue})
	readAndAssertStr(t, client, "OK")

	Dispatch(client, "qnotify", []string{"1"})
	readAndAssertNil(t, client)

	// Spawn a routine which will trigger a notify. We don't need to read the
	// response of the QLPUSH, it'll all just get garbage collected later
	go func() {
		time.Sleep(100 * time.Millisecond)
		Dispatch(newClient(), "qlpush", []string{queue, "0", "foo"})
	}()

	Dispatch(client, "qnotify", []string{"10"})
	readAndAssertStr(t, client, queue)
}
