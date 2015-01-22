package commands

import (
	"fmt"
	"runtime/debug"
	. "testing"
	"time"

	"github.com/fzzy/radix/redis/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mc0/okq/clients"
)

func readAndAssertStr(t *T, client *clients.Client, expected string) {
	m, err := resp.ReadMessage(client.Conn)
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	s, err := m.Str()
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	assert.Equal(t, expected, s, "m: %v stack:\n%s", m, debug.Stack())
}

func readAndAssertInt(t *T, client *clients.Client, expected int64) {
	m, err := resp.ReadMessage(client.Conn)
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	i, err := m.Int()
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	assert.Equal(t, expected, i, "m: %v stack:\n%s", m, debug.Stack())
}

func readAndAssertNil(t *T, client *clients.Client) {
	m, err := resp.ReadMessage(client.Conn)
	require.Nil(t, err, "stack:\n%s", debug.Stack())
	assert.Equal(t, resp.Nil, m.Type, "m: %v stack:\n%s", m, debug.Stack())
}

func readAndAssertArr(t *T, client *clients.Client, expected []string) {
	m, err := resp.ReadMessage(client.Conn)
	require.Nil(t, err, "stack:\n%s", debug.Stack())

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
	ping(client, []string{})
	readAndAssertStr(t, client, "PONG")
}

func TestQRegister(t *T) {
	client := newClient()
	queues := []string{
		clients.RandQueueName(),
		clients.RandQueueName(),
	}
	qregister(client, queues)
	readAndAssertStr(t, client, "OK")
}

// Test adding jobs and removing them
func TestBasicFunctionality(t *T) {
	client := newClient()
	queue := clients.RandQueueName()
	jobs := []struct{ eventId, job string }{
		{"0", "foo"},
		{"1", "bar"},
		{"2", "baz"},
	}

	for i := range jobs {
		qlpush(client, []string{queue, jobs[i].eventId, jobs[i].job})
		readAndAssertStr(t, client, "OK")
	}

	for i := range jobs {
		qrpop(client, []string{queue})
		readAndAssertArr(t, client, []string{jobs[i].eventId, jobs[i].job})

		qack(client, []string{queue, jobs[i].eventId})
		readAndAssertInt(t, client, 1)
	}
}

func TestQStatus(t *T) {
	client := newClient()
	queues := []string{
		clients.RandQueueName(),
		clients.RandQueueName(),
	}

	qstatus(client, queues)
	readAndAssertArr(t, client, []string{
		qstatusLine(queues[0], 0, 0, 0),
		qstatusLine(queues[1], 0, 0, 0),
	})
}

func TestPeeks(t *T) {
	client := newClient()
	queue := clients.RandQueueName()
	jobs := []struct{ eventId, job string }{
		{"0", "foo"},
		{"1", "bar"},
		{"2", "baz"},
	}
	jobFirst := jobs[0]
	jobLast := jobs[len(jobs)-1]

	qrpeek(client, []string{queue})
	readAndAssertNil(t, client)

	qlpeek(client, []string{queue})
	readAndAssertNil(t, client)

	for i := range jobs {
		qlpush(client, []string{queue, jobs[i].eventId, jobs[i].job})
		readAndAssertStr(t, client, "OK")
	}

	qrpeek(client, []string{queue})
	readAndAssertArr(t, client, []string{jobFirst.eventId, jobFirst.job})

	qlpeek(client, []string{queue})
	readAndAssertArr(t, client, []string{jobLast.eventId, jobLast.job})

	// Make sure the actual status of the queue hasn't been affected
	qstatus(client, []string{queue})
	readAndAssertArr(t, client, []string{qstatusLine(queue, len(jobs), 0, 0)})
}

func TestRPush(t *T) {
	client := newClient()
	queue := clients.RandQueueName()

	qlpush(client, []string{queue, "0", "foo"})
	readAndAssertStr(t, client, "OK")

	qrpush(client, []string{queue, "1", "bar"})
	readAndAssertStr(t, client, "OK")

	qrpeek(client, []string{queue})
	readAndAssertArr(t, client, []string{"1", "bar"})

	qstatus(client, []string{queue})
	readAndAssertArr(t, client, []string{qstatusLine(queue, 2, 0, 0)})
}

func TestQNotify(t *T) {
	client := newClient()
	queue := clients.RandQueueName()

	qregister(client, []string{queue})
	readAndAssertStr(t, client, "OK")

	qnotify(client, []string{"1"})
	readAndAssertNil(t, client)

	// Spawn a routine which will trigger a notify. We don't need to read the
	// response of the QLPUSH, it'll all just get garbage collected later
	go func() {
		time.Sleep(100 * time.Millisecond)
		qlpush(newClient(), []string{queue, "0", "foo"})
	}()

	qnotify(client, []string{"10"})
	readAndAssertStr(t, client, queue)
}
