// Contains all commands callable by a client
package commands

import (
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/fzzy/radix/redis"
	"github.com/fzzy/radix/redis/resp"

	"github.com/mc0/okq/clients"
	"github.com/mc0/okq/clients/consumers"
	"github.com/mc0/okq/db"
	"github.com/mc0/okq/log"
)

type commandFunc func(*clients.Client, []string) (interface{}, error)

type commandInfo struct {
	f       commandFunc
	minArgs int
}

var commandMap = map[string]commandInfo{
	"QREGISTER": {qregister, 0},
	"QRPOP":     {qrpop, 1},
	"QLPEEK":    {qlpeek, 1},
	"QRPEEK":    {qrpeek, 1},
	"QACK":      {qack, 2},
	"QLPUSH":    {qlpush, 3},
	"QRPUSH":    {qrpush, 3},
	"QNOTIFY":   {qnotify, 1},
	"QSTATUS":   {qstatus, 0},
	"PING":      {ping, 0},
}

var okSS = resp.NewSimpleString("OK")

// All commands take in a client whose command has already been read off the
// socket, a list of arguments from that command (not including the command name
// itself), and return an error ONLY if the error is worth logging (disconnect
// from redis, etc...)
func Dispatch(client *clients.Client, cmd string, args []string) {
	cmdInfo, ok := commandMap[strings.ToUpper(cmd)]
	if !ok {
		writeErrf(client.Conn, "ERR unknown command %q", cmd)
		return
	}

	if len(args) < cmdInfo.minArgs {
		writeErrf(client.Conn, "ERR missing args")
		return
	}

	ret, err := cmdInfo.f(client, args)
	if err != nil {
		writeErrf(client.Conn, "ERR unexpected server-side error")
		log.L.Print(client.Sprintf("command %s %#v err: %s", cmd, args, err))
		return
	}

	resp.WriteArbitrary(client.Conn, ret)
}

func parseInt(from, as string) (int, error) {
	i, err := strconv.Atoi(from)
	if err != nil {
		return 0, fmt.Errorf("ERR bad %s value: %s", as, err)
	} else if i < 0 {
		return 0, fmt.Errorf("ERR bad %s value: %d < 0", as, i)
	}
	return i, nil
}

func drainPipeline(redisClient *redis.Client) error {
	for {
		err := redisClient.GetReply().Err
		if err == redis.PipelineQueueEmptyError {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

func writeErrf(w io.Writer, format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	return resp.WriteArbitrary(w, err)
}

func qregister(client *clients.Client, args []string) (interface{}, error) {
	err := consumers.UpdateQueues(client, args)
	if err != nil {
		return nil, fmt.Errorf("QREGISTER UpdateQueues: %s", err)
	}
	client.Queues = args

	return okSS, nil
}

func qlpeek(client *clients.Client, args []string) (interface{}, error) {
	return qpeekgeneric(client, args, false)
}

func qrpeek(client *clients.Client, args []string) (interface{}, error) {
	return qpeekgeneric(client, args, true)
}

func qpeekgeneric(
	client *clients.Client, args []string, peekRight bool,
) (
	interface{}, error,
) {
	queueName := args[0]

	redisClient, err := db.RedisPool.Get()
	if err != nil {
		return nil, fmt.Errorf("QPEEK* RedisPool.Get(): %s", err)
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	unclaimedKey := db.UnclaimedKey(queueName)
	offset := "0"
	if peekRight {
		offset = "-1"
	}

	eventIDs, err := redisClient.Cmd("LRANGE", unclaimedKey, offset, offset).List()
	if err != nil {
		return nil, fmt.Errorf("QPEEK* LRANGE: %s", err)
	} else if len(eventIDs) == 0 {
		return nil, nil
	}

	eventID := eventIDs[0]
	itemsKey := db.ItemsKey(queueName)

	var eventRaw string
	reply := redisClient.Cmd("HGET", itemsKey, eventID)
	if reply.Type == redis.NilReply {
		return nil, nil
	}
	if eventRaw, err = reply.Str(); err != nil {
		return nil, fmt.Errorf("QPEEK* HGET: %s", err)
	}

	return []string{eventID, eventRaw}, nil
}

func qrpop(client *clients.Client, args []string) (interface{}, error) {
	var err error
	queueName := args[0]
	expires := 30

	noack := false
	args = args[1:]
	if len(args) > 1 && strings.ToUpper(args[0]) == "EX" {
		if expires, err = parseInt(args[1], "expires"); err != nil {
			return err, nil
		}
		args = args[2:]
	}
	if len(args) > 0 && strings.ToUpper(args[0]) == "NOACK" {
		noack = true
	}

	redisClient, err := db.RedisPool.Get()
	if err != nil {
		return nil, fmt.Errorf("QRPOP RedisPool.Get(): %s", err)
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	unclaimedKey := db.UnclaimedKey(queueName)
	claimedKey := db.ClaimedKey(queueName)
	reply := redisClient.Cmd("RPOPLPUSH", unclaimedKey, claimedKey)
	if reply.Type == redis.NilReply {
		return nil, nil
	}

	eventID, err := reply.Str()
	if err != nil {
		return nil, fmt.Errorf("QRPOP RPOPLPUSH: %s", err)
	}

	lockKey := db.ItemLockKey(queueName, eventID)
	reply = redisClient.Cmd("SET", lockKey, 1, "EX", expires, "NX")
	if err = reply.Err; err != nil {
		return nil, fmt.Errorf("QRPOP SET: %s", err)
	}

	itemsKey := db.ItemsKey(queueName)
	reply = redisClient.Cmd("HGET", itemsKey, eventID)

	var eventRaw string
	if eventRaw, err = reply.Str(); err != nil {
		return nil, fmt.Errorf("QRPOP HGET: %s", err)
	}

	if noack {
		qack(client, []string{queueName, eventID})
	}

	return []string{eventID, eventRaw}, nil
}

func qack(client *clients.Client, args []string) (interface{}, error) {
	redisClient, err := db.RedisPool.Get()
	if err != nil {
		return nil, fmt.Errorf("QACK RedisPool.Get(): %s", err)
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	queueName, eventID := args[0], args[1]
	claimedKey := db.ClaimedKey(queueName)

	var numRemoved int
	numRemoved, err = redisClient.Cmd("LREM", claimedKey, -1, eventID).Int()
	if err != nil {
		return nil, fmt.Errorf("QACK LREM (claimed): %s", err)
	}

	// If we didn't removed the eventID from the claimed events we see if it can
	// be found in unclaimed. We only do this in the uncommon case that the
	// eventID isn't in claimed since unclaimed can be really large, so LREM is
	// slow on it
	if numRemoved == 0 {
		unclaimedKey := db.UnclaimedKey(queueName)
		numRemoved, err = redisClient.Cmd("LREM", unclaimedKey, -1, eventID).Int()
		if err != nil {
			return nil, fmt.Errorf("QACK LREM (unclaimed): %s", err)
		}
	}

	// We only remove the object data itself if the eventID was actually removed
	// from something
	if numRemoved > 0 {
		itemsKey := db.ItemsKey(queueName)
		lockKey := db.ItemLockKey(queueName, eventID)
		redisClient.Append("HDEL", itemsKey, eventID)
		redisClient.Append("DEL", lockKey)
		if err = drainPipeline(redisClient); err != nil {
			return nil, fmt.Errorf("QACK HDEL/DEL: %s", err)
		}
	}

	return numRemoved, nil
}

func qlpush(client *clients.Client, args []string) (interface{}, error) {
	return qpushgeneric(client, args, false)
}

func qrpush(client *clients.Client, args []string) (interface{}, error) {
	return qpushgeneric(client, args, true)
}

func qpushgeneric(
	client *clients.Client, args []string, pushRight bool,
) (
	interface{}, error,
) {
	redisClient, err := db.RedisPool.Get()
	if err != nil {
		return nil, fmt.Errorf("QPUSH* RedisPool.Get(): %s", err)
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	queueName, eventID, contents := args[0], args[1], args[2]
	itemsKey := db.ItemsKey(queueName)

	created, err := redisClient.Cmd("HSETNX", itemsKey, eventID, contents).Int()
	if err != nil {
		return nil, fmt.Errorf("QPUSH* HSETNX: %s", err)
	} else if created == 0 {
		return fmt.Errorf("ERR duplicate event %s", eventID), nil
	}

	unclaimedKey := db.UnclaimedKey(queueName)
	cmd := "LPUSH"
	if pushRight {
		cmd = "RPUSH"
	}
	channelName := db.QueueChannelNameKey(queueName)

	redisClient.Append(cmd, unclaimedKey, eventID)
	redisClient.Append("PUBLISH", channelName, eventID)
	if err = drainPipeline(redisClient); err != nil {
		return nil, fmt.Errorf("QPUSH* %s/PUBLISH: %s", cmd, err)
	}

	return okSS, nil
}

func qnotify(client *clients.Client, args []string) (interface{}, error) {
	timeout, err := parseInt(args[0], "timeout")
	if err != nil {
		return err, nil
	}

	redisClient, err := db.RedisPool.Get()
	if err != nil {
		return nil, fmt.Errorf("QNOTIFY RedisPool.Get(): %s", err)
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	// ensure the NotifyCh is empty before waiting
	queueName := ""
	client.DrainNotifyCh()

	// check to see if we have any events in the registered queues. We check the
	// list in a randomized order since very active queues in the list may not
	// ever let us check after them in the list, abandoning the rest of the list
	queueNames := client.Queues
	for _, i := range rand.Perm(len(queueNames)) {
		unclaimedKey := db.UnclaimedKey(queueNames[i])

		var unclaimedCount int
		unclaimedCount, err = redisClient.Cmd("LLEN", unclaimedKey).Int()
		if err != nil {
			return nil, fmt.Errorf("QSTATUS LLEN unclaimed): %s", err)
		}

		if unclaimedCount > 0 {
			queueName = queueNames[i]
			break
		}
	}

	if queueName == "" {
		select {
		case <-time.After(time.Duration(timeout) * time.Second):
		case queueName = <-client.NotifyCh:
		}
	}

	if queueName != "" {
		return queueName, nil
	}

	return nil, nil
}

func qstatus(client *clients.Client, args []string) (interface{}, error) {
	var queueNames []string
	if len(args) == 0 {
		queueNames = db.AllQueueNames()
	} else {
		queueNames = args
	}

	redisClient, err := db.RedisPool.Get()
	if err != nil {
		return nil, fmt.Errorf("QSTATUS RedisPool.Get(): %s", err)
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	var queueStatuses []string
	for i := range queueNames {
		queueName := queueNames[i]

		claimedCount := 0
		availableCount := 0
		totalCount := 0
		consumerCount := int64(0)

		unclaimedKey := db.UnclaimedKey(queueName)
		claimedKey := db.ClaimedKey(queueName)

		claimedCount, err = redisClient.Cmd("LLEN", claimedKey).Int()
		if err != nil {
			return nil, fmt.Errorf("QSTATUS LLEN claimed: %s", err)
		}

		availableCount, err = redisClient.Cmd("LLEN", unclaimedKey).Int()
		if err != nil {
			return nil, fmt.Errorf("QSTATUS LLEN unclaimed: %s", err)
		}

		totalCount = availableCount + claimedCount

		consumerCount, err = consumers.QueueConsumerCount(queueName)
		if err != nil {
			return nil, fmt.Errorf("QSTATUS QueueConsumerCount: %s", err)
		}

		queueStatus := fmt.Sprintf(
			"%s total: %d processing: %d consumers: %d",
			queueName, totalCount, claimedCount, consumerCount,
		)
		queueStatuses = append(queueStatuses, queueStatus)
	}

	return queueStatuses, nil
}

var pong = resp.NewSimpleString("PONG")
func ping(client *clients.Client, args []string) (interface{}, error) {
	return pong, nil
}
