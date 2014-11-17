package main

import (
	"io"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/fzzy/radix/redis/resp"
	"strconv"
	"strings"
)

func writeServerErr(w io.Writer, err error) error {
	return writeErrf(w, "ERR server-side: %s", err)
}

func writeErrf(w io.Writer, format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	return resp.WriteArbitrary(w, err)
}

func qregister(client *Client, args []string) error {
	// reset the queue list to what was sent
	client.queues = args
	conn := client.conn

	throttledWriteAllConsumers()

	// TODO add WriteSimpleString to resp
	conn.Write([]byte("+OK\r\n"))
	return nil
}

func qpeekgeneric(client *Client, args []string, peekRight bool) error {
	conn := client.conn
	if len(args) < 1 {
		writeErrf(conn, "ERR missing args")
	}
	return nil
}

func qrpop(client *Client, args []string) error {
	conn := client.conn
	if len(args) < 1 {
		writeErrf(conn, "ERR missing args")
		return nil
	}

	queueName := args[0]
	var err error
	expires := 30

	unsafe := false
	if len(args) > 2 && strings.ToUpper(args[1]) == "EX" {
		expires, err = strconv.Atoi(args[2])
		if err != nil {
			writeErrf(conn, "ERR bad expires value: %s", err)
			return nil
		}
		args = args[3:]
	}
	if len(args) > 0 && strings.ToUpper(args[0]) == "UNSAFE" {
		unsafe = true
	}

	redisClient, err := redisPool.Get()
	if err != nil {
		writeServerErr(conn, err)
		return fmt.Errorf("QRPOP redisPool.Get(): %s", err)
	}

	unclaimedKey := queueKey(queueName)
	claimedKey := queueKey(queueName, "claimed")
	reply := redisClient.Cmd("RPOPLPUSH", unclaimedKey, claimedKey)
	if reply.Type == redis.NilReply {
		resp.WriteArbitrary(conn, nil)
		return nil
	}

	eventID, err := reply.Str()
	if err != nil {
		writeServerErr(conn, err)
		return fmt.Errorf("QRPOP RPOPLPUSH: %s", err)
	}

	lockKey := queueKey(queueName, "lock", eventID)
	reply = redisClient.Cmd("SET", lockKey, 1, "EX", strconv.Itoa(expires), "NX")
	if reply.Err != nil {
		writeServerErr(conn, reply.Err)
		return fmt.Errorf("QRPOP SET: %s", reply.Err)
	}

	itemsKey := queueKey(queueName, "items")
	reply = redisClient.Cmd("HGET", itemsKey)

	var eventRaw string
	if reply.Type != redis.NilReply {
		if eventRaw, err = reply.Str(); err != nil {
			writeServerErr(conn, err)
			return fmt.Errorf("QRPOP HGET: %s", err)
		}
	}

	if unsafe {
		qrem(client, []string{queueName, eventID})
	}

	resp.WriteArbitrary(conn, [2]string{eventID, eventRaw})
	redisPool.Put(redisClient)
	return nil
}

func qrem(client *Client, args []string) error {
	var err error
	conn := client.conn
	if len(args) < 2 {
		writeErrf(conn, "ERR missing args")
		return nil
	}
	redisClient, err := redisPool.Get()
	if err != nil {
		writeServerErr(conn, err)
		return err
	}

	queueName, eventID := args[0], args[1]
	claimedKey := queueKey(queueName, "claimed")
	itemsKey := queueKey(queueName, "items")
	lockKey := queueKey(queueName, "lock", eventID)

	redisClient.Append("LREM", claimedKey, -1, eventID)
	redisClient.Append("HDEL", itemsKey, eventID)
	redisClient.Append("DEL", lockKey)
	var numRemoved int
	for i := 0; i < 3; i++ {
		if reply := redisClient.GetReply(); reply.Err != nil {
			writeServerErr(conn, err)
			return fmt.Errorf("QREM %d: %s", i, reply.Err)
		// reply from LREM
		} else if i == 0 {
			numRemoved, _ = reply.Int()
		}
	}

	resp.WriteArbitrary(conn, numRemoved)
	redisPool.Put(redisClient)
	return nil
}

func qpushgeneric(client *Client, args []string, pushRight bool) error {
	conn := client.conn
	if len(args) < 3 {
		writeErrf(conn, "ERR missing args")
		return nil
	}

	redisClient, err := redisPool.Get()
	if err != nil {
		writeServerErr(conn, err)
		return err
	}

	queueName, eventID, contents := args[0], args[1], args[2]
	itemsKey := queueKey(queueName, "items")

	created, err := redisClient.Cmd("HSETNX", itemsKey, eventID, contents).Int()
	if err != nil {
		writeServerErr(conn, err)
		return fmt.Errorf("QPUSH* HSETNX: %s", err)
	} else if created == 0 {
		writeErrf(conn, "ERR duplicate event %q", eventID)
		redisPool.Put(redisClient)
		return nil
	}

	unclaimedKey := queueKey(queueName)
	cmd := "LPUSH"
	if pushRight {
		cmd = "RPUSH"
	}
	if err := redisClient.Cmd(cmd, unclaimedKey, eventID).Err; err != nil {
		writeServerErr(conn, err)
		return fmt.Errorf("QPUSH* %s: %s", cmd, err)
	}

	// TODO resp simple string
	conn.Write([]byte("+OK\r\n"))
	redisPool.Put(redisClient)
	return nil
}

func qstatus(client *Client, args []string) error {
	conn := client.conn

	redisClient, err := redisPool.Get()
	if err != nil {
		writeServerErr(conn, err)
		return err
	}

	queueNames, err := getAllQueueNames(redisClient)
	if err != nil {
		writeServerErr(conn, err)
		return err
	}

	var queueStatuses []string

	for i := range queueNames {
		queueName := queueNames[i]
		claimedCount := 0
		availableCount := 0
		totalCount := 0

		unclaimedKey := queueKey(queueName, "unclaimed")
		claimedKey := queueKey(queueName, "claimed")

		claimedCount, err = redisClient.Cmd("LLEN", claimedKey).Int()
		if err != nil {
			writeServerErr(conn, err)
			return fmt.Errorf("QSTATUS LLEN claimed: %s", err)
		}

		availableCount, err = redisClient.Cmd("LLEN", unclaimedKey).Int()
		if err != nil {
			writeServerErr(conn, err)
			return fmt.Errorf("QSTATUS LLEN unclaimed): %s", err)
		}

		totalCount = availableCount + claimedCount

		queueStatus := fmt.Sprintf("%s %d %d", queueName, totalCount, claimedCount)
		queueStatuses = append(queueStatuses, queueStatus)
	}

	resp.WriteArbitrary(conn, queueStatuses)
	redisPool.Put(redisClient)
	return nil
}

func unknownCommand(client *Client, command string) {
	writeErrf(client.conn, "ERR unknown command '%s`", command)
}

func queueKey(queueName string, parts ...string) string {
	fullParts := make([]string, 0, len(parts) + 2)
	fullParts = append(fullParts, "queue", "{"+queueName+"}")
	return strings.Join(fullParts, ":")
}
