package main

import (
	"errors"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/fzzy/radix/redis/resp"
	"strconv"
)

func qregister(client *Client, args []string) {
	// reset the queue list to what was sent
	client.queues = args
	conn := *client.conn

	throttledWriteAllConsumers()

	conn.Write([]byte("+OK\r\n"))
}

func qpeekgeneric(client *Client, args []string, direction string) {
	var err error
	conn := *client.conn
	if len(args) < 1 {
		err = errors.New("ERR missing args")
		resp.WriteArbitrary(conn, err)
		return
	}

}

func qrpop(client *Client, args []string) {
	var err error
	conn := *client.conn
	if len(args) < 1 {
		err = errors.New("ERR missing args")
		resp.WriteArbitrary(conn, err)
		return
	}

	queueName := args[0]
	expires := 30
	//delete := false
	if len(args) > 2 && args[1] == "EX" {
		argsRest := args[1:]
		for i := range argsRest {
			switch argsRest[i] {
			case "EX":
				expires, err = strconv.Atoi(args[2])
				if err != nil {
					err = errors.New("ERR bad expires value")
					resp.WriteArbitrary(conn, err)
					return
				}
			case "DELETE":
				//delete = true
			}
		}
	}

	redisClient, err := redisPool.Get()
	if err != nil {
		err = errors.New(fmt.Sprintf("ERR failed to get redis conn %q", err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}
	defer redisPool.Put(redisClient)

	reply := redisClient.Cmd("RPOPLPUSH", "queue:"+queueName, "queue:claimed:"+queueName)
	if reply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR rpoplpush redis replied %q", reply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}
	if reply.Type == redis.NilReply {
		resp.WriteArbitrary(conn, nil)
		return
	}

	eventID, err := reply.Str()
	if err != nil {
		err = errors.New(fmt.Sprintf("ERR cast failed %q", err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	reply = redisClient.Cmd("SET", "queue:lock:"+queueName+":"+eventID, 1, "EX", strconv.Itoa(expires), "NX")
	if reply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR set redis replied %q", reply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	reply = redisClient.Cmd("HGET", "queue:items:"+queueName, eventID)
	if reply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR hget redis replied %q", reply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	var eventRaw string
	if reply.Type != redis.NilReply {
		eventRaw, err = reply.Str()
		if err != nil {
			err = errors.New(fmt.Sprintf("ERR cast failed %q", err))
			logger.Printf("%s", err)
			resp.WriteArbitrary(conn, err)
			return
		}
	}

	result := [2]string{eventID, eventRaw}

	resp.WriteArbitrary(conn, result)

	go func(client Client, queueName string, eventID string) {
		// TODO: don't send the real client here
		//fakeClient := Client{conn: &io.PipeWriter{}}
		qrem(&client, []string{queueName, eventID})
	}(*client, queueName, eventID)
}

func qrem(client *Client, args []string) {
	var err error
	conn := *client.conn
	if len(args) < 2 {
		err = errors.New("ERR missing args")
		resp.WriteArbitrary(conn, err)
		return
	}
	redisClient, err := redisPool.Get()
	if err != nil {
		return
	}
	defer redisPool.Put(redisClient)

	reply := redisClient.Cmd("LREM", "queue:claimed:"+args[0], -1, args[1])
	if reply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR lrem redis replied %q", reply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	delReply := redisClient.Cmd("HDEL", "queue:items:"+args[0], args[1])
	if delReply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR hdel redis replied %q", delReply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	delReply = redisClient.Cmd("DEL", "queue:lock:"+args[0]+":"+args[1])
	if delReply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR del redis replied %q", delReply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	numRemoved, _ := reply.Int()

	resp.WriteArbitrary(conn, numRemoved)
}

func qpushgeneric(client *Client, args []string, direction string) {
	var err error
	conn := *client.conn
	if len(args) < 3 {
		err = errors.New("ERR missing args")
		resp.WriteArbitrary(conn, err)
		return
	}

	redisClient, err := redisPool.Get()
	if err != nil {
		return
	}
	defer redisPool.Put(redisClient)

	reply := redisClient.Cmd("HSETNX", "queue:items:"+args[0], args[1], args[2])
	if reply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR redis replied %q", reply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	created, err := reply.Int()
	if err != nil {
		err = errors.New(fmt.Sprintf("ERR cast failed %q", err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}
	if created == 0 {
		err = errors.New(fmt.Sprintf("ERR duplicate event %q", args[1]))
		resp.WriteArbitrary(conn, err)
		return
	}

	if direction == "left" {
		reply = redisClient.Cmd("LPUSH", "queue:"+args[0], args[1])
	} else {
		reply = redisClient.Cmd("RPUSH", "queue:"+args[0], args[1])
	}
	if reply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR redis replied %q", reply.Err))
		logger.Printf("%s", err)
		resp.WriteArbitrary(conn, err)
		return
	}

	conn.Write([]byte("+OK\r\n"))
}

func qstatus(client *Client, args []string) {
	var err error
	conn := *client.conn
	if len(args) > 0 {
		err = errors.New("wrong number of arguments for 'qstatus' command")
		resp.WriteArbitrary(conn, err)
		return
	}

	redisClient, err := redisPool.Get()
	if err != nil {
		return
	}
	defer redisPool.Put(redisClient)

	queueNames, err := getAllQueueNames(redisClient)
	if err != nil {
		resp.WriteArbitrary(conn, err)
		return
	}

	var queueStatuses []string

	for i := range queueNames {
		queueName := queueNames[i]
		claimedCount := 0
		availableCount := 0
		totalCount := 0

		claimedReply := redisClient.Cmd("LLEN", "queue:claimed:"+queueName)
		if claimedReply.Err != nil {
			err = errors.New(fmt.Sprintf("ERR llen redis replied %q", claimedReply.Err))
			logger.Printf("%s", err)
			resp.WriteArbitrary(conn, err)
			return
		}
		if claimedReply.Type == redis.IntegerReply {
			claimedCount, _ = claimedReply.Int()
		}

		availableReply := redisClient.Cmd("LLEN", "queue:"+queueName)
		if availableReply.Err != nil {
			err = errors.New(fmt.Sprintf("ERR llen redis replied %q", availableReply.Err))
			logger.Printf("%s", err)
			resp.WriteArbitrary(conn, err)
			return
		}
		if availableReply.Type == redis.IntegerReply {
			availableCount, _ = availableReply.Int()
		}

		totalCount = availableCount + claimedCount

		queueStatuses = append(queueStatuses, queueName+" "+strconv.Itoa(totalCount)+" "+strconv.Itoa(claimedCount))
	}

	resp.WriteArbitrary(conn, queueStatuses)
}
