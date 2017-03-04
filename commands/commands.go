// Package commands contains all commands callable by a client
package commands

import (
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mc0/okq/clients"
	"github.com/mc0/okq/clients/consumers"
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/db"
	"github.com/mc0/okq/log"
	"github.com/mediocregopher/radix.v2/redis"
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
	"QFLUSH":    {qflush, 1},
	"QSTATUS":   {qstatus, 0},
	"QINFO":     {qinfo, 0},
	"PING":      {ping, 0},
}

var okSS = redis.NewRespSimple("OK")

// Dispatch takes in a client whose command has already been read off the
// socket, a list of arguments from that command (not including the command name
// itself), and handles that command
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

	redis.NewResp(ret).WriteTo(client.Conn)
}

func init() {
	for i := 0; i < config.BGPushPoolSize; i++ {
		go qpushBGSpin()
	}
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

func isEqualUpper(expected, actual string) bool {
	if expected == actual {
		return true
	}
	return strings.ToUpper(actual) == expected
}

func writeErrf(w io.Writer, format string, args ...interface{}) {
	err := fmt.Errorf(format, args...)
	redis.NewResp(err).WriteTo(w)
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
	unclaimedKey := db.UnclaimedKey(queueName)
	offset := "0"
	if peekRight {
		offset = "-1"
	}

	eventIDs, err := db.Inst.Cmd("LRANGE", unclaimedKey, offset, offset).List()
	if err != nil {
		return nil, fmt.Errorf("QPEEK* LRANGE: %s", err)
	} else if len(eventIDs) == 0 {
		return nil, nil
	}

	eventID := eventIDs[0]
	itemsKey := db.ItemsKey(queueName)

	var eventRaw string
	reply := db.Inst.Cmd("HGET", itemsKey, eventID)
	if reply.IsType(redis.Nil) {
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
	if len(args) > 1 && isEqualUpper("EX", args[0]) {
		if expires, err = parseInt(args[1], "expires"); err != nil {
			return err, nil
		}
		// The expiry of 0 is special and turns the read into at-most-once
		if expires == 0 {
			noack = true
			expires = 30
		}
	}

	unclaimedKey := db.UnclaimedKey(queueName)
	claimedKey := db.ClaimedKey(queueName)
	itemsKey := db.ItemsKey(queueName)

	reply := db.Inst.Lua("RPOPLPUSH_LOCK_HGET", 3, unclaimedKey, claimedKey, itemsKey, queueName, expires)
	if reply.IsType(redis.Nil) {
		return nil, nil
	}

	r, err := reply.List()
	if err != nil {
		return nil, fmt.Errorf("QRPOP RPOPLPUSH_LOCK_HGET: %s", err)
	}

	if noack {
		qack(client, []string{queueName, r[0]})
	}

	return r, nil
}

func qack(client *clients.Client, args []string) (interface{}, error) {
	queueName, eventID := args[0], args[1]
	unclaimedKey := db.UnclaimedKey(queueName)
	claimedKey := db.ClaimedKey(queueName)
	redo := len(args) > 2 && isEqualUpper("REDO", args[2])

	var removeFn func(string) (int, error)
	var fnName string // used for error logging
	if redo {
		fnName = "LREMRPUSH"
		removeFn = func(srcKey string) (int, error) {
			return db.Inst.Lua("LREMRPUSH", 2, srcKey, unclaimedKey, -1, eventID).Int()
		}
	} else {
		fnName = "LREM"
		removeFn = func(srcKey string) (int, error) {
			return db.Inst.Cmd("LREM", srcKey, -1, eventID).Int()
		}
	}

	numRemoved, err := removeFn(claimedKey)
	if err != nil {
		return nil, fmt.Errorf("QACK %s (claimed): %s", fnName, err)
	} else if redo {
		// If the QACK was for a redo then we have nothing more to do here,
		// regardless of what the numRemoved was. We don't want to remove it
		// from unclaimed because the goal is to put the event there, and we
		// don't want to delete the item key cause we want it redone.
		return numRemoved, nil
	}

	// If we didn't removed the eventID from the claimed events we see if it can
	// be found in unclaimed. We only do this in the uncommon case that the
	// eventID isn't in claimed since unclaimed can be really large, so LREM is
	// slow on it
	if numRemoved == 0 {
		var err error
		numRemoved, err = removeFn(unclaimedKey)
		if err != nil {
			return nil, fmt.Errorf("QACK %s (unclaimed): %s", fnName, err)
		}
	}

	// We only remove the object data itself if the eventID was actually removed
	// from something
	if numRemoved > 0 {
		itemsKey := db.ItemsKey(queueName)
		lockKey := db.ItemLockKey(queueName, eventID)
		_, err := db.Inst.Pipe(
			db.PP("HDEL", itemsKey, eventID),
			db.PP("DEL", lockKey),
		)
		if err != nil {
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

type qpushBG struct {
	args      []string // Arg list minus the NOBLOCK
	pushRight bool
}

var qpushBGCh = make(chan *qpushBG)

func qpushBGSpin() {
	for q := range qpushBGCh {
		qpushgeneric(nil, q.args, q.pushRight)
	}
}

// If client is needed in qpushgeneric for any reason we'll have to refactor how
// qpushBG is being handled probably

func qpushgeneric(
	_ *clients.Client, args []string, pushRight bool,
) (
	interface{}, error,
) {
	queueName, eventID, contents := args[0], args[1], args[2]
	restArgs := args[3:]
	if len(restArgs) > 0 && isEqualUpper("NOBLOCK", restArgs[0]) {
		coreArgs := args[:3]
		select {
		case qpushBGCh <- &qpushBG{coreArgs, pushRight}:
			return okSS, nil
		default:
			return fmt.Errorf("ERR too busy to process NOBLOCK event"), nil
		}
	}

	itemsKey := db.ItemsKey(queueName)

	created, err := db.Inst.Cmd("HSETNX", itemsKey, eventID, contents).Int()
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

	_, err = db.Inst.Pipe(
		db.PP(cmd, unclaimedKey, eventID),
		db.PP("PUBLISH", channelName, eventID),
	)
	if err != nil {
		return nil, fmt.Errorf("QPUSH* %s/PUBLISH: %s", cmd, err)
	}

	return okSS, nil
}

func qnotify(client *clients.Client, args []string) (interface{}, error) {
	timeout, err := parseInt(args[0], "timeout")
	if err != nil {
		return err, nil
	}

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
		unclaimedCount, err = db.Inst.Cmd("LLEN", unclaimedKey).Int()
		if err != nil {
			return nil, fmt.Errorf("QNOTIFY LLEN unclaimed): %s", err)
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

func qflush(client *clients.Client, args []string) (interface{}, error) {
	queue := args[0]
	unclaimedKey := db.UnclaimedKey(queue)
	claimedKey := db.ClaimedKey(queue)
	itemsKey := db.ItemsKey(queue)

	err := db.Inst.Cmd("DEL", unclaimedKey, claimedKey, itemsKey).Err
	if err != nil {
		return nil, fmt.Errorf("QFLUSH DEL: %s", err)
	}
	return okSS, nil
}

func qstatus(client *clients.Client, args []string) (interface{}, error) {
	var queueNames []string
	if len(args) == 0 {
		queueNames = db.AllQueueNames()
		sort.Strings(queueNames)
	} else {
		queueNames = args
	}

	queueInfos := make([][]interface{}, 0, len(queueNames))
	for i := range queueNames {
		queueName := queueNames[i]

		claimedCount := 0
		availableCount := 0
		totalCount := 0
		consumerCount := int64(0)

		unclaimedKey := db.UnclaimedKey(queueName)
		claimedKey := db.ClaimedKey(queueName)

		claimedCount, err := db.Inst.Cmd("LLEN", claimedKey).Int()
		if err != nil {
			return nil, fmt.Errorf("QSTATUS LLEN claimed: %s", err)
		}

		availableCount, err = db.Inst.Cmd("LLEN", unclaimedKey).Int()
		if err != nil {
			return nil, fmt.Errorf("QSTATUS LLEN unclaimed: %s", err)
		}

		totalCount = availableCount + claimedCount

		consumerCount, err = consumers.QueueConsumerCount(queueName)
		if err != nil {
			return nil, fmt.Errorf("QSTATUS QueueConsumerCount: %s", err)
		}

		queueInfos = append(queueInfos, []interface{}{
			queueName,
			int64(totalCount),
			int64(claimedCount),
			consumerCount,
		})
	}

	return queueInfos, nil
}

// Helper method for qinfo. Given an integer and a string or another integer,
// returns the max of the given int and the length of the given string or the
// string form of the given integer
//
//	maxLength(2, "foo", 0) // 3
//	maxLength(2, "", 400) // 3
//	maxLength(2, "", 4) // 2
//
func maxLength(oldMax int64, elStr string, elInt int64) int64 {
	if elStrL := int64(len(elStr)); elStrL > oldMax {
		return elStrL
	}
	if elIntL := int64(len(strconv.FormatInt(elInt, 10))); elIntL > oldMax {
		return elIntL
	}
	return oldMax
}

// Used to temporarily hold info for the qinfo command while it figures out the
// maximum width to make the various columns.
type queueInfo struct {
	queueName       string
	totalCount      int64
	processingCount int64
	consumerCount   int64
}

func qinfo(client *clients.Client, args []string) (interface{}, error) {
	queueInfosRaw, err := qstatus(client, args)
	if err != nil {
		return nil, err
	}
	queueInfosRawInt := queueInfosRaw.([][]interface{})

	queueInfos := make([]queueInfo, 0, len(queueInfosRawInt))
	var (
		maxQueueNameLen       int64
		maxTotalCountLen      int64
		maxProcessingCountLen int64
		maxConsumerCountLen   int64
	)
	for i := range queueInfosRawInt {
		queueName := queueInfosRawInt[i][0].(string)
		totalCount := queueInfosRawInt[i][1].(int64)
		processingCount := queueInfosRawInt[i][2].(int64)
		consumerCount := queueInfosRawInt[i][3].(int64)

		queueInfos = append(queueInfos, queueInfo{
			queueName:       queueName,
			totalCount:      totalCount,
			processingCount: processingCount,
			consumerCount:   consumerCount,
		})

		maxQueueNameLen = maxLength(maxQueueNameLen, queueName, 0)
		maxTotalCountLen = maxLength(maxTotalCountLen, "", int64(totalCount))
		maxProcessingCountLen = maxLength(maxProcessingCountLen, "", int64(processingCount))
		maxConsumerCountLen = maxLength(maxConsumerCountLen, "", consumerCount)
	}

	fmtStr := fmt.Sprintf(
		"%%-%ds  total: %%-%dd  processing: %%-%dd  consumers: %%-%dd",
		maxQueueNameLen,
		maxTotalCountLen,
		maxProcessingCountLen,
		maxConsumerCountLen,
	)

	queueInfoLines := make([]string, 0, len(queueInfos))
	for i := range queueInfos {
		line := fmt.Sprintf(
			fmtStr,
			queueInfos[i].queueName,
			queueInfos[i].totalCount,
			queueInfos[i].processingCount,
			queueInfos[i].consumerCount,
		)
		queueInfoLines = append(queueInfoLines, line)
	}

	return queueInfoLines, nil
}

var pongSS = redis.NewRespSimple("PONG")

func ping(client *clients.Client, args []string) (interface{}, error) {
	return pongSS, nil
}
