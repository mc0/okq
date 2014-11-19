package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/fzzy/radix/extra/pool"
	"github.com/fzzy/radix/redis"
	"github.com/fzzy/radix/redis/resp"
	"github.com/mc0/redeque/clients"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const PORT = 4777
const STALE_CONSUMER_TIMEOUT = 30

var (
	redisPool   *pool.Pool
	redisServer = flag.String("localhost", ":6379", "")
	logger      *log.Logger
)

func main() {
	logger = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)

	server, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if server == nil {
		panic(fmt.Sprintf("couldn't start listening: %q\n", err))
	}

	redisPool, err = pool.NewPool("tcp", *redisServer, 50)
	if err != nil {
		panic(fmt.Sprintf("pool failed: %q\n", err))
	}

	logger.Print("ready")

	incomingConns := make(chan net.Conn)

	setupRestoringTimedOutEvents()
	setupConsumers()

	go acceptConns(server, incomingConns)

	for {
		conn := <-incomingConns
		client := clients.NewClient(conn)
		if client == nil {
			conn.Close()
			continue
		}

		logger.Printf("serving client %v %v", client.ClientId, client.Conn.RemoteAddr())
		go serveClient(client)
	}
}

func acceptConns(listener net.Listener, incomingConns chan net.Conn) {
	for {
		conn, err := listener.Accept()
		if conn == nil {
			logger.Printf("couldn't accept: %q", err)
			continue
		}

		logger.Printf("opened conn %v", conn.RemoteAddr())
		incomingConns <- conn
	}
}

func serveClient(client *clients.Client) {
	conn := client.Conn
	defer conn.Close()

	for {
		err := conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			return
		}

		m, err := resp.ReadMessage(conn)
		var command string
		var args []string

		if err != nil {
			if err == io.EOF {
				if client.Queues != nil && len(client.Queues) > 0 {
					clients.QueueCleanupCh <- clients.CleanupRequest{ClientId: client.ClientId, Queues: client.Queues}
				}
				client.Close()
				logger.Printf("closed client %v %v", client.ClientId, client.Conn.RemoteAddr())
				return
			}
			if t, ok := err.(*net.OpError); ok && t.Timeout() {
				continue
			}

			logger.Printf("unknown error %q", err)
			continue
		}

		parts, err := m.Array()
		if err != nil {
			logger.Printf("error parsing message to array: %q", err)
			continue
		}
		for i := range parts {
			val, err := parts[i].Str()
			if err != nil {
				continue
			}
			if i == 0 {
				command = strings.ToUpper(val)
				continue
			}
			args = append(args, val)
		}

		if len(command) == 0 {
			err := errors.New("ERR no command found")
			resp.WriteArbitrary(conn, err)
			return
		}

		switch command {
		case "QREGISTER":
			err = qregister(client, args)
		case "QRPOP":
			err = qrpop(client, args)
		case "QLPEEK":
			err = qpeekgeneric(client, args, false)
		case "QRPEEK":
			err = qpeekgeneric(client, args, true)
		case "QREM":
			err = qrem(client, args)
		case "QLPUSH":
			err = qpushgeneric(client, args, false)
		case "QRPUSH":
			err = qpushgeneric(client, args, true)
		case "QSTATUS":
			err = qstatus(client, args)
		default:
			unknownCommand(client, command)
		}

		if err != nil {
			logger.Println(err)
		}
	}
}

func getAllQueueNames(redisClient *redis.Client) ([]string, error) {
	var queueNames []string
	var err error

	queueKeysReply := redisClient.Cmd("KEYS", queueKey("*", "items"))
	if queueKeysReply.Err != nil {
		err = errors.New(fmt.Sprintf("ERR keys redis replied %q", queueKeysReply.Err))
		return queueNames, err
	}
	if queueKeysReply.Type == redis.NilReply {
		return queueNames, nil
	}

	queueKeys, _ := queueKeysReply.List()
	for i := range queueKeys {
		keyParts := strings.Split(queueKeys[i], ":")
		queueName := keyParts[1]
		queueNames = append(queueNames, queueName[1:len(queueName)-1])
	}

	return queueNames, nil
}
