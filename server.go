package main

import (
	"flag"
	"fmt"
	"github.com/fzzy/radix/redis/resp"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mc0/redeque/clients"
	"github.com/mc0/redeque/commands"
)

const PORT = 4777
const STALE_CONSUMER_TIMEOUT = 30

var (
	redisServer = flag.String("localhost", ":6379", "")
	logger      *log.Logger
)

func main() {
	logger = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)

	server, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if server == nil {
		panic(fmt.Sprintf("couldn't start listening: %q\n", err))
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

		// TODO move most of the rest of this into commands as well
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

		if err = commands.Dispatch(client, command, args); err != nil {
			logger.Println(err)
		}
	}
}
