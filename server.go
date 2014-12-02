package main

import (
	"fmt"
	"github.com/fzzy/radix/redis/resp"
	"io"
	"net"
	"strings"
	"time"

	"github.com/mc0/redeque/clients"
	"github.com/mc0/redeque/commands"
	"github.com/mc0/redeque/config"
	"github.com/mc0/redeque/log"
)

func main() {
	server, err := net.Listen("tcp", config.ListenAddr)
	if server == nil {
		panic(fmt.Sprintf("couldn't start listening: %q\n", err))
	}

	log.L.Print("ready")

	incomingConns := make(chan net.Conn)

	setupRestoringTimedOutEvents()

	go acceptConns(server, incomingConns)

	for {
		conn := <-incomingConns
		client := clients.NewClient(conn)
		if client == nil {
			conn.Close()
			continue
		}

		log.L.Printf("serving client %v %v", client.ClientId, client.Conn.RemoteAddr())
		go serveClient(client)
	}
}

func acceptConns(listener net.Listener, incomingConns chan net.Conn) {
	for {
		conn, err := listener.Accept()
		if conn == nil {
			log.L.Printf("couldn't accept: %q", err)
			continue
		}

		log.L.Printf("opened conn %v", conn.RemoteAddr())
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
				client.Close()
				log.L.Printf("closed client %v %v", client.ClientId, client.Conn.RemoteAddr())
				return
			}
			if t, ok := err.(*net.OpError); ok && t.Timeout() {
				continue
			}

			log.L.Printf("unknown error %q", err)
			continue
		}

		parts, err := m.Array()
		if err != nil {
			log.L.Printf("error parsing message to array: %q", err)
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
			log.L.Println(err)
		}
	}
}
