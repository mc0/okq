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
		log.L.Fatal(err)
	}

	log.L.Printf("listening on %s", config.ListenAddr)

	incomingConns := make(chan net.Conn)

	setupRestoringTimedOutEvents()

	go acceptConns(server, incomingConns)

	for {
		conn := <-incomingConns
		client := clients.NewClient(conn)

		log.L.Debug(client.Sprintf("serving"))
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
		incomingConns <- conn
	}
}

func serveClient(client *clients.Client) {
	conn := client.Conn
	defer conn.Close()

outer:
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
				client.Close()
				log.L.Debug(client.Sprintf("closed"))
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
			log.L.Debug(client.Sprintf("error parsing to array: %q", err))
			resp.WriteArbitrary(conn, fmt.Errorf("ERR invalid command"))
			continue outer
		}
		for i := range parts {
			val, err := parts[i].Str()
			if err != nil {
				log.L.Debug(client.Sprintf("invalid command part %#v: %s", parts[i], err))
				resp.WriteArbitrary(conn, fmt.Errorf("ERR invalid command"))
				continue outer
			}
			if i == 0 {
				command = strings.ToUpper(val)
			} else {
				args = append(args, val)
			}
		}

		log.L.Print(client.Sprintf("%s %#v", command, args))
		if err = commands.Dispatch(client, command, args); err != nil {
			log.L.Print(client.Sprintf("command %s %#v err:", command, args, err))
		}
	}
}
