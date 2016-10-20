package main

import (
	"errors"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	"github.com/mc0/okq/clients"
	"github.com/mc0/okq/clients/consumers"
	"github.com/mc0/okq/commands"
	"github.com/mc0/okq/config"
	"github.com/mc0/okq/log"
	_ "github.com/mc0/okq/restore"
	"github.com/mediocregopher/radix.v2/redis"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	server, err := net.Listen("tcp", config.ListenAddr)
	if server == nil {
		log.L.Fatal(err)
	}

	log.L.Printf("listening on %s", config.ListenAddr)

	incomingConns := make(chan net.Conn)

	if config.CPUProfile != "" {
		go cpuProfile(config.CPUProfile)
	}

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

var invalidCmdResp = redis.NewResp(errors.New("ERR invalid command"))

func serveClient(client *clients.Client) {
	conn := client.Conn
	rr := redis.NewRespReader(conn)

outer:
	for {
		var command string
		var args []string

		m := rr.Read()
		if m.IsType(redis.IOErr) {
			log.L.Debug(client.Sprintf("client connection error %q", m.Err))
			if len(client.Queues) > 0 {
				consumers.UpdateQueues(client, []string{})
			}
			client.Close()
			return
		}

		parts, err := m.Array()
		if err != nil {
			log.L.Debug(client.Sprintf("error parsing to array: %q", err))
			continue outer
		}
		for i := range parts {
			val, err := parts[i].Str()
			if err != nil {
				log.L.Debug(client.Sprintf("invalid command part %#v: %s", parts[i], err))
				invalidCmdResp.WriteTo(conn)
				continue outer
			}
			if i == 0 {
				command = val
			} else {
				args = append(args, val)
			}
		}

		log.L.Debug(client.Sprintf("%s %#v", command, args))
		commands.Dispatch(client, command, args)
	}
}

func cpuProfile(filename string) {
	log.L.Printf("starting cpu profile, writing to %q", filename)
	f, err := os.Create(filename)
	if err != nil {
		log.L.Fatalf("could not open cpu profile %q: %s", filename, err)
	}
	pprof.StartCPUProfile(f)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	pprof.StopCPUProfile()
	signal.Stop(ch)
	log.L.Printf("stopping cpu profile, ctrl-c again to exit")
}
