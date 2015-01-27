// This script simply does as many Push/Pop/Ack operations as it can, as fast as
// it can, without regard to letting the consumers actually catch up to the
// workers. Useful for seeing how okq handles high load during high memory usage
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log"
	"runtime"

	"github.com/grooveshark/golib/agg"
	"github.com/mediocregopher/okq-go/okq"
)

var addr = flag.String("okq-addr", "localhost:4777", "Location of okq instance to test")

func randString() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func main() {
	flag.Parse()
	agg.CreateInterrupt(1)
	n := runtime.NumCPU()
	runtime.GOMAXPROCS(n)
	okq.Debug = true

	queueCh := make(chan string)
	qs := make([]string, 10)
	for i := range qs {
		qs[i] = randString()
	}
	go func() {
		for {
			for i := range qs {
				queueCh <- qs[i]
			}
		}
	}()

	for i := 0; i < n; i++ {
		go func() {
			cl := okq.New(*addr)
			for {
				if err := cl.Push(<-queueCh, randString()); err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	ch := make(chan *okq.ConsumerEvent)
	for i := 0; i < n; i++ {
		go func() {
			cl := okq.New(*addr)
			for {
				err := cl.Consumer(ch, nil, qs...)
				if err != nil {
					log.Printf("got error consuming: %s", err)
				}
			}
		}()
	}

	for i := 0; i < n; i++ {
		go func() {
			for a := range ch {
				a.Ack()
			}
		}()
	}

	select {}
}
