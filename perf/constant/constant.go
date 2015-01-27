// This script goes through the entire Push/Pop/Ack process for each event,
// tracking how long it takes to fully process each one. It generates a
// significant amount of load, but the consumers will keep up the number of
// events so not as much as the brute script
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/grooveshark/golib/agg"
	"github.com/mediocregopher/okq-go/okq"
)

var addr = flag.String("okq-addr", "localhost:4777", "Location of okq instance to test")
var stopCh = make(chan bool)
var wg sync.WaitGroup

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

	triggerJobCh := make(chan bool, n*10)
	for i := 0; i < n*10; i++ {
		triggerJobCh <- true
	}

	for i := 0; i < n; i++ {
		go func() {
			cl := okq.New(*addr)
			for _ = range triggerJobCh {
				eventB, err := time.Now().MarshalBinary()
				if err != nil {
					log.Fatal(err)
				}
				if err := cl.Push(<-queueCh, string(eventB)); err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	ch := make(chan *okq.ConsumerEvent)
	var chwg sync.WaitGroup
	for i := 0; i < n; i++ {
		chwg.Add(1)
		go func() {
			cl := okq.New(*addr)
			myCh := make(chan *okq.ConsumerEvent)
			go func() {
				for a := range myCh {
					ch <- a
				}
				chwg.Done()
			}()
			err := cl.Consumer(myCh, stopCh, qs...)
			if err != nil {
				log.Fatalf("got error consuming: %s", err)
			}
		}()
	}
	go func() {
		chwg.Wait()
		close(ch)
	}()

	for i := 0; i < n; i++ {
		go func() {
			wg.Add(1)
			for a := range ch {
				a.Ack()

				eventB := []byte(a.Event.Contents)
				var then time.Time
				if err := then.UnmarshalBinary(eventB); err != nil {
					log.Fatal(err)
				}
				agg.Agg("event", time.Since(then).Seconds())

				triggerJobCh <- true
			}
			wg.Done()
		}()
	}

	select {}
}
