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
var noBlock = flag.Bool("no-block", false, "Whether to set NOBLOCK when pushing events")
var stopCh = make(chan bool)

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

	pushFlag := okq.Normal
	if *noBlock {
		log.Println("using NOBLOCK")
		pushFlag = okq.NoBlock
	}
	for i := 0; i < n; i++ {
		go func() {
			cl := okq.New(*addr)
			for range triggerJobCh {
				eventB, err := time.Now().MarshalBinary()
				if err != nil {
					log.Fatal(err)
				}
				err = cl.Push(<-queueCh, string(eventB), pushFlag)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	fn := func(e *okq.Event) bool {
		eventB := []byte(e.Contents)
		var then time.Time
		if err := then.UnmarshalBinary(eventB); err != nil {
			log.Fatal(err)
		}
		agg.Agg("event", time.Since(then).Seconds())
		triggerJobCh <- true
		return true
	}

	var chwg sync.WaitGroup
	for i := 0; i < n; i++ {
		chwg.Add(1)
		go func() {
			cl := okq.New(*addr)
			err := cl.Consumer(fn, stopCh, qs...)
			if err != nil {
				log.Fatalf("got error consuming: %s", err)
			}
		}()
	}
	chwg.Wait()
}
