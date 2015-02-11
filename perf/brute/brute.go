// This script simply does as many Push/Pop/Ack operations as it can, as fast as
// it can, without regard to letting the consumers actually catch up to the
// consumers. Useful for seeing how okq handles high load during high memory
// usage
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
var noBlock = flag.Bool("no-block", false, "Whether to set NOBLOCK when pushing events")

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

	fn := func(e *okq.Event) bool {
		return true
	}

	pushFlag := okq.Normal
	if *noBlock {
		log.Println("using NOBLOCK")
		pushFlag = okq.NoBlock
	}
	for i := 0; i < n; i++ {
		go func() {
			cl := okq.New(*addr)
			for {
				err := cl.Push(<-queueCh, randString(), pushFlag)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	for i := 0; i < n; i++ {
		go func() {
			cl := okq.New(*addr)
			for {
				err := cl.Consumer(fn, nil, qs...)
				if err != nil {
					log.Printf("got error consuming: %s", err)
				}
			}
		}()
	}

	select {}
}
