package db

import (
	"github.com/fzzy/radix/redis"
	"github.com/mc0/okq/log"
)

// ScanResult is read from the channel returned by Scan. If Err is non-nil than
// the channel will be closed by Scan immediately after
type ScanResult struct {
	Result string
	Err    error
}

// Scan performs a SCAN command on the given redis client, returning a channel
// which will output each individual scan result. The channel will be closed if
// there are no more results (the scan is over). If there is an error mid-scan
// the Err field of the ScanResult will be filled with that error and the
// channel will be closed. The redis client passed in should not be used again
// until the channel is closed.
func Scan(redisClient *redis.Client, pattern string) <-chan *ScanResult {
	retCh := make(chan *ScanResult)
	go func() {
		defer close(retCh)
		cursor := "0"
		for {
			r := redisClient.Cmd("SCAN", cursor, "MATCH", pattern)
			if r.Type == redis.ErrorReply {
				retCh <- &ScanResult{Err: r.Err}
				return
			}
			results, err := r.Elems[1].List()
			if err != nil {
				retCh <- &ScanResult{Err: err}
				return
			}
			for i := range results {
				retCh <- &ScanResult{Result: results[i]}
			}
			if cursor, err = r.Elems[0].Str(); err != nil {
				retCh <- &ScanResult{Err: err}
				return
			} else if cursor == "0" {
				return
			}
		}
	}()

	return retCh
}

// ScanWrapped is the same as Scan, except it handles Get'ing and Put'ing the
// connection on the pool, and doesn't return errors (it logs them instead). It
// will close the channel when there are no more results to give, or when there
// has been an error
func ScanWrapped(pattern string) <-chan string {
	retCh := make(chan string)
	go func() {
		defer close(retCh)
		redisClient, err := RedisPool.Get()
		if err != nil {
			log.L.Printf("starting ScanWrapped(%s): %s", pattern, err)
			return
		}
		defer RedisPool.CarefullyPut(redisClient, &err)

		for r := range Scan(redisClient, pattern) {
			if err = r.Err; err != nil {
				log.L.Printf("ScanWrapped(%s): %s", pattern, err)
				return
			}
			retCh <- r.Result
		}

	}()
	return retCh
}
