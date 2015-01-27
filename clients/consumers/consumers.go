package consumers

import (
	"time"

	"github.com/mc0/okq/clients"
	"github.com/mc0/okq/db"
)

var callCh = make(chan func(*state))

func init() {
	go func() {
		s := state{
			queueM: map[string]map[string]*clients.Client{},
		}
		for f := range callCh {
			f(&s)
		}
	}()
	go subSpin()
	go activeSpin()
}

// Holds the current state of all consumer related data, primarily the
// registered queue to client mapping
//
// All methods on this struct are NOT thread-safe, be careful with that
type state struct {
	// queue -> []client
	queueM map[string]map[string]*clients.Client
}

// Returns a slice containing a client's current queues, or empty slice if there
// are none registered
func (s *state) clientQueues(clientID string) []string {
	queues := make([]string, 0, 8)
	for q, clients := range s.queueM {
		for _, client := range clients {
			if client.ID == clientID {
				queues = append(queues, q)
			}
		}
	}
	return queues
}

// Records the given client as being registered for the given queues, whether or
// not it was previously registered
func (s *state) addClientQueues(client *clients.Client, queues []string) {
	for _, q := range queues {
		if mc := s.queueM[q]; mc != nil {
			mc[client.ID] = client
		} else {
			s.queueM[q] = map[string]*clients.Client{client.ID: client}
		}
	}
}

// Records the given client as not being registered for the given queues
func (s *state) removeClientQueues(client *clients.Client, queues []string) {
	for _, q := range queues {
		if mc := s.queueM[q]; mc != nil {
			delete(mc, client.ID)
			if len(mc) == 0 {
				delete(s.queueM, q)
			}
		}
	}
}

// Returns a slice containing a queue's currently registered clients, or empty
// slice if there are none registered
func (s *state) queueClients(queue string) []*clients.Client {
	mc := s.queueM[queue]
	if mc == nil {
		return []*clients.Client{}
	}
	cs := make([]*clients.Client, 0, len(mc))
	for _, c := range mc {
		cs = append(cs, c)
	}
	return cs
}

// Returns a slice of all queues currently registered by at least one client
func (s *state) registeredQueues() []string {
	qs := make([]string, 0, len(s.queueM))
	for q := range s.queueM {
		qs = append(qs, q)
	}
	return qs
}

// UpdateQueues should be called whenever a client changes what queues it's
// registered for. The new full list of registered queues should be passed in,
// this method will do a diff and figure it out what was removed
func UpdateQueues(client *clients.Client, queues []string) error {
	respCh := make(chan error)
	callCh <- func(s *state) {
		oldQueues := s.clientQueues(client.ID)
		removed := stringSliceSub(oldQueues, queues)
		s.addClientQueues(client, queues)
		s.removeClientQueues(client, removed)

		select {
		case updateNotifyCh <- struct{}{}:
		default:
		}

		redisClient, err := db.RedisPool.Get()
		if err != nil {
			respCh <- err
			return
		}
		defer db.RedisPool.CarefullyPut(redisClient, &err)

		ts := time.Now().Unix()
		pipelineSize := 0

		for _, queueName := range removed {
			consumersKey := db.ConsumersKey(queueName)
			redisClient.Append("ZREM", consumersKey, client.ID)
			pipelineSize++
		}
		for _, queueName := range queues {
			consumersKey := db.ConsumersKey(queueName)
			redisClient.Append("ZADD", consumersKey, ts, client.ID)
			pipelineSize++
		}
		for i := 0; i < pipelineSize; i++ {
			if err = redisClient.GetReply().Err; err != nil {
				respCh <- err
				return
			}
		}

		respCh <- nil
	}
	return <-respCh
}

// Returns all currently registered to queue names
func registeredQueues() []string {
	respCh := make(chan []string)
	callCh <- func(s *state) {
		respCh <- s.registeredQueues()
	}
	return <-respCh
}

// Returns all the clients currently registered for the given queue
func queueClients(queue string) []*clients.Client {
	respCh := make(chan []*clients.Client)
	callCh <- func(s *state) {
		respCh <- s.queueClients(queue)
	}
	return <-respCh
}

// Returns all strings that are in s1 but not in s2 (i.e. subtracts s2 from s1)
func stringSliceSub(s1, s2 []string) []string {
	ret := make([]string, 0, len(s1))
outer:
	for _, s1Val := range s1 {
		for _, s2Val := range s2 {
			if s1Val == s2Val {
				continue outer
			}
		}
		ret = append(ret, s1Val)
	}
	return ret
}

// QueueConsumerCount returns the total number of consumers registered for the
// given queue, either on this okq instance or others
func QueueConsumerCount(queue string) (int64, error) {
	consumersKey := db.ConsumersKey(queue)

	redisClient, err := db.RedisPool.Get()
	if err != nil {
		return 0, err
	}
	defer db.RedisPool.CarefullyPut(redisClient, &err)

	i, err := redisClient.Cmd("ZCARD", consumersKey).Int64()
	return i, err
}
