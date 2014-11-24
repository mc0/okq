package clients

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

var (
	callCh         = make(chan func())
	activeClients  = make(map[string]*Client)
	clientsLastID  = 0
)

func init() {
	go func(){
		for call := range callCh {
			call()
		}
	}()
	go consumersUpdater()
}

type Client struct {
	ClientId string
	queues   []string
	Conn     net.Conn
}

func NewClient(conn net.Conn) *Client {
	client := &Client{Conn: conn, queues: []string{}}

	respChan := make(chan *Client, 1)

	callCh <- func() {
		client.setupInitial()
		respChan <- client
	}

	return <-respChan
}

func (client *Client) setupInitial() {
	clientsLastID++
	clientIdTime := int64(time.Now().UnixNano())
	clientId := fmt.Sprintf("%v:%v", strconv.FormatInt(clientIdTime, 10), strconv.Itoa(clientsLastID))
	client.ClientId = clientId

	// add it to the clients map
	activeClients[clientId] = client
}

func (client *Client) Close() {
	callCh <- func() {
		delete(activeClients, client.ClientId)
	}

	client.UpdateQueues([]string{})
	client.Conn.Close()
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
