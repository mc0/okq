package clients

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

var (
	callCh        = make(chan func())
	activeClients = make(map[string]*Client)
	clientsLastID = 0
)

func init() {
	go func() {
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

// Returns the given formatted string with a little extra info about the client
// prepended to it
func (client *Client) Sprintf(format string, args ...interface{}) error {
	fullFormat := "client %v %v - " + format
	fullArgs := make([]interface{}, 0, len(args)+2)
	fullArgs = append(fullArgs, client.ClientId)
	fullArgs = append(fullArgs, client.Conn.RemoteAddr())
	fullArgs = append(fullArgs, args...)

	return fmt.Errorf(fullFormat, fullArgs...)
}
