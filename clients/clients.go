package clients

import (
	"fmt"
	"io"
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
	go notifyConsumersEvents()
}

// Obstensibly a net.Conn, but for testing we don't want to have to set up a
// real listen socket and all that noise
type ClientConn interface {
	io.ReadWriteCloser
	RemoteAddr() net.Addr
}

type Client struct {
	ClientId string
	queues   []string
	Conn     ClientConn
	NotifyCh chan string
}

func NewClient(conn ClientConn) *Client {
	client := &Client{Conn: conn, queues: []string{}, NotifyCh: make(chan string, 1)}

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

func (client *Client) GetQueues() []string {
	respChan := make(chan []string)
	callCh <- func() {
		queuesCopy := make([]string, len(client.queues))
		copy(queuesCopy, client.queues)
		respChan <- queuesCopy
	}

	return <-respChan
}

func (client *Client) Notify(queueName string) {
	select {
	case client.NotifyCh <- queueName:
	default:
	}
}

func (client *Client) DrainNotifCh() {
	select {
	case <-client.NotifyCh:
	default:
	}
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
