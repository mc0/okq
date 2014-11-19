package clients

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

var (
	CallCh         = make(chan func())
	QueueCleanupCh = make(chan CleanupRequest)
	Active         = make(map[string]*Client)
	clientsLastID  = 0
)

func init() {
	go listenForClientsRequests()
}

type ClientsRequest struct {
	client   *Client
	name     string
	value    interface{}
	respChan chan ClientsResponse
}

// TODO: move this back to consumers once/if it's a package
type CleanupRequest struct {
	ClientId string
	Queues   []string
}

type ClientsResponse struct {
	client  *Client
	request ClientsRequest
	result  string
	value   interface{}
}

type Client struct {
	ClientId string
	Queues   []string
	Conn     net.Conn
}

func NewClient(conn net.Conn) *Client {
	client := &Client{Conn: conn, Queues: []string{}}

	respChan := make(chan *Client, 1)

	CallCh <- func() {
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
	Active[clientId] = client
}

func (client *Client) UpdateQueues(queues []string) error {
	var differences []string

	if len(client.Queues) != 0 {
		differences = differenceStrings(queues, client.Queues)
	}

	CallCh <- func() {
		client.Queues = queues
	}

	if differences != nil {
		QueueCleanupCh <- CleanupRequest{Queues: differences, ClientId: client.ClientId}
	}

	return nil
}

func (client *Client) Close() {
	CallCh <- func() {
		delete(Active, client.ClientId)
	}

	client.Conn.Close()
}

func listenForClientsRequests() {
	for call := range CallCh {
		call()
	}
}

func differenceStrings(s1 []string, s2 []string) []string {
	differences := []string{}
	allValues := map[string]int{}

	for _, s1Val := range s1 {
		allValues[s1Val] = 1
	}
	for _, s2Val := range s2 {
		allValues[s2Val] = allValues[s2Val] + 1
	}

	for value, instances := range allValues {
		if instances != 1 {
			continue
		}

		differences = append(differences, value)
	}

	return differences
}
