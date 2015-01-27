package clients

import (
	"fmt"
	"io"
	"net"

	"code.google.com/p/go-uuid/uuid"
)

var uuidCh = make(chan string, 128)

func init() {
	go func() {
		for {
			uuidCh <- uuid.New()
		}
	}()
}

// ClientConn is obstensibly a net.Conn, but for testing we don't want to have
// to set up a real listen socket and all that noise
type ClientConn interface {
	io.ReadWriteCloser
	RemoteAddr() net.Addr
}

// Client represents a single okq client, either one just submitting events or a
// consumer. It expects to be handled in a single threaded context, except for
// methods marked otherwise (specifically Notify and DrainNotifyCh)
type Client struct {
	ID       string
	Queues   []string
	Conn     ClientConn
	NotifyCh chan string
}

// NewClient creates a new Client structure around the given ClientConn
func NewClient(conn ClientConn) *Client {
	client := &Client{
		ID:       <-uuidCh,
		Conn:     conn,
		Queues:   []string{},
		NotifyCh: make(chan string, 1),
	}

	return client
}

// Notify notifies the client that queueName has an event on it. This may be
// called from another thread besides the one which "owns" the client
func (client *Client) Notify(queueName string) {
	select {
	case client.NotifyCh <- queueName:
	default:
	}
}

// DrainNotifyCh removes any queue notifications that may be buffered in the
// client. This may be called from another thread besides the one which "owns"
// the client
func (client *Client) DrainNotifyCh() {
	select {
	case <-client.NotifyCh:
	default:
	}
}

// Close closes the client's connection
func (client *Client) Close() {
	client.Conn.Close()
}

// Sprintf returns the given formatted string with a little extra info about the
// client prepended to it
func (client *Client) Sprintf(format string, args ...interface{}) error {
	fullFormat := "client %v %v - " + format
	fullArgs := make([]interface{}, 0, len(args)+2)
	fullArgs = append(fullArgs, client.ID)
	fullArgs = append(fullArgs, client.Conn.RemoteAddr())
	fullArgs = append(fullArgs, args...)

	return fmt.Errorf(fullFormat, fullArgs...)
}
