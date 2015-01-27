package clients

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"net"
)

// FakeClientConn implements ClientConn, but isn't an actual network connection.
// Can still be passed into resp.ReadMessage and other things that take in
// io.Readers and io.Writers. Used for testing here and other places
type FakeClientConn struct {
	*bytes.Buffer
}

// NewFakeClientConn initializes a FakeClientConn and returns it
func NewFakeClientConn() *FakeClientConn {
	return &FakeClientConn{
		bytes.NewBuffer(make([]byte, 0, 1024)),
	}
}

// Close does nothing and always returns nil. It is only here to implement the
// ClientConn interface
func (fconn *FakeClientConn) Close() error {
	return nil
}

// RemoteAddr always returns nil. It is only here to implement the ClientConn
// interface
func (fconn *FakeClientConn) RemoteAddr() net.Addr {
	return nil
}

// Kind of an odd place for this, but it has to go somewhere.

// RandQueueName returns a random string to use as a queue name. Used for
// testing in a few places
func RandQueueName() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
