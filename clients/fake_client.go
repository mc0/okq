package clients

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"net"
)

// Implements clients.ClientConn, but isn't an actual network connection. Can
// still be passed into resp.ReadMessage and other things that take in
// io.Readers and io.Writers. Used for testing here and other places
type FakeClientConn struct {
	*bytes.Buffer
}

func NewFakeClientConn() *FakeClientConn {
	return &FakeClientConn{
		bytes.NewBuffer(make([]byte, 0, 1024)),
	}
}

func (fconn *FakeClientConn) Close() error {
	return nil
}

func (fconn *FakeClientConn) RemoteAddr() net.Addr {
	return nil
}

// Kind of an odd place for this, but it has to go somewhere. Returns a random
// string to use as a queue name. Used for testing in a few places
func RandQueueName() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
