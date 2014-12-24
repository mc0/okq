package clients

import (
	"github.com/stretchr/testify/assert"
	. "testing"
)

func TestOpenClose(t *T) {
	client := NewClient(NewFakeClientConn())
	assert.NotEqual(t, "", client.ClientId)
	callCh <- func() {
		c := activeClients[client.ClientId]
		assert.Exactly(t, client, c)
	}

	client.Close()
	callCh <- func() {
		c, ok := activeClients[client.ClientId]
		assert.Nil(t, c)
		assert.Equal(t, false, ok)
	}
}
