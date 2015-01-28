package restore

import (
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mc0/okq/clients"
	"github.com/mc0/okq/db"
)

func TestRestore(t *T) {

	// Add an item to claimed list and call validateClaimedEvents. Since it
	// doesn't have a lock it should be moved to the unclaimed list
	q := clients.RandQueueName()
	unclaimedKey := db.UnclaimedKey(q)
	claimedKey := db.ClaimedKey(q)
	itemsKey := db.ItemsKey(q)
	require.Nil(t, db.Cmd("HSET", itemsKey, "foo", "bar").Err)
	require.Nil(t, db.Cmd("LPUSH", claimedKey, "foo").Err)

	validateClaimedEvents()

	l, err := db.Cmd("LRANGE", claimedKey, 0, -1).List()
	require.Nil(t, err)
	assert.Empty(t, l)

	l, err = db.Cmd("LRANGE", unclaimedKey, 0, -1).List()
	require.Nil(t, err)
	assert.Equal(t, []string{"foo"}, l)
}

// There's a race condition where an item get's ackd just after being found by
// the restore process. The process checks if the item lock exists, it doesn't
// because the ack just deleted it, and then attempts to move the item from the
// claimed to the unclaimed queue. We need to make sure it doesn't appear in the
// unclaimed queue in this case
func TestRestoreRace(t *T) {

	q := clients.RandQueueName()
	unclaimedKey := db.UnclaimedKey(q)
	claimedKey := db.ClaimedKey(q)

	require.Nil(t, db.Cmd("LPUSH", unclaimedKey, "buz", "boz").Err)
	require.Nil(t, db.Cmd("LPUSH", claimedKey, "bar", "baz").Err)

	// restore an event which is in the claimed list, and one which is not
	require.Nil(t, restoreEventToQueue(q, "bar"))
	require.Nil(t, restoreEventToQueue(q, "foo"))

	l, err := db.Cmd("LRANGE", claimedKey, 0, -1).List()
	require.Nil(t, err)
	assert.Equal(t, []string{"baz"}, l)

	l, err = db.Cmd("LRANGE", unclaimedKey, 0, -1).List()
	require.Nil(t, err)
	assert.Equal(t, []string{"boz", "buz", "bar"}, l)
}
