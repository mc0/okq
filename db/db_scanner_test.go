package db

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	. "testing"
)

func TestScan(t *T) {
	redisClient, err := RedisPool.Get()
	require.Nil(t, err)

	keys := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		keys[fmt.Sprintf("scantest:%d", i)] = struct{}{}
	}

	for key := range keys {
		require.Nil(t, redisClient.Cmd("SET", key, key).Err)
	}

	output := map[string]struct{}{}
	for r := range Scan(redisClient, "scantest:*") {
		require.Nil(t, r.Err)
		output[r.Result] = struct{}{}
	}
	assert.Equal(t, keys, output)

	// Also test ScanWrapped while we're here
	output2 := map[string]struct{}{}
	for r := range ScanWrapped("scantest:*") {
		output2[r] = struct{}{}
	}
	assert.Equal(t, keys, output2)

}
