package db

import (
	"fmt"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScan(t *T) {
	keys := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		keys[fmt.Sprintf("scantest:%d", i)] = struct{}{}
	}

	for key := range keys {
		require.Nil(t, Cmd("SET", key, key).Err)
	}

	output := map[string]struct{}{}
	for r := range Scan("scantest:*") {
		output[r] = struct{}{}
	}
	assert.Equal(t, keys, output)

}
