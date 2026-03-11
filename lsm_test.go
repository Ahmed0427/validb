package validb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLSMBasics(t *testing.T) {
	tmp := t.TempDir()
	lsm, err := NewLSMTree(tmp, 1024, []int{2, 2})
	require.NoError(t, err, "Failed to initialize LSM Tree")
	defer lsm.Close()

	key, val := []byte("foo"), []byte("bar")

	lsm.Set(key, val)
	got, ok := lsm.Get(key)
	assert.True(t, ok, "Key should exist")
	assert.Equal(t, val, got, "Value mismatch on initial Get")

	newVal := []byte("baz")
	lsm.Set(key, newVal)
	got, ok = lsm.Get(key)
	assert.True(t, ok)
	assert.Equal(t, newVal, got, "Value should be updated to 'baz'")

	lsm.Delete(key)
	_, ok = lsm.Get(key)
	assert.False(t, ok, "Key should be deleted (tombstoned)")
}

func TestLSMRecovery(t *testing.T) {
	tmp := t.TempDir()
	lsm, err := NewLSMTree(tmp, 10*1024, []int{5})
	require.NoError(t, err)

	key, val := []byte("persist-me"), []byte("data")
	lsm.Set(key, val)
	lsm.Close()

	lsm2, err := NewLSMTree(tmp, 10*1024, []int{5})
	require.NoError(t, err, "Failed to reopen LSM Tree for recovery")
	defer lsm2.Close()

	got, ok := lsm2.Get(key)
	assert.True(t, ok, "Data should be recovered from WAL")
	assert.Equal(t, val, got)
}

func TestLSMCompactionTrigger(t *testing.T) {
	tmp := t.TempDir()
	lsm, err := NewLSMTree(tmp, 20*20, []int{2, 2})
	require.NoError(t, err)
	defer lsm.Close()

	v := []byte("xxxx")
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("key-%02d", i))
		lsm.Set(k, v)
	}

	got, ok := lsm.Get([]byte("key-00"))
	assert.True(t, ok, "key-00 should still exist after compaction")
	assert.Equal(t, v, got)
}
