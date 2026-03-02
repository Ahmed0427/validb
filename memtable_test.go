package validb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemTableBasicSetGet(t *testing.T) {
	mt := NewMemTable(1024)
	key := []byte("user:123")
	val := []byte("gemini")

	mt.Set(key, val)

	retrieved, ok := mt.Get(key)
	require.True(t, ok)
	assert.Equal(t, val, retrieved)
}

func TestMemTableSetNil(t *testing.T) {
	mt := NewMemTable(1024)

	key1 := []byte("user:1")

	mt.Set(key1, nil)
	val, ok := mt.Get(key1)
	require.True(t, ok)
	assert.Nil(t, val)

	key2 := []byte("user:2")

	mt.Set(key2, []byte{})
	val, ok = mt.Get(key2)
	require.True(t, ok)
	assert.NotNil(t, val)
}

func TestMemTableUpdateAndSize(t *testing.T) {
	mt := NewMemTable(20)
	key := []byte("k")
	val1 := []byte("v1")

	mt.Set(key, val1)
	assert.Equal(t, 3, mt.byteSize)
	assert.False(t, mt.IsFull())

	val2 := []byte("value_two")
	mt.Set(key, val2)

	assert.Equal(t, 10, mt.byteSize, "Size should reflect the new value length only")

	val, ok := mt.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val2, val)
}

func TestMemTableOrdering(t *testing.T) {
	mt := NewMemTable(1000)

	mt.Set([]byte("c"), []byte("3"))
	mt.Set([]byte("a"), []byte("1"))
	mt.Set([]byte("b"), []byte("2"))

	require.Equal(t, mt.Size(), 3)

	entries := mt.AllEntries()
	require.Equal(t, mt.Size(), len(entries))

	assert.Equal(t, []byte("a"), entries[0].Key)
	assert.Equal(t, []byte("b"), entries[1].Key)
	assert.Equal(t, []byte("c"), entries[2].Key)
}

func TestMemTableIsFull(t *testing.T) {
	threshold := 10
	mt := NewMemTable(threshold)

	mt.Set([]byte("1234"), []byte("567890"))

	assert.True(t, mt.IsFull(), "Should be full when byteSize == threshold")

	mt.Set([]byte("a"), []byte("b"))
	assert.True(t, mt.IsFull())
}

func TestMemTableReset(t *testing.T) {
	mt := NewMemTable(100)
	mt.Set([]byte("key"), []byte("val"))

	mt.Reset()

	assert.Equal(t, 0, mt.byteSize)
	assert.Equal(t, 0, mt.list.Len())

	_, ok := mt.Get([]byte("key"))
	assert.False(t, ok)
}

func TestMemTable_RaceConditionStress(t *testing.T) {
	mt := NewMemTable(1024 * 1024)
	wg := sync.WaitGroup{}

	numGoroutines := 100
	opsPerGoroutine := 500

	// 1. Concurrent Writers
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				val := []byte("value")
				mt.Set(key, val)
			}
		}(i)
	}

	// 2. Concurrent Readers (Heavy Contention)
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				mt.Get([]byte("key-0-0")) // Everyone fights for the same key
				mt.IsFull()
			}
		}()
	}

	// 3. The "Chaos" Goroutine (Frequent Resets)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			mt.Reset()
			mt.AllEntries()
		}
	}()

	wg.Wait()

	// Final sanity check: If we didn't crash or hang, we passed the deadlock test.
	// If 'go test -race' is clean, we passed the race test.
	assert.GreaterOrEqual(t, mt.byteSize, 0)
}
