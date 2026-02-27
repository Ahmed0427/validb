package validb

import (
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALAppendAndIterate(t *testing.T) {
	tmpFile := "test.wal"
	defer os.Remove(tmpFile)

	wal, err := NewWAL(tmpFile)
	require.NoError(t, err)

	// Define test cases
	tests := []struct {
		op    uint8
		key   []byte
		value []byte
	}{
		{OpSet, []byte("user:1"), []byte("alice")},
		{OpSet, []byte("user:2"), []byte("bob")},
		{OpDelete, []byte("user:1"), nil},
	}

	for _, tc := range tests {
		err := wal.Append(tc.op, tc.key, tc.value)
		assert.NoError(t, err)
	}
	wal.Close()

	wal, err = NewWAL(tmpFile)
	require.NoError(t, err)
	defer wal.Close()

	next := wal.NewIterator()

	for _, tc := range tests {
		op, key, val, err := next()

		require.NoError(t, err)
		assert.Equal(t, tc.op, op)
		assert.Equal(t, tc.key, key)

		if tc.value == nil {
			assert.Empty(t, val)
		} else {
			assert.Equal(t, tc.value, val)
		}
	}
}

func TestWALCorruption(t *testing.T) {
	tmpFile := "corrupt.wal"
	defer os.Remove(tmpFile)

	wal, err := NewWAL(tmpFile)
	require.NoError(t, err)

	err = wal.Append(OpSet, []byte("secure"), []byte("data"))
	require.NoError(t, err)
	wal.Close()

	// MANUALLY CORRUPT THE FILE
	data, _ := os.ReadFile(tmpFile)
	data[len(data)-1] ^= 0xFF
	os.WriteFile(tmpFile, data, 0644)

	wal, _ = NewWAL(tmpFile)
	defer wal.Close()
	next := wal.NewIterator()

	_, _, _, err = next()
	assert.ErrorIs(t, err, ErrWALCorrupted)
}

func TestWALTornWrite(t *testing.T) {
	path := "torn.wal"
	defer os.Remove(path)

	// 1. Manually write a half-finished record
	// Write just 5 bytes of a 21-byte header
	f, _ := os.Create(path)
	f.Write([]byte{0, 0, 0, 0, 1})
	f.Sync()
	f.Close()

	wal, _ := NewWAL(path)
	next := wal.NewIterator()
	_, _, _, err := next()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestWALConcurrentAppends(t *testing.T) {
	tmpFile := "concurrent.wal"
	defer os.Remove(tmpFile)

	wal, err := NewWAL(tmpFile)
	require.NoError(t, err)

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100
	totalExpected := numGoroutines * opsPerGoroutine

	writtenKeys := sync.Map{}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				keyStr := fmt.Sprintf("g%d-key-%d", gID, j)
				key := []byte(keyStr)
				val := []byte(fmt.Sprintf("value-%d", j))

				err := wal.Append(OpSet, key, val)
				if assert.NoError(t, err) {
					writtenKeys.Store(keyStr, true)
				}
			}
		}(i)
	}

	wg.Wait()

	// close the wal to ensure the background flusher drains
	err = wal.Close()
	require.NoError(t, err)

	recoveryWAL, err := NewWAL(tmpFile)
	require.NoError(t, err)
	defer recoveryWAL.Close()

	next := recoveryWAL.NewIterator()
	count := 0

	for {
		op, key, _, err := next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err, "Should not have CRC or corruption errors")
		assert.Equal(t, OpSet, op)

		_, exists := writtenKeys.Load(string(key))
		assert.True(t, exists, "Recovered key was never written: %s", string(key))

		count++
	}

	assert.Equal(t, totalExpected, count)
}
