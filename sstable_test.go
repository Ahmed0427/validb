package validb

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableFullLifecycle(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "validb_sstable")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	path := tmpFile.Name()
	tmpFile.Close()

	mt := NewMemTable(1024 * 1024)
	mt.Set([]byte("user:1"), []byte("Alice"))
	mt.Set([]byte("user:2"), []byte("Bob"))
	mt.Set([]byte("user:3"), []byte("Charlie"))
	mt.Set([]byte("user:2"), nil) // Delete Bob (Tombstone)

	writer, err := NewSSTableWriter(path, mt.Size())
	require.NoError(t, err)
	err = writer.WriteFromMemTable(mt)
	require.NoError(t, err)

	reader, err := OpenSSTable(path)
	require.NoError(t, err)
	defer reader.Close()

	t.Run("Retrieve Active Key", func(t *testing.T) {
		val, found, err := reader.Get([]byte("user:1"))
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("Alice"), val)
	})

	t.Run("Handle Tombstone Persistence", func(t *testing.T) {
		val, found, err := reader.Get([]byte("user:2"))
		assert.NoError(t, err)
		assert.True(t, found, "Tombstone should be found to prevent checking older levels")
		assert.Nil(t, val, "Tombstone value should be nil")
	})

	t.Run("Non-Existent Key", func(t *testing.T) {
		val, found, err := reader.Get([]byte("user:99"))
		assert.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, val)
	})
}

func TestSSTableLargeVolume(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "validb_large")
	path := tmpFile.Name()
	defer os.Remove(path)
	tmpFile.Close()

	count := 2000
	mt := NewMemTable(10 * 1024 * 1024)

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		mt.Set(key, val)
	}

	writer, _ := NewSSTableWriter(path, count)
	require.NoError(t, writer.WriteFromMemTable(mt))

	reader, _ := OpenSSTable(path)
	defer reader.Close()

	samples := []int{0, 127, 128, 129, 500, 1999}
	for _, i := range samples {
		key := []byte(fmt.Sprintf("key-%05d", i))
		expected := []byte(fmt.Sprintf("val-%05d", i))

		val, found, err := reader.Get(key)
		assert.NoError(t, err)
		assert.True(t, found, "Failed at index %d", i)
		assert.Equal(t, expected, val)
	}
}

func TestSSTableForEach(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "validb_foreach")
	path := tmpFile.Name()
	defer os.Remove(path)
	tmpFile.Close()

	count := 2000
	mt := NewMemTable(10 * 1024 * 1024)

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		mt.Set(key, val)
	}

	writer, _ := NewSSTableWriter(path, count)
	require.NoError(t, writer.WriteFromMemTable(mt))

	reader, _ := OpenSSTable(path)
	defer reader.Close()

	i := 0
	err := reader.ForEach(func(key, val []byte) bool {
		originalKey := []byte(fmt.Sprintf("key-%05d", i))
		originalVal := []byte(fmt.Sprintf("val-%05d", i))
		assert.True(t, bytes.Compare(originalKey, key) == 0)
		assert.True(t, bytes.Compare(originalVal, val) == 0)
		i++

		return true
	})
	require.Nil(t, err)
	assert.Equal(t, i, count)
}

func TestSSTableForEachTombstones(t *testing.T) {
	path := "test_tombstone.sst"
	defer os.Remove(path)

	mt := NewMemTable(1024)
	mt.Set([]byte("key-1"), []byte("value-1"))
	mt.Set([]byte("key-2"), nil)

	writer, _ := NewSSTableWriter(path, 2)
	writer.WriteFromMemTable(mt)

	reader, _ := OpenSSTable(path)

	foundDeleted := false
	reader.ForEach(func(key, val []byte) bool {
		if string(key) == "key-2" && val == nil {
			foundDeleted = true
		}
		return true
	})
	assert.True(t, foundDeleted)
}

func TestSSTableTombstoneProgression(t *testing.T) {
	path := "tombstone_trap.sst"
	defer os.Remove(path)

	mt := NewMemTable(1024)
	mt.Set([]byte("key1"), []byte("val1"))
	mt.Set([]byte("key2"), nil)
	mt.Set([]byte("key3"), []byte("val3"))

	// Write it out
	writer, _ := NewSSTableWriter(path, 3)
	require.NoError(t, writer.WriteFromMemTable(mt))

	reader, _ := OpenSSTable(path)
	defer reader.Close()

	var keysRead []string
	err := reader.ForEach(func(k, v []byte) bool {
		keysRead = append(keysRead, string(k))
		return true
	})

	require.NoError(t, err)
	assert.Equal(t, 3, len(keysRead))
	assert.Equal(t, "key3", keysRead[2])
}

func TestSSTableHardSemantics(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "validb_hard")
	path := tmpFile.Name()
	defer os.Remove(path)
	tmpFile.Close()

	mt := NewMemTable(1024)
	mt.Set([]byte("c_deleted"), nil)
	mt.Set([]byte("a_empty"), []byte(""))
	mt.Set([]byte("b_data"), []byte("val"))
	mt.Set([]byte("d_last"), []byte("end"))

	writer, _ := NewSSTableWriter(path, mt.Size())
	require.NoError(t, writer.WriteFromMemTable(mt))

	reader, _ := OpenSSTable(path)
	defer reader.Close()

	expectedKeys := []string{"a_empty", "b_data", "c_deleted", "d_last"}
	idx := 0

	err := reader.ForEach(func(k, v []byte) bool {
		require.Less(t, idx, len(expectedKeys), "Read more keys than were written")

		assert.Equal(t, expectedKeys[idx], string(k), "Keys out of order or missing")

		switch string(k) {
		case "a_empty":
			assert.NotNil(t, v, "Empty key should be non-nil []byte{}")
			assert.Equal(t, 0, len(v))
		case "c_deleted":
			assert.Nil(t, v, "Tombstone key must be explicitly nil")
		case "b_data", "d_last":
			assert.NotNil(t, v)
			assert.Greater(t, len(v), 0)
		}

		idx++
		return true
	})

	require.NoError(t, err)
	assert.Equal(t, 4, idx)
}

func TestSSTableBloomFilterFalsePositives(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "sstable_bloom")
	path := tmpFile.Name()
	defer os.Remove(path)

	mt := NewMemTable(1024)
	mt.Set([]byte("apple"), []byte("1"))
	mt.Set([]byte("banana"), []byte("2"))

	writer, _ := NewSSTableWriter(path, 2)
	writer.WriteFromMemTable(mt)

	reader, _ := OpenSSTable(path)
	defer reader.Close()

	assert.True(
		t,
		reader.bloomFilter.Test([]byte("apple")),
		"Bloom filter should have apple",
	)
	assert.False(
		t,
		reader.bloomFilter.Test([]byte("dragonfruit")),
		"Bloom filter should NOT have dragonfruit",
	)

	_, found, _ := reader.Get([]byte("dragonfruit"))
	assert.False(t, found)
}

func TestSSTableSparseIndexBoundaries(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "sstable_sparse")
	path := tmpFile.Name()
	defer os.Remove(path)

	mt := NewMemTable(10 * 1024 * 1024)
	interval := 128
	totalEntries := interval*2 + 10

	for i := 0; i < totalEntries; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		val := []byte(fmt.Sprintf("v%04d", i))
		mt.Set(key, val)
	}

	writer, _ := NewSSTableWriter(path, totalEntries)
	writer.indexInterval = interval
	writer.WriteFromMemTable(mt)

	reader, _ := OpenSSTable(path)
	defer reader.Close()

	assert.Equal(t, 3, len(reader.sparseIndex))

	testCases := []struct {
		idx int
		msg string
	}{
		{0, "First key in first block"},
		{127, "Last key in first block"},
		{128, "First key in second block (index entry)"},
		{129, "Second key in second block"},
		{255, "Last key in second block"},
		{256, "First key in third block"},
	}

	for _, tc := range testCases {
		key := []byte(fmt.Sprintf("k%04d", tc.idx))
		val, found, err := reader.Get(key)
		require.NoError(t, err)
		assert.True(t, found, tc.msg)
		assert.Equal(t, []byte(fmt.Sprintf("v%04d", tc.idx)), val, tc.msg)
	}
}
