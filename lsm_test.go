package validb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

	// Threshold small enough that each Set flushes: "key-XX"(6) + "xxxx"(4) = 10 bytes per entry.
	// Threshold of 1 guarantees a flush on every write, forcing L0 files to pile up fast.
	lsm, err := NewLSMTree(tmp, 1, []int{2, 2})
	require.NoError(t, err)

	v := []byte("xxxx")
	keys := make([][]byte, 20)
	for i := 0; i < 20; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%02d", i))
		require.NoError(t, lsm.Set(keys[i], v))
	}

	// Count SST files and assert compaction has actually happened.
	// With threshold=1 and maxFiles=[2,2], 20 writes produce 20 L0 files,
	// which must have cascaded — we should see far fewer files than 20.
	sstFiles := listSSTFiles(t, tmp)
	t.Logf("SST files after 20 writes: %v", sstFiles)
	assert.Less(t, len(sstFiles), 20,
		"compaction should have reduced the number of SST files")

	// No L0 files should remain — they must have been compacted into L1/L2.
	for _, f := range sstFiles {
		assert.False(t, strings.HasPrefix(f, "L0"),
			"unexpected L0 file after compaction: %s", f)
	}

	// All written keys must still be readable.
	for _, k := range keys {
		got, ok := lsm.Get(k)
		assert.True(t, ok, "key %s should exist after compaction", k)
		assert.Equal(t, v, got, "key %s has wrong value", k)
	}

	// Overwrite half the keys — compaction must not resurrect the old values.
	v2 := []byte("yyyy")
	for i := 0; i < 10; i++ {
		require.NoError(t, lsm.Set(keys[i], v2))
	}
	for i := 0; i < 20; i++ {
		got, ok := lsm.Get(keys[i])
		assert.True(t, ok, "key %s missing after overwrite", keys[i])
		if i < 10 {
			assert.Equal(t, v2, got, "key %s should have new value", keys[i])
		} else {
			assert.Equal(t, v, got, "key %s should still have old value", keys[i])
		}
	}

	// Delete half the keys — compaction must honour tombstones.
	for i := 0; i < 10; i++ {
		require.NoError(t, lsm.Delete(keys[i]))
	}
	for i := 0; i < 20; i++ {
		got, ok := lsm.Get(keys[i])
		if i < 10 {
			assert.False(t, ok, "key %s should be deleted", keys[i])
			assert.Nil(t, got)
		} else {
			assert.True(t, ok, "key %s should still exist", keys[i])
			assert.Equal(t, v, got)
		}
	}

	require.NoError(t, lsm.Close())

	// Reopen and verify everything survived.
	lsm2, err := NewLSMTree(tmp, 1, []int{2, 2})
	require.NoError(t, err)
	defer lsm2.Close()

	for i := 0; i < 20; i++ {
		got, ok := lsm2.Get(keys[i])
		if i < 10 {
			assert.False(t, ok, "key %s should still be deleted after reopen", keys[i])
			assert.Nil(t, got)
		} else {
			assert.True(t, ok, "key %s should survive reopen", keys[i])
			assert.Equal(t, string(v), string(got))
		}
	}
}

func TestSetCopiesCallerBuffers(t *testing.T) {
	db, err := NewLSMTree(t.TempDir(), 1<<20, []int{4})
	require.NoError(t, err)
	defer db.Close()

	key, val := []byte("k1"), []byte("v1")
	require.NoError(t, db.Set(key, val))
	copy(key, "k2") // reuse the buffers for the next write
	copy(val, "v2")
	require.NoError(t, db.Set(key, val))

	got, ok := db.Get([]byte("k1"))
	assert.True(t, ok)
	assert.Equal(t, "v1", string(got))
	got, ok = db.Get([]byte("k2"))
	assert.True(t, ok)
	assert.Equal(t, "v2", string(got))
}

func TestDeletesFlushMemtable(t *testing.T) {
	db, err := NewLSMTree(t.TempDir(), 64, []int{100})
	require.NoError(t, err)
	defer db.Close()

	for i := 0; i < 100; i++ {
		require.NoError(t, db.Delete([]byte(fmt.Sprintf("key-%03d", i))))
	}
	assert.False(t, db.memTable.IsFull(), "memtable grew past its limit")
}

func TestLevelConfigNotShared(t *testing.T) {
	levels := make([]int, 2, 3)
	levels[0], levels[1] = 4, 4
	db, err := NewLSMTree(t.TempDir(), 1<<20, levels)
	require.NoError(t, err)
	defer db.Close()

	levels = append(levels, 1) // caller reuses its slice
	assert.Equal(t, math.MaxInt, db.levelMan.maxFilesPerLevel[2])
}

// A crash in the middle of a WAL write leaves a half record at the end.
// Reopen must keep every complete record, cut off the half one, and
// keep working (new writes must not land after the garbage).
func TestTornWALTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	db, err := NewLSMTree(dir, 1<<20, []int{4})
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("a"), []byte("1")))
	require.NoError(t, db.wal.Close()) // crash: skip db.Close()

	f, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{1, 2, 3, 4, 0, 0, 0}) // 7 of the 21 header bytes
	require.NoError(t, err)
	require.NoError(t, f.Close())

	db, err = NewLSMTree(dir, 1<<20, []int{4})
	require.NoError(t, err, "a torn tail must not stop the DB from opening")
	require.NoError(t, db.Set([]byte("b"), []byte("2")))
	require.NoError(t, db.wal.Close()) // crash again

	db, err = NewLSMTree(dir, 1<<20, []int{4})
	require.NoError(t, err)
	defer db.Close()
	for _, k := range []string{"a", "b"} {
		_, ok := db.Get([]byte(k))
		assert.True(t, ok, k)
	}
}

func listSSTFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}
