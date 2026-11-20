package validb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	db, err := NewLSMTree(dir, 1<<20, []int{4})
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("old"), []byte("x")))

	var b Batch
	b.Set([]byte("a"), []byte("1"))
	b.Set([]byte("empty"), []byte{}) // empty value is NOT a delete
	b.Delete([]byte("old"))
	require.NoError(t, db.Write(&b))

	require.NoError(t, db.wal.Close())

	db, err = NewLSMTree(dir, 1<<20, []int{4})
	require.NoError(t, err)
	defer db.Close()

	got, ok := db.Get([]byte("a"))
	assert.True(t, ok)
	assert.Equal(t, "1", string(got))
	got, ok = db.Get([]byte("empty"))
	assert.True(t, ok)
	assert.Empty(t, got)
	_, ok = db.Get([]byte("old"))
	assert.False(t, ok)
}

func TestBatchValidation(t *testing.T) {
	db, err := NewLSMTree(t.TempDir(), 1<<20, []int{4})
	require.NoError(t, err)
	defer db.Close()

	var b Batch
	b.Set([]byte("k"), nil)
	assert.ErrorIs(t, db.Write(&b), ErrValueIsNil)

	b = Batch{}
	b.Delete(nil)
	assert.ErrorIs(t, db.Write(&b), ErrKeyIsEmpty)

	assert.NoError(t, db.Write(&Batch{}), "empty batch is a no-op")
}
