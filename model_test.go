package validb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRandomOpsMatchModel does random Sets and Deletes on the database and,
// at the same time, on a plain Go map (the "model"). The memtable is tiny,
// so flushes and compactions happen all the time, and the database is
// closed and reopened between rounds. After every round, every key must
// give the same answer as the map.
func TestRandomOpsMatchModel(t *testing.T) {
	const (
		rounds       = 5
		opsPerRound  = 1000
		keySpace     = 200 // few keys => many overwrites and deletes
		memtableSize = 512 // bytes => a flush every ~20 writes
	)
	maxFiles := []int{2, 2} // => compactions L0->L1->L2 all the time

	dir := t.TempDir()
	rng := rand.New(rand.NewSource(42)) // fixed seed: failures are reproducible
	model := map[string]string{}

	db, err := NewLSMTree(dir, memtableSize, maxFiles)
	require.NoError(t, err)

	for round := 0; round < rounds; round++ {
		for i := 0; i < opsPerRound; i++ {
			key := fmt.Sprintf("key-%03d", rng.Intn(keySpace))
			if rng.Intn(4) == 0 { // 25% deletes
				require.NoError(t, db.Delete([]byte(key)))
				delete(model, key)
			} else {
				val := fmt.Sprintf("val-%d-%d", round, i)
				require.NoError(t, db.Set([]byte(key), []byte(val)))
				model[key] = val
			}
		}
		checkAgainstModel(t, db, model, keySpace)

		// reopen: everything must survive a Close + NewLSMTree
		require.NoError(t, db.Close())
		db, err = NewLSMTree(dir, memtableSize, maxFiles)
		require.NoError(t, err)
		checkAgainstModel(t, db, model, keySpace)
	}
	require.NoError(t, db.Close())
}

func checkAgainstModel(t *testing.T, db *LSMTree, model map[string]string, keySpace int) {
	t.Helper()
	for i := 0; i < keySpace; i++ {
		key := fmt.Sprintf("key-%03d", i)
		got, ok := db.Get([]byte(key))
		want, exists := model[key]
		require.Equal(t, exists, ok, "key %s: exists in model=%v, found in db=%v", key, exists, ok)
		if exists {
			require.Equal(t, want, string(got), "key %s has wrong value", key)
		}
	}
}
