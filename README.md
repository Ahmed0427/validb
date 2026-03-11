# validb

A persistent key-value store built on a Log-Structured Merge-tree (LSM-tree), written in Go. Built from scratch, including the WAL, MemTable, and SSTable layers.

## Features

- **Write-ahead log (WAL)** with CRC checksums and batched fsync
- **MemTable** backed by a skip list, with tombstone support for deletes
- **SSTables** with a bloom filter, sparse index, and support for tombstones
- **K-way merge compaction** using a min-heap, with key deduplication across levels
- **WAL recovery** on startup, unflushed memtable entries are replayed automatically
- **Leveled compaction** files cascade from L0 → L1 → L2 as thresholds are exceeded

## Usage

```go
lsm, err := validb.NewLSMTree(
    "/path/to/data",  // directory for SSTables and WAL
    8 * 1024 * 1024,  // memtable flush threshold in bytes
    []int{5, 5, 5},   // max SST files per level before compaction triggers
)
if err != nil {
    log.Fatal(err)
}
defer lsm.Close()

// Write
err = lsm.Set([]byte("hello"), []byte("world"))

// Read
val, ok := lsm.Get([]byte("hello"))

// Delete (writes a tombstone)
err = lsm.Delete([]byte("hello"))

```

`Close` flushes any remaining memtable entries to disk before shutting down.

## Architecture

```
Write path:   WAL append → MemTable → (flush on full) → L0 SSTable → compaction → L1, L2, ...
Read path:    Get → MemTable → L0 SSTables (newest first) → L1 → L2 → ...

```

### WAL

Every write is appended to the WAL before touching the memtable. On startup, any entries not yet flushed to an SSTable are replayed to reconstruct the memtable. The WAL is cleared after each successful flush.

### MemTable

An in-memory skip list ordered by key. Setting a key to `nil` writes a tombstone, which shadows any older value in the SSTables below. The memtable is flushed to a new L0 SSTable when its size exceeds the configured threshold.

### SSTable file layout

Each `.sst` file contains, in order:

```
[data block]    key/value pairs, length-prefixed, tombstones included
[index block]   sparse index of (key, offset) pairs for binary search
[bloom filter]  probabilistic membership test to skip unnecessary reads
[footer]        offsets to the index and bloom filter blocks

```

Files are named `L{level}_{timestamp}.sst` and sorted lexicographically within each level. Newer files within a level are searched first.

### Compaction

When a level exceeds its file limit, all files at that level are merged into a single SSTable at the next level. Merging uses a min-heap across all iterators (k-way merge), and duplicate keys are deduplicated, the entry from the newest file wins. Tombstones are preserved through compaction until they reach the deepest level.

Compaction is triggered inline on every flush and cascades upward if the target level also exceeds its limit after receiving the new file.
