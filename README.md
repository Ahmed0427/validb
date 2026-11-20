# validb

An embedded key-value store for Go, built on a log-structured merge-tree (LSM-tree). The
WAL, memtable, SSTables and compaction are all written from scratch.

## Features

- Every write is in the WAL and fsynced before `Set`, `Delete` or `Write` returns.
- `Batch` groups many sets and deletes into one WAL record: atomic, with a single fsync.
- Recovery replays the WAL on open and cuts off a torn tail left by a crash.
- SSTables are written to a `.tmp` file, fsynced, then renamed, so a crash never leaves a
  half-written table.
- Each SSTable has a bloom filter and a sparse index.
- Leveled compaction keeps the last level as one table and drops tombstones there.

## Install

```
go get github.com/ahmed0427/validb
```

## Usage

```go
package main

import (
	"fmt"
	"log"

	"github.com/ahmed0427/validb"
)

func main() {
	db, err := validb.NewLSMTree(
		"data",      // directory for the WAL and the .sst files
		4<<20,       // flush the memtable to disk once it holds 4 MiB
		[]int{4, 4}, // max tables in L0 and L1; one more level holds the rest
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close() // flushes the memtable

	if err := db.Set([]byte("user:1"), []byte("alice")); err != nil {
		log.Fatal(err)
	}
	if val, ok := db.Get([]byte("user:1")); ok {
		fmt.Println(string(val)) // alice
	}
	if err := db.Delete([]byte("user:1")); err != nil {
		log.Fatal(err)
	}

	// Many writes, one fsync. Either all of them survive a crash or none does.
	var b validb.Batch
	b.Set([]byte("user:2"), []byte("bob"))
	b.Set([]byte("user:3"), []byte("carol"))
	b.Delete([]byte("user:4"))
	if err := db.Write(&b); err != nil {
		log.Fatal(err)
	}
}
```

Notes:

- Keys must be non-empty. Values must be non-nil, but an empty value is allowed.
- `Set`, `Delete` and `Batch` copy their arguments, so the caller may reuse its buffers.
- A single `Set` costs one fsync, about 1 ms on most disks. Use a `Batch` for bulk loads.
- All methods are safe for concurrent use. Writes are serialized by one lock.

## Demo

`cmd/demo` writes and reads keys, then prints throughput, heap use and the files on disk:

```
go run ./cmd/demo -n 2000              # one Set per key
go run ./cmd/demo -n 2000 -batch 100   # 100 keys per Batch
go run ./cmd/demo -n 2000 -del         # also delete every key after writing it
```

## How it works

```
write: WAL (fsync) -> memtable -> full? flush to L0 table -> L0 full? compact into L1 ...
read:  memtable -> L0 (newest first) -> L1 -> ... -> last level
```

**WAL.** Each record is `[crc32][op][timestamp][keyLen][valLen][key][value]`. A batch is
one record whose value holds all its operations. The WAL is truncated after every flush.

**Memtable.** A skip list sorted by key. A delete stores a nil value (a tombstone) that
hides older values in the tables below.

**SSTable.** A file named `L{level}_{unixnano}.sst`, laid out as:

```
[data]    [keyLen 4][valLen 4][key][value] ... sorted; valLen 0xFFFFFFFF is a tombstone
[bloom]   bloom filter, skips tables that cannot hold the key
[index]   [keyLen 4][key][offset 8] for every 128th entry
[footer]  [bloom offset 8][index offset 8][magic 4]
```

**Compaction.** When a level reaches its table limit, all its tables are merged with a
k-way heap merge into one table on the next level. For a key that appears more than once,
the newest version wins. Merges into the last level also take in that level's existing
table, so it stays a single table with nothing older below it, and tombstones are dropped.

## Testing

```
go test -race ./...
```

The tests include a randomized test that checks the store against a Go map across reopens,
and crash tests for torn WAL tails, batches and unfinished SSTables.
