package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/ahmed0427/validb"
)

func main() {
	dir := flag.String("dir", "demo-data", "data directory")
	n := flag.Int("n", 200, "number of keys to write")
	del := flag.Bool("del", false, "delete every key after writing it")
	vsize := flag.Int("v", 100, "value size in bytes")
	batch := flag.Int("batch", 1, "keys per write: 1 = Set, more = Batch")
	flag.Parse()

	db, err := validb.NewLSMTree(*dir, 64*1024, []int{4, 4})
	if err != nil {
		log.Fatal(err)
	}

	// writes
	val := make([]byte, *vsize)
	writes := 0
	start := time.Now()
	for i := 0; i < *n; i += *batch {
		var b validb.Batch
		for j := i; j < i+*batch && j < *n; j++ {
			b.Set([]byte(fmt.Sprintf("key-%08d", j)), val)
		}
		if err := db.Write(&b); err != nil {
			log.Fatal(err)
		}
		writes += b.Len()
	}
	if *del {
		for i := 0; i < *n; i++ {
			if err := db.Delete([]byte(fmt.Sprintf("key-%08d", i))); err != nil {
				log.Fatal(err)
			}
			writes++
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("writes: %d in %v (%.0f writes/s)\n",
		writes, elapsed.Round(time.Millisecond), float64(writes)/elapsed.Seconds())

	// reads
	found := 0
	start = time.Now()
	for i := 0; i < *n; i++ {
		if _, ok := db.Get([]byte(fmt.Sprintf("key-%08d", i))); ok {
			found++
		}
	}
	fmt.Printf("reads:  %d/%d found in %v\n", found, *n,
		time.Since(start).Round(time.Millisecond))

	// memory
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("memory: %.1f MiB heap in use\n", float64(m.HeapInuse)/(1<<20))

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// disk
	entries, err := os.ReadDir(*dir)
	if err != nil {
		log.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			log.Fatal(err)
		}
		total += info.Size()
		fmt.Printf("  %-30s %9d bytes\n", e.Name(), info.Size())
	}
	raw := *n * (12 + *vsize) // 12 = len("key-00000000")
	fmt.Printf("disk:   %d files, %.1f KiB (keys+values written: %.1f KiB)\n",
		len(entries), float64(total)/1024, float64(raw)/1024)
}
