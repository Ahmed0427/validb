package validb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	DefaultThresholdBytes = 32 * 1024 * 1024 // 32MB
	MaxSstableCount       = 4
)

var (
	ErrValueIsNil     = errors.New("value is nil")
	ErrBasePathIsFile = errors.New("base path is a file, expected a directory")
)

type LSMTree struct {
	mu       sync.Mutex
	memTable *MemTable
	sstables []string
	wal      *WAL
	basePath string
}

func NewLSMTree(basePath string, thresholdBytes int) (*LSMTree, error) {
	if err := ensureDir(basePath); err != nil {
		return nil, err
	}

	pathWAL := filepath.Join(basePath, "wal.wal")
	wal, err := NewWAL(pathWAL)
	if err != nil {
		return nil, fmt.Errorf("new WAL failed: %w", err)
	}

	memTable := NewMemTable(thresholdBytes)
	if err := recoverFromWAL(wal, memTable); err != nil {
		return nil, err
	}

	sstables, err := loadSSTableList(basePath)
	if err != nil {
		return nil, err
	}

	return &LSMTree{
		sstables: sstables,
		memTable: memTable,
		basePath: basePath,
		wal:      wal,
	}, nil
}

func (l *LSMTree) Set(key, value []byte) error {
	if value == nil {
		return ErrValueIsNil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.wal.Append(OpSet, key, value); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}

	l.memTable.Set(key, value)

	if l.memTable.IsFull() {
		return l.flushMemTable()
	}
	return nil
}

func (l *LSMTree) Get(key []byte) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Check MemTable
	if val, ok := l.memTable.Get(key); ok {
		return val, val != nil // nil is a tombstone
	}

	// 2. Check SSTables
	return l.searchSSTables(key)
}

func (l *LSMTree) Delete(key []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.wal.Append(OpDelete, key, nil); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}

	l.memTable.Set(key, nil)
	return nil
}

func ensureDir(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(path, 0755)
		}
		return err
	}
	if !stat.IsDir() {
		return ErrBasePathIsFile
	}
	return nil
}

func recoverFromWAL(wal *WAL, memTable *MemTable) error {
	next := wal.NewIterator()
	for {
		op, key, val, err := next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("WAL recovery failed: %w", err)
		}

		if op == OpSet {
			memTable.Set(key, val)
		} else {
			memTable.Set(key, nil)
		}
	}
	return nil
}

func loadSSTableList(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var sstables []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sst" {
			sstables = append(sstables, entry.Name())
		}
	}
	sort.Strings(sstables)
	return sstables, nil
}

func (l *LSMTree) flushMemTable() error {
	name := fmt.Sprintf("%d.sst", time.Now().UnixNano())
	path := filepath.Join(l.basePath, name)

	writer, err := NewSSTableWriter(path, l.memTable.Size())
	if err != nil {
		return err
	}

	if err := writer.WriteFromMemTable(l.memTable); err != nil {
		return err
	}

	l.sstables = append(l.sstables, name)
	l.memTable.Reset()
	return l.wal.Clear()
}

func (l *LSMTree) searchSSTables(key []byte) ([]byte, bool) {
	for i := len(l.sstables) - 1; i >= 0; i-- {
		path := filepath.Join(l.basePath, l.sstables[i])
		r, err := OpenSSTable(path)
		if err != nil {
			continue
		}

		val, found, err := r.Get(key)
		r.Close()

		if err == nil && found {
			return val, val != nil
		}
	}
	return nil, false
}
