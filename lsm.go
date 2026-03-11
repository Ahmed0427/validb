package validb

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrValueIsNil       = errors.New("value is nil")
	ErrCorruptedBaseDir = errors.New("corrupted base dir")
	ErrBasePathIsFile   = errors.New("base path is a file, expected a directory")
)

type levelManager struct {
	numLevels        int
	maxFilesPerLevel []int
	levels           [][]string
}

func newLevelManager(basePath string, maxFilesPerLevel []int) (*levelManager, error) {
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	maxFilesPerLevel = append(maxFilesPerLevel, math.MaxInt)
	numLevels := len(maxFilesPerLevel)
	lm := &levelManager{
		numLevels:        numLevels,
		maxFilesPerLevel: maxFilesPerLevel,
		levels:           make([][]string, numLevels),
	}

	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) != ".sst" {
			continue
		}
		var lvl int
		if _, err := fmt.Sscanf(name, "L%d_", &lvl); err != nil || lvl < 0 || lvl >= numLevels {
			return nil, ErrCorruptedBaseDir
		}
		lm.levels[lvl] = append(lm.levels[lvl], name)
	}

	for i := range lm.levels {
		sort.Strings(lm.levels[i])
	}
	return lm, nil
}

func (lm *levelManager) add(level int, name string) {
	lm.levels[level] = append(lm.levels[level], name)
	sort.Strings(lm.levels[level])
}

func (lm *levelManager) remove(level int, name string) {
	files := lm.levels[level]
	for i, f := range files {
		if f == name {
			lm.levels[level] = append(files[:i], files[i+1:]...)
			return
		}
	}
}

func (lm *levelManager) needsCompaction(level int) bool {
	return level < len(lm.maxFilesPerLevel) &&
		len(lm.levels[level]) >= lm.maxFilesPerLevel[level]
}

func (lm *levelManager) allFiles() []string {
	var out []string
	for lvl := 0; lvl < lm.numLevels; lvl++ {
		files := lm.levels[lvl]
		for i := len(files) - 1; i >= 0; i-- {
			out = append(out, files[i])
		}
	}
	return out
}

type LSMTree struct {
	mu       sync.Mutex
	basePath string
	memTable *MemTable
	levelMan *levelManager
	wal      *WAL
}

func NewLSMTree(basePath string, thresholdBytes int,
	maxFilesPerLevel []int) (*LSMTree, error) {
	if err := ensureDir(basePath); err != nil {
		return nil, err
	}

	wal, err := NewWAL(filepath.Join(basePath, "wal"))
	if err != nil {
		return nil, fmt.Errorf("new WAL failed: %w", err)
	}

	memTable := NewMemTable(thresholdBytes)
	if err := recoverFromWAL(wal, memTable); err != nil {
		return nil, err
	}

	levelMan, err := newLevelManager(basePath, maxFilesPerLevel)
	if err != nil {
		return nil, err
	}

	return &LSMTree{
		basePath: basePath,
		memTable: memTable,
		levelMan: levelMan,
		wal:      wal,
	}, nil
}

func (l *LSMTree) Set(key, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if value == nil {
		return ErrValueIsNil
	}
	if err := l.wal.Append(OpSet, key, value); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}
	l.memTable.Set(key, value)
	if l.memTable.IsFull() {
		return l.flush()
	}
	return nil
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

func (l *LSMTree) Get(key []byte) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if val, ok := l.memTable.Get(key); ok {
		if val == nil { // tombstone
			return nil, false
		}
		return val, true
	}
	return l.searchSSTables(key)
}

func (l *LSMTree) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.memTable.Size() > 0 {
		if err := l.flush(); err != nil {
			return err
		}
	}
	return l.wal.Close()
}

func (l *LSMTree) flush() error {
	name := fmt.Sprintf("L0_%d.sst", time.Now().UnixNano())
	path := filepath.Join(l.basePath, name)

	w, err := newSSTableWriter(path, l.memTable.Size())
	if err != nil {
		return err
	}
	if err := w.writeFromMemTable(l.memTable); err != nil {
		os.Remove(path)
		return err
	}

	l.levelMan.add(0, name)
	l.memTable.Reset()

	if err := l.wal.Clear(); err != nil {
		return err
	}

	for lvl := 0; lvl < l.levelMan.numLevels-1; lvl++ {
		if l.levelMan.needsCompaction(lvl) {
			if err := l.compact(lvl); err != nil {
				return err
			}
		}
	}

	return nil
}

func (l *LSMTree) compact(level int) error {
	files := make([]string, len(l.levelMan.levels[level]))
	copy(files, l.levelMan.levels[level])

	readers := make([]*SSTableReader, 0, len(files))
	iterators := make([]*SSTableIterator, 0, len(files))
	expectedEntries := 0

	for _, name := range files {
		fullPath := filepath.Join(l.basePath, name)

		r, err := openSSTable(fullPath)
		if err != nil {
			continue
		}
		readers = append(readers, r)
		iterators = append(iterators, r.newIterator())
		expectedEntries += 256
	}

	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()

	h := &mergeHeap{}
	heap.Init(h)
	for i, it := range iterators {
		if it.next() {
			heap.Push(h, mergeItem{key: it.ent.key, value: it.ent.value, readerIdx: i})
		}
	}

	newName := fmt.Sprintf("L%d_%d.sst", level+1, time.Now().UnixNano())
	newPath := filepath.Join(l.basePath, newName)

	w, err := newSSTableWriter(newPath, expectedEntries)
	if err != nil {
		return err
	}

	var lastKey []byte
	var entriesProcessed int

	for h.Len() > 0 {
		item := heap.Pop(h).(mergeItem)
		if it := iterators[item.readerIdx]; it.next() {
			heap.Push(h, mergeItem{
				key: it.ent.key, value: it.ent.value, readerIdx: item.readerIdx,
			})
		}

		if lastKey != nil && bytes.Equal(lastKey, item.key) {
			continue
		}

		if err := w.writeEntry(item.key, item.value); err != nil {
			w.close()
			os.Remove(newPath)
			return err
		}
		lastKey = item.key
		entriesProcessed++
	}

	if err := w.writeMetadata(); err != nil {
		w.close()
		os.Remove(newPath)
		return err
	}
	w.close()

	for _, name := range files {
		l.levelMan.remove(level, name)
		oldPath := filepath.Join(l.basePath, name)
		if err := os.Remove(oldPath); err != nil {
		}
	}

	l.levelMan.add(level+1, newName)

	return nil
}

func (l *LSMTree) searchSSTables(key []byte) ([]byte, bool) {
	for _, name := range l.levelMan.allFiles() {
		r, err := openSSTable(filepath.Join(l.basePath, name))
		if err != nil {
			continue
		}
		val, found, err := r.get(key)
		r.close()
		if err == nil && found {
			if val == nil { // tombstone
				return nil, false
			}
			return val, true
		}
	}
	return nil, false
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

type mergeItem struct {
	key       []byte
	value     []byte
	readerIdx int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if cmp := bytes.Compare(h[i].key, h[j].key); cmp != 0 {
		return cmp < 0
	}
	return h[i].readerIdx > h[j].readerIdx
}
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
