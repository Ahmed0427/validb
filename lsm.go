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
		_, err := fmt.Sscanf(name, "L%d_", &lvl)
		if err != nil || lvl < 0 || lvl >= numLevels {
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
	if level < len(lm.maxFilesPerLevel) {
		return len(lm.levels[level]) > lm.maxFilesPerLevel[level]
	}
	return false
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
	mu        sync.Mutex
	basePath  string
	memTable  *MemTable
	levelMan  *levelManager
	compactCh chan int
	closeCh   chan struct{}
	wal       *WAL
	wg        sync.WaitGroup
}

func NewLSMTree(basePath string, thresholdBytes int,
	maxFilesPerLevel []int) (*LSMTree, error) {

	if err := ensureDir(basePath); err != nil {
		return nil, err
	}

	pathWAL := filepath.Join(basePath, "wal")
	wal, err := NewWAL(pathWAL)
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

	l := &LSMTree{
		memTable:  memTable,
		basePath:  basePath,
		levelMan:  levelMan,
		compactCh: make(chan int, levelMan.numLevels),
		closeCh:   make(chan struct{}),
		wal:       wal,
	}

	l.wg.Add(1)
	go l.compactionWorker()

	return l, nil
}

func (l *LSMTree) compactionWorker() {
	defer l.wg.Done()
	for {
		select {
		case <-l.closeCh:
			return
		case level := <-l.compactCh:
			l.compact(level)
		}
	}
}

type mergeItem struct {
	key       []byte
	value     []byte // nil == tombstone
	readerIdx int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if cmp := bytes.Compare(h[i].key, h[j].key); cmp != 0 {
		return cmp < 0
	}
	// same key: higher readeridx is newer, so it should come first (min-heap
	// pops the smallest, so "smaller" here means "should be emitted first").
	return h[i].readerIdx > h[j].readerIdx
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(mergeItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (l *LSMTree) compact(level int) {
	if level >= l.levelMan.numLevels-1 {
		return
	}
	l.mu.Lock()
	files := make([]string, len(l.levelMan.levels[level]))
	copy(files, l.levelMan.levels[level])
	l.mu.Unlock()

	if len(files) == 0 {
		return
	}

	readers := make([]*SSTableReader, 0, len(files))
	iterators := make([]*SSTableIterator, len(readers))
	for _, name := range files {
		path := filepath.Join(l.basePath, name)
		r, err := openSSTable(path)
		if err != nil {
			continue
		}
		readers = append(readers, r)
		iterators = append(iterators, r.newIterator())
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
	w, err := newSSTableWriter(newPath, 0)
	if err != nil {
		return
	}

	var lastWrittenKey []byte = nil
	for h.Len() > 0 {
		item := heap.Pop(h).(mergeItem)
		if ok := iterators[item.readerIdx].next(); ok {
			heap.Push(h, mergeItem{
				key:       iterators[item.readerIdx].ent.key,
				value:     iterators[item.readerIdx].ent.value,
				readerIdx: item.readerIdx,
			})
		}

		if lastWrittenKey != nil && bytes.Equal(lastWrittenKey, item.key) {
			continue
		}

		if err := w.writeEntry(item.key, item.value); err != nil {
			w.close()
			os.Remove(newPath)
			return
		}
		lastWrittenKey = item.key
	}

	if err := w.writeMetadata(); err != nil {
		w.close()
		os.Remove(newPath)
		return
	}

	l.mu.Lock()
	for _, name := range files {
		l.levelMan.remove(level, name)
		os.Remove(filepath.Join(l.basePath, name))
	}
	l.levelMan.add(level+1, newName)
	needNext := l.levelMan.needsCompaction(level + 1)
	l.mu.Unlock()

	if needNext {
		select {
		case l.compactCh <- level + 1:
		default:
		}
	}
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

	if val, ok := l.memTable.Get(key); ok {
		l.mu.Unlock()
		return val, val != nil // nil is a tombstone
	}
	l.mu.Unlock()

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

func (l *LSMTree) flushMemTable() error {
	name := fmt.Sprintf("L0_%d.sst", time.Now().UnixNano())
	path := filepath.Join(l.basePath, name)

	writer, err := newSSTableWriter(path, l.memTable.Size())
	if err != nil {
		return err
	}
	if err := writer.writeFromMemTable(l.memTable); err != nil {
		return err
	}

	l.levelMan.add(0, name)
	l.memTable.Reset()
	if err := l.wal.Clear(); err != nil {
		return err
	}

	if l.levelMan.needsCompaction(0) {
		select {
		case l.compactCh <- 0:
		default:
		}
	}
	return nil
}

func (l *LSMTree) searchSSTables(key []byte) ([]byte, bool) {
	allFiles := l.levelMan.allFiles()
	for _, name := range allFiles {
		path := filepath.Join(l.basePath, name)
		r, err := openSSTable(path)
		if err != nil {
			continue
		}

		val, found, err := r.get(key)
		r.close()

		if err == nil && found {
			return val, val != nil
		}
	}
	return nil, false
}

func (l *LSMTree) Close() error {
	close(l.closeCh)
	l.wg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.memTable.Size() > 0 {
		return l.flushMemTable()
	}
	return l.wal.Close()
}
