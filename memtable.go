package validb

import (
	"sync"

	"github.com/huandu/skiplist"
)

type Entry struct {
	Key   []byte
	Value []byte
}

type MemTable struct {
	list      *skiplist.SkipList
	byteSize  int64
	threshold int64
	mu        sync.RWMutex
}

func NewMemTable(thresholdBytes int64) *MemTable {
	return &MemTable{
		list:      skiplist.New(skiplist.Bytes),
		threshold: thresholdBytes,
		byteSize:  0,
	}
}

func (m *MemTable) Set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	newEntrySize := int64(len(key) + len(value))

	if oldElem := m.list.Get(key); oldElem != nil {
		oldVal := oldElem.Value.([]byte)
		m.byteSize -= int64(len(key) + len(oldVal))
	}

	m.list.Set(key, value)
	m.byteSize += newEntrySize
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.list.GetValue(key)
	if !ok {
		return nil, false
	}
	return val.([]byte), true
}

func (m *MemTable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byteSize >= m.threshold
}

func (m *MemTable) AllEntries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := make([]Entry, 0, m.list.Len())
	for elem := m.list.Front(); elem != nil; elem = elem.Next() {
		entries = append(entries, Entry{
			Key:   elem.Key().([]byte),
			Value: elem.Value.([]byte),
		})
	}
	return entries
}

func (m *MemTable) ForEach(fn func(key, value []byte) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for elem := m.list.Front(); elem != nil; elem = elem.Next() {
		if !fn(elem.Key().([]byte), elem.Value.([]byte)) {
			break // allow stopping early
		}
	}
}
func (m *MemTable) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.Init()
	m.byteSize = 0
}
