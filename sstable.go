package validb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"

	"github.com/bits-and-blooms/bloom"
)

/*
SSTable File Layout:
-------------------------------------------------------------------------
| Data Block (Sorted Key-Value Pairs)                                   |
|   - [KeyLen (4b)][ValueLen (4b)][Key (N bytes)][Value (M bytes)]      |
|   - [KeyLen (4b)][ValueLen (4b)][Key (N bytes)][Value (M bytes)]      |
|   ... (Repeating entries)                                             |
-------------------------------------------------------------------------
| Bloom Filter                                                          |
|   - Serialized bits-and-blooms/bloom filter data                      |
-------------------------------------------------------------------------
| Sparse Index                                                          |
|   - [KeyLen (4b)][Key (N bytes)][Offset (8b)]                         |
|   - ... (Entries typically every 128 keys)                            |
-------------------------------------------------------------------------
| Footer (Fixed 20 bytes)                                               |
|   - [Bloom Filter Offset (8 bytes)]                                   |
|   - [Sparse Index Offset (8 bytes)]                                   |
|   - [Magic Number (0xDEADBEEF) (4 bytes)]                             |
-------------------------------------------------------------------------

Format Details:
1. Data Block: Contains the actual KV pairs sorted by key. A ValueLen
   of 0xFFFFFFFF (MaxUint32) indicates a "tombstone" (deleted record).

2. Bloom Filter: Used for fast membership testing to avoid unnecessary
   disk I/O if a key definitely doesn't exist in this file.

3. Sparse Index: Stores the key and file offset for every Nth record.
   Allows the reader to binary search for a range and then scan a
   small chunk of data.

4. Footer: Located at the very end of the file. Since it has a fixed
   size, the reader can seek to (FileSize - 20) to find the "map"
   required to parse the rest of the file.
*/

const (
	// footer: [BloomOffset(8)][IndexOffset(8)][MagicNumber(4)] = 20 bytes
	FooterSize           = 20
	MagicNumber          = 0xDEADBEEF
	DefaultIndexInterval = 128
)

var (
	ErrFileTooSmall       = errors.New("file too small")
	ErrInvalidMagicNumber = errors.New("invalid magic number")
)

type IndexEntry struct {
	Key    string
	Offset int64
}

type SSTableWriter struct {
	file          *os.File
	sparseIndex   []IndexEntry
	bloomFilter   *bloom.BloomFilter
	indexInterval int
}

func NewSSTableWriter(path string, expectedEntries int) (*SSTableWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &SSTableWriter{
		file:          f,
		sparseIndex:   []IndexEntry{},
		bloomFilter:   bloom.NewWithEstimates(uint(expectedEntries), 0.01),
		indexInterval: DefaultIndexInterval,
	}, nil
}

func (w *SSTableWriter) WriteFromMemTable(mt *MemTable) error {
	var count int
	var writeErr error
	var currentOffset int64

	mt.ForEach(func(key, value []byte) bool {
		w.bloomFilter.Add(key)

		if count%w.indexInterval == 0 {
			w.sparseIndex = append(w.sparseIndex, IndexEntry{
				Offset: currentOffset,
				Key:    string(key),
			})
		}

		n, err := w.writeEntry(key, value)
		if err != nil {
			writeErr = err
			return false
		}
		count++
		currentOffset += int64(n)
		return true
	})

	if writeErr != nil {
		return writeErr
	}

	bloomOffset := currentOffset
	n, err := w.bloomFilter.WriteTo(w.file)
	if err != nil {
		return err
	}
	currentOffset += n

	indexOffset := currentOffset
	idxBuf := new(bytes.Buffer)
	tmp := make([]byte, 12)

	for _, entry := range w.sparseIndex {
		binary.BigEndian.PutUint32(tmp[:4], uint32(len(entry.Key)))
		idxBuf.Write(tmp[:4])
		idxBuf.WriteString(entry.Key)
		binary.BigEndian.PutUint64(tmp[:8], uint64(entry.Offset))
		idxBuf.Write(tmp[:8])
	}
	idxN, err := w.file.Write(idxBuf.Bytes())
	if err != nil {
		return err
	}
	currentOffset += int64(idxN)

	footer := make([]byte, FooterSize)
	binary.BigEndian.PutUint64(footer[0:8], uint64(bloomOffset))
	binary.BigEndian.PutUint64(footer[8:16], uint64(indexOffset))
	binary.BigEndian.PutUint32(footer[16:20], uint32(MagicNumber))

	if _, err := w.file.Write(footer); err != nil {
		return err
	}

	return w.file.Close()
}

func (w *SSTableWriter) writeEntry(key, value []byte) (int, error) {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], uint32(len(key)))

	// handle tombstone: if value is nil, use max uint32 as a marker
	vLen := uint32(0xFFFFFFFF)
	if value != nil {
		vLen = uint32(len(value))
	}
	binary.BigEndian.PutUint32(header[4:], vLen)

	buf := append(header, key...)
	if value != nil {
		buf = append(buf, value...)
	}

	return w.file.Write(buf)
}

type SSTableReader struct {
	file        *os.File
	sparseIndex []IndexEntry
	bloomFilter *bloom.BloomFilter
	dataEnd     int64
}

func OpenSSTable(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, _ := f.Stat()
	fileSize := stat.Size()
	if fileSize < FooterSize {
		return nil, ErrFileTooSmall
	}

	footer := make([]byte, FooterSize)
	if _, err := f.ReadAt(footer, fileSize-FooterSize); err != nil {
		return nil, err
	}

	if binary.BigEndian.Uint32(footer[16:20]) != MagicNumber {
		return nil, ErrInvalidMagicNumber
	}

	bloomOff := int64(binary.BigEndian.Uint64(footer[:8]))
	indexOff := int64(binary.BigEndian.Uint64(footer[8:16]))

	bloomLen := indexOff - bloomOff
	bloomData := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomData, bloomOff); err != nil {
		return nil, err
	}

	bf := &bloom.BloomFilter{}
	if _, err := bf.ReadFrom(bytes.NewReader(bloomData)); err != nil {
		return nil, err
	}

	indexLen := (fileSize - FooterSize) - indexOff
	indexData := make([]byte, indexLen)
	if _, err := f.ReadAt(indexData, indexOff); err != nil {
		return nil, err
	}
	idx := deserializeIndex(indexData)

	return &SSTableReader{
		file:        f,
		sparseIndex: idx,
		bloomFilter: bf,
		dataEnd:     bloomOff,
	}, nil
}

func deserializeIndex(data []byte) []IndexEntry {
	var index []IndexEntry
	cursor := 0
	for cursor < len(data) {
		kLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4

		key := string(data[cursor : cursor+kLen])
		cursor += kLen

		offset := int64(binary.BigEndian.Uint64(data[cursor : cursor+8]))
		cursor += 8

		index = append(index, IndexEntry{
			Key:    key,
			Offset: offset,
		})
	}
	return index
}

func (r *SSTableReader) Get(key []byte) ([]byte, bool, error) {
	if !r.bloomFilter.Test(key) {
		return nil, false, nil
	}

	idx := sort.Search(len(r.sparseIndex), func(i int) bool {
		return r.sparseIndex[i].Key >= string(key)
	})

	startOffset := int64(0)
	if idx > 0 {
		// If sort.Search finds an exact match at idx, we start there.
		// If it finds a key larger than our target, we must start from the previous block.
		if idx < len(r.sparseIndex) && r.sparseIndex[idx].Key == string(key) {
			startOffset = r.sparseIndex[idx].Offset
		} else {
			startOffset = r.sparseIndex[idx-1].Offset
		}
	} else if len(r.sparseIndex) > 0 {
		// If idx == 0, the key is either in the first block or doesn't exist.
		startOffset = r.sparseIndex[0].Offset
	}

	return r.scanData(startOffset, key)
}

func (r *SSTableReader) scanData(offset int64, targetKey []byte) ([]byte, bool, error) {
	for offset < r.dataEnd {
		header := make([]byte, 8)
		if _, err := r.file.ReadAt(header, offset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}

		kLen := binary.BigEndian.Uint32(header[:4])
		vLen := binary.BigEndian.Uint32(header[4:])
		offset += 8

		// Read Key
		keyBuf := make([]byte, kLen)
		if _, err := r.file.ReadAt(keyBuf, offset); err != nil {
			return nil, false, err
		}

		res := bytes.Compare(keyBuf, targetKey)
		if res == 0 {
			// check for tombstone marker
			if vLen == 0xFFFFFFFF {
				// found, but it's a tombstone
				return nil, true, nil
			}

			// found it - read value
			valBuf := make([]byte, vLen)
			if _, err := r.file.ReadAt(valBuf, offset+int64(kLen)); err != nil {
				return nil, false, err
			}
			return valBuf, true, nil
		}

		if res > 0 {
			// since SSTable is sorted, if current key > target, it's not here
			break
		}

		offset += int64(kLen + vLen)
	}
	return nil, false, nil
}
