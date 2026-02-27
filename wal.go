package validb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"time"
)

const (
	OpSet uint8 = iota
	OpDelete
)

var (
	ErrKeyIsNil      = errors.New("key is nil")
	ErrWALCorrupted  = errors.New("WAL file corrupted")
	ErrInvalidOpType = errors.New("invalid operation type")
)

const (
	crcOffset       = 0
	typeOffset      = 4
	timestampOffset = 5
	keyLenOffset    = 13
	valLenOffset    = 17
	headerSize      = 21
)

type appendRequest struct {
	opType  uint8
	key     []byte
	val     []byte
	errChan chan error // channel to send the result back to the caller
}

type WAL struct {
	Path  string
	file  *os.File
	queue chan appendRequest
	stop  chan struct{}
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:  f,
		Path:  path,
		queue: make(chan appendRequest, 1024),
		stop:  make(chan struct{}),
	}

	go w.runFlusher()
	return w, nil
}

func (w *WAL) Append(opType uint8, key, val []byte) error {
	errChan := make(chan error, 1)
	w.queue <- appendRequest{opType, key, val, errChan}
	return <-errChan
}

func (w *WAL) runFlusher() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var batch []chan error

	for {
		select {
		case req := <-w.queue:
			err := w.writeToBuffer(req.opType, req.key, req.val)
			if err != nil {
				req.errChan <- err
				continue
			}
			batch = append(batch, req.errChan)

			if len(batch) >= 1000 {
				w.flushBatch(&batch)
			}

		case <-ticker.C:
			w.flushBatch(&batch)
		case <-w.stop:
			w.flushBatch(&batch)
			return
		}
	}
}

func (w *WAL) flushBatch(batch *[]chan error) {
	if len(*batch) == 0 {
		return
	}
	err := w.file.Sync()
	for _, ch := range *batch {
		ch <- err
	}
	*batch = (*batch)[:0]
}

func (w *WAL) writeToBuffer(opType uint8, key, val []byte) error {
	if opType > OpDelete {
		return ErrInvalidOpType
	}
	if len(key) == 0 {
		return ErrKeyIsNil
	}

	keyLen, valLen := len(key), len(val)
	totalSize := headerSize + keyLen + valLen

	buf := make([]byte, totalSize)

	buf[typeOffset] = opType
	binary.BigEndian.PutUint64(buf[timestampOffset:], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(buf[keyLenOffset:], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[valLenOffset:], uint32(valLen))

	copy(buf[headerSize:], key)
	copy(buf[headerSize+keyLen:], val)

	checksum := crc32.ChecksumIEEE(buf[typeOffset:])
	binary.BigEndian.PutUint32(buf[crcOffset:], checksum)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	return nil
}

func (w *WAL) NewIterator() func() (uint8, []byte, []byte, error) {
	// _, err := w.file.Seek(0, 0)
	// if err != nil {
	// 	return func() (uint8, []byte, []byte, error) {
	// 		return 0, nil, nil, err
	// 	}
	// }

	next := func() (uint8, []byte, []byte, error) {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF {
			return 0, nil, nil, io.EOF
		}
		if err != nil {
			return 0, nil, nil, err
		}

		storedCRC := binary.BigEndian.Uint32(header[crcOffset:])
		keyLen := binary.BigEndian.Uint32(header[keyLenOffset:])
		valLen := binary.BigEndian.Uint32(header[valLenOffset:])
		opType := header[typeOffset]

		payload := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			return 0, nil, nil, err
		}

		h := crc32.NewIEEE()
		h.Write(header[typeOffset:])
		h.Write(payload)
		actualCRC := h.Sum32()

		if storedCRC != actualCRC {
			return 0, nil, nil, ErrWALCorrupted
		}

		return opType, payload[:keyLen], payload[keyLen:], nil
	}

	return next
}

func (w *WAL) Close() error {
	return w.file.Close()
}
