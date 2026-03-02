package validb

import (
	"bufio"
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

const (
	DefaultWriteBufferSize = 64 * 1024
	MaxQueueSize           = 1024
	MaxBatchSize           = 1000
	FlushInterval          = 10 * time.Millisecond
	DefaultFilePerm        = 0644
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
	Path   string
	file   *os.File
	writer *bufio.Writer
	queue  chan appendRequest
	stop   chan struct{}
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, DefaultFilePerm)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		Path:   path,
		file:   file,
		writer: bufio.NewWriterSize(file, DefaultWriteBufferSize),
		queue:  make(chan appendRequest, MaxQueueSize),
		stop:   make(chan struct{}),
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
	ticker := time.NewTicker(FlushInterval)
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

			if len(batch) >= MaxBatchSize {
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
	err := w.writer.Flush()
	if err != nil {
		err = w.file.Sync()
	}
	for _, ch := range *batch {
		ch <- err
	}
	*batch = (*batch)[:0]
}

func (w *WAL) writeToBuffer(opType uint8, key, val []byte) error {
	if opType != OpSet && opType != OpDelete {
		return ErrInvalidOpType
	}
	if len(key) == 0 {
		return ErrKeyIsNil
	}

	var hBuf [headerSize]byte
	hBuf[typeOffset] = opType
	binary.BigEndian.PutUint64(hBuf[timestampOffset:], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(hBuf[keyLenOffset:], uint32(len(key)))
	binary.BigEndian.PutUint32(hBuf[valLenOffset:], uint32(len(val)))

	digest := crc32.NewIEEE()
	digest.Write(hBuf[typeOffset:])
	digest.Write(key)
	digest.Write(val)
	checksum := digest.Sum32()

	binary.BigEndian.PutUint32(hBuf[crcOffset:], checksum)

	if _, err := w.writer.Write(hBuf[:]); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}
	if _, err := w.writer.Write(val); err != nil {
		return err
	}

	return nil
}

func (w *WAL) NewIterator() func() (uint8, []byte, []byte, error) {
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

func (w *WAL) Clear() error {
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Close() error {
	close(w.stop)
	return w.file.Close()
}
