package validb

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var ErrCorruptedBatch = errors.New("corrupted batch")

// Batch collects Sets and Deletes that LSMTree.Write applies atomically.
// They are stored as ONE WAL record, so they cost ONE fsync together, and
// after a crash either all of them are there or none of them is.
type Batch struct {
	ops []batchOp
}

type batchOp struct {
	op    uint8
	key   []byte
	value []byte
}

// Set queues key=value. Key and value are copied.
func (b *Batch) Set(key, value []byte) {
	b.ops = append(b.ops, batchOp{OpSet, bytes.Clone(key), bytes.Clone(value)})
}

// Delete queues a delete of key. The key is copied.
func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{OpDelete, bytes.Clone(key), nil})
}

// Len returns how many operations are queued.
func (b *Batch) Len() int { return len(b.ops) }

// encode packs the operations back to back:
// [op (1 byte)][uvarint keyLen][uvarint valLen][key][value] ...
func (b *Batch) encode() []byte {
	var buf []byte
	for _, o := range b.ops {
		buf = append(buf, o.op)
		buf = binary.AppendUvarint(buf, uint64(len(o.key)))
		buf = binary.AppendUvarint(buf, uint64(len(o.value)))
		buf = append(buf, o.key...)
		buf = append(buf, o.value...)
	}
	return buf
}

// decodeBatch is the reverse of encode.
func decodeBatch(buf []byte) ([]batchOp, error) {
	var ops []batchOp
	for len(buf) > 0 {
		op := buf[0]
		if op != OpSet && op != OpDelete {
			return nil, ErrCorruptedBatch
		}
		buf = buf[1:]

		kLen, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, ErrCorruptedBatch
		}
		buf = buf[n:]
		vLen, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, ErrCorruptedBatch
		}
		buf = buf[n:]
		if kLen > uint64(len(buf)) || vLen > uint64(len(buf))-kLen {
			return nil, ErrCorruptedBatch
		}

		o := batchOp{op: op, key: buf[:kLen]}
		if op == OpSet {
			o.value = buf[kLen : kLen+vLen] // non-nil even when empty
		}
		ops = append(ops, o)
		buf = buf[kLen+vLen:]
	}
	return ops, nil
}
