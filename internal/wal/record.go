package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// RecordType represents the type of operation.
type RecordType byte

const (
	RecordPut RecordType = iota
	RecordDelete
)

// Record represents a single WAL record.
type Record struct {
	Type   RecordType
	Key    []byte
	Value  []byte
	SeqNum uint64
}

// Encode encodes a record to bytes with checksum.
// Format: [length:4][checksum:4][type:1][seq:8][key_len:4][key][val_len:4][val]
func (r *Record) Encode() []byte {
	// Calculate payload size
	payloadSize := 1 + // type
		8 + // seq
		4 + len(r.Key) + // key len + key
		4 + len(r.Value) // val len + val

	// Total size = 4 (length) + 4 (checksum) + payload
	totalSize := 4 + 4 + payloadSize
	buf := make([]byte, totalSize)

	// Write length
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadSize))

	// Build payload
	pos := 8 // skip length and checksum
	buf[pos] = byte(r.Type)
	pos++

	binary.BigEndian.PutUint64(buf[pos:pos+8], r.SeqNum)
	pos += 8

	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(r.Key)))
	pos += 4
	copy(buf[pos:], r.Key)
	pos += len(r.Key)

	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(r.Value)))
	pos += 4
	copy(buf[pos:], r.Value)

	// Calculate checksum on payload
	checksum := crc32.ChecksumIEEE(buf[8:])
	binary.BigEndian.PutUint32(buf[4:8], checksum)

	return buf
}

// Decode decodes bytes to a record, validating checksum.
func Decode(buf []byte) (*Record, error) {
	if len(buf) < 8 {
		return nil, errors.New("record too short")
	}

	payloadLen := binary.BigEndian.Uint32(buf[0:4])
	checksum := binary.BigEndian.Uint32(buf[4:8])

	if len(buf) < int(8+payloadLen) {
		return nil, errors.New("record truncated")
	}

	// Verify checksum
	expectedChecksum := crc32.ChecksumIEEE(buf[8 : 8+payloadLen])
	if checksum != expectedChecksum {
		return nil, errors.New("checksum mismatch")
	}

	pos := 8
	rec := &Record{}

	rec.Type = RecordType(buf[pos])
	pos++

	rec.SeqNum = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8

	keyLen := binary.BigEndian.Uint32(buf[pos : pos+4])
	pos += 4
	rec.Key = make([]byte, keyLen)
	copy(rec.Key, buf[pos:pos+int(keyLen)])
	pos += int(keyLen)

	valLen := binary.BigEndian.Uint32(buf[pos : pos+4])
	pos += 4
	rec.Value = make([]byte, valLen)
	copy(rec.Value, buf[pos:pos+int(valLen)])

	return rec, nil
}
