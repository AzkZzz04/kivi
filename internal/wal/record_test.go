package wal

import (
	"testing"
)

func TestRecordEncodeDecode(t *testing.T) {
	rec := &Record{
		Type:   RecordPut,
		Key:    []byte("test-key"),
		Value:  []byte("test-value"),
		SeqNum: 123,
	}

	encoded := rec.Encode()
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.Type != rec.Type {
		t.Errorf("Type mismatch: expected %v, got %v", rec.Type, decoded.Type)
	}
	if string(decoded.Key) != string(rec.Key) {
		t.Errorf("Key mismatch: expected %s, got %s", rec.Key, decoded.Key)
	}
	if string(decoded.Value) != string(rec.Value) {
		t.Errorf("Value mismatch: expected %s, got %s", rec.Value, decoded.Value)
	}
	if decoded.SeqNum != rec.SeqNum {
		t.Errorf("SeqNum mismatch: expected %d, got %d", rec.SeqNum, decoded.SeqNum)
	}
}

func TestRecordChecksum(t *testing.T) {
	rec := &Record{
		Type:   RecordDelete,
		Key:    []byte("delete-key"),
		Value:  nil,
		SeqNum: 456,
	}

	encoded := rec.Encode()
	// Corrupt the checksum
	encoded[4] ^= 0xFF

	_, err := Decode(encoded)
	if err == nil {
		t.Error("Expected checksum error")
	}
}
