package wal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/arthurzhang/kivi/internal/testutil"
)

func TestWALOpenClose(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	wal, err := OpenWithOptions(filepath.Join(dir, "wal.log"), Options{GroupCommit: false, BufferSize: 64 * 1024})
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}

func TestWALAppendSingle(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	wal, err := OpenWithOptions(filepath.Join(dir, "wal.log"), Options{GroupCommit: false, BufferSize: 64 * 1024})
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	rec := &Record{
		Type:   RecordPut,
		Key:    []byte("test-key"),
		Value:  []byte("test-value"),
		SeqNum: 1,
	}

	if err := wal.Append(rec); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}
}

func TestWALReplay(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	walPath := filepath.Join(dir, "wal.log")

	// Write some records
	{
		wal, err := Open(walPath)
		if err != nil {
			t.Fatalf("Failed to open WAL: %v", err)
		}

		for i := 0; i < 10; i++ {
			rec := &Record{
				Type:   RecordPut,
				Key:    []byte{byte(i)},
				Value:  []byte{byte(i * 2)},
				SeqNum: uint64(i),
			}
			wal.Append(rec)
		}
		wal.Sync()
		wal.Close()
	}

	// Replay
	recs := []*Record{}
	callback := func(rec *Record) error {
		recs = append(recs, rec)
		return nil
	}

	reader, err := NewReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}

	if err := reader.Replay(callback); err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	if len(recs) != 10 {
		t.Errorf("Expected 10 records, got %d", len(recs))
	}

	for i, rec := range recs {
		if rec.SeqNum != uint64(i) {
			t.Errorf("Record %d: expected seq %d, got %d", i, i, rec.SeqNum)
		}
	}
}

func TestWALReplayCorrupted(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	walPath := filepath.Join(dir, "wal.log")

	// Write a valid record
	{
		wal, err := Open(walPath)
		if err != nil {
			t.Fatalf("Failed to open WAL: %v", err)
		}

		rec := &Record{
			Type:   RecordPut,
			Key:    []byte("key"),
			Value:  []byte("val"),
			SeqNum: 1,
		}
		wal.Append(rec)
		wal.Sync()
		wal.Close()
	}

	// Corrupt the file
	{
		f, err := os.OpenFile(walPath, os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("Failed to open for corruption: %v", err)
		}
		// Seek to middle and write junk
		f.Seek(10, 0)
		f.Write([]byte{0xFF, 0xFF, 0xFF})
		f.Close()
	}

	reader, err := NewReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}

	err = reader.Replay(func(rec *Record) error { return nil })
	if err == nil {
		t.Error("Expected error on corrupted WAL, got none")
	}
}

func TestWALGroupCommit(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	wal, err := OpenWithOptions(filepath.Join(dir, "wal.log"), Options{
		GroupCommit:   true,
		GroupCommitMS: 10,
	})
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Append multiple records rapidly
	for i := 0; i < 5; i++ {
		rec := &Record{
			Type:   RecordPut,
			Key:    []byte{byte(i)},
			Value:  []byte{byte(i)},
			SeqNum: uint64(i),
		}
		wal.Append(rec)
	}

	// Should not sync immediately
	// Give group commit time to batch
	wal.WaitForPending()

	// Now replay and verify
	walPath := filepath.Join(dir, "wal.log")
	st, _ := os.Stat(walPath)
	if st != nil && st.Size() == 0 {
		t.Fatalf("wal size is zero after sync")
	}
	reader, err := NewReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}

	count := 0
	reader.Replay(func(rec *Record) error {
		count++
		return nil
	})

	if count != 5 {
		t.Errorf("Expected 5 records, got %d", count)
	}
}

func TestWALReplayTruncatedTail(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	walPath := filepath.Join(dir, "wal.log")

	wal, err := Open(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write several complete records
	for i := 0; i < 5; i++ {
		rec := &Record{Type: RecordPut, Key: []byte{byte(i)}, Value: []byte{byte(i)}, SeqNum: uint64(i)}
		if err := wal.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// Write one more and do not fully sync its payload by truncating later
	last := &Record{Type: RecordPut, Key: []byte("last"), Value: make([]byte, 1024), SeqNum: 999}
	if err := wal.Append(last); err != nil {
		t.Fatalf("append last: %v", err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	wal.Close()

	// Truncate a few bytes off the end to simulate crash during write
	f, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open for truncate: %v", err)
	}
	st, _ := f.Stat()
	if st.Size() > 4 {
		_ = f.Truncate(st.Size() - 2)
	}
	f.Close()

	reader, err := NewReader(walPath)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	cnt := 0
	if err := reader.Replay(func(r *Record) error { cnt++; return nil }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if cnt < 5 {
		t.Fatalf("expected at least 5 complete records, got %d", cnt)
	}
}

func TestWALDeleteAndZeroLength(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	wal, err := Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer wal.Close()

	// Zero-length key and value should be supported
	if err := wal.Append(&Record{Type: RecordPut, Key: []byte{}, Value: []byte{}, SeqNum: 1}); err != nil {
		t.Fatalf("append empty: %v", err)
	}
	// Delete record
	if err := wal.Append(&Record{Type: RecordDelete, Key: []byte("k"), Value: nil, SeqNum: 2}); err != nil {
		t.Fatalf("append del: %v", err)
	}
	wal.Sync()

	reader, err := NewReader(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	types := []RecordType{}
	if err := reader.Replay(func(r *Record) error { types = append(types, r.Type); return nil }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(types) != 2 || types[0] != RecordPut || types[1] != RecordDelete {
		t.Fatalf("unexpected types: %v", types)
	}
}

func TestWALLargeRecord(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	wal, err := OpenWithOptions(filepath.Join(dir, "wal.log"), Options{GroupCommit: false, BufferSize: 8 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer wal.Close()

	large := make([]byte, 256*1024)
	if err := wal.Append(&Record{Type: RecordPut, Key: []byte("k"), Value: large, SeqNum: 1}); err != nil {
		t.Fatalf("append large: %v", err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	reader, _ := NewReader(filepath.Join(dir, "wal.log"))
	var got *Record
	if err := reader.Replay(func(r *Record) error { got = r; return nil }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got == nil || len(got.Value) != len(large) {
		t.Fatalf("large value mismatch: got %d want %d", len(got.Value), len(large))
	}
}

func TestWALConcurrentAppend(t *testing.T) {
	dir := testutil.MustTempDir(t)
	defer os.RemoveAll(dir)

	wal, err := OpenWithOptions(filepath.Join(dir, "wal.log"), Options{GroupCommit: true, GroupCommitMS: 5})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer wal.Close()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = wal.Append(&Record{Type: RecordPut, Key: []byte{byte(i)}, Value: []byte{1}, SeqNum: uint64(i)})
		}()
	}
	wg.Wait()
	wal.WaitForPending()

	reader, _ := NewReader(filepath.Join(dir, "wal.log"))
	cnt := 0
	_ = reader.Replay(func(r *Record) error { cnt++; return nil })
	if cnt != n {
		t.Fatalf("expected %d records, got %d", n, cnt)
	}
}
