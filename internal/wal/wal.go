package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// Options configure WAL behavior.
type Options struct {
	GroupCommit   bool
	GroupCommitMS int
	BufferSize    int
}

// WAL is a write-ahead log.
type WAL struct {
	file    *os.File
	buf     *bufio.Writer
	mu      sync.Mutex
	options Options

	// Group commit state
	groupCh  chan *Record
	groupBuf []*Record
	groupMu  sync.Mutex
	wg       sync.WaitGroup

	path string
	// barrier for WaitForPending
	barrierCh chan chan struct{}
}

// DefaultOptions returns default WAL options.
func DefaultOptions() Options {
	return Options{
		GroupCommit:   true,
		GroupCommitMS: 10,
		BufferSize:    64 * 1024,
	}
}

// Open opens an existing WAL or creates a new one.
func Open(path string) (*WAL, error) {
	return OpenWithOptions(path, DefaultOptions())
}

// OpenWithOptions opens a WAL with custom options.
func OpenWithOptions(path string, opts Options) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:      file,
		buf:       bufio.NewWriterSize(file, opts.BufferSize),
		options:   opts,
		path:      path,
		groupCh:   make(chan *Record, 100),
		groupBuf:  make([]*Record, 0, 10),
		barrierCh: make(chan chan struct{}, 1),
	}

	if opts.GroupCommit {
		wal.wg.Add(1)
		go wal.groupCommitLoop()
	}

	return wal, nil
}

// Append appends a record to the WAL.
func (w *WAL) Append(rec *Record) error {
	if w.options.GroupCommit {
		// Send to group commit channel
		w.groupCh <- rec
		return nil
	}

	// Direct write
	w.mu.Lock()
	defer w.mu.Unlock()

	data := rec.Encode()
	_, err := w.buf.Write(data)
	return err
}

// Sync flushes buffered data and syncs to disk.
func (w *WAL) Sync() error {
	if w.options.GroupCommit {
		// Send a nil to trigger immediate commit
		w.groupCh <- nil
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close closes the WAL and flushes any pending data.
func (w *WAL) Close() error {
	if w.options.GroupCommit {
		close(w.groupCh)
		w.wg.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// groupCommitLoop runs in the background to batch commits.
func (w *WAL) groupCommitLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(time.Duration(w.options.GroupCommitMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case rec, ok := <-w.groupCh:
			if !ok {
				// Channel closed, flush remaining
				w.flushBatch()
				return
			}

			if rec == nil {
				// Explicit flush request
				w.flushBatch()
				continue
			}

			w.groupMu.Lock()
			w.groupBuf = append(w.groupBuf, rec)
			shouldFlush := len(w.groupBuf) >= 10
			w.groupMu.Unlock()

			if shouldFlush {
				w.flushBatch()
			}

		case <-ticker.C:
			w.flushBatch()
		case done := <-w.barrierCh:
			// Drain pending items quickly
			for {
				select {
				case r := <-w.groupCh:
					if r == nil {
						w.flushBatch()
						continue
					}
					w.groupMu.Lock()
					w.groupBuf = append(w.groupBuf, r)
					w.groupMu.Unlock()
				default:
					w.flushBatch()
					close(done)
					goto barrierDone
				}
			}
		barrierDone:
		}
	}
}

// flushBatch flushes the current batch of records.
func (w *WAL) flushBatch() {
	w.groupMu.Lock()
	if len(w.groupBuf) == 0 {
		w.groupMu.Unlock()
		return
	}
	batch := w.groupBuf
	w.groupBuf = w.groupBuf[:0]
	w.groupMu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, rec := range batch {
		data := rec.Encode()
		w.buf.Write(data)
	}
	w.buf.Flush()
	w.file.Sync()
}

// WaitForPending waits for all pending writes to be committed.
func (w *WAL) WaitForPending() {
	if !w.options.GroupCommit {
		_ = w.Sync()
		return
	}
	done := make(chan struct{})
	w.barrierCh <- done
	<-done
}

// Reader reads records from a WAL file.
type Reader struct {
	file   *os.File
	reader *bufio.Reader
}

// NewReader creates a new WAL reader.
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &Reader{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

// Replay replays all records, calling callback for each.
func (r *Reader) Replay(callback func(*Record) error) error {
	count := 0
	for {
		rec, err := r.ReadRecord()
		if err == io.EOF {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if count == 0 {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		count++
		if err := callback(rec); err != nil {
			return err
		}
	}
}

// ReadRecord reads a single record from the WAL.
func (r *Reader) ReadRecord() (*Record, error) {
	// Read length
	var lenBytes [4]byte
	if _, err := io.ReadFull(r.reader, lenBytes[:]); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBytes[:])

	// Read checksum
	var checksumBytes [4]byte
	if _, err := io.ReadFull(r.reader, checksumBytes[:]); err != nil {
		return nil, err
	}

	// Read payload
	buf := make([]byte, payloadLen)
	if _, err := io.ReadFull(r.reader, buf); err != nil {
		return nil, err
	}

	// Verify checksum
	checksum := binary.BigEndian.Uint32(checksumBytes[:])
	expectedChecksum := crc32.ChecksumIEEE(buf)
	if checksum != expectedChecksum {
		// Try to decode anyway for error message
		fullBuf := make([]byte, 8+payloadLen)
		copy(fullBuf[0:4], lenBytes[:])
		copy(fullBuf[4:8], checksumBytes[:])
		copy(fullBuf[8:], buf)
		_, _ = Decode(fullBuf) // for error message
		return nil, errChecksumMismatch
	}

	// Reconstruct full buffer for decoding
	fullBuf := make([]byte, 8+payloadLen)
	copy(fullBuf[0:4], lenBytes[:])
	copy(fullBuf[4:8], checksumBytes[:])
	copy(fullBuf[8:], buf)

	return Decode(fullBuf)
}

var (
	errChecksumMismatch = errors.New("checksum mismatch")
)
