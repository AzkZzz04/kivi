package memtable

import (
	"sort"
	"sync"
)

// Arena is a placeholder for a future arena allocator.
// It is currently unused but kept to satisfy the constructor signature.
// (real implementation in arena.go)

// entry represents the latest visible state for a user key.
type entry struct {
	seq     uint64
	value   []byte
	deleted bool
}

// Skiplist is a simple ordered in-memory map that provides
// the API required by the memtable tests. It uses a RWMutex
// for concurrency and maintains a sorted list of keys for
// iterator operations.
type Skiplist struct {
	mu      sync.RWMutex
	entries map[string]entry
	keys    []string // sorted ascending; contains keys that may be deleted
	arena   *Arena
}

// NewSkiplist creates a new Skiplist. The arena parameter is reserved
// for future use and can be nil.
func NewSkiplist(arena *Arena) *Skiplist {
	return &Skiplist{
		entries: make(map[string]entry),
		keys:    make([]string, 0, 1024),
		arena:   arena,
	}
}

// Put inserts or updates a key with the given value and sequence number.
// If a newer sequence already exists for the key, this call is ignored.
func (s *Skiplist) Put(key, val []byte, seq uint64) error {
	k := string(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	if cur, ok := s.entries[k]; ok {
		if seq <= cur.seq {
			return nil
		}
	}

	var stored []byte
	if s.arena != nil {
		stored = s.arena.Copy(val)
	} else {
		stored = clone(val)
	}
	s.entries[k] = entry{seq: seq, value: stored, deleted: false}
	// Ensure key is tracked in keys slice
	if !s.keyPresent(k) {
		s.keys = append(s.keys, k)
		s.sortKeys()
	}
	return nil
}

// Delete marks a key as deleted at the given sequence. Older writes are ignored.
func (s *Skiplist) Delete(key []byte, seq uint64) error {
	k := string(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	if cur, ok := s.entries[k]; ok {
		if seq <= cur.seq {
			return nil
		}
	}

	s.entries[k] = entry{seq: seq, value: nil, deleted: true}
	if !s.keyPresent(k) {
		s.keys = append(s.keys, k)
		s.sortKeys()
	}
	return nil
}

// Get returns the visible value for a key, if present and not deleted.
func (s *Skiplist) Get(key []byte) ([]byte, bool) {
	k := string(key)

	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.entries[k]
	if !ok || e.deleted {
		return nil, false
	}
	return clone(e.value), true
}

// Iterator provides forward iteration over visible keys in ascending order.
type Iterator struct {
	keys [][]byte
	vals [][]byte
	idx  int
}

// NewIterator returns a snapshot iterator over the current visible state.
func (s *Skiplist) NewIterator() *Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([][]byte, 0, len(s.keys))
	vals := make([][]byte, 0, len(s.keys))
	for _, k := range s.keys {
		if e, ok := s.entries[k]; ok && !e.deleted {
			keys = append(keys, []byte(k))
			vals = append(vals, clone(e.value))
		}
	}
	// keys are already sorted lexicographically due to s.keys
	return &Iterator{keys: keys, vals: vals, idx: -1}
}

// SeekGE positions the iterator at the first key >= target.
func (it *Iterator) SeekGE(target []byte) {
	lo, hi := 0, len(it.keys)
	for lo < hi {
		mid := (lo + hi) / 2
		if string(it.keys[mid]) < string(target) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	it.idx = lo - 1
	it.Next()
}

// Valid returns whether the iterator is at a valid position.
func (it *Iterator) Valid() bool { return it.idx >= 0 && it.idx < len(it.keys) }

// Key returns the current key.
func (it *Iterator) Key() []byte { return it.keys[it.idx] }

// Value returns the current value.
func (it *Iterator) Value() []byte { return it.vals[it.idx] }

// Next advances the iterator.
func (it *Iterator) Next() { it.idx++ }

// keyPresent checks if k is present in the sorted keys slice.
func (s *Skiplist) keyPresent(k string) bool {
	// binary search
	i := sort.SearchStrings(s.keys, k)
	return i < len(s.keys) && s.keys[i] == k
}

func (s *Skiplist) sortKeys() { sort.Strings(s.keys) }
