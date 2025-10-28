package memtable

import "sync"

// Memtable wraps a mutable skiplist plus a single immutable skiplist created on flip.
// It provides merged reads across current and immutable, and exposes an iterator
// that merges the two.
type Memtable struct {
	mu        sync.RWMutex
	current   *Skiplist
	imm       *Skiplist
	threshold int // approximate threshold in bytes to trigger flip
	arenaCap  int
	sizeBytes int // rough accounting: key+val sizes of current
}

// NewMemtable creates a new memtable with a size threshold in bytes.
func NewMemtable(threshold int) *Memtable {
	return &Memtable{current: NewSkiplist(NewArena(1 << 20)), threshold: threshold, arenaCap: 1 << 20}
}

func (m *Memtable) Put(key, val []byte, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Flip if exceeding threshold (simple heuristic)
	projected := m.sizeBytes + len(key) + len(val)
	if m.imm == nil && m.threshold > 0 && projected > m.threshold {
		m.imm = m.current
		m.current = NewSkiplist(NewArena(m.arenaCap))
		m.sizeBytes = 0
	}
	if err := m.current.Put(key, val, seq); err == nil {
		m.sizeBytes += len(key) + len(val)
	}
	return nil
}

func (m *Memtable) Delete(key []byte, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Deletions always go to current
	return m.current.Delete(key, seq)
}

func (m *Memtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	cur := m.current
	imm := m.imm
	m.mu.RUnlock()

	if v, ok := cur.Get(key); ok {
		return v, true
	}
	if imm != nil {
		return imm.Get(key)
	}
	return nil, false
}

// HasImmutable reports whether an immutable memtable exists.
func (m *Memtable) HasImmutable() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.imm != nil
}

// PopImmutable returns the immutable skiplist and clears it.
func (m *Memtable) PopImmutable() *Skiplist {
	m.mu.Lock()
	defer m.mu.Unlock()
	imm := m.imm
	m.imm = nil
	return imm
}

// Merged iterator of current and immutable, choosing the newest value by presence in current first.
type mergedIterator struct {
	curIt *Iterator
	immIt *Iterator
	curK  []byte
	immK  []byte
	curV  []byte
	immV  []byte
	valid bool
}

func (m *Memtable) NewIterator() *mergedIterator {
	m.mu.RLock()
	cur := m.current.NewIterator()
	var imm *Iterator
	if m.imm != nil {
		imm = m.imm.NewIterator()
	}
	m.mu.RUnlock()
	return &mergedIterator{curIt: cur, immIt: imm}
}

func (it *mergedIterator) SeekGE(key []byte) {
	if it.curIt != nil {
		it.curIt.SeekGE(key)
	}
	if it.immIt != nil {
		it.immIt.SeekGE(key)
	}
	it.advance()
}

func (it *mergedIterator) Valid() bool { return it.valid }
func (it *mergedIterator) Key() []byte {
	if it.curK != nil {
		return it.curK
	}
	return it.immK
}
func (it *mergedIterator) Value() []byte {
	if it.curK != nil {
		return it.curV
	}
	return it.immV
}

func (it *mergedIterator) Next() { it.advanceFromCurrentKey() }

func (it *mergedIterator) advance() {
	// Choose smallest user key among current/imm, but prefer current on equality
	if it.curIt != nil && it.curIt.Valid() {
		it.curK, it.curV = it.curIt.Key(), it.curIt.Value()
	} else {
		it.curK, it.curV = nil, nil
	}
	if it.immIt != nil && it.immIt.Valid() {
		it.immK, it.immV = it.immIt.Key(), it.immIt.Value()
	} else {
		it.immK, it.immV = nil, nil
	}

	if it.curK == nil && it.immK == nil {
		it.valid = false
		return
	}
	if it.curK == nil {
		it.valid = true
		return
	}
	if it.immK == nil {
		it.valid = true
		return
	}
	// both valid: keep as is, but ensure selection via accessors prefers curK when equal
	it.valid = true
}

func (it *mergedIterator) advanceFromCurrentKey() {
	if !it.valid {
		return
	}
	// Determine current chosen key
	var chosen []byte
	if it.curK != nil && (it.immK == nil || string(it.curK) <= string(it.immK)) {
		chosen = it.curK
		it.curIt.Next()
	} else {
		chosen = it.immK
		it.immIt.Next()
	}
	// Skip duplicate key on the other iterator if equal
	if it.curIt != nil && it.curIt.Valid() && string(it.curIt.Key()) == string(chosen) {
		it.curIt.Next()
	}
	if it.immIt != nil && it.immIt.Valid() && string(it.immIt.Key()) == string(chosen) {
		it.immIt.Next()
	}
	it.advance()
}
