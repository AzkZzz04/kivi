package memtable

import "sync"

// Arena is a simple bump-pointer allocator for byte slices.
// It reduces GC pressure by allocating from a contiguous buffer.
type Arena struct {
	mu  sync.Mutex
	buf []byte
	off int
}

// NewArena creates an arena with a given initial capacity in bytes.
func NewArena(capacity int) *Arena {
	return &Arena{buf: make([]byte, capacity), off: 0}
}

// ensure reserves at least n bytes available, growing the buffer if needed.
func (a *Arena) ensure(n int) {
	if len(a.buf)-a.off >= n {
		return
	}
	// grow: double until enough
	need := a.off + n
	cap := len(a.buf)
	if cap == 0 {
		cap = 1024
	}
	for cap < need {
		cap *= 2
	}
	nb := make([]byte, cap)
	copy(nb, a.buf[:a.off])
	a.buf = nb
}

// Alloc reserves n bytes and returns a slice backed by the arena.
func (a *Arena) Alloc(n int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensure(n)
	start := a.off
	a.off += n
	return a.buf[start:a.off]
}

// Copy allocates space and copies src into the arena, returning the new slice.
func (a *Arena) Copy(src []byte) []byte {
	b := a.Alloc(len(src))
	copy(b, src)
	return b
}
