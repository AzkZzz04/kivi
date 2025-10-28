package memtable

import (
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
)

type kv struct{ k, v []byte }

// Helper to create byte slices from strings without allocations in tests
func b(s string) []byte { return []byte(s) }

// Expected public API under test:
// - NewSkiplist(arena *Arena) *Skiplist
// - (*Skiplist) Put(key, val []byte, seq uint64) error
// - (*Skiplist) Delete(key []byte, seq uint64) error
// - (*Skiplist) Get(key []byte) (val []byte, ok bool)
// - (*Skiplist) NewIterator() *Iterator
// - Iterator: SeekGE(key []byte); Valid() bool; Key() []byte; Value() []byte; Next()

func TestPutGetBasic(t *testing.T) {
	sl := NewSkiplist(nil)
	if _, ok := sl.Get(b("a")); ok {
		t.Fatalf("expected not found on empty")
	}
	if err := sl.Put(b("a"), b("1"), 1); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, ok := sl.Get(b("a"))
	if !ok || string(v) != "1" {
		t.Fatalf("get mismatch: ok=%v v=%q", ok, string(v))
	}
}

func TestOverwriteBySeq(t *testing.T) {
	sl := NewSkiplist(nil)
	_ = sl.Put(b("k"), b("old"), 1)
	_ = sl.Put(b("k"), b("new"), 2)
	v, ok := sl.Get(b("k"))
	if !ok || string(v) != "new" {
		t.Fatalf("want new, got %q ok=%v", string(v), ok)
	}
}

func TestDeleteHidesValue(t *testing.T) {
	sl := NewSkiplist(nil)
	_ = sl.Put(b("k"), b("v"), 10)
	_ = sl.Delete(b("k"), 11)
	if _, ok := sl.Get(b("k")); ok {
		t.Fatalf("expected not found after delete")
	}
}

func TestZeroLengthKeyAndValue(t *testing.T) {
	sl := NewSkiplist(nil)
	_ = sl.Put([]byte{}, []byte{}, 1)
	v, ok := sl.Get([]byte{})
	if !ok || len(v) != 0 {
		t.Fatalf("zero key/value mismatch: ok=%v len=%d", ok, len(v))
	}
	_ = sl.Delete([]byte{}, 2)
	if _, ok := sl.Get([]byte{}); ok {
		t.Fatalf("expected empty key deleted")
	}
}

func TestIteratorOrderAndStability(t *testing.T) {
	sl := NewSkiplist(nil)
	pairs := []kv{{b("a"), b("1")}, {b("b"), b("2")}, {b("c"), b("3")}}
	for i, p := range pairs {
		_ = sl.Put(p.k, p.v, uint64(i+1))
	}
	// Overwrite b with higher seq
	_ = sl.Put(b("b"), b("22"), 99)

	it := sl.NewIterator()
	it.SeekGE(b("a"))
	var got []kv
	for it.Valid() {
		got = append(got, kv{clone(it.Key()), clone(it.Value())})
		it.Next()
	}
	// Expect user-keys in ascending order with latest values
	expected := []kv{{b("a"), b("1")}, {b("b"), b("22")}, {b("c"), b("3")}}
	if len(got) != len(expected) {
		t.Fatalf("iter len mismatch: got %d want %d", len(got), len(expected))
	}
	for i := range got {
		if string(got[i].k) != string(expected[i].k) || string(got[i].v) != string(expected[i].v) {
			t.Fatalf("iter[%d] got (%s,%s) want (%s,%s)", i, got[i].k, got[i].v, expected[i].k, expected[i].v)
		}
	}
}

func TestIteratorSeekGE(t *testing.T) {
	sl := NewSkiplist(nil)
	keys := []string{"a", "b", "c", "d"}
	for i, k := range keys {
		_ = sl.Put(b(k), b("v"), uint64(i+1))
	}
	it := sl.NewIterator()
	it.SeekGE(b("bb"))
	if !it.Valid() || string(it.Key()) != "c" {
		t.Fatalf("seekGE failed, got %q", string(it.Key()))
	}
}

func TestIteratorExcludesDeleted(t *testing.T) {
	sl := NewSkiplist(nil)
	_ = sl.Put(b("a"), b("1"), 1)
	_ = sl.Put(b("b"), b("2"), 2)
	_ = sl.Delete(b("a"), 3)
	it := sl.NewIterator()
	it.SeekGE(b(""))
	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}
	if len(keys) != 1 || keys[0] != "b" {
		t.Fatalf("expected only 'b', got %v", keys)
	}
}

func TestIteratorSeekBounds(t *testing.T) {
	sl := NewSkiplist(nil)
	for i := 0; i < 5; i++ {
		_ = sl.Put(b(keyOf(i)), b("v"), uint64(i+1))
	}
	it := sl.NewIterator()
	it.SeekGE(b("zzzz"))
	if it.Valid() {
		t.Fatalf("seek beyond last should be invalid")
	}
	it.SeekGE(b(""))
	if !it.Valid() || string(it.Key()) != keyOf(0) {
		t.Fatalf("seek to start failed: %q", string(it.Key()))
	}
}

func TestIteratorSnapshotConsistency(t *testing.T) {
	sl := NewSkiplist(nil)
	_ = sl.Put(b("a"), b("1"), 1)
	_ = sl.Put(b("b"), b("2"), 2)
	it := sl.NewIterator()
	it.SeekGE(b("a"))
	// mutate after iterator creation
	_ = sl.Delete(b("b"), 3)
	_ = sl.Put(b("c"), b("3"), 4)
	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}
	// iterator snapshot should have seen a and b only
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("snapshot inconsistency: %v", keys)
	}
}

func TestSeqWithDeletesOrdering(t *testing.T) {
	sl := NewSkiplist(nil)
	// Put then delete with lower seq should not hide
	_ = sl.Put(b("k"), b("v1"), 10)
	_ = sl.Delete(b("k"), 5)
	if v, ok := sl.Get(b("k")); !ok || string(v) != "v1" {
		t.Fatalf("lower-seq delete should not win")
	}
	// Delete with higher seq should hide
	_ = sl.Delete(b("k"), 11)
	if _, ok := sl.Get(b("k")); ok {
		t.Fatalf("higher-seq delete should hide")
	}
	// Put with lower seq after delete should be ignored
	_ = sl.Put(b("k"), b("v2"), 9)
	if _, ok := sl.Get(b("k")); ok {
		t.Fatalf("lower-seq put after delete should not resurrect")
	}
	// Put with higher seq after delete should resurrect
	_ = sl.Put(b("k"), b("v3"), 12)
	if v, ok := sl.Get(b("k")); !ok || string(v) != "v3" {
		t.Fatalf("higher-seq put should resurrect")
	}
}

func TestConcurrentPutsAndGets(t *testing.T) {
	sl := NewSkiplist(nil)
	const N = 1000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 1; i <= N; i++ {
			_ = sl.Put([]byte(keyOf(i)), []byte(valOf(i)), uint64(i))
		}
	}()
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(1))
		for i := 0; i < N; i++ {
			k := []byte(keyOf(r.Intn(N) + 1))
			_, _ = sl.Get(k)
		}
	}()
	wg.Wait()
	// Verify last value visible for a sample
	for _, i := range []int{1, N / 2, N} {
		v, ok := sl.Get([]byte(keyOf(i)))
		if !ok || string(v) != valOf(i) {
			t.Fatalf("post-concurrency get mismatch for %d: ok=%v v=%q", i, ok, string(v))
		}
	}
}

func TestRandomOpsPropertyAgainstMap(t *testing.T) {
	sl := NewSkiplist(nil)
	m := map[string]kv{}
	r := rand.New(rand.NewSource(42))
	const ops = 2000
	seq := uint64(0)
	keys := []string{}
	for i := 0; i < ops; i++ {
		op := r.Intn(3)
		k := keyOf(r.Intn(200))
		if op == 0 { // put
			seq++
			v := valOf(int(seq))
			_ = sl.Put(b(k), b(v), seq)
			m[k] = kv{b(k), b(v)}
			if !contains(keys, k) {
				keys = append(keys, k)
			}
		} else if op == 1 { // delete
			seq++
			_ = sl.Delete(b(k), seq)
			delete(m, k)
		} else { // get
			_, _ = sl.Get(b(k))
		}
	}
	// Compare final state via iterator
	sort.Strings(keys)
	it := sl.NewIterator()
	it.SeekGE(b(""))
	seen := map[string]string{}
	for it.Valid() {
		seen[string(it.Key())] = string(it.Value())
		it.Next()
	}
	for _, k := range keys {
		if want, ok := m[k]; ok {
			if got, ok2 := seen[k]; !ok2 || got != string(want.v) {
				t.Fatalf("key %s: got %q want %q", k, got, string(want.v))
			}
		} else {
			if _, ok2 := seen[k]; ok2 {
				t.Fatalf("key %s unexpectedly present", k)
			}
		}
	}
}

// helpers
func keyOf(i int) string     { return "k" + strconv.Itoa(i) }
func valOf(i int) string     { return "v" + strconv.Itoa(i) }
func clone(bz []byte) []byte { cp := make([]byte, len(bz)); copy(cp, bz); return cp }
func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
