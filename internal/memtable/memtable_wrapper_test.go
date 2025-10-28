package memtable

import (
	"sync"
	"testing"
)

func TestMemtableFlipOnThreshold(t *testing.T) {
	mt := NewMemtable(4) // tiny threshold
	_ = mt.Put(b("a"), b("1"), 1)
	if mt.HasImmutable() {
		t.Fatalf("should not have imm yet")
	}
	// This put should exceed threshold and trigger flip
	_ = mt.Put(b("bb"), b("22"), 2)
	if !mt.HasImmutable() {
		t.Fatalf("expected immutable after flip")
	}
	imm := mt.PopImmutable()
	if imm == nil {
		t.Fatalf("expected non-nil immutable")
	}
	// imm should contain the first key
	if v, ok := imm.Get(b("a")); !ok || string(v) != "1" {
		t.Fatalf("immutable content mismatch")
	}
}

func TestMemtableGetAcrossCurrentAndImm(t *testing.T) {
	mt := NewMemtable(16)
	_ = mt.Put(b("a"), b("1"), 1)
	_ = mt.Put(b("bb"), b("22"), 2) // flip likely
	_ = mt.Put(b("c"), b("3"), 3)   // goes to current

	if v, ok := mt.Get(b("a")); !ok || string(v) != "1" {
		t.Fatalf("get from imm failed")
	}
	if v, ok := mt.Get(b("c")); !ok || string(v) != "3" {
		t.Fatalf("get from current failed")
	}
}

func TestMemtableDeleteAcrossHeaps(t *testing.T) {
	mt := NewMemtable(16)
	_ = mt.Put(b("a"), b("1"), 1)
	_ = mt.Put(b("bb"), b("22"), 2) // flip
	// delete in current should hide imm value
	_ = mt.Delete(b("a"), 3)
	if _, ok := mt.Get(b("a")); ok {
		t.Fatalf("delete in current did not hide immutable value")
	}
}

func TestMemtableCombinedIterator(t *testing.T) {
	mt := NewMemtable(32)
	_ = mt.Put(b("a"), b("1"), 1)
	_ = mt.Put(b("bb"), b("22"), 2) // flip
	_ = mt.Put(b("b"), b("2"), 3)   // current newer b
	_ = mt.Delete(b("a"), 4)        // hide a

	it := mt.NewIterator()
	it.SeekGE(b(""))
	keys := []string{}
	vals := []string{}
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
		it.Next()
	}
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "bb" || vals[0] != "2" || vals[1] != "22" {
		t.Fatalf("merge iter mismatch keys=%v vals=%v", keys, vals)
	}
}

func TestMemtableConcurrentFlipAndGet(t *testing.T) {
	mt := NewMemtable(64)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = mt.Put(b(keyOf(i)), b(valOf(i)), uint64(i+1))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = mt.Get(b(keyOf(i)))
		}
	}()
	wg.Wait()

	// Spot check correctness
	if v, ok := mt.Get(b(keyOf(50))); !ok || string(v) != valOf(50) {
		t.Fatalf("post-concurrency get mismatch: %q ok=%v", string(v), ok)
	}
}
