package metrics

import (
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	// Use global metrics instance
	m := GlobalMetrics

	// Test operation recording
	m.RecordOp("get", 100*time.Microsecond)
	m.RecordOp("put", 150*time.Microsecond)
	m.RecordOp("del", 120*time.Microsecond)

	if m.GetCount.String() != "1" {
		t.Errorf("Expected GetCount=1, got %s", m.GetCount.String())
	}
	if m.PutCount.String() != "1" {
		t.Errorf("Expected PutCount=1, got %s", m.PutCount.String())
	}
	if m.DelCount.String() != "1" {
		t.Errorf("Expected DelCount=1, got %s", m.DelCount.String())
	}
}

func TestConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MemtableMB != 64 {
		t.Errorf("Expected MemtableMB=64, got %d", cfg.MemtableMB)
	}
	if cfg.BlockSizeKB != 16 {
		t.Errorf("Expected BlockSizeKB=16, got %d", cfg.BlockSizeKB)
	}
}
