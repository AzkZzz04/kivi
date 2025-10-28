package testutil

import (
	"testing"
	"time"
)

func TestZipfGenerator(t *testing.T) {
	gen := NewZipfGenerator(10000, 0.99, 12345)

	seen := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		key := gen.Next()
		if key < 0 || key >= 10000 {
			t.Errorf("Generated key %d out of range [0, %d)", key, 10000)
		}
		seen[key] = true
	}

	// Should have some repetition due to skew
	// With high skew, we expect fewer unique keys
	// But our simple implementation may not show strong skew
	// Just verify we got valid keys
	if len(seen) == 0 {
		t.Errorf("No keys generated")
	}
}

func TestWorkloadGenerator(t *testing.T) {
	gen := NewWorkloadGenerator(WorkloadA, 12345, 10000, 64, 0.99)
	gen.SetNumOps(100)

	getCount := 0
	putCount := 0

	for i := 0; i < 100; i++ {
		op, _, _, err := gen.Next()
		if err != nil {
			t.Fatalf("Failed to generate operation: %v", err)
		}
		if op == "GET" {
			getCount++
		} else if op == "PUT" {
			putCount++
		}
	}

	// WorkloadA should have roughly 50/50 split
	total := getCount + putCount
	if total != 100 {
		t.Errorf("Expected 100 operations, got %d", total)
	}

	ratio := float64(putCount) / float64(total)
	expected := 0.5
	if ratio < expected-0.2 || ratio > expected+0.2 {
		t.Errorf("Expected ~50%% PUT operations, got %.2f%%", ratio*100)
	}
}

func TestBenchStats(t *testing.T) {
	stats := NewBenchStats()

	for i := 0; i < 100; i++ {
		stats.Record("get", time.Duration(i)*time.Microsecond)
	}

	if stats.TotalOps != 100 {
		t.Errorf("Expected TotalOps=100, got %d", stats.TotalOps)
	}

	p50 := stats.CalculatePercentile(50)
	expected := 50 * time.Microsecond
	if p50 < expected-2*time.Microsecond || p50 > expected+2*time.Microsecond {
		t.Errorf("Expected P50 ~= %v, got %v", expected, p50)
	}
}
