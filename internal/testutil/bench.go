package testutil

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// ZipfGenerator generates keys following a Zipfian distribution.
type ZipfGenerator struct {
	zipf *big.Int
	seed int64
	n    int64
	s    float64
	v    float64
	x    float64
}

// NewZipfGenerator creates a new Zipfian generator.
// n: key space size, s: skewness (higher = more skew)
func NewZipfGenerator(n int64, s float64, seed int64) *ZipfGenerator {
	return &ZipfGenerator{
		n:    n,
		s:    s,
		seed: seed,
		zipf: big.NewInt(seed),
		v:    math.Pow(math.E, -1.0/s),
	}
}

// Next returns the next key in the Zipfian distribution.
func (z *ZipfGenerator) Next() int64 {
	// Simple inverse CDF method for Zipfian
	u := z.nextRandom()
	x := int64((float64(z.n) * u) + 1)
	// Apply power-law skew
	skewed := math.Pow(float64(x)/float64(z.n), z.s)
	return int64(skewed * float64(z.n))
}

func (z *ZipfGenerator) nextRandom() float64 {
	z.zipf.Add(z.zipf, big.NewInt(1103515245))
	z.zipf.Mul(z.zipf, big.NewInt(12345))
	z.zipf.Mod(z.zipf, big.NewInt(1<<31))
	return float64(z.zipf.Int64()) / (1 << 31)
}

// WorkloadType represents different workload patterns.
type WorkloadType int

const (
	WorkloadA WorkloadType = iota // 50% read, 50% update
	WorkloadB                     // 95% read, 5% update
	WorkloadC                     // 100% read
	WorkloadE                     // 95% read, 5% insert
	WorkloadF                     // 50% read, 50% read-modify-write
)

// WorkloadGenerator generates operations according to a workload spec.
type WorkloadGenerator struct {
	rng       *RandSeeded
	keyGen    *ZipfGenerator
	workload  WorkloadType
	valueSize int
	numOps    int
	opCount   int
	keyCount  int64
}

// NewWorkloadGenerator creates a new workload generator.
func NewWorkloadGenerator(workload WorkloadType, seed int64, numKeys int64, valueSize int, skew float64) *WorkloadGenerator {
	return &WorkloadGenerator{
		rng:       NewRandSeeded(seed),
		keyGen:    NewZipfGenerator(numKeys, skew, seed),
		workload:  workload,
		valueSize: valueSize,
		numOps:    1000000, // default
		keyCount:  numKeys,
	}
}

// SetNumOps sets the total number of operations to generate.
func (wg *WorkloadGenerator) SetNumOps(n int) {
	wg.numOps = n
}

// Next generates the next operation type and key/value.
func (wg *WorkloadGenerator) Next() (op string, key []byte, val []byte, err error) {
	if wg.opCount >= wg.numOps {
		return "", nil, nil, fmt.Errorf("workload exhausted")
	}
	wg.opCount++

	keyIdx := wg.keyGen.Next()
	key = make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(keyIdx))

	var shouldRead, shouldUpdate bool
	switch wg.workload {
	case WorkloadA:
		shouldRead = wg.rng.Float64() < 0.5
		shouldUpdate = !shouldRead
	case WorkloadB:
		shouldRead = wg.rng.Float64() < 0.95
		shouldUpdate = !shouldRead
	case WorkloadC:
		shouldRead = true
		shouldUpdate = false
	case WorkloadE:
		shouldRead = wg.rng.Float64() < 0.95
		shouldUpdate = !shouldRead
	case WorkloadF:
		shouldUpdate = wg.rng.Float64() < 0.5
		shouldRead = false // RMW reads internally
	}

	if shouldUpdate {
		op = "PUT"
		val = make([]byte, wg.valueSize)
		rand.Read(val)
	} else {
		op = "GET"
	}

	return op, key, val, nil
}

// RandSeeded is a simple seeded RNG for deterministic randomness.
type RandSeeded struct {
	state int64
}

func NewRandSeeded(seed int64) *RandSeeded {
	return &RandSeeded{state: seed}
}

func (r *RandSeeded) Int() int64 {
	r.state = ((r.state * 1103515245) + 12345) & 0x7fffffff
	return r.state
}

func (r *RandSeeded) Float64() float64 {
	return float64(r.Int()) / (1 << 31)
}

// EnsureDir ensures a directory exists, creating it if necessary.
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

// TempDir creates a temporary directory for tests.
func TempDir() (string, error) {
	return os.MkdirTemp("", "tinyrocks-test-*")
}

// CleanupDir removes a directory and all its contents.
func CleanupDir(path string) error {
	return os.RemoveAll(path)
}

// WriteTestFile writes test data to a file.
func WriteTestFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := EnsureDir(dir); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	return err
}

// ReadLines reads all lines from a file.
func ReadLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// RetryWithBackoff retries a function with exponential backoff.
func RetryWithBackoff(fn func() error, maxRetries int, initialDelay time.Duration) error {
	delay := initialDelay
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if i < maxRetries-1 {
			time.Sleep(delay)
			delay *= 2
		}
	}
	return lastErr
}

// WaitFor condition with timeout.
func WaitFor(condition func() bool, timeout time.Duration, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("timeout waiting for condition")
}
