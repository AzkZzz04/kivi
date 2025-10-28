package testutil

import (
	"io"
	"log"
	"os"
	"time"
)

// Logger provides structured logging.
type Logger struct {
	*log.Logger
	level int
}

const (
	LevelDebug = iota
	LevelInfo
	LevelWarn
	LevelError
)

// NewLogger creates a new logger.
func NewLogger(w io.Writer, prefix string, level int) *Logger {
	return &Logger{
		Logger: log.New(w, prefix, log.LstdFlags),
		level:  level,
	}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= LevelDebug {
		l.Printf("[DEBUG] "+format, args...)
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= LevelInfo {
		l.Printf("[INFO] "+format, args...)
	}
}

func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= LevelWarn {
		l.Printf("[WARN] "+format, args...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= LevelError {
		l.Printf("[ERROR] "+format, args...)
	}
}

// Timer measures elapsed time.
type Timer struct {
	start time.Time
	name  string
}

func NewTimer(name string) *Timer {
	return &Timer{
		start: time.Now(),
		name:  name,
	}
}

func (t *Timer) Elapsed() time.Duration {
	return time.Since(t.start)
}

func (t *Timer) Log(logger *Logger) {
	logger.Info("%s took %v", t.name, t.Elapsed())
}

// BenchStats tracks benchmark statistics.
type BenchStats struct {
	TotalOps     int64
	TotalLatency time.Duration
	MinLatency   time.Duration
	MaxLatency   time.Duration
	Latencies    []time.Duration
}

func NewBenchStats() *BenchStats {
	return &BenchStats{
		MinLatency: time.Hour,
	}
}

func (bs *BenchStats) Record(op string, latency time.Duration) {
	bs.TotalOps++
	bs.TotalLatency += latency
	if latency < bs.MinLatency {
		bs.MinLatency = latency
	}
	if latency > bs.MaxLatency {
		bs.MaxLatency = latency
	}
	bs.Latencies = append(bs.Latencies, latency)
}

func (bs *BenchStats) OpsPerSec() float64 {
	if bs.TotalLatency == 0 {
		return 0
	}
	return float64(bs.TotalOps) / bs.TotalLatency.Seconds()
}

// CalculatePercentile calculates the p-th percentile latency.
func (bs *BenchStats) CalculatePercentile(p float64) time.Duration {
	if len(bs.Latencies) == 0 {
		return 0
	}

	idx := int(float64(len(bs.Latencies)) * p / 100.0)
	if idx >= len(bs.Latencies) {
		idx = len(bs.Latencies) - 1
	}
	return bs.Latencies[idx]
}

// Print prints benchmark statistics.
func (bs *BenchStats) Print(logger *Logger) {
	avg := time.Duration(0)
	if bs.TotalOps > 0 {
		avg = bs.TotalLatency / time.Duration(bs.TotalOps)
	}

	logger.Info("Benchmark Results:")
	logger.Info("  Total Ops: %d", bs.TotalOps)
	logger.Info("  Avg Latency: %v", avg)
	logger.Info("  Min Latency: %v", bs.MinLatency)
	logger.Info("  Max Latency: %v", bs.MaxLatency)
	logger.Info("  Throughput: %.2f ops/sec", bs.OpsPerSec())

	if len(bs.Latencies) > 0 {
		logger.Info("  P50: %v", bs.CalculatePercentile(50))
		logger.Info("  P95: %v", bs.CalculatePercentile(95))
		logger.Info("  P99: %v", bs.CalculatePercentile(99))
	}
}

// LogConfig writes a debug info.
func LogConfig(logger *Logger, cfg interface{}) {
	logger.Debug("Configuration: %+v", cfg)
}

// SetupLogging sets up global logging.
func SetupLogging(path string, level int) (*Logger, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	multiWriter := io.MultiWriter(os.Stdout, file)
	logger := NewLogger(multiWriter, "", level)

	return logger, nil
}

// Global logger instance
var defaultLogger = NewLogger(os.Stdout, "", LevelInfo)

// SetLevel sets the global logger level.
func SetLevel(level int) {
	defaultLogger.level = level
}

func Debug(format string, args ...interface{}) {
	defaultLogger.Debug(format, args...)
}

func Info(format string, args ...interface{}) {
	defaultLogger.Info(format, args...)
}

func Warn(format string, args ...interface{}) {
	defaultLogger.Warn(format, args...)
}

func Error(format string, args ...interface{}) {
	defaultLogger.Error(format, args...)
}
