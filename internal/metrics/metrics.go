package metrics

import (
	"expvar"
	"sync/atomic"
	"time"
)

// Metrics tracks TinyRocks performance metrics.
type Metrics struct {
	// Operations
	GetCount  *expvar.Int
	PutCount  *expvar.Int
	DelCount  *expvar.Int
	ScanCount *expvar.Int

	// Latencies (microseconds)
	GetLatency  *expvar.Float
	PutLatency  *expvar.Float
	DelLatency  *expvar.Float
	ScanLatency *expvar.Float

	// Compaction metrics
	FlushCount        *expvar.Int
	CompactionCount   *expvar.Int
	FlushLatency      *expvar.Float
	CompactionLatency *expvar.Float
	BytesFlushed      *expvar.Int
	BytesCompacted    *expvar.Int

	// Level metrics
	L0Count    *expvar.Int
	L0Size     *expvar.Int
	LevelSizes *expvar.Map

	// WAL metrics
	WALBytes        *expvar.Int
	WALGroupCommits *expvar.Int
	WALFsyncLatency *expvar.Float

	// Queue depths
	FlushQueueDepth      atomic.Int64
	CompactionQueueDepth atomic.Int64

	// Cache metrics
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64
	CacheBytes  atomic.Int64
}

var GlobalMetrics *Metrics

func init() {
	GlobalMetrics = NewMetrics()
}

func NewMetrics() *Metrics {
	m := &Metrics{
		GetCount:  expvar.NewInt("ops_get"),
		PutCount:  expvar.NewInt("ops_put"),
		DelCount:  expvar.NewInt("ops_del"),
		ScanCount: expvar.NewInt("ops_scan"),

		GetLatency:  expvar.NewFloat("lat_get_us"),
		PutLatency:  expvar.NewFloat("lat_put_us"),
		DelLatency:  expvar.NewFloat("lat_del_us"),
		ScanLatency: expvar.NewFloat("lat_scan_us"),

		FlushCount:        expvar.NewInt("flush_count"),
		CompactionCount:   expvar.NewInt("compaction_count"),
		FlushLatency:      expvar.NewFloat("flush_lat_us"),
		CompactionLatency: expvar.NewFloat("compaction_lat_us"),
		BytesFlushed:      expvar.NewInt("bytes_flushed"),
		BytesCompacted:    expvar.NewInt("bytes_compacted"),

		L0Count:    expvar.NewInt("level0_count"),
		L0Size:     expvar.NewInt("level0_size_bytes"),
		LevelSizes: expvar.NewMap("level_sizes"),

		WALBytes:        expvar.NewInt("wal_bytes"),
		WALGroupCommits: expvar.NewInt("wal_group_commits"),
		WALFsyncLatency: expvar.NewFloat("wal_fsync_lat_us"),
	}
	return m
}

// RecordOp records an operation with latency.
func (m *Metrics) RecordOp(op string, latency time.Duration) {
	latencyUs := float64(latency.Microseconds())

	switch op {
	case "get":
		m.GetCount.Add(1)
		m.GetLatency.Set(latencyUs)
	case "put":
		m.PutCount.Add(1)
		m.PutLatency.Set(latencyUs)
	case "del":
		m.DelCount.Add(1)
		m.DelLatency.Set(latencyUs)
	case "scan":
		m.ScanCount.Add(1)
		m.ScanLatency.Set(latencyUs)
	}
}

// RecordFlush records a flush operation.
func (m *Metrics) RecordFlush(latency time.Duration, bytes int64) {
	m.FlushCount.Add(1)
	m.FlushLatency.Set(float64(latency.Microseconds()))
	m.BytesFlushed.Add(bytes)
}

// RecordCompaction records a compaction operation.
func (m *Metrics) RecordCompaction(latency time.Duration, bytes int64) {
	m.CompactionCount.Add(1)
	m.CompactionLatency.Set(float64(latency.Microseconds()))
	m.BytesCompacted.Add(bytes)
}

// RecordCacheHit records a cache hit.
func (m *Metrics) RecordCacheHit(bytes int64) {
	m.CacheHits.Add(1)
	m.CacheBytes.Add(bytes)
}

// RecordCacheMiss records a cache miss.
func (m *Metrics) RecordCacheMiss(bytes int64) {
	m.CacheMisses.Add(1)
	m.CacheBytes.Add(bytes)
}
