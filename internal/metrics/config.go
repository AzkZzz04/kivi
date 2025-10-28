package metrics

import (
	"encoding/json"
	"io"
	"os"
)

// Config holds TinyRocks configuration.
type Config struct {
	// WAL configuration
	WALDir           string `json:"wal_dir"`
	WALGroupCommitMS int    `json:"wal_group_commit_ms"`

	// Memtable configuration
	MemtableMB int `json:"memtable_mb"`

	// SSTable configuration
	BlockSizeKB     int `json:"block_size_kb"`
	RestartInterval int `json:"restart_interval"`
	BloomBitsPerKey int `json:"bloom_bits_per_key"`

	// Compaction configuration
	Fanout                   int `json:"fanout"`
	MaxBackgroundCompactions int `json:"max_background_compactions"`
	L0Slowdown               int `json:"l0_slowdown"`
	L0Stop                   int `json:"l0_stop"`
	CompactionRateLimitMBps  int `json:"compaction_rate_limit_mb_s"`

	// Flush configuration
	FlushParallelism int `json:"flush_parallelism"`

	// Cache configuration
	BlockCacheMB   int  `json:"block_cache_mb"`
	PrefetchOnSeek bool `json:"prefetch_on_seek"`

	// Data directory
	DataDir string `json:"data_dir"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		WALDir:                   "wal",
		WALGroupCommitMS:         10,
		MemtableMB:               64,
		BlockSizeKB:              16,
		RestartInterval:          16,
		BloomBitsPerKey:          10,
		Fanout:                   10,
		MaxBackgroundCompactions: 2,
		L0Slowdown:               8,
		L0Stop:                   12,
		CompactionRateLimitMBps:  512,
		FlushParallelism:         2,
		BlockCacheMB:             256,
		PrefetchOnSeek:           false,
		DataDir:                  "data",
	}
}

// LoadConfig loads configuration from a JSON file.
func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return LoadConfigFromReader(f)
}

// LoadConfigFromReader loads configuration from an io.Reader.
func LoadConfigFromReader(r io.Reader) (*Config, error) {
	var cfg Config
	if err := json.NewDecoder(r).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Save writes the configuration to a JSON file.
func (c *Config) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(c)
}
