package tinyrocks

import (
	"github.com/arthurzhang/kivi/internal/metrics"
)

// Store represents the TinyRocks key-value store.
type Store struct {
	config  *metrics.Config
	metrics *metrics.Metrics
	// TODO: Add actual storage structures
}

// NewStore creates a new TinyRocks store.
func NewStore(config *metrics.Config) *Store {
	if config == nil {
		config = metrics.DefaultConfig()
	}

	return &Store{
		config:  config,
		metrics: metrics.GlobalMetrics,
	}
}

// Get retrieves a value by key.
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	// TODO: Implement
	return nil, false, nil
}

// Put stores a key-value pair.
func (s *Store) Put(key, val []byte) error {
	// TODO: Implement
	return nil
}

// Delete removes a key.
func (s *Store) Delete(key []byte) error {
	// TODO: Implement
	return nil
}

// Iterator provides range scans.
type Iterator interface {
	Seek(key []byte)
	Next() bool
	Key() []byte
	Value() []byte
	Close() error
}

// NewIterator creates a new iterator.
func (s *Store) NewIterator(start, end []byte) Iterator {
	// TODO: Implement
	return nil
}
