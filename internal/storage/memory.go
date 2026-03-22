package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// MemoryStorage is an in-memory Storage implementation used in tests.
// It is safe for concurrent use.
type MemoryStorage struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{objects: make(map[string][]byte)}
}

func (m *MemoryStorage) Upload(_ context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading upload data for %q: %w", key, err)
	}
	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return nil
}

func (m *MemoryStorage) Download(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	data, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("object %q not found", key)
	}
	// Return a copy so callers can't mutate internal state.
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MemoryStorage) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *MemoryStorage) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	_, ok := m.objects[key]
	m.mu.RUnlock()
	return ok, nil
}

func (m *MemoryStorage) Close() error { return nil }

// ── Test helpers ──────────────────────────────────────────────────────────────

// Has reports whether the given key exists.
func (m *MemoryStorage) Has(key string) bool {
	m.mu.RLock()
	_, ok := m.objects[key]
	m.mu.RUnlock()
	return ok
}

// Size returns the byte size of the stored object, or 0 if it doesn't exist.
func (m *MemoryStorage) Size(key string) int {
	m.mu.RLock()
	data := m.objects[key]
	m.mu.RUnlock()
	return len(data)
}

// Keys returns all stored object keys.
func (m *MemoryStorage) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	return keys
}

// Get returns the raw bytes stored for key (test helper).
func (m *MemoryStorage) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	data, ok := m.objects[key]
	m.mu.RUnlock()
	return data, ok
}
