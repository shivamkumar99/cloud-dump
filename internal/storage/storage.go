package storage

import (
	"context"
	"fmt"
	"io"
)

// Storage defines the contract for any cloud storage backend.
// Implementations: Storj (internal/storage/storj.go), future: S3, Azure.
type Storage interface {
	// Upload streams r to the given key. The key is the full object path.
	Upload(ctx context.Context, key string, r io.Reader) error

	// Download returns a stream for the given key. Caller must close it.
	Download(ctx context.Context, key string) (io.ReadCloser, error)

	// Exists returns true if the object key exists in storage.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns all object keys that share the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Close releases the underlying connection / client resources.
	Close() error
}

// Config holds provider-agnostic + provider-specific config fields.
type Config struct {
	Provider string // "storj", "s3", "azure"

	// Storj — Option 1: pre-serialized access grant (simplest)
	StorjAccess string
	// Storj — Option 2: individual credentials (API key + satellite + passphrase)
	StorjAPIKey     string
	StorjSatellite  string
	StorjPassphrase string
	// Storj — common
	StorjBucket string
}

// NewStorage is the factory that returns the correct Storage implementation.
// Adding a new backend: implement Storage, add a case here — nothing else changes.
func NewStorage(ctx context.Context, cfg Config) (Storage, error) {
	switch cfg.Provider {
	case "storj":
		return newStorjStorage(ctx, cfg)
	default:
		return nil, fmt.Errorf("unknown storage provider %q (supported: storj)", cfg.Provider)
	}
}
