package pgbackup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// Tablespace describes one PostgreSQL tablespace included in the backup.
type Tablespace struct {
	OID      int32  `json:"oid"`
	Location string `json:"location"`    // original path on the source server
	StorKey  string `json:"storage_key"` // object key in storage (e.g. "base.tar.gz")
}

// Manifest records everything needed to locate and restore a backup.
type Manifest struct {
	BackupName      string       `json:"backup_name"`
	Timestamp       time.Time    `json:"timestamp"`
	PostgresVersion string       `json:"postgres_version"`
	SystemID        string       `json:"system_id"`
	BackupLabel     string       `json:"backup_label"`
	StartLSN        string       `json:"start_lsn"`
	EndLSN          string       `json:"end_lsn"`
	Encrypted       bool         `json:"encrypted"`
	Tablespaces     []Tablespace `json:"tablespaces"`
}

// manifestKey returns the storage key for the manifest of a named backup.
func manifestKey(backupName string) string {
	return backupName + "/manifest.json"
}

// WriteManifest serialises m and uploads it to storage.
func WriteManifest(ctx context.Context, st storage.Storage, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling manifest: %w", err)
	}

	if err := st.Upload(ctx, manifestKey(m.BackupName), bytes.NewReader(data)); err != nil {
		return fmt.Errorf("uploading manifest: %w", err)
	}
	return nil
}

// ReadManifest downloads and deserialises the manifest for backupName.
func ReadManifest(ctx context.Context, st storage.Storage, backupName string) (*Manifest, error) {
	rc, err := st.Download(ctx, manifestKey(backupName))
	if err != nil {
		return nil, fmt.Errorf("downloading manifest for %q: %w", backupName, err)
	}
	defer rc.Close()

	var m Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, fmt.Errorf("decoding manifest for %q: %w", backupName, err)
	}
	return &m, nil
}
