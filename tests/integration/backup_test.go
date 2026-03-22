//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// TestBackup_CreatesExpectedObjects verifies that a plain backup:
//  1. Creates base.tar.gz and manifest.json in storage.
//  2. Manifest carries correct metadata (LSNs, version, system ID).
//  3. base.tar.gz is a valid gzip stream containing real PGDATA files.
func TestBackup_CreatesExpectedObjects(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("backup-basic")

	manifest, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	})
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Manifest fields.
	if manifest.BackupName != name {
		t.Errorf("BackupName: got %q, want %q", manifest.BackupName, name)
	}
	if manifest.Encrypted {
		t.Error("Encrypted should be false for NoopEncryptor")
	}
	if manifest.StartLSN == "" || manifest.EndLSN == "" {
		t.Errorf("LSNs must not be empty: start=%q end=%q", manifest.StartLSN, manifest.EndLSN)
	}
	if manifest.PostgresVersion == "" {
		t.Error("PostgresVersion must not be empty")
	}
	if manifest.SystemID == "" {
		t.Error("SystemID must not be empty")
	}

	// Storage objects.
	if !st.Has(name + "/manifest.json") {
		t.Errorf("manifest.json not found; keys: %v", st.Keys())
	}
	if !st.Has(name + "/base.tar.gz") {
		t.Errorf("base.tar.gz not found; keys: %v", st.Keys())
	}

	// base.tar.gz must be a valid gzip stream containing real PGDATA files.
	baseData, _ := st.Get(name + "/base.tar.gz")
	if len(baseData) == 0 {
		t.Fatal("base.tar.gz is empty")
	}
	if baseData[0] != 0x1f || baseData[1] != 0x8b {
		t.Error("base.tar.gz does not start with gzip magic bytes")
	}

	found, err := tarHasEntry(baseData, "PG_VERSION")
	if err != nil {
		t.Fatalf("reading base tar: %v", err)
	}
	if !found {
		found, err = tarHasEntry(baseData, "global/pg_control")
		if err != nil {
			t.Fatalf("reading base tar: %v", err)
		}
	}
	if !found {
		t.Error("base.tar.gz does not contain PG_VERSION or global/pg_control")
	}
}

// TestBackup_ManifestRoundTrip verifies that the manifest written during backup
// can be read back from storage with identical fields.
func TestBackup_ManifestRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("backup-manifest")

	written, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   1,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	})
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	read, err := pgbackup.ReadManifest(ctx, st, name)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}

	if written.StartLSN != read.StartLSN {
		t.Errorf("StartLSN: written=%q read=%q", written.StartLSN, read.StartLSN)
	}
	if written.EndLSN != read.EndLSN {
		t.Errorf("EndLSN: written=%q read=%q", written.EndLSN, read.EndLSN)
	}
	if written.SystemID != read.SystemID {
		t.Errorf("SystemID: written=%q read=%q", written.SystemID, read.SystemID)
	}
	if len(written.Tablespaces) != len(read.Tablespaces) {
		t.Errorf("Tablespaces: written=%d read=%d", len(written.Tablespaces), len(read.Tablespaces))
	}
}

// TestBackup_EncryptedPassphrase verifies that a passphrase-encrypted backup:
//  1. Stores ciphertext (not readable as gzip).
//  2. Sets Encrypted=true in the manifest.
//  3. Uses the .age key suffix.
func TestBackup_EncryptedPassphrase(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("backup-enc-pass")

	manifest, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NewPassphraseEncryptor("integration-test-passphrase"),
		Storage:    st,
		Log:        testLogger(t),
	})
	if err != nil {
		t.Fatalf("Backup (passphrase): %v", err)
	}

	if !manifest.Encrypted {
		t.Error("Encrypted must be true for PassphraseEncryptor")
	}

	baseKey := name + "/base.tar.gz.age"
	if !st.Has(baseKey) {
		t.Errorf(".age key not found; keys: %v", st.Keys())
	}

	data, _ := st.Get(baseKey)
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		t.Error("encrypted output starts with gzip magic — encryption was skipped")
	}
}

// TestBackup_EncryptedKeyPair verifies that an age X25519 key-pair backup
// stores opaque ciphertext and marks the manifest as encrypted.
func TestBackup_EncryptedKeyPair(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("backup-enc-keypair")

	recipientFile, _ := generateKeyPairFiles(t)

	manifest, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NewKeyPairEncryptor(recipientFile, ""),
		Storage:    st,
		Log:        testLogger(t),
	})
	if err != nil {
		t.Fatalf("Backup (key-pair): %v", err)
	}

	if !manifest.Encrypted {
		t.Error("Encrypted must be true for KeyPairEncryptor")
	}

	baseKey := name + "/base.tar.gz.age"
	if !st.Has(baseKey) {
		t.Errorf(".age key not found; keys: %v", st.Keys())
	}
}

// TestBackup_MultipleBackups_Listable verifies that two consecutive backups
// use distinct storage keys with no prefix collisions.
func TestBackup_MultipleBackups_Listable(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	names := []string{uniqueName("multi-a"), uniqueName("multi-b")}

	for _, name := range names {
		if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
			DBUrl:      pgURL(t),
			BackupName: name,
			Parallel:   2,
			Encryptor:  crypto.NoopEncryptor{},
			Storage:    st,
			Log:        testLogger(t),
		}); err != nil {
			t.Fatalf("Backup %q: %v", name, err)
		}
	}

	for _, name := range names {
		if !st.Has(name + "/manifest.json") {
			t.Errorf("manifest.json for %q not found", name)
		}
		if !st.Has(name + "/base.tar.gz") {
			t.Errorf("base.tar.gz for %q not found", name)
		}
	}

	// No key must belong to more than one backup prefix.
	for _, k := range st.Keys() {
		count := 0
		for _, name := range names {
			if strings.HasPrefix(k, name+"/") {
				count++
			}
		}
		if count > 1 {
			t.Errorf("key %q matches multiple backup prefixes — collision", k)
		}
	}
}
