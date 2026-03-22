package pgbackup_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

func baseManifest() *pgbackup.Manifest {
	return &pgbackup.Manifest{
		BackupName:      "test-backup-2026-03-07",
		Timestamp:       time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC),
		PostgresVersion: "16.2",
		SystemID:        "7234567890123456789",
		BackupLabel:     "test-backup-2026-03-07",
		StartLSN:        "0/3000060",
		EndLSN:          "0/5000000",
		Encrypted:       false,
		Tablespaces: []pgbackup.Tablespace{
			{OID: 0, Location: "", StorKey: "test-backup-2026-03-07/base.tar.gz"},
		},
	}
}

// TestWriteReadManifest_RoundTrip verifies that WriteManifest + ReadManifest
// produces a manifest equal to the original.
func TestWriteReadManifest_RoundTrip(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	original := baseManifest()

	if err := pgbackup.WriteManifest(ctx, st, original); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	got, err := pgbackup.ReadManifest(ctx, st, original.BackupName)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}

	if got.BackupName != original.BackupName {
		t.Errorf("BackupName: got %q, want %q", got.BackupName, original.BackupName)
	}
	if got.PostgresVersion != original.PostgresVersion {
		t.Errorf("PostgresVersion: got %q, want %q", got.PostgresVersion, original.PostgresVersion)
	}
	if got.SystemID != original.SystemID {
		t.Errorf("SystemID: got %q, want %q", got.SystemID, original.SystemID)
	}
	if got.StartLSN != original.StartLSN {
		t.Errorf("StartLSN: got %q, want %q", got.StartLSN, original.StartLSN)
	}
	if got.EndLSN != original.EndLSN {
		t.Errorf("EndLSN: got %q, want %q", got.EndLSN, original.EndLSN)
	}
	if got.Encrypted != original.Encrypted {
		t.Errorf("Encrypted: got %v, want %v", got.Encrypted, original.Encrypted)
	}
	if !got.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp: got %v, want %v", got.Timestamp, original.Timestamp)
	}
}

// TestWriteReadManifest_Tablespaces verifies multi-tablespace manifests round-trip correctly.
func TestWriteReadManifest_Tablespaces(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	original := baseManifest()
	original.Tablespaces = []pgbackup.Tablespace{
		{OID: 0, Location: "", StorKey: "mybackup/base.tar.gz"},
		{OID: 16384, Location: "/var/lib/pgsql/ts1", StorKey: "mybackup/16384.tar.gz"},
		{OID: 16385, Location: "/var/lib/pgsql/ts2", StorKey: "mybackup/16385.tar.gz"},
	}

	if err := pgbackup.WriteManifest(ctx, st, original); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	got, err := pgbackup.ReadManifest(ctx, st, original.BackupName)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}

	if len(got.Tablespaces) != len(original.Tablespaces) {
		t.Fatalf("Tablespaces length: got %d, want %d", len(got.Tablespaces), len(original.Tablespaces))
	}

	for i, want := range original.Tablespaces {
		g := got.Tablespaces[i]
		if g.OID != want.OID {
			t.Errorf("Tablespace[%d].OID: got %d, want %d", i, g.OID, want.OID)
		}
		if g.Location != want.Location {
			t.Errorf("Tablespace[%d].Location: got %q, want %q", i, g.Location, want.Location)
		}
		if g.StorKey != want.StorKey {
			t.Errorf("Tablespace[%d].StorKey: got %q, want %q", i, g.StorKey, want.StorKey)
		}
	}
}

// TestWriteManifest_StorageKey verifies the manifest is stored under the expected key.
func TestWriteManifest_StorageKey(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	m := baseManifest()
	m.BackupName = "prod-2026-03-07"

	if err := pgbackup.WriteManifest(ctx, st, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	expectedKey := "prod-2026-03-07/manifest.json"
	if !st.Has(expectedKey) {
		t.Errorf("expected manifest at %q, but key not found; stored keys: %v", expectedKey, st.Keys())
	}
}

// TestWriteManifest_ValidJSON verifies the stored manifest is valid JSON
// and contains expected fields.
func TestWriteManifest_ValidJSON(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	m := baseManifest()
	m.BackupName = "json-test"

	if err := pgbackup.WriteManifest(ctx, st, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	data, ok := st.Get("json-test/manifest.json")
	if !ok {
		t.Fatal("manifest not found in storage")
	}

	s := string(data)
	for _, field := range []string{`"backup_name"`, `"timestamp"`, `"postgres_version"`,
		`"system_id"`, `"start_lsn"`, `"end_lsn"`, `"encrypted"`, `"tablespaces"`} {
		if !strings.Contains(s, field) {
			t.Errorf("manifest JSON missing field %s", field)
		}
	}
}

// TestWriteManifest_EncryptedFlag verifies the encrypted flag is preserved correctly.
func TestWriteManifest_EncryptedFlag(t *testing.T) {
	for _, encrypted := range []bool{true, false} {
		t.Run("encrypted="+boolStr(encrypted), func(t *testing.T) {
			ctx := context.Background()
			st := storage.NewMemoryStorage()

			m := baseManifest()
			m.Encrypted = encrypted

			if err := pgbackup.WriteManifest(ctx, st, m); err != nil {
				t.Fatalf("WriteManifest: %v", err)
			}

			got, err := pgbackup.ReadManifest(ctx, st, m.BackupName)
			if err != nil {
				t.Fatalf("ReadManifest: %v", err)
			}

			if got.Encrypted != encrypted {
				t.Errorf("Encrypted: got %v, want %v", got.Encrypted, encrypted)
			}
		})
	}
}

// TestReadManifest_NotFound verifies a useful error is returned for missing backups.
func TestReadManifest_NotFound(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	_, err := pgbackup.ReadManifest(ctx, st, "nonexistent-backup")
	if err == nil {
		t.Fatal("expected error for missing backup, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent-backup") {
		t.Errorf("error should mention backup name, got: %v", err)
	}
}

// TestWriteManifest_Overwrites verifies that writing twice replaces the first manifest.
func TestWriteManifest_Overwrites(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	m1 := baseManifest()
	m1.PostgresVersion = "16.1"
	if err := pgbackup.WriteManifest(ctx, st, m1); err != nil {
		t.Fatalf("first WriteManifest: %v", err)
	}

	m2 := baseManifest()
	m2.PostgresVersion = "16.2"
	if err := pgbackup.WriteManifest(ctx, st, m2); err != nil {
		t.Fatalf("second WriteManifest: %v", err)
	}

	got, err := pgbackup.ReadManifest(ctx, st, m2.BackupName)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.PostgresVersion != "16.2" {
		t.Errorf("expected overwritten version 16.2, got %q", got.PostgresVersion)
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
