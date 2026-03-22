//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// mustExistInPGData fatals if any of the named files/dirs are absent under pgdata.
func mustExistInPGData(t *testing.T, pgdata string, names ...string) {
	t.Helper()
	for _, name := range names {
		if _, err := os.Stat(filepath.Join(pgdata, name)); os.IsNotExist(err) {
			t.Errorf("expected %q to exist in PGDATA", name)
		}
	}
}

// TestRestore_Basic performs a plain backup → restore and verifies the canonical
// PGDATA structure and PostgreSQL recovery files.
func TestRestore_Basic(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("restore-basic")

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	pgdata := emptyDir(t)
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  pgdata,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Plain restore: no PITR requested, so recovery.signal must NOT be written
	// (PG12+ requires restore_command whenever recovery.signal is present).
	// PostgreSQL performs crash recovery automatically via backup_label.
	mustExistInPGData(t, pgdata, "PG_VERSION", "global", "base")

	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write recovery.signal — use PITR flags to enable it")
	}
}

// TestRestore_Passphrase_Encrypted verifies a full backup → restore cycle with
// passphrase encryption.
func TestRestore_Passphrase_Encrypted(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("restore-enc-pass")
	passphrase := "test-roundtrip-passphrase"

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NewPassphraseEncryptor(passphrase),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup (passphrase): %v", err)
	}

	pgdata := emptyDir(t)
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  pgdata,
		Parallel:   2,
		Encryptor:  crypto.NewPassphraseEncryptor(passphrase),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Restore (passphrase): %v", err)
	}

	mustExistInPGData(t, pgdata, "PG_VERSION", "base")
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write recovery.signal")
	}
}

// TestRestore_KeyPair_Encrypted verifies a full backup → restore cycle with
// age X25519 key-pair encryption.
func TestRestore_KeyPair_Encrypted(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("restore-enc-keypair")

	recipientFile, identityFile := generateKeyPairFiles(t)

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NewKeyPairEncryptor(recipientFile, ""),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup (key-pair): %v", err)
	}

	pgdata := emptyDir(t)
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  pgdata,
		Parallel:   2,
		Encryptor:  crypto.NewKeyPairEncryptor("", identityFile),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Restore (key-pair): %v", err)
	}

	mustExistInPGData(t, pgdata, "PG_VERSION", "base")
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write recovery.signal")
	}
}

// TestRestore_WrongPassphrase verifies that restore with the wrong passphrase
// returns an error — the backup cannot be silently corrupted.
func TestRestore_WrongPassphrase(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("restore-wrong-pass")

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   1,
		Encryptor:  crypto.NewPassphraseEncryptor("correct-passphrase"),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	pgdata := emptyDir(t)
	err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  pgdata,
		Parallel:   1,
		Encryptor:  crypto.NewPassphraseEncryptor("wrong-passphrase"),
		Storage:    st,
		Log:        testLogger(t),
	})
	if err == nil {
		t.Fatal("expected error when restoring with wrong passphrase, got nil")
	}
}

// TestRestore_DownloadApplySplit exercises the two-phase restore API.
// Download fetches archives to a staging directory; Apply extracts them
// separately — this allows setting recovery targets between the phases.
func TestRestore_DownloadApplySplit(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("restore-split")

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	pgdata := emptyDir(t)
	cfg := pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  pgdata,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}

	// Phase 1: Download.
	result, err := pgbackup.Download(ctx, cfg)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer os.RemoveAll(result.StagingDir)

	entries, err := os.ReadDir(result.StagingDir)
	if err != nil {
		t.Fatalf("reading staging dir: %v", err)
	}
	if len(entries) == 0 {
		t.Error("staging dir must have at least one blob after Download")
	}
	if result.Manifest == nil || result.Manifest.BackupName != name {
		t.Errorf("Download manifest incorrect: %+v", result.Manifest)
	}

	// Phase 2: Apply.
	if err := pgbackup.Apply(ctx, cfg, result); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	mustExistInPGData(t, pgdata, "PG_VERSION", "base")
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore (Download+Apply) must NOT write recovery.signal")
	}
}

// TestRestore_PITRConfig verifies that all PITR recovery parameters
// (target time, LSN, restore_command) are written into postgresql.auto.conf.
func TestRestore_PITRConfig(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("restore-pitr")

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   1,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	pgdata := emptyDir(t)
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName:         name,
		PGDataDir:          pgdata,
		Parallel:           1,
		Encryptor:          crypto.NoopEncryptor{},
		Storage:            st,
		Log:                testLogger(t),
		RecoveryTargetTime: "2026-03-07 14:30:00 UTC",
		RecoveryTargetLSN:  "0/5200000",
		RestoreCommand:     "cloud-dump wal-fetch %f %p --storage storj",
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	conf, err := os.ReadFile(filepath.Join(pgdata, "postgresql.auto.conf"))
	if err != nil {
		t.Fatalf("reading postgresql.auto.conf: %v", err)
	}
	s := string(conf)
	for _, want := range []string{
		"restore_command = 'cloud-dump wal-fetch %f %p --storage storj'",
		"recovery_target_time = '2026-03-07 14:30:00 UTC'",
		"recovery_target_lsn = '0/5200000'",
		"recovery_target_action = 'promote'",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("postgresql.auto.conf missing:\n  %s\ngot:\n%s", want, s)
		}
	}

	// PITR restore MUST write recovery.signal.
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); os.IsNotExist(err) {
		t.Error("PITR restore must write recovery.signal")
	}
}

// TestRestore_PlainNoRecoverySignal verifies that a plain restore (no PITR flags)
// does NOT write recovery.signal. PostgreSQL 12+ requires restore_command whenever
// recovery.signal is present — writing it without restore_command causes a FATAL.
func TestRestore_PlainNoRecoverySignal(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := uniqueName("restore-no-signal")

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:     pgURL(t),
		BackupName: name,
		Parallel:  1,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	pgdata := emptyDir(t)
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  pgdata,
		Parallel:   1,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
		// No RestoreCommand, RecoveryTargetTime, or RecoveryTargetLSN set.
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write recovery.signal — PostgreSQL would fail to start without restore_command")
	}
}

// TestBackup_ClusterLayout verifies that when a cluster prefix is embedded in
// the backup name, objects are stored under <cluster>/backups/<name>/ and WAL
// can be pushed to <cluster>/wal/ — both in the same storage instance.
func TestBackup_ClusterLayout(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	clusterName := uniqueName("cluster")
	backupName := "2026-03-22-0800"
	fullKey := clusterName + "/backup/" + backupName

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: fullKey,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Objects must be stored under the cluster-prefixed path.
	if !st.Has(fullKey + "/manifest.json") {
		t.Errorf("manifest not found under cluster path; keys: %v", st.Keys())
	}
	if !st.Has(fullKey + "/base.tar.gz") {
		t.Errorf("base.tar.gz not found under cluster path; keys: %v", st.Keys())
	}

	// No keys should exist at the bare backup name (non-cluster path).
	for _, k := range st.Keys() {
		if strings.HasPrefix(k, backupName+"/") {
			t.Errorf("found key %q at non-cluster path — cluster prefix not applied", k)
		}
	}

	// Push a WAL segment to the cluster WAL prefix.
	walPath, walName := fakeWALSegment(t, 1024)
	walPrefix := clusterName + "/wal_archive"
	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush to cluster WAL prefix: %v", err)
	}

	walKey := walPrefix + "/" + walName + ".gz"
	if !st.Has(walKey) {
		t.Errorf("WAL segment not found at cluster path %q; keys: %v", walKey, st.Keys())
	}
}

// TestRestore_ClusterLayout verifies that a backup stored under a cluster prefix
// can be restored using the full key path, and that PITR config correctly
// references the cluster WAL prefix.
func TestRestore_ClusterLayout(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	clusterName := uniqueName("cluster")
	fullKey := clusterName + "/backup/2026-03-22-0800"
	walPrefix := clusterName + "/wal_archive"

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: fullKey,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	pgdata := emptyDir(t)
	restoreCmd := "cloud-dump wal-fetch %f %p --storage storj --wal-prefix " + walPrefix
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName:         fullKey,
		PGDataDir:          pgdata,
		Parallel:           2,
		Encryptor:          crypto.NoopEncryptor{},
		Storage:            st,
		Log:                testLogger(t),
		RecoveryTargetTime: "2026-03-22 10:30:00 UTC",
		RestoreCommand:     restoreCmd,
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	mustExistInPGData(t, pgdata, "recovery.signal", "PG_VERSION", "base")

	conf, _ := os.ReadFile(filepath.Join(pgdata, "postgresql.auto.conf"))
	s := string(conf)
	if !strings.Contains(s, walPrefix) {
		t.Errorf("postgresql.auto.conf should reference WAL prefix %q; got:\n%s", walPrefix, s)
	}
	if !strings.Contains(s, "recovery_target_time = '2026-03-22 10:30:00 UTC'") {
		t.Errorf("postgresql.auto.conf missing recovery_target_time; got:\n%s", s)
	}
}
