//go:build integration

package integration

// Storj end-to-end tests.
//
// These tests run the full pipeline against a real Storj bucket.
// They are skipped automatically when the required env vars are not set.
//
// Required env vars — use ONE of the two authentication methods:
//
//	Method A (access grant):
//	  STORJ_ACCESS  — serialised Storj access grant
//
//	Method B (API key):
//	  STORJ_API_KEY      — Storj API key
//	  STORJ_SATELLITE    — satellite address (e.g. 121RTSDp…@ap1.storj.io:7777)
//	  STORJ_PASSPHRASE   — Storj encryption passphrase
//
//	Common (both methods):
//	  STORJ_BUCKET  — bucket name (will be created if it does not exist)
//
// WAL PITR tests additionally require:
//
//	postgres17-wal running on localhost:5436 (make wal-build && make wal-up)
//	WAL archiving configured (CLUSTER or WAL_PREFIX env vars match wal-archive.sh)
//
// Run:
//
//	make test-storj

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"storj.io/uplink"

	"github.com/jackc/pgx/v5"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// newStorjStorage opens a real Storj storage instance.  It supports both
// authentication methods:
//
//	Method A: STORJ_ACCESS (serialised access grant)
//	Method B: STORJ_API_KEY + STORJ_SATELLITE + STORJ_PASSPHRASE
//
// STORJ_BUCKET is always required.
// The test is skipped automatically if no complete credential set is present.
//
// The returned cleanup function deletes all objects whose key starts with
// prefix+"/" — call it in t.Cleanup to keep the bucket tidy.
func newStorjStorage(t *testing.T) (storage.Storage, func(prefix string)) {
	t.Helper()

	accessGrant := os.Getenv("STORJ_ACCESS")
	apiKey := os.Getenv("STORJ_API_KEY")
	satellite := os.Getenv("STORJ_SATELLITE")
	storjPassphrase := os.Getenv("STORJ_PASSPHRASE")
	bucket := os.Getenv("STORJ_BUCKET")

	hasGrant := accessGrant != ""
	hasAPIKey := apiKey != "" && satellite != "" && storjPassphrase != ""

	if bucket == "" || (!hasGrant && !hasAPIKey) {
		t.Skip("set STORJ_BUCKET and either STORJ_ACCESS or " +
			"STORJ_API_KEY+STORJ_SATELLITE+STORJ_PASSPHRASE to run Storj end-to-end tests")
	}

	ctx := context.Background()
	st, err := storage.NewStorage(ctx, storage.Config{
		Provider:        "storj",
		StorjAccess:     accessGrant,
		StorjAPIKey:     apiKey,
		StorjSatellite:  satellite,
		StorjPassphrase: storjPassphrase,
		StorjBucket:     bucket,
	})
	if err != nil {
		t.Fatalf("opening Storj storage: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	// cleanup opens a fresh uplink project and deletes every object under prefix.
	cleanup := func(prefix string) {
		t.Helper()
		cleanCtx := context.Background()

		var access *uplink.Access
		var accessErr error
		if hasGrant {
			access, accessErr = uplink.ParseAccess(accessGrant)
		} else {
			access, accessErr = uplink.RequestAccessWithPassphrase(cleanCtx, satellite, apiKey, storjPassphrase)
		}
		if accessErr != nil {
			t.Logf("storj cleanup: build access: %v", accessErr)
			return
		}
		project, err := uplink.OpenProject(cleanCtx, access)
		if err != nil {
			t.Logf("storj cleanup: open project: %v", err)
			return
		}
		defer project.Close()

		iter := project.ListObjects(cleanCtx, bucket, &uplink.ListObjectsOptions{
			Prefix:    prefix + "/",
			Recursive: true,
		})
		var deleted int
		for iter.Next() {
			if _, delErr := project.DeleteObject(cleanCtx, bucket, iter.Item().Key); delErr != nil {
				t.Logf("storj cleanup: delete %q: %v", iter.Item().Key, delErr)
			} else {
				deleted++
			}
		}
		t.Logf("storj cleanup: deleted %d objects under prefix %q", deleted, prefix)
	}

	return st, cleanup
}

// ── Plain backup / restore ────────────────────────────────────────────────────

// TestStorj_Backup_And_Restore runs a plain backup to Storj then restores from
// Storj, verifying the manifest, the .tar.gz object, and the PGDATA structure.
func TestStorj_Backup_And_Restore(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)
	name := uniqueName("ci-storj-restore")
	t.Cleanup(func() { cleanup(name) })

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup to Storj: %v", err)
	}

	// Verify objects were actually written to Storj.
	for _, key := range []string{name + "/manifest.json", name + "/base.tar.gz"} {
		exists, err := st.Exists(ctx, key)
		if err != nil {
			t.Fatalf("Exists(%q): %v", key, err)
		}
		if !exists {
			t.Errorf("expected key %q in Storj — not found", key)
		}
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
		t.Fatalf("Restore from Storj: %v", err)
	}

	// Plain restore must NOT write recovery.signal (PG12+ requires restore_command
	// whenever recovery.signal is present — a plain restore uses backup_label).
	mustExistInPGData(t, pgdata, "PG_VERSION", "global", "base")
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write recovery.signal")
	}
}

// TestStorj_Backup_Encrypted_Restore backs up with a passphrase, verifies
// ciphertext is stored in Storj (not readable as gzip), then restores.
func TestStorj_Backup_Encrypted_Restore(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)
	name := uniqueName("ci-storj-enc")
	passphrase := "storj-integration-test-passphrase"
	t.Cleanup(func() { cleanup(name) })

	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NewPassphraseEncryptor(passphrase),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup (encrypted) to Storj: %v", err)
	}

	// The encrypted object must be stored under the .age key.
	ageKey := name + "/base.tar.gz.age"
	exists, err := st.Exists(ctx, ageKey)
	if err != nil {
		t.Fatalf("Exists(%q): %v", ageKey, err)
	}
	if !exists {
		t.Errorf("expected encrypted key %q in Storj — not found", ageKey)
	}

	// Verify it is not readable as gzip (i.e. is actually ciphertext).
	rc, err := st.Download(ctx, ageKey)
	if err != nil {
		t.Fatalf("Download(%q): %v", ageKey, err)
	}
	header := make([]byte, 2)
	_, _ = rc.Read(header)
	rc.Close()
	if header[0] == 0x1f && header[1] == 0x8b {
		t.Error("encrypted Storj object starts with gzip magic — encryption was skipped")
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
		t.Fatalf("Restore (encrypted) from Storj: %v", err)
	}

	mustExistInPGData(t, pgdata, "PG_VERSION", "base")
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write recovery.signal")
	}
}

// ── Data-verification restore tests ──────────────────────────────────────────

// TestStorj_Backup_Restore_DataVerify takes a physical backup of postgres17,
// restores it to the postgres17-restore container's bind-mounted directory,
// starts the container, and verifies the source row counts are intact.
//
// Requires: make docker-up (postgres17 on :5432, postgres17-restore on :5433)
func TestStorj_Backup_Restore_DataVerify(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)
	name := uniqueName("ci-storj-data-verify")
	t.Cleanup(func() { cleanup(name) })

	// ── 0. Record source counts before backup ─────────────────────────────
	srcInv := pgxConnectRequired(t, sourceQueryDSN("inventory_db"))
	wantItems := queryInt(t, srcInv, "SELECT COUNT(*) FROM items")
	wantWarehouses := queryInt(t, srcInv, "SELECT COUNT(*) FROM warehouses")
	t.Logf("source: items=%d warehouses=%d", wantItems, wantWarehouses)

	srcEcom := pgxConnectRequired(t, sourceQueryDSN("ecommerce_db"))
	wantUsers := queryInt(t, srcEcom, "SELECT COUNT(*) FROM users")
	t.Logf("source: users=%d", wantUsers)

	// ── 1. Backup from postgres17 ──────────────────────────────────────────
	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup to Storj: %v", err)
	}

	// ── 2. Reset the restore target directory ─────────────────────────────
	stopService(t, "postgres17-restore")
	wipeRestoreDir(t, "pg17")

	restoreDir := dockerRestoreDir(t, "pg17")
	t.Cleanup(func() {
		stopService(t, "postgres17-restore")
		wipeRestoreDir(t, "pg17")
	})

	// ── 3. Restore PGDATA ─────────────────────────────────────────────────
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  restoreDir,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Restore from Storj: %v", err)
	}

	// ── 4. Start the restore container and wait for it to be ready ────────
	startService(t, "postgres17-restore", nil)
	waitForPostgres(t, restoreTargetQueryDSN("postgres"), 3*time.Minute)

	// ── 5. Verify source row counts ───────────────────────────────────────
	invConn := pgxConnectRequired(t, restoreTargetQueryDSN("inventory_db"))
	if n := queryInt(t, invConn, "SELECT COUNT(*) FROM items"); n != wantItems {
		t.Errorf("inventory_db.items: want %d rows, got %d", wantItems, n)
	}
	if n := queryInt(t, invConn, "SELECT COUNT(*) FROM warehouses"); n != wantWarehouses {
		t.Errorf("inventory_db.warehouses: want %d rows, got %d", wantWarehouses, n)
	}

	ecomConn := pgxConnectRequired(t, restoreTargetQueryDSN("ecommerce_db"))
	if n := queryInt(t, ecomConn, "SELECT COUNT(*) FROM users"); n != wantUsers {
		t.Errorf("ecommerce_db.users: want %d rows, got %d", wantUsers, n)
	}

	// restore_marker must be gone (it was only in the pre-restore initialisation).
	var exists bool
	err := invConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='restore_marker')").
		Scan(&exists)
	if err != nil {
		t.Logf("checking restore_marker (in inventory_db): %v", err)
	}
	// restore_marker is in the postgres database, not inventory_db — check there.
	pgConn := pgxConnectRequired(t, restoreTargetQueryDSN("postgres"))
	pgConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='restore_marker')").
		Scan(&exists)
	if exists {
		t.Error("restore_marker table still exists — restore did not overwrite the target instance")
	}
}

// TestStorj_Backup_Encrypted_Restore_DataVerify repeats the data-verify cycle
// with passphrase encryption to prove decryption is transparent.
func TestStorj_Backup_Encrypted_Restore_DataVerify(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)
	name := uniqueName("ci-storj-enc-data-verify")
	passphrase := "data-verify-test-passphrase"
	t.Cleanup(func() { cleanup(name) })

	// ── 0. Record source counts before backup ─────────────────────────────
	srcInv := pgxConnectRequired(t, sourceQueryDSN("inventory_db"))
	wantItems := queryInt(t, srcInv, "SELECT COUNT(*) FROM items")
	wantWarehouses := queryInt(t, srcInv, "SELECT COUNT(*) FROM warehouses")
	t.Logf("source: items=%d warehouses=%d", wantItems, wantWarehouses)

	srcEcom := pgxConnectRequired(t, sourceQueryDSN("ecommerce_db"))
	wantUsers := queryInt(t, srcEcom, "SELECT COUNT(*) FROM users")
	t.Logf("source: users=%d", wantUsers)

	// ── 1. Encrypted backup ───────────────────────────────────────────────
	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NewPassphraseEncryptor(passphrase),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup (encrypted) to Storj: %v", err)
	}

	// ── 2. Reset restore target ───────────────────────────────────────────
	stopService(t, "postgres17-restore")
	wipeRestoreDir(t, "pg17")

	restoreDir := dockerRestoreDir(t, "pg17")
	t.Cleanup(func() {
		stopService(t, "postgres17-restore")
		wipeRestoreDir(t, "pg17")
	})

	// ── 3. Restore with decryption ────────────────────────────────────────
	if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
		BackupName: name,
		PGDataDir:  restoreDir,
		Parallel:   2,
		Encryptor:  crypto.NewPassphraseEncryptor(passphrase),
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Restore (encrypted) from Storj: %v", err)
	}

	// ── 4. Start container + wait ─────────────────────────────────────────
	startService(t, "postgres17-restore", nil)
	waitForPostgres(t, restoreTargetQueryDSN("postgres"), 3*time.Minute)

	// ── 5. Verify row counts ──────────────────────────────────────────────
	invConn := pgxConnectRequired(t, restoreTargetQueryDSN("inventory_db"))
	if n := queryInt(t, invConn, "SELECT COUNT(*) FROM items"); n != wantItems {
		t.Errorf("inventory_db.items: want %d rows, got %d", wantItems, n)
	}
	if n := queryInt(t, invConn, "SELECT COUNT(*) FROM warehouses"); n != wantWarehouses {
		t.Errorf("inventory_db.warehouses: want %d rows, got %d", wantWarehouses, n)
	}
	ecomConn := pgxConnectRequired(t, restoreTargetQueryDSN("ecommerce_db"))
	if n := queryInt(t, ecomConn, "SELECT COUNT(*) FROM users"); n != wantUsers {
		t.Errorf("ecommerce_db.users: want %d rows, got %d", wantUsers, n)
	}
}

// ── WAL PITR tests ────────────────────────────────────────────────────────────

// TestStorj_WAL_PITR exercises Point-in-Time Recovery using a live
// postgres17-wal instance (port 5436) with archive_mode=on.
//
// The test:
//  1. Takes a base backup from postgres17-wal.
//  2. Creates a small test table and inserts "before" rows (WAL only).
//  3. Forces a WAL switch and waits for archival to Storj.
//  4. Records beforeTime.
//  5. Inserts "after" rows and archives them.
//
// Sub-test "BeforeInsert": restores to beforeTime → only "before" rows visible.
// Sub-test "AfterInsert":  restores without a time target → all rows visible.
//
// Requires:
//   - postgres17-wal running on localhost:5436 (make wal-build && make wal-up)
//   - Storj credentials + STORJ_BUCKET set
//   - WAL archive prefix consistent with container config (see walArchivePrefix())
func TestStorj_WAL_PITR(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)

	// ── Check postgres17-wal availability ─────────────────────────────────
	walConn := pgxConnect(t, walSourceQueryDSN("postgres")) // skips if unavailable

	walPrefix := walArchivePrefix()
	clusterPrefix := uniqueName("ci-wal-pitr")
	backupName := clusterPrefix + "/backup/pitr-test"
	tableName := fmt.Sprintf("wal_pitr_%d", time.Now().Unix())
	t.Cleanup(func() { cleanup(clusterPrefix) })

	// Drop the test table from the WAL source on exit (best-effort).
	t.Cleanup(func() {
		dropCtx := context.Background()
		conn, err := pgx.Connect(dropCtx, walSourceQueryDSN("postgres"))
		if err != nil {
			return
		}
		defer conn.Close(dropCtx)
		conn.Exec(dropCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	// ── 1. Base backup from postgres17-wal ────────────────────────────────
	t.Log("taking base backup from postgres17-wal...")
	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      walSourceReplicationDSN(),
		BackupName: backupName,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("base backup from postgres17-wal: %v", err)
	}

	// ── 2. Create test table + insert "before" rows ───────────────────────
	execSQL(t, walConn, fmt.Sprintf(
		"CREATE TABLE %s (id SERIAL PRIMARY KEY, marker TEXT, created_at TIMESTAMPTZ DEFAULT NOW())",
		tableName,
	))
	execSQL(t, walConn, fmt.Sprintf(
		"INSERT INTO %s (marker) SELECT 'before' FROM generate_series(1, 10)",
		tableName,
	))

	// ── 3. Force WAL switch so the "before" segment is archived ──────────
	// Record the exact WAL segment name BEFORE switching so we wait for that
	// specific segment — not just any count increase from background archiving.
	var beforeSegName string
	if err := walConn.QueryRow(ctx, "SELECT pg_walfile_name(pg_current_wal_lsn())").Scan(&beforeSegName); err != nil {
		t.Fatalf("query before seg name: %v", err)
	}
	t.Logf("'before' rows in WAL segment: %s", beforeSegName)

	execSQL(t, walConn, "SELECT pg_switch_wal()")
	t.Logf("waiting for WAL segment %q to be archived to Storj...", beforeSegName)
	waitForNamedWALSegment(t, ctx, st, walPrefix, beforeSegName, 5*time.Minute)

	// ── 4. Record the target time for "before" PITR ───────────────────────
	// Sleep so beforeTime is strictly after all 'before' WAL records.
	time.Sleep(3 * time.Second)
	beforeTime := time.Now().UTC()
	t.Logf("beforeTime = %s", beforeTime.Format(time.RFC3339))
	time.Sleep(3 * time.Second) // ensure 'after' inserts happen after beforeTime

	// ── 5. Insert "after" rows + archive ─────────────────────────────────
	execSQL(t, walConn, fmt.Sprintf(
		"INSERT INTO %s (marker) SELECT 'after' FROM generate_series(1, 5)",
		tableName,
	))

	// Capture LSN and segment name for the 'after' rows before switching.
	var afterLSN, afterSegName string
	if err := walConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&afterLSN); err != nil {
		t.Fatalf("query afterLSN: %v", err)
	}
	if err := walConn.QueryRow(ctx, "SELECT pg_walfile_name(pg_current_wal_lsn())").Scan(&afterSegName); err != nil {
		t.Fatalf("query after seg name: %v", err)
	}
	t.Logf("afterLSN = %s  afterSegName = %s", afterLSN, afterSegName)

	execSQL(t, walConn, "SELECT pg_switch_wal()")
	t.Logf("waiting for WAL segment %q to be archived to Storj...", afterSegName)
	waitForNamedWALSegment(t, ctx, st, walPrefix, afterSegName, 5*time.Minute)

	// ── Sub-test: restore to beforeTime ───────────────────────────────────
	t.Run("BeforeInsert", func(t *testing.T) {
		stopService(t, "postgres17-wal-restore")
		wipeRestoreDir(t, "pg17-wal")
		restoreDir := dockerRestoreDir(t, "pg17-wal")
		t.Cleanup(func() {
			stopService(t, "postgres17-wal-restore")
			wipeRestoreDir(t, "pg17-wal")
		})

		if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
			BackupName:         backupName,
			PGDataDir:          restoreDir,
			Parallel:           2,
			Encryptor:          crypto.NoopEncryptor{},
			Storage:            st,
			Log:                testLogger(t),
			RecoveryTargetTime: beforeTime.Format("2006-01-02 15:04:05 UTC"),
			RestoreCommand:     "/usr/local/bin/wal-restore %f %p",
		}); err != nil {
			t.Fatalf("Restore (PITR before): %v", err)
		}

		// Verify recovery.signal was written (PITR restore).
		if _, err := os.Stat(filepath.Join(restoreDir, "recovery.signal")); os.IsNotExist(err) {
			t.Error("PITR restore must write recovery.signal")
		}
		// Verify postgresql.auto.conf has the target time.
		conf, _ := os.ReadFile(filepath.Join(restoreDir, "postgresql.auto.conf"))
		if !strings.Contains(string(conf), beforeTime.Format("2006-01-02 15:04:05")) {
			t.Errorf("postgresql.auto.conf missing recovery_target_time; got:\n%s", conf)
		}

		// Start restore container and wait for full promotion (not just read-only
		// consistent state — WAL replay may still be ongoing at that point).
		startService(t, "postgres17-wal-restore", walRestoreContainerEnv())
		t.Log("waiting for postgres17-wal-restore to complete WAL replay and promote...")
		waitForPromotion(t, walRestoreTargetQueryDSN("postgres"), 10*time.Minute)

		// Verify data: only 'before' rows (10), no 'after' rows.
		conn := pgxConnectRequired(t, walRestoreTargetQueryDSN("postgres"))
		before := queryInt(t, conn, fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE marker = 'before'", tableName))
		after := queryInt(t, conn, fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE marker = 'after'", tableName))

		t.Logf("BeforeInsert PITR: before=%d, after=%d", before, after)
		if before != 10 {
			t.Errorf("expected 10 'before' rows, got %d", before)
		}
		if after != 0 {
			t.Errorf("expected 0 'after' rows, got %d (PITR overshot)", after)
		}
	})

	// ── Sub-test: restore past afterLSN (all rows visible) ───────────────
	// Use LSN-based PITR instead of time-based to avoid the "recovery ended
	// before configured recovery target was reached" FATAL that occurs when
	// the archive runs out of segments before the target timestamp is reached.
	t.Run("AfterInsert", func(t *testing.T) {
		stopService(t, "postgres17-wal-restore")
		wipeRestoreDir(t, "pg17-wal")
		restoreDir := dockerRestoreDir(t, "pg17-wal")
		t.Cleanup(func() {
			stopService(t, "postgres17-wal-restore")
			wipeRestoreDir(t, "pg17-wal")
		})

		if err := pgbackup.Restore(ctx, pgbackup.RestoreConfig{
			BackupName:        backupName,
			PGDataDir:         restoreDir,
			Parallel:          2,
			Encryptor:         crypto.NoopEncryptor{},
			Storage:           st,
			Log:               testLogger(t),
			RecoveryTargetLSN: afterLSN,
			RestoreCommand:    "/usr/local/bin/wal-restore %f %p",
		}); err != nil {
			t.Fatalf("Restore (PITR after LSN): %v", err)
		}

		startService(t, "postgres17-wal-restore", walRestoreContainerEnv())
		t.Log("waiting for postgres17-wal-restore to complete WAL replay and promote...")
		waitForPromotion(t, walRestoreTargetQueryDSN("postgres"), 10*time.Minute)

		conn := pgxConnectRequired(t, walRestoreTargetQueryDSN("postgres"))
		before := queryInt(t, conn, fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE marker = 'before'", tableName))
		after := queryInt(t, conn, fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE marker = 'after'", tableName))

		t.Logf("AfterInsert PITR: before=%d, after=%d", before, after)
		if before != 10 {
			t.Errorf("expected 10 'before' rows, got %d", before)
		}
		if after != 5 {
			t.Errorf("expected 5 'after' rows, got %d", after)
		}
	})
}

// ── WAL push / fetch tests ────────────────────────────────────────────────────

// TestStorj_Wal_PushFetch_Encrypted pushes an encrypted+compressed WAL segment
// to Storj, verifies the object is stored as ciphertext (not gzip), then
// fetches and decrypts it, checking byte-for-byte fidelity.
func TestStorj_Wal_PushFetch_Encrypted(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)
	walPrefix := uniqueName("ci-storj-wal-enc")
	walPath, walName := fakeWALSegment(t, 1024*16)
	passphrase := "storj-wal-integration-passphrase"
	t.Cleanup(func() { cleanup(walPrefix) })

	enc := crypto.NewPassphraseEncryptor(passphrase)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: enc,
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush (encrypted) to Storj: %v", err)
	}

	// Object must be stored under the .gz.age key.
	ageKey := walPrefix + "/" + walName + ".gz.age"
	exists, err := st.Exists(ctx, ageKey)
	if err != nil {
		t.Fatalf("Exists(%q): %v", ageKey, err)
	}
	if !exists {
		t.Fatalf("expected encrypted WAL key %q in Storj — not found", ageKey)
	}

	// Stored object must NOT be readable as gzip — it is ciphertext.
	rc, err := st.Download(ctx, ageKey)
	if err != nil {
		t.Fatalf("Download(%q): %v", ageKey, err)
	}
	header := make([]byte, 2)
	_, _ = rc.Read(header)
	rc.Close()
	if header[0] == 0x1f && header[1] == 0x8b {
		t.Error("encrypted WAL starts with gzip magic — encryption was skipped")
	}

	// Fetch + decrypt: content must match original.
	destPath := filepath.Join(t.TempDir(), walName)
	if err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destPath,
		WalPrefix: walPrefix,
		Encryptor: enc,
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalFetch (encrypted) from Storj: %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched WAL: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("Storj encrypted WAL round-trip mismatch: original %d bytes, fetched %d bytes",
			len(original), len(fetched))
	}
}

// TestStorj_Wal_Idempotent verifies that pushing the same WAL segment to Storj
// twice exits 0 on both calls and does not corrupt the stored object.
// This mirrors the PostgreSQL guarantee: a retried archive_command must be safe.
func TestStorj_Wal_Idempotent(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)
	walPrefix := uniqueName("ci-storj-wal-idem")
	walPath, walName := fakeWALSegment(t, 1024*16)
	t.Cleanup(func() { cleanup(walPrefix) })

	cfg := pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}

	// First push — uploads the object.
	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("first WalPush to Storj: %v", err)
	}

	// Second push — must succeed (exit 0) and skip the upload.
	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("second WalPush to Storj (idempotent): %v", err)
	}

	// Fetch and confirm the stored content matches the original.
	destPath := filepath.Join(t.TempDir(), walName)
	if err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destPath,
		WalPrefix: walPrefix,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalFetch after idempotent push: %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched WAL: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("idempotent push corrupted stored object: original %d bytes, fetched %d bytes",
			len(original), len(fetched))
	}
}

// TestStorj_Wal_PushFetch pushes a WAL segment to Storj, verifies it is stored
// under the expected key, then fetches and verifies byte-for-byte fidelity.
func TestStorj_Wal_PushFetch(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)
	walPrefix := uniqueName("ci-storj-wal")
	walPath, walName := fakeWALSegment(t, 1024*16) // 16 KB
	t.Cleanup(func() { cleanup(walPrefix) })

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush to Storj: %v", err)
	}

	// Segment must be present in Storj under the .gz key.
	gzKey := walPrefix + "/" + walName + ".gz"
	exists, err := st.Exists(ctx, gzKey)
	if err != nil {
		t.Fatalf("Exists(%q): %v", gzKey, err)
	}
	if !exists {
		t.Errorf("expected WAL key %q in Storj — not found", gzKey)
	}

	destPath := filepath.Join(t.TempDir(), walName)
	if err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destPath,
		WalPrefix: walPrefix,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalFetch from Storj: %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched WAL: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("Storj WAL round-trip mismatch: original %d bytes, fetched %d bytes",
			len(original), len(fetched))
	}
}

// ── Cluster folder-structure test ─────────────────────────────────────────────

// TestStorj_ClusterWALLayout verifies that the cluster-prefixed layout is
// correct end-to-end on Storj:
//
//   - WAL pushed with WalPrefix = "<cluster>/wal"  → stored at  <cluster>/wal/<segment>.gz
//   - Backup taken  with BackupName = "<cluster>/backups/<name>" → stored at
//     <cluster>/backups/<name>/manifest.json and <cluster>/backups/<name>/base.tar.gz
//
// All objects enumerated under the cluster root must sit beneath <cluster>/.
// The test logs the full path list so the directory tree is visible in output.
func TestStorj_ClusterWALLayout(t *testing.T) {
	ctx := context.Background()
	st, cleanup := newStorjStorage(t)

	cluster := uniqueName("ci-cluster-layout")
	backupName := cluster + "/backup/test-backup"
	walPrefix := cluster + "/wal_archive"
	t.Cleanup(func() { cleanup(cluster) })

	// ── 1. Push a WAL segment under <cluster>/wal ─────────────────────────
	walPath, walName := fakeWALSegment(t, 1024*16) // 16 KB

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

	// WAL must be at <cluster>/wal/<segment>.gz
	walKey := walPrefix + "/" + walName + ".gz"
	exists, err := st.Exists(ctx, walKey)
	if err != nil {
		t.Fatalf("Exists(%q): %v", walKey, err)
	}
	if !exists {
		t.Errorf("WAL segment not found at cluster WAL path %q", walKey)
	}

	// ── 2. Take a backup under <cluster>/backups/<name> ───────────────────
	if _, err := pgbackup.Backup(ctx, pgbackup.BackupConfig{
		DBUrl:      pgURL(t),
		BackupName: backupName,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        testLogger(t),
	}); err != nil {
		t.Fatalf("Backup to cluster backup prefix: %v", err)
	}

	// Manifest and base archive must be at <cluster>/backups/<name>/...
	for _, key := range []string{
		backupName + "/manifest.json",
		backupName + "/base.tar.gz",
	} {
		exists, err := st.Exists(ctx, key)
		if err != nil {
			t.Fatalf("Exists(%q): %v", key, err)
		}
		if !exists {
			t.Errorf("expected backup object %q in Storj — not found", key)
		}
	}

	// ── 3. List all objects under the cluster root ────────────────────────
	keys, err := st.List(ctx, cluster)
	if err != nil {
		t.Fatalf("List(%q): %v", cluster, err)
	}
	if len(keys) == 0 {
		t.Fatal("no objects found under cluster prefix")
	}

	t.Logf("Cluster folder structure for %q (%d objects):", cluster, len(keys))
	for _, k := range keys {
		t.Logf("  %s", k)
		if !strings.HasPrefix(k, cluster+"/") {
			t.Errorf("object %q does not start with cluster prefix %q", k, cluster+"/")
		}
	}

	// Confirm both sub-trees are present.
	var hasWAL, hasBackup bool
	for _, k := range keys {
		if strings.HasPrefix(k, cluster+"/wal_archive/") {
			hasWAL = true
		}
		if strings.HasPrefix(k, cluster+"/backup/") {
			hasBackup = true
		}
	}
	if !hasWAL {
		t.Errorf("no objects under %s/wal_archive/ — WAL cluster layout broken", cluster)
	}
	if !hasBackup {
		t.Errorf("no objects under %s/backup/ — backup cluster layout broken", cluster)
	}
}
