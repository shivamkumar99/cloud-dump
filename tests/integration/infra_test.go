//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// ── Project root ──────────────────────────────────────────────────────────────

// projectRoot returns the absolute path to the repository root by walking up
// from this source file's location.
func projectRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	// infra_test.go is at <root>/tests/integration/infra_test.go
	root := filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
	return root
}

// ── Docker compose helpers ────────────────────────────────────────────────────

// dockerComposeCLI runs `docker compose -f <root>/docker/docker-compose.yml <args>`
// with extraEnv merged on top of the current process environment.
// Returns the combined output and any error.
func dockerComposeCLI(t *testing.T, extraEnv []string, args ...string) ([]byte, error) {
	t.Helper()
	root := projectRoot(t)
	composeFile := filepath.Join(root, "docker", "docker-compose.yml")
	cmdArgs := append([]string{"compose", "-f", composeFile}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), extraEnv...)
	return cmd.CombinedOutput()
}

// stopService stops a docker compose service, logging but not failing on error.
func stopService(t *testing.T, service string) {
	t.Helper()
	out, err := dockerComposeCLI(t, nil, "stop", service)
	if err != nil {
		t.Logf("docker compose stop %s: %v\n%s", service, err, out)
	}
}

// startService starts a docker compose service detached.
// extraEnv adds/overrides env vars (e.g. WAL_PREFIX=...) that docker compose
// passes into the container.
func startService(t *testing.T, service string, extraEnv []string) {
	t.Helper()
	out, err := dockerComposeCLI(t, extraEnv, "up", "-d", service)
	if err != nil {
		t.Fatalf("docker compose up -d %s: %v\n%s", service, err, out)
	}
}

// wipeRestoreDir stops the container, then erases everything inside
// docker/restore-data/<subdir> using an alpine container to avoid permission
// issues (files may be owned by the postgres uid inside Docker).
func wipeRestoreDir(t *testing.T, subdir string) {
	t.Helper()
	root := projectRoot(t)
	dir := filepath.Join(root, "docker", "restore-data", subdir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %q: %v", dir, err)
	}
	cmd := exec.Command("docker", "run", "--rm",
		"-v", dir+":/data",
		"alpine:latest",
		"sh", "-c", "rm -rf /data/* /data/.[!.]* 2>/dev/null; true",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Logf("wipe %q: %v\n%s", dir, err, out)
	}
}

// dockerRestoreDir returns the absolute path to docker/restore-data/<subdir>.
func dockerRestoreDir(t *testing.T, subdir string) string {
	return filepath.Join(projectRoot(t), "docker", "restore-data", subdir)
}

// ── PostgreSQL connection helpers ─────────────────────────────────────────────

// waitForPostgres polls dsn until a successful connection is made or timeout
// is reached. For PITR restores use waitForPromotion instead.
func waitForPostgres(t *testing.T, dsn string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			conn.Close(context.Background())
			return
		}
		lastErr = err
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("waitForPostgres(%q) timed out after %v: %v", dsn, timeout, lastErr)
}

// waitForPromotion polls dsn until PostgreSQL has fully promoted out of
// recovery (pg_is_in_recovery() = false). Use this for PITR restores: the DB
// accepts read-only connections while still replaying WAL, but we must not
// query user tables until promotion is complete.
func waitForPromotion(t *testing.T, dsn string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			var inRecovery bool
			qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
			qErr := conn.QueryRow(qCtx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
			qCancel()
			conn.Close(context.Background())
			if qErr == nil && !inRecovery {
				return // fully promoted
			}
			lastErr = qErr
		} else {
			lastErr = err
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("waitForPromotion(%q) timed out after %v: %v", dsn, timeout, lastErr)
}

// pgxConnect opens a pgx connection to dsn and registers close in t.Cleanup.
// Skips the test (not fails) when the DSN is unreachable.
func pgxConnect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("pgx.Connect(%q): %v — skipping test (service not available)", dsn, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// pgxConnectRequired opens a pgx connection to dsn and fatals if unavailable.
func pgxConnectRequired(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect(%q): %v", dsn, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// queryInt runs q (must SELECT a single integer) and returns the result.
func queryInt(t *testing.T, conn *pgx.Conn, q string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
		t.Fatalf("queryInt(%q): %v", q, err)
	}
	return n
}

// execSQL runs a SQL statement and fatals on error.
func execSQL(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("execSQL(%q): %v", sql, err)
	}
}

// ── WAL archive helpers ───────────────────────────────────────────────────────

// walArchivePrefix returns the WAL archive prefix as configured on the
// postgres17-wal container. It mirrors the wal-archive.sh resolution logic:
//
//	CLUSTER set, WAL_PREFIX empty → "<cluster>/wal"
//	WAL_PREFIX set → WAL_PREFIX
//	neither set → "wal_archive"
func walArchivePrefix() string {
	if cluster := os.Getenv("CLUSTER"); cluster != "" {
		if os.Getenv("WAL_PREFIX") == "" {
			return cluster + "/wal"
		}
	}
	if p := os.Getenv("WAL_PREFIX"); p != "" {
		return p
	}
	return "wal_archive"
}

// waitForWALObjects polls storage until at least atLeast objects are present
// under walPrefix, or fatals on timeout.
func waitForWALObjects(t *testing.T, ctx context.Context, st storage.Storage, walPrefix string, atLeast int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		keys, err := st.List(ctx, walPrefix)
		if err == nil && len(keys) >= atLeast {
			t.Logf("WAL objects under %q: %d", walPrefix, len(keys))
			return
		}
		time.Sleep(5 * time.Second)
	}
	keys, _ := st.List(ctx, walPrefix)
	t.Fatalf("timed out after %v waiting for %d WAL objects under %q (found %d)",
		timeout, atLeast, walPrefix, len(keys))
}

// waitForNamedWALSegment polls storage until the given WAL segment (with any
// supported extension: .gz, .gz.age, or plain) exists under walPrefix.
// This is more precise than waitForWALObjects because it waits for the
// specific segment containing the test's changes, not just any segment.
func waitForNamedWALSegment(t *testing.T, ctx context.Context, st storage.Storage, walPrefix, segName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, suffix := range []string{".gz", ".gz.age", ""} {
			key := walPrefix + "/" + segName + suffix
			exists, err := st.Exists(ctx, key)
			if err == nil && exists {
				t.Logf("WAL segment %q archived at key %q", segName, key)
				return
			}
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("timed out after %v waiting for WAL segment %q under %q", timeout, segName, walPrefix)
}

// ── Well-known DSNs ───────────────────────────────────────────────────────────

// walSourceReplicationDSN is the replication connection string for postgres17-wal
// (port 5436, used as the backup source in WAL PITR tests).
func walSourceReplicationDSN() string {
	return "postgres://repl_user:repl_password@localhost:5436/postgres?replication=yes"
}

// walSourceQueryDSN is a plain query DSN for postgres17-wal.
func walSourceQueryDSN(db string) string {
	return fmt.Sprintf("postgres://postgres:postgres@localhost:5436/%s", db)
}

// restoreTargetQueryDSN is a plain query DSN for postgres17-restore (port 5433).
func restoreTargetQueryDSN(db string) string {
	return fmt.Sprintf("postgres://postgres:postgres@localhost:5433/%s", db)
}

// walRestoreTargetQueryDSN is a plain query DSN for postgres17-wal-restore
// (port 5438).
func walRestoreTargetQueryDSN(db string) string {
	return fmt.Sprintf("postgres://postgres:postgres@localhost:5438/%s", db)
}

// sourceQueryDSN is a plain query DSN for postgres17 (the backup source, port 5432).
func sourceQueryDSN(db string) string {
	return fmt.Sprintf("postgres://postgres:postgres@localhost:5432/%s", db)
}

// walRestoreContainerEnv returns extra env vars to pass when starting
// postgres17-wal-restore so its wal-restore.sh picks up the right WAL prefix.
func walRestoreContainerEnv() []string {
	var env []string
	// Pass cluster/WAL prefix configuration from the test environment into the
	// container so wal-restore.sh uses the correct Storj path.
	if v := os.Getenv("CLUSTER"); v != "" {
		env = append(env, "CLUSTER="+v)
	}
	if v := os.Getenv("WAL_PREFIX"); v != "" {
		env = append(env, "WAL_PREFIX="+v)
	}
	return env
}
