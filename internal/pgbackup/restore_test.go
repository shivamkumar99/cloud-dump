// Package pgbackup (internal test) covers restore.go helpers without a live
// PostgreSQL connection. Using package pgbackup (not pgbackup_test) gives
// access to unexported functions like validatePGDataDir, extractEntry, etc.
package pgbackup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// fakeTarEntry describes a single entry to add to a synthetic tar archive.
type fakeTarEntry struct {
	name    string
	data    []byte // nil means directory
	symlink string // non-empty means symlink; data must be nil
}

// makeFakeTarGz builds an in-memory gzip-compressed tar from the given entries.
func makeFakeTarGz(t *testing.T, entries []fakeTarEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for _, e := range entries {
		var hdr *tar.Header
		switch {
		case e.symlink != "":
			hdr = &tar.Header{Name: e.name, Typeflag: tar.TypeSymlink, Linkname: e.symlink}
		case e.data == nil:
			hdr = &tar.Header{Name: e.name + "/", Typeflag: tar.TypeDir, Mode: 0755}
		default:
			hdr = &tar.Header{
				Name:     e.name,
				Typeflag: tar.TypeReg,
				Size:     int64(len(e.data)),
				Mode:     0600,
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar WriteHeader %q: %v", e.name, err)
		}
		if e.data != nil && e.symlink == "" {
			if _, err := tw.Write(e.data); err != nil {
				t.Fatalf("tar Write %q: %v", e.name, err)
			}
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip.Close: %v", err)
	}
	return buf.Bytes()
}

// makeSingleEntryTar creates a tar.Reader positioned at the first (only) entry.
func makeSingleEntryTar(t *testing.T, hdr *tar.Header, body []byte) (*tar.Reader, *tar.Header) {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if len(body) > 0 {
		if _, err := tw.Write(body); err != nil {
			t.Fatalf("tw.Write: %v", err)
		}
	}
	tw.Close()

	tr := tar.NewReader(&buf)
	got, err := tr.Next()
	if err != nil {
		t.Fatalf("tr.Next: %v", err)
	}
	return tr, got
}

// setupStorageWithBackup writes a manifest and one gzip+tar tablespace into st.
// Returns the backup name for use with RestoreConfig.
func setupStorageWithBackup(t *testing.T, st *storage.MemoryStorage, tarEntries []fakeTarEntry) string {
	t.Helper()
	ctx := context.Background()
	name := "unit-test-backup"

	tarGz := makeFakeTarGz(t, tarEntries)

	m := &Manifest{
		BackupName:      name,
		Timestamp:       time.Now().UTC(),
		PostgresVersion: "16.0",
		SystemID:        "1234567890",
		StartLSN:        "0/3000060",
		EndLSN:          "0/5000000",
		Encrypted:       false,
		Tablespaces: []Tablespace{
			{OID: 0, Location: "", StorKey: name + "/base.tar.gz"},
		},
	}
	if err := WriteManifest(ctx, st, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := st.Upload(ctx, name+"/base.tar.gz", bytes.NewReader(tarGz)); err != nil {
		t.Fatalf("Upload tablespace: %v", err)
	}
	return name
}

// ── validatePGDataDir ─────────────────────────────────────────────────────────

func TestValidatePGDataDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := validatePGDataDir(dir); err != nil {
		t.Errorf("empty dir should pass validation, got: %v", err)
	}
}

func TestValidatePGDataDir_NotExist(t *testing.T) {
	err := validatePGDataDir("/nonexistent/path/pgdata")
	if err == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error should mention 'does not exist', got: %v", err)
	}
}

func TestValidatePGDataDir_NonEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "PG_VERSION"), []byte("16\n"), 0600); err != nil {
		t.Fatalf("seeding dir: %v", err)
	}

	err := validatePGDataDir(dir)
	if err == nil {
		t.Fatal("expected error for non-empty directory, got nil")
	}
	if !strings.Contains(err.Error(), "not empty") {
		t.Errorf("error should mention 'not empty', got: %v", err)
	}
}

// ── stagingPath ───────────────────────────────────────────────────────────────

func TestStagingPath(t *testing.T) {
	tests := []struct {
		stagingDir string
		oid        int32
		want       string
	}{
		{"/tmp/staging", 0, "/tmp/staging/ts_0.blob"},
		{"/tmp/staging", 16384, "/tmp/staging/ts_16384.blob"},
	}
	for _, tc := range tests {
		got := stagingPath(tc.stagingDir, tc.oid)
		if got != tc.want {
			t.Errorf("stagingPath(%q, %d) = %q, want %q", tc.stagingDir, tc.oid, got, tc.want)
		}
	}
}

// ── writeRecoveryConfig ───────────────────────────────────────────────────────

// TestWriteRecoveryConfig_PlainNoSignal verifies that a plain restore (no PITR
// fields set) does NOT write recovery.signal or postgresql.auto.conf.
// PG12+ requires restore_command whenever recovery.signal is present — writing
// it without a restore_command causes "must specify restore_command" FATAL.
func TestWriteRecoveryConfig_PlainNoSignal(t *testing.T) {
	pgdata := t.TempDir()
	cfg := RestoreConfig{PGDataDir: pgdata}

	if err := writeRecoveryConfig(cfg); err != nil {
		t.Fatalf("writeRecoveryConfig: %v", err)
	}

	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write recovery.signal — PG12+ requires restore_command with it")
	}
	if _, err := os.Stat(filepath.Join(pgdata, "postgresql.auto.conf")); !os.IsNotExist(err) {
		t.Error("plain restore must NOT write postgresql.auto.conf")
	}
}

// TestWriteRecoveryConfig_PITRWritesSignal verifies that when PITR is configured,
// recovery.signal IS written and postgresql.auto.conf contains the target action.
func TestWriteRecoveryConfig_PITRWritesSignal(t *testing.T) {
	pgdata := t.TempDir()
	cfg := RestoreConfig{
		PGDataDir:      pgdata,
		RestoreCommand: "cloud-dump wal-fetch %f %p",
	}

	if err := writeRecoveryConfig(cfg); err != nil {
		t.Fatalf("writeRecoveryConfig: %v", err)
	}

	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); os.IsNotExist(err) {
		t.Error("PITR restore must write recovery.signal")
	}

	conf, err := os.ReadFile(filepath.Join(pgdata, "postgresql.auto.conf"))
	if err != nil {
		t.Fatalf("reading postgresql.auto.conf: %v", err)
	}
	if !strings.Contains(string(conf), "recovery_target_action = 'promote'") {
		t.Errorf("postgresql.auto.conf missing recovery_target_action; got:\n%s", conf)
	}
}

func TestWriteRecoveryConfig_PITRFields(t *testing.T) {
	pgdata := t.TempDir()
	cfg := RestoreConfig{
		PGDataDir:          pgdata,
		RecoveryTargetTime: "2026-03-07 14:30:00 UTC",
		RecoveryTargetLSN:  "0/5200000",
		RestoreCommand:     "cloud-dump wal-fetch %f %p --storage storj",
	}

	if err := writeRecoveryConfig(cfg); err != nil {
		t.Fatalf("writeRecoveryConfig: %v", err)
	}

	conf, _ := os.ReadFile(filepath.Join(pgdata, "postgresql.auto.conf"))
	s := string(conf)

	checks := []string{
		"restore_command = 'cloud-dump wal-fetch %f %p --storage storj'",
		"recovery_target_time = '2026-03-07 14:30:00 UTC'",
		"recovery_target_lsn = '0/5200000'",
		"recovery_target_action = 'promote'",
	}
	for _, want := range checks {
		if !strings.Contains(s, want) {
			t.Errorf("postgresql.auto.conf missing:\n  %s\ngot:\n%s", want, s)
		}
	}
}

func TestWriteRecoveryConfig_RestoreCommandOnly(t *testing.T) {
	pgdata := t.TempDir()
	cfg := RestoreConfig{
		PGDataDir:      pgdata,
		RestoreCommand: "cloud-dump wal-fetch %f %p",
	}

	if err := writeRecoveryConfig(cfg); err != nil {
		t.Fatalf("writeRecoveryConfig: %v", err)
	}

	conf, _ := os.ReadFile(filepath.Join(pgdata, "postgresql.auto.conf"))
	s := string(conf)

	if !strings.Contains(s, "restore_command = 'cloud-dump wal-fetch %f %p'") {
		t.Errorf("restore_command missing from postgresql.auto.conf:\n%s", s)
	}
	if strings.Contains(s, "recovery_target_time") {
		t.Error("recovery_target_time should NOT be written when not set")
	}
	if strings.Contains(s, "recovery_target_lsn") {
		t.Error("recovery_target_lsn should NOT be written when not set")
	}
}

// ── extractEntry ─────────────────────────────────────────────────────────────

func TestExtractEntry_RegularFile(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()
	content := []byte("PG_VERSION data\n")

	tr, hdr := makeSingleEntryTar(t, &tar.Header{
		Name:     "PG_VERSION",
		Typeflag: tar.TypeReg,
		Size:     int64(len(content)),
		Mode:     0644,
	}, content)

	if err := extractEntry(tr, hdr, targetDir, log); err != nil {
		t.Fatalf("extractEntry: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(targetDir, "PG_VERSION"))
	if err != nil {
		t.Fatalf("reading extracted file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("extracted content = %q, want %q", got, content)
	}
}

func TestExtractEntry_RegularFile_NestedPath(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()
	content := []byte("fake pg_control")

	tr, hdr := makeSingleEntryTar(t, &tar.Header{
		Name:     "global/pg_control",
		Typeflag: tar.TypeReg,
		Size:     int64(len(content)),
		Mode:     0600,
	}, content)

	if err := extractEntry(tr, hdr, targetDir, log); err != nil {
		t.Fatalf("extractEntry nested path: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(targetDir, "global", "pg_control"))
	if err != nil {
		t.Fatalf("reading nested file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("nested content mismatch: got %q, want %q", got, content)
	}
}

func TestExtractEntry_Directory(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()

	tr, hdr := makeSingleEntryTar(t, &tar.Header{
		Name:     "base/",
		Typeflag: tar.TypeDir,
		Mode:     0755,
	}, nil)

	if err := extractEntry(tr, hdr, targetDir, log); err != nil {
		t.Fatalf("extractEntry dir: %v", err)
	}

	info, err := os.Stat(filepath.Join(targetDir, "base"))
	if os.IsNotExist(err) {
		t.Fatal("directory was not created")
	}
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected a directory, got a regular file")
	}
}

func TestExtractEntry_Symlink(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()

	tr, hdr := makeSingleEntryTar(t, &tar.Header{
		Name:     "pg_wal",
		Typeflag: tar.TypeSymlink,
		Linkname: "../wal_archive",
	}, nil)

	if err := extractEntry(tr, hdr, targetDir, log); err != nil {
		t.Fatalf("extractEntry symlink: %v", err)
	}

	link, err := os.Readlink(filepath.Join(targetDir, "pg_wal"))
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if link != "../wal_archive" {
		t.Errorf("symlink target = %q, want %q", link, "../wal_archive")
	}
}

// TestExtractEntry_PathTraversal is a security test.
//
// How the protection works in Go:
//   - extractEntry prepends "/" to hdr.Name before filepath.Clean, so "../etc/passwd"
//     becomes "/etc/passwd" — an absolute path but NOT an OS escape.
//   - Go's filepath.Join concatenates (unlike Python), so
//     filepath.Join(targetDir, "/etc/passwd") = targetDir+"/etc/passwd" — still safe.
//   - The only entries that escape the prefix check are those whose cleaned path
//     is exactly "/" (= targetDir itself, not inside it), e.g. ".", "../../..", "///".
//
// We test both: a deep traversal attempt that resolves to "/" (rejected), and a
// shallow "../foo" that resolves safely inside targetDir (accepted).
func TestExtractEntry_PathTraversal_RejectsRootResolution(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()

	// "../../.." → filepath.Clean("/" + "../../..") = "/" →
	// filepath.Join(targetDir, "/") = targetDir (no trailing sep) → rejected.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "../../..", Typeflag: tar.TypeDir, Mode: 0755})
	tw.Close()

	tr := tar.NewReader(&buf)
	hdr, _ := tr.Next()

	err := extractEntry(tr, hdr, targetDir, log)
	if err == nil {
		t.Fatal("expected 'escapes target directory' error for deep traversal, got nil")
	}
	if !strings.Contains(err.Error(), "escapes target directory") {
		t.Errorf("error should mention 'escapes target directory', got: %v", err)
	}
}

func TestExtractEntry_PathTraversal_ShallowIsReRooted(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()

	// "../safe_file" → filepath.Clean("/../safe_file") = "/safe_file" →
	// filepath.Join(targetDir, "/safe_file") = targetDir+"/safe_file" — inside targetDir.
	// The code accepts this (re-roots to targetDir) rather than escaping.
	content := []byte("re-rooted content")
	tr, hdr := makeSingleEntryTar(t, &tar.Header{
		Name:     "../safe_file",
		Typeflag: tar.TypeReg,
		Size:     int64(len(content)),
		Mode:     0600,
	}, content)

	if err := extractEntry(tr, hdr, targetDir, log); err != nil {
		t.Fatalf("shallow traversal should be re-rooted safely, got: %v", err)
	}

	// File was re-rooted into targetDir.
	if _, err := os.Stat(filepath.Join(targetDir, "safe_file")); os.IsNotExist(err) {
		t.Error("re-rooted file was not extracted inside targetDir")
	}
}

func TestExtractEntry_UnsupportedType_IsSkipped(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tw.WriteHeader(&tar.Header{Name: "somedevice", Typeflag: tar.TypeBlock})
	tw.Close()

	tr := tar.NewReader(&buf)
	hdr, _ := tr.Next()

	// Should not error — just skip.
	if err := extractEntry(tr, hdr, targetDir, log); err != nil {
		t.Errorf("unsupported tar type should be skipped silently, got: %v", err)
	}
}

// ── downloadTablespace ────────────────────────────────────────────────────────

func TestDownloadTablespace_WritesFileCorrectly(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	data := []byte("fake tar.gz content — raw bytes")
	st.Upload(ctx, "backup/base.tar.gz", bytes.NewReader(data))

	dest := filepath.Join(t.TempDir(), "ts_0.blob")
	if err := downloadTablespace(ctx, st, "backup/base.tar.gz", dest); err != nil {
		t.Fatalf("downloadTablespace: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("reading downloaded file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("downloaded content mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestDownloadTablespace_NotFound(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	dest := filepath.Join(t.TempDir(), "missing.blob")

	err := downloadTablespace(ctx, st, "does/not/exist", dest)
	if err == nil {
		t.Fatal("expected error for missing storage key, got nil")
	}
}

// ── applyTablespace ───────────────────────────────────────────────────────────

func TestApplyTablespace_ExtractsFiles(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()

	content := []byte("PostgreSQL version 16\n")
	tarGz := makeFakeTarGz(t, []fakeTarEntry{
		{name: "PG_VERSION", data: content},
		{name: "global/pg_control", data: []byte("fake pg_control")},
	})

	staged := filepath.Join(t.TempDir(), "ts_0.blob")
	if err := os.WriteFile(staged, tarGz, 0600); err != nil {
		t.Fatalf("writing staged file: %v", err)
	}

	if err := applyTablespace(crypto.NoopEncryptor{}, staged, targetDir, log); err != nil {
		t.Fatalf("applyTablespace: %v", err)
	}

	// Verify extracted files.
	pgVer, err := os.ReadFile(filepath.Join(targetDir, "PG_VERSION"))
	if err != nil {
		t.Fatalf("reading PG_VERSION: %v", err)
	}
	if !bytes.Equal(pgVer, content) {
		t.Errorf("PG_VERSION: got %q, want %q", pgVer, content)
	}

	if _, err := os.Stat(filepath.Join(targetDir, "global", "pg_control")); os.IsNotExist(err) {
		t.Error("global/pg_control was not extracted")
	}
}

func TestApplyTablespace_BadFile(t *testing.T) {
	targetDir := t.TempDir()
	log := zerolog.Nop()

	// A file that is NOT a valid gzip stream.
	staged := filepath.Join(t.TempDir(), "bad.blob")
	os.WriteFile(staged, []byte("this is not gzip"), 0600)

	err := applyTablespace(crypto.NoopEncryptor{}, staged, targetDir, log)
	if err == nil {
		t.Fatal("expected error for malformed gzip, got nil")
	}
}

func TestApplyTablespace_MissingFile(t *testing.T) {
	log := zerolog.Nop()
	err := applyTablespace(crypto.NoopEncryptor{}, "/nonexistent/ts_0.blob", t.TempDir(), log)
	if err == nil {
		t.Fatal("expected error for missing staged file, got nil")
	}
}

// ── Download (exported) ───────────────────────────────────────────────────────

func TestDownload_CreatesStagingDir(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := setupStorageWithBackup(t, st, []fakeTarEntry{
		{name: "PG_VERSION", data: []byte("16\n")},
	})

	cfg := RestoreConfig{
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        zerolog.Nop(),
	}

	result, err := Download(ctx, cfg)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(result.StagingDir) })

	// Staging dir must exist.
	if _, err := os.Stat(result.StagingDir); os.IsNotExist(err) {
		t.Error("staging directory does not exist after Download")
	}

	// The file for OID=0 must be present.
	blobPath := stagingPath(result.StagingDir, 0)
	if _, err := os.Stat(blobPath); os.IsNotExist(err) {
		t.Errorf("staging blob %q not found; staging dir contents: %v", blobPath, stagingDirContents(t, result.StagingDir))
	}

	// Manifest must be populated.
	if result.Manifest == nil {
		t.Fatal("result.Manifest is nil")
	}
	if result.Manifest.BackupName != name {
		t.Errorf("manifest.BackupName = %q, want %q", result.Manifest.BackupName, name)
	}
}

func TestDownload_MissingManifest(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage() // empty — no manifest

	cfg := RestoreConfig{
		BackupName: "no-such-backup",
		Parallel:   1,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        zerolog.Nop(),
	}

	_, err := Download(ctx, cfg)
	if err == nil {
		t.Fatal("expected error for missing manifest, got nil")
	}
}

// ── Apply (exported) ──────────────────────────────────────────────────────────

func TestApply_ExtractsAndWritesRecoveryConfig(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := setupStorageWithBackup(t, st, []fakeTarEntry{
		{name: "PG_VERSION", data: []byte("16\n")},
		{name: "global/pg_control", data: []byte("fake pg_control")},
	})

	cfg := RestoreConfig{
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        zerolog.Nop(),
	}

	// Download first.
	result, err := Download(ctx, cfg)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(result.StagingDir) })

	// Apply to fresh PGDATA.
	pgdata := t.TempDir()
	cfg.PGDataDir = pgdata

	if err := Apply(ctx, cfg, result); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Verify key PGDATA files were extracted.
	for _, f := range []string{"PG_VERSION", "global"} {
		if _, err := os.Stat(filepath.Join(pgdata, f)); os.IsNotExist(err) {
			t.Errorf("expected %q in PGDATA after Apply", f)
		}
	}

	// Plain restore — no PITR fields set — must NOT write recovery.signal.
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain Apply must NOT write recovery.signal")
	}
}

func TestApply_RejectsNonEmptyPGData(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := setupStorageWithBackup(t, st, []fakeTarEntry{
		{name: "PG_VERSION", data: []byte("16\n")},
	})

	result, err := Download(ctx, RestoreConfig{
		BackupName: name, Parallel: 1,
		Encryptor: crypto.NoopEncryptor{}, Storage: st, Log: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(result.StagingDir) })

	// Pre-populate PGDATA — Apply should refuse it.
	pgdata := t.TempDir()
	os.WriteFile(filepath.Join(pgdata, "PG_VERSION"), []byte("16\n"), 0600)

	err = Apply(ctx, RestoreConfig{
		BackupName: name, PGDataDir: pgdata, Parallel: 1,
		Encryptor: crypto.NoopEncryptor{}, Storage: st, Log: zerolog.Nop(),
	}, result)

	if err == nil {
		t.Fatal("Apply should reject a non-empty PGDATA, got nil")
	}
	if !strings.Contains(err.Error(), "not empty") {
		t.Errorf("error should mention 'not empty', got: %v", err)
	}
}

// ── Download → Apply full pipeline ────────────────────────────────────────────

func TestDownloadApply_FullPipeline(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	name := setupStorageWithBackup(t, st, []fakeTarEntry{
		{name: "PG_VERSION", data: []byte("16\n")},
		{name: "global/pg_control", data: []byte("fake control")},
		{name: "base/1/", data: nil}, // directory entry
	})

	cfg := RestoreConfig{
		BackupName: name,
		Parallel:   2,
		Encryptor:  crypto.NoopEncryptor{},
		Storage:    st,
		Log:        zerolog.Nop(),
	}

	// Phase 1: Download (no PGDATA needed yet).
	result, err := Download(ctx, cfg)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer os.RemoveAll(result.StagingDir)

	// Staging dir should have exactly one blob (OID=0).
	entries, _ := os.ReadDir(result.StagingDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 staging blob, got %d", len(entries))
	}

	// Phase 2: Apply to an empty directory.
	pgdata := t.TempDir()
	cfg.PGDataDir = pgdata

	if err := Apply(ctx, cfg, result); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Plain restore — only data files, no PITR signal.
	mustExist := []string{"PG_VERSION"}
	for _, name := range mustExist {
		if _, err := os.Stat(filepath.Join(pgdata, name)); os.IsNotExist(err) {
			t.Errorf("expected %q to exist in PGDATA after Apply", name)
		}
	}
	if _, err := os.Stat(filepath.Join(pgdata, "recovery.signal")); !os.IsNotExist(err) {
		t.Error("plain Apply must NOT write recovery.signal")
	}

	pgVer, _ := os.ReadFile(filepath.Join(pgdata, "PG_VERSION"))
	if string(pgVer) != "16\n" {
		t.Errorf("PG_VERSION: got %q, want %q", pgVer, "16\n")
	}
}

// ── internal helper ───────────────────────────────────────────────────────────

func stagingDirContents(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []string{"<readdir error: " + err.Error() + ">"}
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
