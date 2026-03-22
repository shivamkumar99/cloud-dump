package pgbackup_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// fakeWALFile creates a temporary file with deterministic content that looks
// like a 16 MB WAL segment (we use a smaller size for speed).
func fakeWALFile(t *testing.T, size int) (path, name string) {
	t.Helper()
	dir := t.TempDir()
	name = "000000010000000000000001"
	path = filepath.Join(dir, name)

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251) // deterministic non-zero pattern
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("writing fake WAL file: %v", err)
	}
	return path, name
}

func testLog(t *testing.T) zerolog.Logger {
	t.Helper()
	return zerolog.New(zerolog.NewTestWriter(t)).With().Timestamp().Logger()
}

// ── WalPush Tests ────────────────────────────────────────────────────────────

func TestWalPush_Uncompressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 1024)

	cfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  false,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}

	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	key := "wal_archive/" + name
	if !st.Has(key) {
		t.Fatalf("expected key %q in storage, got keys: %v", key, st.Keys())
	}

	// Content should match the original file exactly (no compression).
	original, _ := os.ReadFile(path)
	stored, _ := st.Get(key)
	if !bytes.Equal(original, stored) {
		t.Errorf("stored content differs from original (len %d vs %d)", len(stored), len(original))
	}
}

func TestWalPush_Compressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 4096)

	cfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}

	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	key := "wal_archive/" + name + ".gz"
	if !st.Has(key) {
		t.Fatalf("expected key %q in storage, got keys: %v", key, st.Keys())
	}

	// Decompress and verify content matches.
	stored, _ := st.Get(key)
	gz, err := gzip.NewReader(bytes.NewReader(stored))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	decompressed, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("reading gzip: %v", err)
	}
	gz.Close()

	original, _ := os.ReadFile(path)
	if !bytes.Equal(decompressed, original) {
		t.Errorf("decompressed content differs from original (len %d vs %d)", len(decompressed), len(original))
	}
}

func TestWalPush_Idempotent(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 512)

	cfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  false,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}

	// First push.
	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("first WalPush: %v", err)
	}

	firstData, _ := st.Get("wal_archive/" + name)

	// Second push — should skip, not error.
	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("second WalPush (idempotent): %v", err)
	}

	secondData, _ := st.Get("wal_archive/" + name)
	if !bytes.Equal(firstData, secondData) {
		t.Error("second push modified the stored data — not idempotent")
	}
}

func TestWalPush_Encrypted(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 2048)

	enc := crypto.NewPassphraseEncryptor("test-wal-passphrase")

	cfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  true,
		Encryptor: enc,
		Storage:   st,
		Log:       testLog(t),
	}

	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	key := "wal_archive/" + name + ".gz.age"
	if !st.Has(key) {
		t.Fatalf("expected key %q in storage, got keys: %v", key, st.Keys())
	}

	// Raw stored data should NOT be valid gzip (it's encrypted).
	stored, _ := st.Get(key)
	if _, err := gzip.NewReader(bytes.NewReader(stored)); err == nil {
		t.Error("expected stored data to be encrypted (not valid gzip), but gzip reader succeeded")
	}
}

// ── WalFetch Tests ───────────────────────────────────────────────────────────

func TestWalFetch_Uncompressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 1024)

	// Push first.
	pushCfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  false,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalPush(ctx, pushCfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	// Fetch.
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, name)

	fetchCfg := pgbackup.WalFetchConfig{
		FileName:  name,
		DestPath:  destPath,
		WalPrefix: "wal_archive",
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalFetch(ctx, fetchCfg); err != nil {
		t.Fatalf("WalFetch: %v", err)
	}

	// Verify content.
	original, _ := os.ReadFile(path)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched file: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("fetched content differs from original (len %d vs %d)", len(fetched), len(original))
	}
}

func TestWalFetch_Compressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 4096)

	// Push compressed.
	pushCfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalPush(ctx, pushCfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	// Fetch — should auto-decompress.
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, name)

	fetchCfg := pgbackup.WalFetchConfig{
		FileName:  name,
		DestPath:  destPath,
		WalPrefix: "wal_archive",
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalFetch(ctx, fetchCfg); err != nil {
		t.Fatalf("WalFetch: %v", err)
	}

	original, _ := os.ReadFile(path)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched file: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("fetched content differs from original (len %d vs %d)", len(fetched), len(original))
	}
}

func TestWalFetch_EncryptedRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 2048)

	enc := crypto.NewPassphraseEncryptor("test-wal-passphrase")

	// Push encrypted + compressed.
	pushCfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  true,
		Encryptor: enc,
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalPush(ctx, pushCfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	// Fetch with same passphrase.
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, name)

	fetchCfg := pgbackup.WalFetchConfig{
		FileName:  name,
		DestPath:  destPath,
		WalPrefix: "wal_archive",
		Encryptor: enc,
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalFetch(ctx, fetchCfg); err != nil {
		t.Fatalf("WalFetch: %v", err)
	}

	original, _ := os.ReadFile(path)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched file: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("fetched content differs from original (len %d vs %d)", len(fetched), len(original))
	}
}

func TestWalFetch_NotFound(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "000000010000000000000099")

	fetchCfg := pgbackup.WalFetchConfig{
		FileName:  "000000010000000000000099",
		DestPath:  destPath,
		WalPrefix: "wal_archive",
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}

	err := pgbackup.WalFetch(ctx, fetchCfg)
	if err == nil {
		t.Fatal("expected ErrWalNotFound, got nil")
	}
	if err != pgbackup.ErrWalNotFound {
		t.Fatalf("expected ErrWalNotFound, got: %v", err)
	}

	// Destination file should NOT exist.
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		t.Error("expected destination file to not exist after WalFetch failure")
	}
}

func TestWalPush_CustomPrefix(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 512)

	cfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "my-cluster/wal",
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}

	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	key := "my-cluster/wal/" + name + ".gz"
	if !st.Has(key) {
		t.Fatalf("expected key %q in storage, got keys: %v", key, st.Keys())
	}
}

// TestWalPush_FileNotFound verifies a useful error is returned when the local
// WAL file does not exist (e.g. PostgreSQL passed a wrong path).
func TestWalPush_FileNotFound(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	cfg := pgbackup.WalPushConfig{
		FilePath:  "/nonexistent/wal/segment",
		FileName:  "000000010000000000000001",
		WalPrefix: "wal_archive",
		Compress:  false,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}

	err := pgbackup.WalPush(ctx, cfg)
	if err == nil {
		t.Fatal("expected error for non-existent WAL file, got nil")
	}
}

// TestWalFetch_WrongPassphrase verifies that fetching an encrypted WAL with the
// wrong passphrase returns an error and does not write the destination file.
func TestWalFetch_WrongPassphrase(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 1024)

	// Push with the correct passphrase.
	pushCfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  true,
		Encryptor: crypto.NewPassphraseEncryptor("correct-passphrase"),
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalPush(ctx, pushCfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	destPath := filepath.Join(t.TempDir(), name)
	fetchCfg := pgbackup.WalFetchConfig{
		FileName:  name,
		DestPath:  destPath,
		WalPrefix: "wal_archive",
		Encryptor: crypto.NewPassphraseEncryptor("wrong-passphrase"),
		Storage:   st,
		Log:       testLog(t),
	}

	err := pgbackup.WalFetch(ctx, fetchCfg)
	if err == nil {
		t.Fatal("expected error when fetching with wrong passphrase, got nil")
	}
	// Destination file must not exist — partial writes are dangerous for WAL.
	if _, statErr := os.Stat(destPath); !os.IsNotExist(statErr) {
		t.Error("destination file must not exist after a failed WalFetch")
	}
}

// TestWalFetch_EncryptedNoDecryptor verifies that fetching an encrypted WAL
// with NoopEncryptor (no decryption capability) returns a clear error — NOT
// ErrWalNotFound, because the file exists but cannot be decrypted.
func TestWalFetch_EncryptedNoDecryptor(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	path, name := fakeWALFile(t, 512)

	// Push with passphrase encryption.
	pushCfg := pgbackup.WalPushConfig{
		FilePath:  path,
		FileName:  name,
		WalPrefix: "wal_archive",
		Compress:  true,
		Encryptor: crypto.NewPassphraseEncryptor("secret"),
		Storage:   st,
		Log:       testLog(t),
	}
	if err := pgbackup.WalPush(ctx, pushCfg); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	// Fetch with no decryption capability.
	destPath := filepath.Join(t.TempDir(), name)
	fetchCfg := pgbackup.WalFetchConfig{
		FileName:  name,
		DestPath:  destPath,
		WalPrefix: "wal_archive",
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLog(t),
	}

	err := pgbackup.WalFetch(ctx, fetchCfg)
	if err == nil {
		t.Fatal("expected error when fetching encrypted WAL without a decryptor, got nil")
	}
	if err == pgbackup.ErrWalNotFound {
		t.Error("got ErrWalNotFound, but the file exists — expected a 'no decryption key' error")
	}
}
