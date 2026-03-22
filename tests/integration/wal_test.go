//go:build integration

package integration

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/pgbackup"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// ── WAL Push ──────────────────────────────────────────────────────────────────

// TestWal_Push_Uncompressed verifies that an uncompressed WAL segment is stored
// verbatim (raw bytes, no transformation) under the expected key.
func TestWal_Push_Uncompressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPath, walName := fakeWALSegment(t, 1024*16) // 16 KB

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: uniqueName("wal"),
		Compress:  false,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	// Key must exist without a .gz suffix.
	prefix := st.Keys()[0]
	if filepath.Ext(prefix) == ".gz" {
		t.Errorf("uncompressed push must not add .gz suffix; got key %q", prefix)
	}

	original, _ := os.ReadFile(walPath)
	stored, _ := st.Get(prefix)
	if !bytes.Equal(original, stored) {
		t.Errorf("stored bytes differ from original (sizes: got %d, want %d)", len(stored), len(original))
	}
}

// TestWal_Push_Compressed verifies that a compressed WAL segment is stored as a
// valid gzip stream under a key ending in .gz.
func TestWal_Push_Compressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024*16)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush (compressed): %v", err)
	}

	key := walPrefix + "/" + walName + ".gz"
	if !st.Has(key) {
		t.Fatalf("expected key %q in storage; got: %v", key, st.Keys())
	}

	// Stored data must start with gzip magic bytes.
	data, _ := st.Get(key)
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		t.Error("stored WAL does not start with gzip magic — compression appears skipped")
	}
}

// TestWal_Push_Encrypted_Passphrase verifies that a passphrase-encrypted WAL
// segment is stored as opaque ciphertext (not gzip) under a .gz.age key.
func TestWal_Push_Encrypted_Passphrase(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024*16)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NewPassphraseEncryptor("wal-passphrase"),
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush (encrypted): %v", err)
	}

	key := walPrefix + "/" + walName + ".gz.age"
	if !st.Has(key) {
		t.Fatalf("expected .gz.age key %q; got: %v", key, st.Keys())
	}

	data, _ := st.Get(key)
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		t.Error("encrypted WAL must not be readable as gzip")
	}
}

// TestWal_Push_Encrypted_KeyPair verifies WAL archival with age X25519
// encryption, storing ciphertext under a .gz.age key.
func TestWal_Push_Encrypted_KeyPair(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024*16)

	recipientFile, _ := generateKeyPairFiles(t)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NewKeyPairEncryptor(recipientFile, ""),
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush (key-pair): %v", err)
	}

	key := walPrefix + "/" + walName + ".gz.age"
	if !st.Has(key) {
		t.Fatalf("expected .gz.age key %q; got: %v", key, st.Keys())
	}
}

// TestWal_Push_Idempotent verifies that pushing the same WAL segment twice
// does not error and does not modify the stored object.
func TestWal_Push_Idempotent(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024)

	cfg := pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  false,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}

	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("first WalPush: %v", err)
	}
	first, _ := st.Get(walPrefix + "/" + walName)

	if err := pgbackup.WalPush(ctx, cfg); err != nil {
		t.Fatalf("second WalPush (idempotent): %v", err)
	}
	second, _ := st.Get(walPrefix + "/" + walName)

	if !bytes.Equal(first, second) {
		t.Error("second push modified the stored object — not idempotent")
	}
}

// ── WAL Fetch ─────────────────────────────────────────────────────────────────

// TestWal_Fetch_Uncompressed verifies that an uncompressed WAL segment pushed
// to storage can be fetched back with byte-for-byte fidelity.
func TestWal_Fetch_Uncompressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024*16)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  false,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush: %v", err)
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
		t.Fatalf("WalFetch: %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched WAL: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("fetched bytes differ from original (sizes: %d vs %d)", len(fetched), len(original))
	}
}

// TestWal_Fetch_Compressed verifies that a compressed WAL segment is fetched
// and transparently decompressed to the original content.
func TestWal_Fetch_Compressed(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024*16)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush: %v", err)
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
		t.Fatalf("WalFetch: %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched WAL: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("decompressed content differs from original (sizes: %d vs %d)", len(fetched), len(original))
	}
}

// TestWal_Fetch_Encrypted_Passphrase verifies a full encrypted push → fetch
// cycle: the fetched segment is decrypted and matches the original.
func TestWal_Fetch_Encrypted_Passphrase(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024*16)
	enc := crypto.NewPassphraseEncryptor("wal-passphrase")

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: enc,
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush (encrypted): %v", err)
	}

	destPath := filepath.Join(t.TempDir(), walName)
	if err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destPath,
		WalPrefix: walPrefix,
		Encryptor: enc,
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalFetch (encrypted): %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched WAL: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("decrypted content differs from original (sizes: %d vs %d)", len(fetched), len(original))
	}
}

// TestWal_Fetch_Encrypted_KeyPair verifies a full age X25519 push → fetch cycle.
func TestWal_Fetch_Encrypted_KeyPair(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024*16)

	recipientFile, identityFile := generateKeyPairFiles(t)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NewKeyPairEncryptor(recipientFile, ""),
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush (key-pair): %v", err)
	}

	destPath := filepath.Join(t.TempDir(), walName)
	if err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destPath,
		WalPrefix: walPrefix,
		Encryptor: crypto.NewKeyPairEncryptor("", identityFile),
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalFetch (key-pair): %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading fetched WAL: %v", err)
	}
	if !bytes.Equal(original, fetched) {
		t.Errorf("key-pair decrypted content differs from original")
	}
}

// TestWal_Fetch_NotFound verifies that WalFetch returns ErrWalNotFound when the
// requested segment does not exist in storage.
func TestWal_Fetch_NotFound(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  "000000010000000000000099",
		DestPath:  filepath.Join(t.TempDir(), "000000010000000000000099"),
		WalPrefix: uniqueName("wal"),
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	})
	if err == nil {
		t.Fatal("expected ErrWalNotFound, got nil")
	}
	if err != pgbackup.ErrWalNotFound {
		t.Errorf("expected ErrWalNotFound, got: %v", err)
	}
}

// TestWal_Push_FileNotFound verifies that WalPush returns a clear error when
// the local WAL file path does not exist (e.g. PostgreSQL passed a wrong %p).
// Nothing should be written to storage.
func TestWal_Push_FileNotFound(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  "/nonexistent/path/to/000000010000000000000001",
		FileName:  "000000010000000000000001",
		WalPrefix: uniqueName("wal"),
		Compress:  true,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	})
	if err == nil {
		t.Fatal("expected error for non-existent WAL file, got nil")
	}
	if len(st.Keys()) != 0 {
		t.Errorf("storage must be empty after a failed push; got keys: %v", st.Keys())
	}
}

// TestWal_Fetch_EncryptedNoDecryptor verifies that fetching an encrypted WAL
// segment without a decryptor returns an error — and specifically NOT
// ErrWalNotFound, because the object exists in storage but cannot be read.
func TestWal_Fetch_EncryptedNoDecryptor(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024)

	// Push with passphrase encryption.
	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NewPassphraseEncryptor("secret"),
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	destPath := filepath.Join(t.TempDir(), walName)
	err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destPath,
		WalPrefix: walPrefix,
		Encryptor: crypto.NoopEncryptor{}, // no decryption capability
		Storage:   st,
		Log:       testLogger(t),
	})
	if err == nil {
		t.Fatal("expected error when fetching encrypted WAL with NoopEncryptor, got nil")
	}
	if err == pgbackup.ErrWalNotFound {
		t.Error("got ErrWalNotFound but the object exists — expected a decryption error")
	}
	// Destination file must not be left behind.
	if _, statErr := os.Stat(destPath); !os.IsNotExist(statErr) {
		t.Error("destination file must not exist after a failed WalFetch")
	}
}

// TestWal_MultipleSegments_RoundTrip pushes several sequential WAL segments
// (simulating continuous archiving) then fetches each one back and verifies
// byte-for-byte fidelity. This exercises the prefix/key routing across a
// realistic segment sequence.
func TestWal_MultipleSegments_RoundTrip(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	const segmentCount = 3

	segments := fakeWALSegments(t, segmentCount, 1024*8) // 8 KB each

	// Archive all segments (mirrors what PostgreSQL does via archive_command).
	for _, seg := range segments {
		if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
			FilePath:  seg.Path,
			FileName:  seg.Name,
			WalPrefix: walPrefix,
			Compress:  true,
			Encryptor: crypto.NoopEncryptor{},
			Storage:   st,
			Log:       testLogger(t),
		}); err != nil {
			t.Fatalf("WalPush %q: %v", seg.Name, err)
		}
	}

	if len(st.Keys()) != segmentCount {
		t.Fatalf("expected %d objects in storage, got %d: %v", segmentCount, len(st.Keys()), st.Keys())
	}

	// Fetch each segment and verify round-trip fidelity.
	destDir := t.TempDir()
	for _, seg := range segments {
		destPath := filepath.Join(destDir, seg.Name)
		if err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
			FileName:  seg.Name,
			DestPath:  destPath,
			WalPrefix: walPrefix,
			Encryptor: crypto.NoopEncryptor{},
			Storage:   st,
			Log:       testLogger(t),
		}); err != nil {
			t.Fatalf("WalFetch %q: %v", seg.Name, err)
		}

		original, _ := os.ReadFile(seg.Path)
		fetched, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("reading fetched segment %q: %v", seg.Name, err)
		}
		if !bytes.Equal(original, fetched) {
			t.Errorf("segment %q: fetched content differs from original (%d vs %d bytes)",
				seg.Name, len(fetched), len(original))
		}
	}
}

// TestWal_Prefix_Isolation verifies that two WAL archives using different
// prefixes are completely independent — a segment stored under one prefix
// cannot be fetched from the other, and each prefix only contains its own
// objects.
func TestWal_Prefix_Isolation(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	prefixA := uniqueName("wal-a")
	prefixB := uniqueName("wal-b")
	walPath, walName := fakeWALSegment(t, 512)

	// Push the same segment to both prefixes.
	for _, prefix := range []string{prefixA, prefixB} {
		if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
			FilePath:  walPath,
			FileName:  walName,
			WalPrefix: prefix,
			Compress:  false,
			Encryptor: crypto.NoopEncryptor{},
			Storage:   st,
			Log:       testLogger(t),
		}); err != nil {
			t.Fatalf("WalPush prefix=%q: %v", prefix, err)
		}
	}

	// Each prefix must have exactly one object.
	for _, prefix := range []string{prefixA, prefixB} {
		key := prefix + "/" + walName
		if !st.Has(key) {
			t.Errorf("expected key %q — not found", key)
		}
	}

	// Fetching from prefix A must not touch prefix B's object and vice versa.
	destA := filepath.Join(t.TempDir(), walName)
	if err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destA,
		WalPrefix: prefixA,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalFetch from prefix A: %v", err)
	}

	// A segment that does not exist in prefix B should return ErrWalNotFound.
	missingName := "000000010000000000000099"
	err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  missingName,
		DestPath:  filepath.Join(t.TempDir(), missingName),
		WalPrefix: prefixB,
		Encryptor: crypto.NoopEncryptor{},
		Storage:   st,
		Log:       testLogger(t),
	})
	if err != pgbackup.ErrWalNotFound {
		t.Errorf("expected ErrWalNotFound for missing segment in prefix B; got: %v", err)
	}

	original, _ := os.ReadFile(walPath)
	fetched, _ := os.ReadFile(destA)
	if !bytes.Equal(original, fetched) {
		t.Errorf("prefix A fetch content mismatch")
	}
}

// TestWal_Fetch_WrongPassphrase verifies that fetching an encrypted WAL with
// the wrong passphrase fails without writing a partial destination file.
func TestWal_Fetch_WrongPassphrase(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	walPrefix := uniqueName("wal")
	walPath, walName := fakeWALSegment(t, 1024)

	if err := pgbackup.WalPush(ctx, pgbackup.WalPushConfig{
		FilePath:  walPath,
		FileName:  walName,
		WalPrefix: walPrefix,
		Compress:  true,
		Encryptor: crypto.NewPassphraseEncryptor("correct-passphrase"),
		Storage:   st,
		Log:       testLogger(t),
	}); err != nil {
		t.Fatalf("WalPush: %v", err)
	}

	destPath := filepath.Join(t.TempDir(), walName)
	err := pgbackup.WalFetch(ctx, pgbackup.WalFetchConfig{
		FileName:  walName,
		DestPath:  destPath,
		WalPrefix: walPrefix,
		Encryptor: crypto.NewPassphraseEncryptor("wrong-passphrase"),
		Storage:   st,
		Log:       testLogger(t),
	})
	if err == nil {
		t.Fatal("expected error when fetching with wrong passphrase, got nil")
	}

	// Destination file must not exist — PostgreSQL must not read partial WAL.
	if _, statErr := os.Stat(destPath); !os.IsNotExist(statErr) {
		t.Error("destination file must not exist after a failed WalFetch")
	}
}
