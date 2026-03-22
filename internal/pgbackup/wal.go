package pgbackup

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// ── WAL Push (archive_command) ───────────────────────────────────────────────

// WalPushConfig holds parameters for archiving a single WAL file.
type WalPushConfig struct {
	FilePath  string // local path to the WAL file (%p)
	FileName  string // WAL file name (%f), e.g. "000000010000000000000003"
	WalPrefix string // storage prefix (default "wal_archive")
	Compress  bool   // gzip before upload
	Encryptor crypto.Encryptor
	Storage   storage.Storage
	Log       zerolog.Logger
}

// walObjectKey returns the storage key for a WAL file, including any
// compression / encryption suffixes.
func walObjectKey(prefix, name string, compress, encrypted bool) string {
	key := prefix + "/" + name
	if compress {
		key += ".gz"
	}
	if encrypted {
		key += ".age"
	}
	return key
}

// WalPush uploads a single WAL file to cloud storage.
//
// It is designed to be called from PostgreSQL's archive_command:
//
//	archive_command = 'cloud-dump wal-push %p %f --storage storj ...'
//
// Idempotent: if the object already exists in storage, the upload is skipped
// and nil is returned. PostgreSQL may retry archive_command after timeouts,
// so idempotency prevents duplicate work.
func WalPush(ctx context.Context, cfg WalPushConfig) error {
	log := cfg.Log.With().
		Str("wal_file", cfg.FileName).
		Str("prefix", cfg.WalPrefix).
		Logger()

	encrypted := !isNoop(cfg.Encryptor)
	key := walObjectKey(cfg.WalPrefix, cfg.FileName, cfg.Compress, encrypted)

	// ── 1. Idempotency check ─────────────────────────────────────────────
	exists, err := cfg.Storage.Exists(ctx, key)
	if err != nil {
		return fmt.Errorf("checking existence of %q: %w", key, err)
	}
	if exists {
		log.Info().Str("key", key).Msg("WAL file already archived — skipping")
		return nil
	}

	// ── 2. Open local WAL file ───────────────────────────────────────────
	f, err := os.Open(cfg.FilePath)
	if err != nil {
		return fmt.Errorf("opening WAL file %q: %w", cfg.FilePath, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL file %q: %w", cfg.FilePath, err)
	}

	log.Info().
		Str("key", key).
		Str("size", formatBytes(stat.Size())).
		Bool("compress", cfg.Compress).
		Bool("encrypt", encrypted).
		Msg("archiving WAL file")

	t0 := time.Now()

	// ── 3. Build pipeline: file → [gzip] → [encrypt] → upload ───────────
	pr, pw := io.Pipe()

	var uploadErr error
	done := make(chan struct{})

	go func() {
		defer close(done)
		uploadErr = cfg.Storage.Upload(ctx, key, pr)
		if uploadErr != nil {
			pr.CloseWithError(uploadErr)
		}
	}()

	writeErr := func() error {
		// Outermost writer: encryption layer (wraps pw).
		encW, err := cfg.Encryptor.Encrypt(pw)
		if err != nil {
			return fmt.Errorf("initialising encryption: %w", err)
		}

		// Middle layer: optional gzip compression.
		var dst io.WriteCloser
		if cfg.Compress {
			dst = gzip.NewWriter(encW)
		} else {
			dst = nopWriteCloser{encW}
		}

		_, copyErr := io.Copy(dst, f)

		if closeErr := dst.Close(); copyErr == nil {
			copyErr = closeErr
		}
		if closeErr := encW.Close(); copyErr == nil {
			copyErr = closeErr
		}
		return copyErr
	}()

	pw.CloseWithError(writeErr)
	<-done

	if writeErr != nil {
		return fmt.Errorf("compressing/encrypting WAL %q: %w", cfg.FileName, writeErr)
	}
	if uploadErr != nil {
		return fmt.Errorf("uploading WAL %q: %w", key, uploadErr)
	}

	elapsed := time.Since(t0)
	log.Info().
		Str("key", key).
		Str("elapsed", formatDuration(elapsed)).
		Str("speed", formatSpeed(stat.Size(), elapsed)).
		Msg("WAL file archived")

	return nil
}

// ── WAL Fetch (restore_command) ──────────────────────────────────────────────

// WalFetchConfig holds parameters for fetching a single WAL file.
type WalFetchConfig struct {
	FileName  string // WAL file name (%f)
	DestPath  string // destination path (%p)
	WalPrefix string // storage prefix (default "wal_archive")
	Encryptor crypto.Encryptor
	Storage   storage.Storage
	Log       zerolog.Logger
}

// WalFetch downloads a WAL file from cloud storage, decompresses and decrypts
// it if needed, and writes it to DestPath.
//
// It is designed to be called from PostgreSQL's restore_command:
//
//	restore_command = 'cloud-dump wal-fetch %f %p --storage storj ...'
//
// Returns ErrWalNotFound when no matching object exists — the caller should
// exit non-zero so PostgreSQL knows recovery is complete.
func WalFetch(ctx context.Context, cfg WalFetchConfig) error {
	log := cfg.Log.With().
		Str("wal_file", cfg.FileName).
		Str("prefix", cfg.WalPrefix).
		Logger()

	encrypted := !isNoop(cfg.Encryptor)

	// ── 1. Try candidate keys in order of most likely suffix ─────────────
	// We try multiple suffixes because the WAL may have been archived with
	// or without compression / encryption.
	candidates := []struct {
		key        string
		compressed bool
		encrypted  bool
	}{
		{walObjectKey(cfg.WalPrefix, cfg.FileName, true, true), true, true},
		{walObjectKey(cfg.WalPrefix, cfg.FileName, true, false), true, false},
		{walObjectKey(cfg.WalPrefix, cfg.FileName, false, true), false, true},
		{walObjectKey(cfg.WalPrefix, cfg.FileName, false, false), false, false},
	}

	// If we know whether encryption is expected, prefer matching candidates.
	// But we still try all of them for robustness.

	for _, c := range candidates {
		exists, err := cfg.Storage.Exists(ctx, c.key)
		if err != nil {
			log.Warn().Err(err).Str("key", c.key).Msg("error checking WAL existence")
			continue
		}
		if !exists {
			continue
		}

		// Need decryptor if WAL is encrypted.
		if c.encrypted && !encrypted {
			return fmt.Errorf("WAL file %q is encrypted but no decryption key/passphrase provided", c.key)
		}

		log.Info().
			Str("key", c.key).
			Bool("compressed", c.compressed).
			Bool("encrypted", c.encrypted).
			Msg("fetching WAL file")

		t0 := time.Now()

		if err := fetchAndWrite(ctx, cfg.Storage, cfg.Encryptor, c.key, cfg.DestPath, c.compressed, c.encrypted); err != nil {
			return fmt.Errorf("fetching WAL %q: %w", c.key, err)
		}

		log.Info().
			Str("key", c.key).
			Str("dest", cfg.DestPath).
			Str("elapsed", formatDuration(time.Since(t0))).
			Msg("WAL file fetched")

		return nil
	}

	// No candidate found — WAL segment does not exist in the archive.
	return ErrWalNotFound
}

// ErrWalNotFound is returned by WalFetch when the requested WAL segment
// does not exist in cloud storage. The caller should exit non-zero so
// PostgreSQL interprets this as "no more WAL available" and ends recovery.
var ErrWalNotFound = fmt.Errorf("WAL file not found in archive")

// fetchAndWrite downloads an object, applies decryption/decompression, and
// writes the result to destPath atomically (write temp → rename).
func fetchAndWrite(
	ctx context.Context,
	st storage.Storage,
	enc crypto.Encryptor,
	key, destPath string,
	compressed, encrypted bool,
) error {
	rc, err := st.Download(ctx, key)
	if err != nil {
		return fmt.Errorf("downloading %q: %w", key, err)
	}
	defer rc.Close()

	var r io.Reader = rc

	// Decrypt layer.
	if encrypted {
		dr, err := enc.Decrypt(rc)
		if err != nil {
			return fmt.Errorf("decrypting %q: %w", key, err)
		}
		r = dr
	}

	// Decompress layer.
	if compressed {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return fmt.Errorf("creating gzip reader for %q: %w", key, err)
		}
		defer gz.Close()
		r = gz
	}

	// Write to a temp file first, then rename for atomicity.
	// This prevents PostgreSQL from reading a partially-written WAL file.
	tmpPath := destPath + ".cloud-dump.tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("creating temp file %q: %w", tmpPath, err)
	}

	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("writing WAL to %q: %w", tmpPath, err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing temp file %q: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("renaming %q → %q: %w", tmpPath, destPath, err)
	}

	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// nopWriteCloser wraps a Writer with a no-op Close method.
type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
