// Package pgbackup (internal test) covers pure helpers in backup.go that do
// not require a live PostgreSQL connection.
package pgbackup

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// ── tablespaceKey ─────────────────────────────────────────────────────────────

func TestTablespaceKey(t *testing.T) {
	tests := []struct {
		name       string
		backupName string
		oid        int32
		encrypted  bool
		want       string
	}{
		{"base plain", "mybackup", 0, false, "mybackup/base.tar.gz"},
		{"base encrypted", "mybackup", 0, true, "mybackup/base.tar.gz.age"},
		{"tablespace plain", "mybackup", 16384, false, "mybackup/16384.tar.gz"},
		{"tablespace encrypted", "mybackup", 16384, true, "mybackup/16384.tar.gz.age"},
		{"nested backup name", "prod/2026-03-07", 0, false, "prod/2026-03-07/base.tar.gz"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tablespaceKey(tc.backupName, tc.oid, tc.encrypted)
			if got != tc.want {
				t.Errorf("tablespaceKey(%q, %d, %v) = %q, want %q",
					tc.backupName, tc.oid, tc.encrypted, got, tc.want)
			}
		})
	}
}

// ── isNoop ────────────────────────────────────────────────────────────────────

func TestIsNoop(t *testing.T) {
	if !isNoop(crypto.NoopEncryptor{}) {
		t.Error("NoopEncryptor must be detected as noop")
	}
	if isNoop(crypto.NewPassphraseEncryptor("secret")) {
		t.Error("PassphraseEncryptor must NOT be detected as noop")
	}
}

// ── formatBytes ───────────────────────────────────────────────────────────────

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1024 * 1024, "1.00 MB"},
		{5 * 1024 * 1024, "5.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{int64(2.5 * 1024 * 1024 * 1024), "2.50 GB"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := formatBytes(tc.input)
			if got != tc.want {
				t.Errorf("formatBytes(%d) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// ── formatDuration ────────────────────────────────────────────────────────────

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{500 * time.Millisecond, "0.5s"},
		{1500 * time.Millisecond, "1.5s"},
		{59 * time.Second, "59.0s"},
		{90 * time.Second, "1m30s"},
		{61 * time.Second, "1m01s"},
		{3661 * time.Second, "1h01m01s"},
		{7200 * time.Second, "2h00m00s"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := formatDuration(tc.input)
			if got != tc.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// ── formatSpeed ───────────────────────────────────────────────────────────────

func TestFormatSpeed_ZeroDuration(t *testing.T) {
	got := formatSpeed(1_000_000, 0)
	if got != "0 B/s" {
		t.Errorf("formatSpeed with zero duration: got %q, want %q", got, "0 B/s")
	}
}

func TestFormatSpeed_OneMBPerSecond(t *testing.T) {
	got := formatSpeed(1024*1024, time.Second)
	if got != "1.00 MB/s" {
		t.Errorf("got %q, want %q", got, "1.00 MB/s")
	}
}

func TestFormatSpeed_HalfSecond(t *testing.T) {
	// 512 KB in 500ms = 1 MB/s
	got := formatSpeed(512*1024, 500*time.Millisecond)
	if got != "1.00 MB/s" {
		t.Errorf("got %q, want %q", got, "1.00 MB/s")
	}
}

// ── progressTracker ───────────────────────────────────────────────────────────

func TestProgressTracker(t *testing.T) {
	pt := &progressTracker{}

	if pt.Bytes() != 0 {
		t.Fatalf("initial Bytes() should be 0, got %d", pt.Bytes())
	}

	n, err := pt.Write(make([]byte, 100))
	if err != nil || n != 100 {
		t.Fatalf("Write(100): n=%d err=%v", n, err)
	}
	if pt.Bytes() != 100 {
		t.Errorf("after 100-byte write: got %d, want 100", pt.Bytes())
	}

	pt.Write(make([]byte, 55))
	if pt.Bytes() != 155 {
		t.Errorf("after second write: got %d, want 155", pt.Bytes())
	}
}

// ── compressAndEncrypt ────────────────────────────────────────────────────────

// drainPipe reads all bytes from pr into a buf, sending the error on done.
func drainPipe(pr *io.PipeReader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, pr)
	return buf.Bytes(), err
}

func TestCompressAndEncrypt_Noop_RoundTrip(t *testing.T) {
	pr, pw := io.Pipe()
	original := bytes.Repeat([]byte("hello backup payload"), 200)

	done := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, err := drainPipe(pr)
		done <- struct {
			data []byte
			err  error
		}{data, err}
	}()

	writeErr := compressAndEncrypt(crypto.NoopEncryptor{}, pw, bytes.NewReader(original))
	pw.CloseWithError(writeErr)

	result := <-done
	if result.err != nil {
		t.Fatalf("reading from pipe: %v", result.err)
	}
	if writeErr != nil {
		t.Fatalf("compressAndEncrypt: %v", writeErr)
	}

	// Output must be valid gzip.
	gz, err := gzip.NewReader(bytes.NewReader(result.data))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	got, err := io.ReadAll(gz)
	gz.Close()
	if err != nil {
		t.Fatalf("reading gzip: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("compress/decompress round-trip: content mismatch (got %d bytes, want %d)", len(got), len(original))
	}
}

func TestCompressAndEncrypt_Passphrase_NotGzip(t *testing.T) {
	pr, pw := io.Pipe()
	enc := crypto.NewPassphraseEncryptor("test-compress-passphrase")

	done := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, err := drainPipe(pr)
		done <- struct {
			data []byte
			err  error
		}{data, err}
	}()

	writeErr := compressAndEncrypt(enc, pw, bytes.NewReader(bytes.Repeat([]byte("data"), 500)))
	pw.CloseWithError(writeErr)

	result := <-done
	if writeErr != nil {
		t.Fatalf("compressAndEncrypt: %v", writeErr)
	}
	if result.err != nil {
		t.Fatalf("reading: %v", result.err)
	}

	// Encrypted output must NOT be valid gzip.
	if len(result.data) >= 2 && result.data[0] == 0x1f && result.data[1] == 0x8b {
		t.Error("encrypted output starts with gzip magic — encryption appears to have been skipped")
	}
}

// ── uploadStream ─────────────────────────────────────────────────────────────

func TestUploadStream_StoredDataIsGzip(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	original := bytes.Repeat([]byte("test data for upload"), 500)
	log := zerolog.Nop()

	err := uploadStream(ctx, st, crypto.NoopEncryptor{}, "test/object.tar.gz", bytes.NewReader(original), log)
	if err != nil {
		t.Fatalf("uploadStream: %v", err)
	}

	if !st.Has("test/object.tar.gz") {
		t.Fatalf("expected key in storage, got keys: %v", st.Keys())
	}

	data, _ := st.Get("test/object.tar.gz")
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		t.Error("stored data does not start with gzip magic bytes")
	}

	// Decompress and verify content is intact.
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	got, err := io.ReadAll(gz)
	gz.Close()
	if err != nil {
		t.Fatalf("reading gzip: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("upload/decompress round-trip: content mismatch")
	}
}

func TestUploadStream_Encrypted_NotGzip(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	enc := crypto.NewPassphraseEncryptor("upload-test-passphrase")
	log := zerolog.Nop()

	err := uploadStream(ctx, st, enc, "test/encrypted.tar.gz.age", bytes.NewReader([]byte("some data")), log)
	if err != nil {
		t.Fatalf("uploadStream: %v", err)
	}

	data, _ := st.Get("test/encrypted.tar.gz.age")
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		t.Error("encrypted output must not be plain gzip")
	}
}

func TestUploadStream_TracksBytes(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	payload := bytes.Repeat([]byte("x"), 8192)
	log := zerolog.Nop()

	if err := uploadStream(ctx, st, crypto.NoopEncryptor{}, "k", bytes.NewReader(payload), log); err != nil {
		t.Fatalf("uploadStream: %v", err)
	}

	// The stored object must be non-empty (the progressTracker counted at least payload bytes).
	if st.Size("k") == 0 {
		t.Error("expected non-empty stored object")
	}
}
