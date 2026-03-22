package crypto_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"filippo.io/age"

	"github.com/shivamkumar99/cloud-dump/internal/crypto"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// roundTrip encrypts plaintext with enc, then decrypts and returns the result.
func roundTrip(t *testing.T, enc crypto.Encryptor, plaintext []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	w, err := enc.Encrypt(&buf)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := enc.Decrypt(&buf)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return out
}

func largePayload(n int) []byte {
	return []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", n))
}

// isGzip returns true if data starts with the gzip magic bytes.
func isGzip(data []byte) bool {
	return len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

// ── NoopEncryptor ─────────────────────────────────────────────────────────────

func TestNoopEncryptor_Passthrough(t *testing.T) {
	enc := crypto.NoopEncryptor{}
	original := []byte("hello noop world")

	out := roundTrip(t, enc, original)
	if !bytes.Equal(out, original) {
		t.Errorf("noop round-trip changed data: got %q, want %q", out, original)
	}
}

func TestNoopEncryptor_DoesNotEncrypt(t *testing.T) {
	enc := crypto.NoopEncryptor{}
	payload := largePayload(100)

	var buf bytes.Buffer
	w, _ := enc.Encrypt(&buf)
	w.Write(payload)
	w.Close()

	// The noop encryptor must not alter the bytes at all.
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Error("NoopEncryptor altered bytes — it should be a passthrough")
	}
}

func TestNoopEncryptor_EmptyInput(t *testing.T) {
	enc := crypto.NoopEncryptor{}
	out := roundTrip(t, enc, []byte{})
	if len(out) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(out))
	}
}

// ── PassphraseEncryptor ───────────────────────────────────────────────────────

func TestPassphraseEncryptor_RoundTrip(t *testing.T) {
	enc := crypto.NewPassphraseEncryptor("correct-horse-battery-staple")
	original := largePayload(500)

	out := roundTrip(t, enc, original)
	if !bytes.Equal(out, original) {
		t.Error("passphrase round-trip: decrypted data does not match original")
	}
}

func TestPassphraseEncryptor_OutputIsNotPlaintext(t *testing.T) {
	enc := crypto.NewPassphraseEncryptor("secret")
	payload := largePayload(100)

	var buf bytes.Buffer
	w, _ := enc.Encrypt(&buf)
	w.Write(payload)
	w.Close()

	if bytes.Equal(buf.Bytes(), payload) {
		t.Error("encrypted output should differ from plaintext")
	}
	if isGzip(buf.Bytes()) {
		t.Error("encrypted output should not be plain gzip (it must be encrypted)")
	}
}

func TestPassphraseEncryptor_LargeData(t *testing.T) {
	enc := crypto.NewPassphraseEncryptor("large-data-passphrase")
	// Simulate compressing then encrypting (as the backup pipeline does).
	original := largePayload(10000) // ~450 KB

	// Gzip the payload first (as the backup does).
	var gzBuf bytes.Buffer
	gz := gzip.NewWriter(&gzBuf)
	gz.Write(original)
	gz.Close()

	out := roundTrip(t, enc, gzBuf.Bytes())

	// Decompress and verify.
	gr, err := gzip.NewReader(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	recovered, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("reading gzip: %v", err)
	}
	if !bytes.Equal(recovered, original) {
		t.Error("gzip+encrypt round-trip: final data does not match original")
	}
}

func TestPassphraseEncryptor_WrongPassphrase(t *testing.T) {
	encWriter := crypto.NewPassphraseEncryptor("right-passphrase")

	var buf bytes.Buffer
	w, _ := encWriter.Encrypt(&buf)
	w.Write([]byte("secret payload"))
	w.Close()

	encReader := crypto.NewPassphraseEncryptor("wrong-passphrase")
	_, err := encReader.Decrypt(&buf)
	if err == nil {
		t.Fatal("expected error when decrypting with wrong passphrase, got nil")
	}
}

func TestPassphraseEncryptor_EmptyPassphrase(t *testing.T) {
	// age requires a non-empty passphrase; verify we surface the error.
	enc := crypto.NewPassphraseEncryptor("")
	var buf bytes.Buffer
	_, err := enc.Encrypt(&buf)
	if err == nil {
		t.Fatal("expected error for empty passphrase, got nil")
	}
}

// ── KeyPairEncryptor ──────────────────────────────────────────────────────────

// generateTempKeyPair writes a fresh age X25519 key pair to temp files.
// Returns (recipientFile, identityFile).
func generateTempKeyPair(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()

	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity: %v", err)
	}

	recipientFile := filepath.Join(dir, "key.pub")
	identityFile := filepath.Join(dir, "key")

	if err := os.WriteFile(recipientFile, []byte(identity.Recipient().String()+"\n"), 0600); err != nil {
		t.Fatalf("writing recipient file: %v", err)
	}
	if err := os.WriteFile(identityFile, []byte(identity.String()+"\n"), 0600); err != nil {
		t.Fatalf("writing identity file: %v", err)
	}
	return recipientFile, identityFile
}

func TestKeyPairEncryptor_RoundTrip(t *testing.T) {
	recipientFile, identityFile := generateTempKeyPair(t)

	encWriter := crypto.NewKeyPairEncryptor(recipientFile, "")  // encrypt only needs recipient
	encReader := crypto.NewKeyPairEncryptor("", identityFile)    // decrypt only needs identity

	original := largePayload(200)

	var buf bytes.Buffer
	w, err := encWriter.Encrypt(&buf)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := w.Write(original); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := encReader.Decrypt(&buf)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(out, original) {
		t.Error("key-pair round-trip: decrypted data does not match original")
	}
}

func TestKeyPairEncryptor_WrongKey(t *testing.T) {
	recipientFile1, _          := generateTempKeyPair(t)
	_,              identityFile2 := generateTempKeyPair(t)

	encWriter := crypto.NewKeyPairEncryptor(recipientFile1, "")
	encReader := crypto.NewKeyPairEncryptor("", identityFile2)

	var buf bytes.Buffer
	w, _ := encWriter.Encrypt(&buf)
	w.Write([]byte("secret payload"))
	w.Close()

	_, err := encReader.Decrypt(&buf)
	if err == nil {
		t.Fatal("expected error when decrypting with wrong private key, got nil")
	}
}

func TestKeyPairEncryptor_MissingRecipientFile(t *testing.T) {
	enc := crypto.NewKeyPairEncryptor("/nonexistent/path/key.pub", "")
	_, err := enc.Encrypt(&bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error for missing recipient file, got nil")
	}
}

func TestKeyPairEncryptor_MissingIdentityFile(t *testing.T) {
	enc := crypto.NewKeyPairEncryptor("", "/nonexistent/path/key")
	_, err := enc.Decrypt(&bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error for missing identity file, got nil")
	}
}
