//go:build integration

// Package integration provides end-to-end tests for the backup/restore/WAL pipeline.
//
// Prerequisites:
//
//	docker compose up -d   (starts PostgreSQL on localhost:5432)
//
// Run:
//
//	go test -tags integration -v ./tests/integration/... -timeout 10m
//
// Environment variables (all optional, defaults match docker-compose.yml):
//
//	PGURL      — replication DSN  (default: postgres://repl_user:repl_password@localhost:5432/postgres?replication=yes)
//	PGDATA_DIR — where to restore (default: temp dir created per test)
package integration

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/rs/zerolog"
)

// pgURL returns the PostgreSQL replication DSN, falling back to the
// docker-compose default when PGURL is not set.
func pgURL(t *testing.T) string {
	t.Helper()
	if u := os.Getenv("PGURL"); u != "" {
		return u
	}
	return "postgres://repl_user:repl_password@localhost:5432/postgres?replication=yes"
}

// testLogger returns a zerolog logger that writes through t.Log.
func testLogger(t *testing.T) zerolog.Logger {
	t.Helper()
	return zerolog.New(zerolog.NewTestWriter(t)).With().Timestamp().Logger()
}

// uniqueName returns a name that is unique within a test run.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// emptyDir creates a temporary directory for PGDATA restore targets and
// registers cleanup with t.Cleanup.
func emptyDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cloud-dump-restore-*")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// tarHasEntry returns true when the gzip-compressed TAR in data contains at
// least one entry whose name starts with prefix.
func tarHasEntry(data []byte, prefix string) (bool, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return false, fmt.Errorf("gzip reader: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("tar.Next: %w", err)
		}
		if strings.HasPrefix(hdr.Name, prefix) {
			return true, nil
		}
	}
}

// generateKeyPairFiles writes a fresh age X25519 identity to temp files and
// returns (recipientFile, identityFile) paths.
func generateKeyPairFiles(t *testing.T) (recipientFile, identityFile string) {
	t.Helper()
	dir := t.TempDir()

	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity: %v", err)
	}

	recipientFile = filepath.Join(dir, "key.pub")
	identityFile = filepath.Join(dir, "key")

	if err := os.WriteFile(recipientFile, []byte(identity.Recipient().String()+"\n"), 0600); err != nil {
		t.Fatalf("writing recipient file: %v", err)
	}
	if err := os.WriteFile(identityFile, []byte(identity.String()+"\n"), 0600); err != nil {
		t.Fatalf("writing identity file: %v", err)
	}
	return recipientFile, identityFile
}

// fakeWALSegment writes a fake WAL segment of the given size to a temp
// directory and returns (filePath, fileName).
func fakeWALSegment(t *testing.T, size int) (path, name string) {
	t.Helper()
	dir := t.TempDir()
	name = "000000010000000000000001"
	path = filepath.Join(dir, name)

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251) // deterministic, non-zero
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("writing fake WAL segment: %v", err)
	}
	return path, name
}

// fakeWALSegments generates n sequential fake WAL segments in a single temp
// directory. Each segment has a unique name matching the PostgreSQL WAL
// naming convention (timeline=1, incrementing LSN). Returns a slice of
// (filePath, fileName) pairs in LSN order.
//
// Segment names follow the pattern: 0000000100000000000000XX (XX = 01..n).
func fakeWALSegments(t *testing.T, n, size int) []struct{ Path, Name string } {
	t.Helper()
	dir := t.TempDir()
	segments := make([]struct{ Path, Name string }, n)
	for i := range n {
		name := fmt.Sprintf("00000001000000000000%04X", i+1)
		path := filepath.Join(dir, name)
		data := make([]byte, size)
		for j := range data {
			// Each segment has a distinct byte pattern so fetch results can be
			// verified independently.
			data[j] = byte((i*7 + j) % 251)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatalf("writing fake WAL segment %d: %v", i+1, err)
		}
		segments[i] = struct{ Path, Name string }{path, name}
	}
	return segments
}
