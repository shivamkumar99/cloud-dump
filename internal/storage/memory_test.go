package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/shivamkumar99/cloud-dump/internal/storage"
)

// ── Upload / Download ─────────────────────────────────────────────────────────

func TestMemoryStorage_UploadDownload(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	data := []byte("hello cloud-dump")

	if err := st.Upload(ctx, "key1", bytes.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := st.Download(ctx, "key1")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestMemoryStorage_Upload_IsolatesCopy(t *testing.T) {
	// The stored bytes must be independent of the original slice.
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	data := []byte("original")

	st.Upload(ctx, "k", bytes.NewReader(data))
	data[0] = 'X' // mutate after upload

	rc, _ := st.Download(ctx, "k")
	got, _ := io.ReadAll(rc)
	rc.Close()

	if got[0] == 'X' {
		t.Error("Download returned a slice that shares memory with the upload source")
	}
}

func TestMemoryStorage_Download_NotFound(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	_, err := st.Download(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key, got nil")
	}
}

func TestMemoryStorage_Download_LargeObject(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	large := bytes.Repeat([]byte{0xAB}, 10*1024*1024) // 10 MB

	if err := st.Upload(ctx, "large", bytes.NewReader(large)); err != nil {
		t.Fatalf("Upload large: %v", err)
	}

	rc, err := st.Download(ctx, "large")
	if err != nil {
		t.Fatalf("Download large: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, large) {
		t.Errorf("large object mismatch: got %d bytes, want %d", len(got), len(large))
	}
}

// ── Overwrite ─────────────────────────────────────────────────────────────────

func TestMemoryStorage_Overwrite(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	st.Upload(ctx, "key", strings.NewReader("first"))
	st.Upload(ctx, "key", strings.NewReader("second"))

	rc, _ := st.Download(ctx, "key")
	got, _ := io.ReadAll(rc)
	rc.Close()

	if string(got) != "second" {
		t.Errorf("expected overwritten value %q, got %q", "second", got)
	}
}

// ── Exists ────────────────────────────────────────────────────────────────────

func TestMemoryStorage_Exists_Absent(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	ok, err := st.Exists(ctx, "absent")
	if err != nil {
		t.Fatalf("Exists: unexpected error: %v", err)
	}
	if ok {
		t.Error("Exists should return false for a key that was never uploaded")
	}
}

func TestMemoryStorage_Exists_Present(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	st.Upload(ctx, "present", strings.NewReader("data"))

	ok, err := st.Exists(ctx, "present")
	if err != nil {
		t.Fatalf("Exists: unexpected error: %v", err)
	}
	if !ok {
		t.Error("Exists should return true after Upload")
	}
}

// ── List ──────────────────────────────────────────────────────────────────────

func TestMemoryStorage_List_ByPrefix(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	for _, k := range []string{"prefix/a", "prefix/b", "other/c", "prefix/d"} {
		st.Upload(ctx, k, strings.NewReader("data"))
	}

	listed, err := st.List(ctx, "prefix/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 3 {
		t.Errorf("List(\"prefix/\"): expected 3 keys, got %d: %v", len(listed), listed)
	}
	for _, k := range listed {
		if !strings.HasPrefix(k, "prefix/") {
			t.Errorf("listed key %q does not match prefix", k)
		}
	}
}

func TestMemoryStorage_List_EmptyPrefix(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	st.Upload(ctx, "a", strings.NewReader(""))
	st.Upload(ctx, "b", strings.NewReader(""))

	listed, err := st.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 2 {
		t.Errorf("List(\"\"): expected 2 keys, got %d: %v", len(listed), listed)
	}
}

func TestMemoryStorage_List_NoMatch(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	st.Upload(ctx, "foo/bar", strings.NewReader(""))

	listed, err := st.List(ctx, "baz/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("expected empty list for non-matching prefix, got: %v", listed)
	}
}

// ── Test helpers (Has / Size / Keys / Get) ────────────────────────────────────

func TestMemoryStorage_Has(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	if st.Has("missing") {
		t.Error("Has should return false for a key that was never uploaded")
	}

	st.Upload(ctx, "present", strings.NewReader("x"))

	if !st.Has("present") {
		t.Error("Has should return true after Upload")
	}
}

func TestMemoryStorage_Size(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	if st.Size("missing") != 0 {
		t.Error("Size of missing key should be 0")
	}

	data := bytes.Repeat([]byte{0xFF}, 42)
	st.Upload(ctx, "k", bytes.NewReader(data))

	if st.Size("k") != 42 {
		t.Errorf("Size: got %d, want 42", st.Size("k"))
	}
}

func TestMemoryStorage_Keys_Empty(t *testing.T) {
	st := storage.NewMemoryStorage()
	if len(st.Keys()) != 0 {
		t.Errorf("expected no keys on fresh storage, got: %v", st.Keys())
	}
}

func TestMemoryStorage_Keys_AfterUploads(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	st.Upload(ctx, "a", strings.NewReader(""))
	st.Upload(ctx, "b", strings.NewReader(""))
	st.Upload(ctx, "c", strings.NewReader(""))

	keys := st.Keys()
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestMemoryStorage_Get_Missing(t *testing.T) {
	st := storage.NewMemoryStorage()
	_, ok := st.Get("missing")
	if ok {
		t.Error("Get should return false for a missing key")
	}
}

func TestMemoryStorage_Get_Present(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()
	data := []byte("test payload")

	st.Upload(ctx, "k", bytes.NewReader(data))

	got, ok := st.Get("k")
	if !ok {
		t.Fatal("Get should return true for an existing key")
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Get content mismatch: got %q, want %q", got, data)
	}
}

// ── Close ─────────────────────────────────────────────────────────────────────

func TestMemoryStorage_Close(t *testing.T) {
	st := storage.NewMemoryStorage()
	if err := st.Close(); err != nil {
		t.Errorf("Close returned unexpected error: %v", err)
	}
}

// ── Concurrent safety ─────────────────────────────────────────────────────────

func TestMemoryStorage_ConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStorage()

	const goroutines = 50
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", n)

			st.Upload(ctx, key, strings.NewReader("data"))
			st.Exists(ctx, key)

			rc, err := st.Download(ctx, key)
			if err == nil {
				io.ReadAll(rc)
				rc.Close()
			}

			st.List(ctx, "key-")
		}(i)
	}

	wg.Wait()

	// All goroutines uploaded one key each.
	if len(st.Keys()) != goroutines {
		t.Errorf("expected %d keys after concurrent uploads, got %d", goroutines, len(st.Keys()))
	}
}
