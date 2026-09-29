package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	base := filepath.Join(t.TempDir(), "store")
	s, err := New(base)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutGetRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	payload := []byte("hello production storage")

	info, err := s.Put(ctx, "../../not-a-path", bytes.NewReader(payload), Upsert)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(payload)) {
		t.Fatalf("size=%d want=%d", info.Size, len(payload))
	}

	dest := filepath.Join(t.TempDir(), "nested", "file.bin")
	if _, err := s.GetFile(ctx, "../../not-a-path", dest, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q want %q", got, payload)
	}
}

func TestCreateOnlyAndUpdateOnly(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, err := s.Put(ctx, "k", bytes.NewBufferString("one"), UpdateOnly); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: got %v", err)
	}
	if _, err := s.Put(ctx, "k", bytes.NewBufferString("one"), CreateOnly); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "k", bytes.NewBufferString("two"), CreateOnly); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("create existing: got %v", err)
	}
	if _, err := s.Put(ctx, "k", bytes.NewBufferString("two"), UpdateOnly); err != nil {
		t.Fatal(err)
	}
}

func TestGetDoesNotOverwriteByDefault(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.Put(ctx, "k", bytes.NewBufferString("new"), Upsert); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "dest")
	if err := os.WriteFile(dest, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetFile(ctx, "k", dest, false); err == nil {
		t.Fatal("expected destination exists error")
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("existing destination changed: %q", got)
	}
}

func TestConcurrentUpsertsSameIDProduceWholeObject(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	a := bytes.Repeat([]byte("A"), 128*1024)
	b := bytes.Repeat([]byte("B"), 128*1024)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, payload := range [][]byte{a, b} {
		payload := payload
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Put(ctx, "same", bytes.NewReader(payload), Upsert)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	dest := filepath.Join(t.TempDir(), "result")
	if _, err := s.GetFile(ctx, "same", dest, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, a) && !bytes.Equal(got, b) {
		t.Fatal("stored data is a torn/interleaved write")
	}
}

func TestDeleteMissing(t *testing.T) {
	s := newTestStore(t)
	if err := s.Delete(context.Background(), "missing", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	if err := s.Delete(context.Background(), "missing", true); err != nil {
		t.Fatal(err)
	}
}

func TestObjectPathIsContained(t *testing.T) {
	s := newTestStore(t)
	path, err := s.ObjectPath("../../etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(s.objectsDir, path)
	if err != nil {
		t.Fatal(err)
	}
	if rel == ".." || len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator) {
		t.Fatalf("object path escaped store: %s", path)
	}
}

func TestCalculateObjectPath(t *testing.T) {
	root := t.TempDir()
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	p, err := CalculateObjectPath(root, "my-object")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(p, resolvedRoot) {
		t.Fatalf("unexpected prefix: %s", p)
	}

	if _, err := CalculateObjectPath("", "id"); err == nil {
		t.Fatal("expected error on empty baseDir")
	}
	if _, err := CalculateObjectPath(root, ""); err == nil {
		t.Fatal("expected error on empty id")
	}

	f := filepath.Join(root, "not-a-dir")
	_ = os.WriteFile(f, []byte("x"), 0o600)
	if _, err := CalculateObjectPath(f, "id"); err == nil {
		t.Fatal("expected error when baseDir is a file")
	}
}

func TestReadOnlyStoreReadLocks(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	s, err := New(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	mustPut(t, s, "ro-id", "read-only payload")

	h, _ := keyHash("ro-id")
	lockPath := s.lockPathFromHash(h)
	if err := os.Chmod(lockPath, 0o400); err != nil {
		t.Skip(err)
	}
	defer os.Chmod(lockPath, 0o600)

	info, err := s.Stat(ctx, "ro-id", true)
	if err != nil || !info.Verified {
		t.Fatalf("stat on read-only lock file failed: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "dest.bin")
	if _, err := s.GetFile(ctx, "ro-id", dest, false); err != nil {
		t.Fatalf("get on read-only lock file failed: %v", err)
	}
}
