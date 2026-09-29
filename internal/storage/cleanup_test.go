package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func oldTemp(t *testing.T, dir, prefix string) string {
	t.Helper()
	os.MkdirAll(dir, 0700)
	f, err := os.CreateTemp(dir, prefix)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(f.Name(), old, old); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}
func TestCleanupRecognizesOnlySafeOrphans(t *testing.T) {
	s := newTestStore(t)
	h, _ := keyHash("id")
	dir := filepath.Dir(s.objectPathFromHash(h))
	orphan := oldTemp(t, dir, tempPrefix+h+"-")
	oldTemp(t, dir, tempPrefix+"invalid-")
	fresh, err := os.CreateTemp(dir, tempPrefix+h+"-")
	if err != nil {
		t.Fatal(err)
	}
	fresh.Close()
	report, err := s.CleanupTemps(context.Background(), time.Hour)
	if err != nil || report.Removed != 1 || report.Skipped != 2 {
		t.Fatalf("%+v %v", report, err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan retained")
	}
	lockedTemp := oldTemp(t, dir, tempPrefix+h+"-")
	unlock, err := s.acquireObjectReadLock(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	report, err = s.CleanupTemps(context.Background(), time.Hour)
	unlock()
	if err != nil || report.Removed != 0 {
		t.Fatalf("%+v %v", report, err)
	}
	if _, err := os.Stat(lockedTemp); err != nil {
		t.Fatal("active file removed")
	}
	report, err = s.CleanupTemps(context.Background(), time.Hour)
	if err != nil || report.Removed != 1 {
		t.Fatalf("%+v %v", report, err)
	}
	for _, age := range []time.Duration{0, -time.Second} {
		if _, err := s.CleanupTemps(context.Background(), age); err == nil {
			t.Fatal("invalid age")
		}
	}
	// Recognized names in the wrong shard must not be removed.
	misplaced := oldTemp(t, s.objectsDir, tempPrefix+h+"-")
	if _, err := s.CleanupTemps(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(misplaced); err != nil {
		t.Fatal("removed misplaced file")
	}
}
func TestDownloadCleanup(t *testing.T) {
	s := newTestStore(t)
	other := newTestStore(t)
	h, _ := keyHash("id")
	dir := t.TempDir()
	orphan := oldTemp(t, dir, s.downloadPrefix()+h+"-")
	foreign := oldTemp(t, dir, other.downloadPrefix()+h+"-")
	nested := oldTemp(t, filepath.Join(dir, "nested"), s.downloadPrefix()+h+"-")
	unlock, err := s.acquireObjectReadLock(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.CleanupDownloads(context.Background(), dir, time.Hour)
	unlock()
	if err != nil || r.Removed != 0 || r.Skipped != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	r, err = s.CleanupDownloads(context.Background(), dir, time.Hour)
	if err != nil || r.Removed != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan retained")
	}
	for _, p := range []string{foreign, nested} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal("unrelated file removed")
		}
	}
	for _, p := range []string{"", filepath.Join(dir, "missing")} {
		if _, err := s.CleanupDownloads(context.Background(), p, time.Hour); err == nil {
			t.Fatal("invalid directory")
		}
	}
}
func TestTempParser(t *testing.T) {
	h := strings.Repeat("a", 64)
	for _, name := range []string{"", tempPrefix, tempPrefix + h, tempPrefix + h + "-", tempPrefix + strings.Repeat("z", 64) + "-1", tempPrefix + strings.ToUpper(h) + "-1"} {
		if _, ok := hashFromTempName(name); ok {
			t.Fatalf("accepted %q", name)
		}
	}
	if got, ok := hashFromTempName(tempPrefix + h + "-123"); !ok || got != h {
		t.Fatal(got, ok)
	}
}
func TestCleanupDoesNotFollowSymlinks(t *testing.T) {
	s := newTestStore(t)
	h, _ := keyHash("id")
	dir := filepath.Dir(s.objectPathFromHash(h))
	os.MkdirAll(dir, 0700)
	target := filepath.Join(t.TempDir(), "keep")
	os.WriteFile(target, []byte("keep"), 0600)
	link := filepath.Join(dir, tempPrefix+h+"-123")
	if err := os.Symlink(target, link); err != nil {
		t.Skip(err)
	}
	r, err := s.CleanupTemps(context.Background(), time.Nanosecond)
	if err != nil || r.Removed != 0 || r.Skipped != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatal(err)
	}
}
func TestCleanupDirectoryFailure(t *testing.T) {
	s := newTestStore(t)
	os.Remove(s.objectsDir)
	if _, err := s.CleanupTemps(context.Background(), time.Hour); err == nil {
		t.Fatal("missing root")
	}
	h, _ := keyHash("id")
	dir := filepath.Dir(s.objectPathFromHash(h))
	oldTemp(t, dir, tempPrefix+h+"-")
	normal := s.ops.syncDir
	boom := errors.New("directory I/O failure")
	// Allow lock-directory preparation, fail after unlink in the object shard.
	s.ops.syncDir = func(p string) error {
		if p == dir {
			return boom
		}
		return normal(p)
	}
	r, err := s.CleanupTemps(context.Background(), time.Hour)
	if r.Removed != 1 || !errors.Is(err, ErrCommitUncertain) || !errors.Is(err, boom) {
		t.Fatalf("%+v %v", r, err)
	}
}
