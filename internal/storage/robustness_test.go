package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func mustPut(t *testing.T, s *Store, id, payload string) ObjectInfo {
	t.Helper()
	info, err := s.Put(context.Background(), id, strings.NewReader(payload), Upsert)
	if err != nil {
		t.Fatal(err)
	}
	return info
}
func requirePayload(t *testing.T, s *Store, id, payload string) {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "out")
	info, err := s.GetFile(context.Background(), id, dest, false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != payload || !info.Verified {
		t.Fatalf("payload=%q info=%+v err=%v", got, info, err)
	}
}
func TestValidation(t *testing.T) {
	for _, id := range []string{"", "a\x00b", strings.Repeat("x", MaxIDBytes+1)} {
		if !errors.Is(ValidateID(id), ErrInvalidID) {
			t.Fatalf("accepted invalid ID")
		}
	}
	if err := ValidateID(strings.Repeat("x", MaxIDBytes)); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []WriteMode{Upsert, CreateOnly, UpdateOnly, 99} {
		if mode.String() == "" {
			t.Fatal("empty mode")
		}
	}
	for _, path := range []string{"", "  ", string([]byte{0})} {
		if _, err := New(path); err == nil {
			t.Fatal("accepted bad root")
		}
	}
	if _, err := NewWithOptions(t.TempDir(), Options{MaxObjectBytes: -1}); err == nil {
		t.Fatal("negative limit")
	}
	p := filepath.Join(t.TempDir(), "file")
	os.WriteFile(p, []byte("x"), 0600)
	if _, err := New(p); err == nil {
		t.Fatal("file accepted as root")
	}
	if runtime.GOOS != "windows" {
		root := t.TempDir()
		os.Chmod(root, 0755)
		if _, err := New(root); !errors.Is(err, ErrInsecurePermissions) {
			t.Fatalf("permissions: %v", err)
		}
	}
	s := newTestStore(t)
	if !filepath.IsAbs(s.BaseDir()) {
		t.Fatal("relative base")
	}
	ctx := context.Background()
	if _, err := s.Put(ctx, "x", nil, Upsert); err == nil {
		t.Fatal("nil reader")
	}
	if _, err := s.Put(ctx, "x", strings.NewReader(""), 99); err == nil {
		t.Fatal("invalid mode")
	}
	for _, id := range []string{"", "\x00"} {
		if _, err := s.ObjectPath(id); !errors.Is(err, ErrInvalidID) {
			t.Fatal(err)
		}
		if _, err := s.Put(ctx, id, strings.NewReader(""), Upsert); !errors.Is(err, ErrInvalidID) {
			t.Fatal(err)
		}
		if _, err := s.PutFile(ctx, id, p, Upsert); !errors.Is(err, ErrInvalidID) {
			t.Fatal(err)
		}
		if _, err := s.GetFile(ctx, id, p, false); !errors.Is(err, ErrInvalidID) {
			t.Fatal(err)
		}
		if _, err := s.Stat(ctx, id, true); !errors.Is(err, ErrInvalidID) {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, id, false); !errors.Is(err, ErrInvalidID) {
			t.Fatal(err)
		}
	}
	for _, path := range []string{t.TempDir(), filepath.Join(t.TempDir(), "missing")} {
		if _, err := s.PutFile(ctx, "x", path, Upsert); err == nil {
			t.Fatal("accepted invalid source")
		}
	}
}

func TestIntegrityAndStat(t *testing.T) {
	for _, payload := range []string{"", "hello", strings.Repeat("abc", 400000)} {
		t.Run(fmt.Sprint(len(payload)), func(t *testing.T) {
			s := newTestStore(t)
			written := mustPut(t, s, "id", payload)
			for _, verify := range []bool{false, true} {
				st, err := s.Stat(context.Background(), "id", verify)
				if err != nil || st.Size != int64(len(payload)) || st.SHA256 != written.SHA256 || st.Verified != verify {
					t.Fatalf("%+v %v", st, err)
				}
			}
			requirePayload(t, s, "id", payload)
		})
	}
	for _, mutation := range []string{"payload", "magic", "id", "size", "truncate", "append", "raw"} {
		t.Run(mutation, func(t *testing.T) {
			s := newTestStore(t)
			mustPut(t, s, "id", "payload")
			p, _ := s.ObjectPath("id")
			b, _ := os.ReadFile(p)
			switch mutation {
			case "payload":
				b[headerSize] ^= 1
			case "magic":
				b[0] ^= 1
			case "id":
				b[48] ^= 1
			case "size":
				b[8] = 255
			case "truncate":
				b = b[:4]
			case "append":
				b = append(b, 1)
			case "raw":
				b = []byte("old raw format")
			}
			os.WriteFile(p, b, 0600)
			if _, err := s.Stat(context.Background(), "id", true); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("stat: %v", err)
			}
			dst := filepath.Join(t.TempDir(), "out")
			os.WriteFile(dst, []byte("preserved"), 0600)
			if _, err := s.GetFile(context.Background(), "id", dst, true); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("get: %v", err)
			}
			got, _ := os.ReadFile(dst)
			if string(got) != "preserved" {
				t.Fatal("corrupt data was published")
			}
			matches, _ := filepath.Glob(filepath.Join(filepath.Dir(dst), tempPrefix+"*"))
			if len(matches) != 0 {
				t.Fatal("leaked download")
			}
		})
	}
}

func TestDestinationSafety(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "a", "AAAA")
	mustPut(t, s, "b", "BBBB")
	ctx := context.Background()
	b, _ := s.ObjectPath("b")
	for _, p := range []string{b, s.BaseDir(), filepath.Join(s.BaseDir(), "new", "nested", "x")} {
		if _, err := s.GetFile(ctx, "a", p, true); !errors.Is(err, ErrUnsafeDestination) {
			t.Fatalf("%s: %v", p, err)
		}
	}
	requirePayload(t, s, "b", "BBBB")
	if _, err := s.GetFile(ctx, "a", "", false); err == nil {
		t.Fatal("empty destination")
	}
	if _, err := s.GetFile(ctx, "a", t.TempDir(), true); !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	parent := t.TempDir()
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(s.BaseDir(), alias); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := s.GetFile(ctx, "a", filepath.Join(alias, "new", "x"), true); !errors.Is(err, ErrUnsafeDestination) {
		t.Fatal(err)
	}
	target := filepath.Join(parent, "regular")
	os.WriteFile(target, []byte("keep"), 0600)
	link := filepath.Join(parent, "link")
	os.Symlink(target, link)
	if _, err := s.GetFile(ctx, "a", link, true); !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	dangling := filepath.Join(parent, "dangling")
	os.Symlink(filepath.Join(parent, "absent"), dangling)
	if _, err := CanonicalDestination(filepath.Join(dangling, "x")); err == nil {
		t.Fatal("dangling parent")
	}
	if _, err := CanonicalDestination(" "); err == nil {
		t.Fatal("blank path")
	}
	if _, err := CanonicalDestination("bad\x00/child"); err == nil {
		t.Fatal("invalid path")
	}
	if _, err := s.GetFile(ctx, "a", target, true); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(target)
	if string(got) != "AAAA" {
		t.Fatal(string(got))
	}
}

func TestSourceAndInternalSymlinksRejected(t *testing.T) {
	s := newTestStore(t)
	outside := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skip(err)
	}
	if _, err := s.PutFile(context.Background(), "id", link, Upsert); err == nil {
		t.Fatal("source symlink")
	}
	// A preexisting objects alias must not redirect writes outside the store.
	os.Remove(s.objectsDir)
	os.Symlink(outside, s.objectsDir)
	if _, err := New(s.BaseDir()); err == nil {
		t.Fatal("accepted internal alias")
	}
	if _, err := s.Put(context.Background(), "id", strings.NewReader("x"), Upsert); err == nil {
		t.Fatal("write followed alias")
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

type cancelReader struct{ cancel context.CancelFunc }

func (r cancelReader) Read(p []byte) (int, error) { r.cancel(); copy(p, "x"); return 1, io.EOF }

func TestFailedAndCancelledWritePreservesObject(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "id", "old")
	ctx := context.Background()
	boom := errors.New("disk/input failure")
	if _, err := s.Put(ctx, "id", failingReader{boom}, Upsert); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	actions := []func() error{
		func() error { _, e := s.Put(cancelled, "id", strings.NewReader("bad"), Upsert); return e },
		func() error { _, e := s.PutFile(cancelled, "id", "missing", Upsert); return e },
		func() error { _, e := s.GetFile(cancelled, "id", filepath.Join(t.TempDir(), "out"), false); return e },
		func() error { _, e := s.Stat(cancelled, "id", true); return e },
		func() error { return s.Delete(cancelled, "id", false) },
		func() error { _, e := s.CleanupTemps(cancelled, time.Hour); return e },
	}
	for _, action := range actions {
		if err := action(); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	during, cancelDuring := context.WithCancel(ctx)
	if _, err := s.Put(during, "id", cancelReader{cancelDuring}, Upsert); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	requirePayload(t, s, "id", "old")
	p, _ := s.ObjectPath("id")
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(p), tempPrefix+"*"))
	if len(matches) != 0 {
		t.Fatal("leaked temp")
	}
}

func TestLimits(t *testing.T) {
	s, err := NewWithOptions(filepath.Join(t.TempDir(), "store"), Options{MaxObjectBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "id", "abc")
	if _, err := s.Put(context.Background(), "id", strings.NewReader("abcd"), Upsert); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	requirePayload(t, s, "id", "abc")
	mustPut(t, s, "empty", "")
}

func TestPublicationFailures(t *testing.T) {
	boom := errors.New("injected I/O error")
	for _, stage := range []string{"file-sync", "commit", "directory-sync", "cancel-after-sync"} {
		t.Run(stage, func(t *testing.T) {
			s := newTestStore(t)
			mustPut(t, s, "id", "old")
			normal := s.ops
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch stage {
			case "file-sync":
				s.ops.syncFile = func(*os.File) error { return boom }
			case "commit":
				s.ops.commit = func(string, string, bool) error { return boom }
			case "directory-sync":
				s.ops.commit = func(a, b string, over bool) error {
					if err := commitFile(a, b, over); err != nil {
						return err
					}
					s.ops.syncDir = func(string) error { return boom }
					return nil
				}
			case "cancel-after-sync":
				s.ops.syncFile = func(f *os.File) error { cancel(); return f.Sync() }
			}
			info, err := s.Put(ctx, "id", strings.NewReader("new"), Upsert)
			if stage == "cancel-after-sync" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, boom) {
				t.Fatal(err)
			}
			s.ops = normal
			if stage == "directory-sync" {
				var ce *CommitError
				if !errors.Is(err, ErrCommitUncertain) || !errors.As(err, &ce) || info.Size != 3 || !strings.Contains(err.Error(), "state changed") {
					t.Fatalf("%+v %v", info, err)
				}
				requirePayload(t, s, "id", "new")
			} else {
				requirePayload(t, s, "id", "old")
			}
		})
	}
}

func TestDurableDirectoryOrderAndFailure(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "a", "b")
	var synced []string
	err := durableMkdirAll(p, func(path string) error { synced = append(synced, path); return nil })
	if err != nil || len(synced) < 3 {
		t.Fatalf("%v %v", synced, err)
	}
	resolved, _ := filepath.EvalSymlinks(p)
	if synced[0] != resolved || synced[1] != filepath.Dir(resolved) {
		t.Fatal(synced)
	}
	boom := errors.New("sync failure")
	if err := durableMkdirAll(p, func(string) error { return boom }); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	f := filepath.Join(root, "file")
	os.WriteFile(f, nil, 0600)
	if err := durableMkdirAll(filepath.Join(f, "child"), func(string) error { return nil }); err == nil {
		t.Fatal("mkdir should fail")
	}
}

func TestMigration(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id := "legacy"
	p, _ := s.ObjectPath(id)
	os.MkdirAll(filepath.Dir(p), 0700)
	payload := []byte("legacy data")
	os.WriteFile(p, payload, 0600)
	sum := sha256.Sum256(payload)
	expected := hex.EncodeToString(sum[:])
	if _, err := s.Migrate(ctx, id, "bad"); err == nil {
		t.Fatal("invalid checksum")
	}
	if _, err := s.Migrate(ctx, id, strings.Repeat("0", 64)); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	original, _ := os.ReadFile(p)
	if !bytes.Equal(original, payload) {
		t.Fatal("changed unverified legacy")
	}
	for i := 0; i < 2; i++ {
		info, err := s.Migrate(ctx, id, expected)
		if err != nil || !info.Verified || info.SHA256 != expected {
			t.Fatalf("%+v %v", info, err)
		}
	}
	requirePayload(t, s, id, string(payload))
	if _, err := s.Migrate(ctx, id, strings.Repeat("0", 64)); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	if _, err := s.Migrate(ctx, "", expected); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
	if _, err := s.Migrate(ctx, "missing", expected); err == nil {
		t.Fatal("missing migration")
	}
}

func TestBackupRestore(t *testing.T) {
	source := newTestStore(t)
	mustPut(t, source, "a", "one")
	mustPut(t, source, "b", "two")
	restored := newTestStore(t)
	// A quiescent snapshot: immutable committed containers are copied whole.
	for _, id := range []string{"a", "b"} {
		from, _ := source.ObjectPath(id)
		to, _ := restored.ObjectPath(id)
		os.MkdirAll(filepath.Dir(to), 0700)
		b, err := os.ReadFile(from)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(to, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	requirePayload(t, restored, "a", "one")
	requirePayload(t, restored, "b", "two")
}

func TestMissingAndNonRegularObjects(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.Stat(ctx, "missing", true); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := s.GetFile(ctx, "missing", filepath.Join(t.TempDir(), "out"), false); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	p, _ := s.ObjectPath("directory")
	os.MkdirAll(p, 0700)
	if _, err := s.Stat(ctx, "directory", false); !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "directory", false); !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "directory", strings.NewReader("x"), Upsert); !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	mustPut(t, s, "remove", "data")
	if err := s.Delete(ctx, "remove", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(ctx, "remove", false); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestPutFileRoundTripAndDestinationKey(t *testing.T) {
	s := newTestStore(t)
	src := filepath.Join(t.TempDir(), "source")
	os.WriteFile(src, []byte("source bytes"), 0600)
	if _, err := s.PutFile(context.Background(), "id", src, Upsert); err != nil {
		t.Fatal(err)
	}
	requirePayload(t, s, "id", "source bytes")
	key, err := DestinationKey(filepath.Join(t.TempDir(), "FILE"))
	if err != nil || !strings.HasSuffix(key, "file") {
		t.Fatal(key, err)
	}
	if _, err := DestinationKey(""); err == nil {
		t.Fatal("blank destination")
	}
}
func TestCancelledCopyAndLimitReaderFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := copyWithContext(ctx, io.Discard, strings.NewReader("data")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s, err := NewWithOptions(filepath.Join(t.TempDir(), "store"), Options{MaxObjectBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("source failed at size boundary")
	reader := io.MultiReader(strings.NewReader("abc"), failingReader{boom})
	if _, err := s.Put(context.Background(), "id", reader, Upsert); !errors.Is(err, boom) {
		t.Fatal(err)
	}
}
func TestDownloadPublicationFailures(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		s := newTestStore(t)
		mustPut(t, s, "id", "new")
		dest := filepath.Join(t.TempDir(), "dest")
		os.WriteFile(dest, []byte("old"), 0600)
		boom := errors.New("download I/O failure")
		if uncertain {
			s.ops.commit = func(a, b string, overwrite bool) error {
				if err := commitFile(a, b, overwrite); err != nil {
					return err
				}
				s.ops.syncDir = func(string) error { return boom }
				return nil
			}
		} else {
			s.ops.syncFile = func(*os.File) error { return boom }
		}
		_, err := s.GetFile(context.Background(), "id", dest, true)
		if !errors.Is(err, boom) || errors.Is(err, ErrCommitUncertain) != uncertain {
			t.Fatal(err)
		}
		b, _ := os.ReadFile(dest)
		want := "old"
		if uncertain {
			want = "new"
		}
		if string(b) != want {
			t.Fatal(string(b))
		}
	}
}
func TestDeleteUncertainOutcome(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "id", "old")
	target, _ := s.ObjectPath("id")
	normal := s.ops.syncDir
	boom := errors.New("delete directory sync")
	s.ops.syncDir = func(dir string) error {
		if dir == filepath.Dir(target) {
			if _, err := os.Stat(target); os.IsNotExist(err) {
				return boom
			}
		}
		return normal(dir)
	}
	err := s.Delete(context.Background(), "id", false)
	if !errors.Is(err, ErrCommitUncertain) || !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("deletion did not happen")
	}
}
func TestLockAndFilesystemFailures(t *testing.T) {
	s := newTestStore(t)
	h, _ := keyHash("id")
	p := s.lockPathFromHash(h)
	os.MkdirAll(p, 0700)
	if _, err := s.acquireObjectLock(context.Background(), h); !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	if _, _, err := s.tryAcquireObjectLock(h); !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	if _, err := s.PutFile(context.Background(), "id", filepath.Join(t.TempDir(), "missing"), Upsert); err == nil {
		t.Fatal("missing source")
	}
	if _, err := regularFileExists("bad\x00"); err == nil {
		t.Fatal("invalid path")
	}
	if err := commitFile("missing-source", filepath.Join(t.TempDir(), "dest"), false); err == nil {
		t.Fatal("link missing")
	}
	if err := replaceFile("missing-source", filepath.Join(t.TempDir(), "dest")); err == nil {
		t.Fatal("rename missing")
	}
	if directorySyncSupported {
		if err := syncDir(filepath.Join(t.TempDir(), "missing")); err == nil {
			t.Fatal("sync missing")
		}
	}
	if err := validateBasePermissions(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("permission missing")
	}
	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, nil, 0600)
	if err := validateBasePermissions(file); err == nil {
		t.Fatal("permission file")
	}
}
func TestPublishPrecommitRecheck(t *testing.T) {
	s := newTestStore(t)
	dir := t.TempDir()
	target := filepath.Join(dir, "dest")
	_, err := s.publish(context.Background(), target, "stage-", true, func(f *os.File) (ObjectInfo, error) {
		os.Mkdir(target, 0700)
		_, e := f.Write([]byte("payload"))
		return ObjectInfo{}, e
	})
	if !errors.Is(err, ErrNotRegularFile) {
		t.Fatal(err)
	}
	// A vanished parent fails before any publication.
	_, err = s.publish(context.Background(), filepath.Join(dir, "missing", "out"), "stage-", true, func(*os.File) (ObjectInfo, error) { t.Fatal("called writer"); return ObjectInfo{}, nil })
	if err == nil {
		t.Fatal("missing parent")
	}
}
func TestDurabilityPolicy(t *testing.T) {
	_, err := NewWithOptions(filepath.Join(t.TempDir(), "store"), Options{RequireDurability: true})
	if directorySyncSupported && err != nil {
		t.Fatal(err)
	}
	if !directorySyncSupported && err == nil {
		t.Fatal("unsupported durability accepted")
	}
}
