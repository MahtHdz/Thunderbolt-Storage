package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type crashReader struct {
	ready string
	first bool
}

func (r *crashReader) Read(p []byte) (int, error) {
	if !r.first {
		r.first = true
		return copy(p, "partial replacement"), nil
	}
	os.WriteFile(r.ready, []byte("ready"), 0600)
	for {
		time.Sleep(time.Hour)
	}
}

// Invoked only by subprocess tests. The parent controls lifetime and store.
func TestStorageProcess(t *testing.T) {
	mode := os.Getenv("THUNDERBOLT_TEST_PROCESS")
	if mode == "" {
		return
	}
	s, err := New(os.Getenv("THUNDERBOLT_TEST_STORE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	switch mode {
	case "crash":
		_, err = s.Put(context.Background(), "id", &crashReader{ready: os.Getenv("THUNDERBOLT_TEST_READY")}, Upsert)
	case "create":
		_, err = s.Put(context.Background(), "id", strings.NewReader("created by child"), CreateOnly)
	}
	if errors.Is(err, ErrAlreadyExists) {
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	os.Exit(0)
}
func storageChild(t *testing.T, s *Store, mode, ready string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStorageProcess$")
	cmd.Env = append(os.Environ(), "THUNDERBOLT_TEST_PROCESS="+mode, "THUNDERBOLT_TEST_STORE="+s.BaseDir(), "THUNDERBOLT_TEST_READY="+ready)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}
func TestKilledWriterRecovery(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "id", "previous committed value")
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := storageChild(t, s, "crash", ready)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not start staging")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A real process holds the lock; cancellation must bound waiting.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := s.Stat(ctx, "id", true)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock wait: %v", err)
	}
	r, err := s.CleanupTemps(context.Background(), time.Nanosecond)
	if err != nil || r.Removed != 0 || r.Skipped == 0 {
		t.Fatalf("cleanup active writer: %+v %v", r, err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	requirePayload(t, s, "id", "previous committed value")
	r, err = s.CleanupTemps(context.Background(), time.Nanosecond)
	if err != nil || r.Removed != 1 {
		t.Fatalf("cleanup killed writer: %+v %v", r, err)
	}
	mustPut(t, s, "id", "recovered")
	requirePayload(t, s, "id", "recovered")
}
func TestCrossProcessCreateOnly(t *testing.T) {
	s := newTestStore(t)
	cmds := []*exec.Cmd{storageChild(t, s, "create", ""), storageChild(t, s, "create", "")}
	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
	}
	success, exists := 0, 0
	for _, cmd := range cmds {
		err := cmd.Wait()
		if err == nil {
			success++
		} else if cmd.ProcessState.ExitCode() == 2 {
			exists++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || exists != 1 {
		t.Fatalf("success=%d exists=%d", success, exists)
	}
	requirePayload(t, s, "id", "created by child")
}
func TestReadLocksAndCancellation(t *testing.T) {
	s := newTestStore(t)
	h, _ := keyHash("id")
	ctx := context.Background()
	first, err := s.acquireObjectReadLock(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	second, err := s.acquireObjectReadLock(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	second()
	if unlock, locked, err := s.tryAcquireObjectLock(h); err != nil || locked {
		if locked {
			unlock()
		}
		t.Fatalf("%v %v", locked, err)
	}
	timeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := s.acquireObjectLock(timeout, h); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
