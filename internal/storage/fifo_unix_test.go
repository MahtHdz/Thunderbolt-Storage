//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package storage

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestFIFORejectedWithoutBlocking(t *testing.T) {
	s := newTestStore(t)
	fifo := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.PutFile(context.Background(), "id", fifo, Upsert); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotRegularFile) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO open blocked")
	}
}
