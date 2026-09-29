//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const lockRetryInterval = 25 * time.Millisecond

func (s *Store) acquireObjectLock(ctx context.Context, hash string) (func(), error) {
	return s.acquireObjectLockMode(ctx, hash, syscall.LOCK_EX)
}

func (s *Store) acquireObjectReadLock(ctx context.Context, hash string) (func(), error) {
	return s.acquireObjectLockMode(ctx, hash, syscall.LOCK_SH)
}

func (s *Store) acquireObjectLockMode(ctx context.Context, hash string, mode int) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.openLockFile(hash)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				_ = f.Close()
			}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = f.Close()
			return nil, err
		}

		timer := time.NewTimer(lockRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			_ = f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Store) tryAcquireObjectLock(hash string) (func(), bool, error) {
	f, err := s.openLockFile(hash)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, true, nil
}

func (s *Store) openLockFile(hash string) (*os.File, error) {
	path := s.lockPathFromHash(hash)
	if err := s.ensureInternalDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("create lock shard: %w", err)
	}
	if _, err := regularFileExists(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return f, nil
}
