//go:build windows

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"
)

const (
	lockFileExclusiveLock   = 0x00000002
	lockFileFailImmediately = 0x00000001
	lockRetryInterval       = 25 * time.Millisecond
	errorLockViolation      = syscall.Errno(33)
	errorSharingViolation   = syscall.Errno(32)
)

var (
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

func (s *Store) acquireObjectLock(ctx context.Context, hash string) (func(), error) {
	return s.acquireObjectLockMode(ctx, hash, true)
}

func (s *Store) acquireObjectReadLock(ctx context.Context, hash string) (func(), error) {
	return s.acquireObjectLockMode(ctx, hash, false)
}

func (s *Store) acquireObjectLockMode(ctx context.Context, hash string, exclusive bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.openLockFile(hash, exclusive)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		ov := new(syscall.Overlapped)
		err = lockWindowsFile(f, ov, exclusive)
		if err == nil {
			return func() {
				_ = unlockWindowsFile(f, ov)
				_ = f.Close()
			}, nil
		}
		if err != errorLockViolation && err != errorSharingViolation {
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
	f, err := s.openLockFile(hash, true)
	if err != nil {
		return nil, false, err
	}
	ov := new(syscall.Overlapped)
	if err := lockWindowsFile(f, ov, true); err != nil {
		_ = f.Close()
		if err == errorLockViolation || err == errorSharingViolation {
			return nil, false, nil
		}
		return nil, false, err
	}
	return func() {
		_ = unlockWindowsFile(f, ov)
		_ = f.Close()
	}, true, nil
}

func (s *Store) openLockFile(hash string, exclusive bool) (*os.File, error) {
	path := s.lockPathFromHash(hash)
	if !exclusive {
		if f, err := os.OpenFile(path, os.O_RDONLY, 0); err == nil {
			return f, nil
		}
	}
	if err := s.ensureInternalDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("create lock shard: %w", err)
	}
	if _, err := regularFileExists(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil && !exclusive {
		return os.OpenFile(path, os.O_RDONLY, 0)
	}
	return f, err
}

func lockWindowsFile(f *os.File, ov *syscall.Overlapped, exclusive bool) error {
	flags := uintptr(lockFileFailImmediately)
	if exclusive {
		flags |= lockFileExclusiveLock
	}
	r1, _, callErr := procLockFileEx.Call(
		f.Fd(),
		flags,
		0,
		uintptr(^uint32(0)),
		uintptr(^uint32(0)),
		uintptr(unsafe.Pointer(ov)),
	)
	if r1 == 0 {
		if callErr != syscall.Errno(0) {
			return callErr
		}
		return syscall.EINVAL
	}
	return nil
}

func unlockWindowsFile(f *os.File, ov *syscall.Overlapped) error {
	r1, _, callErr := procUnlockFileEx.Call(
		f.Fd(),
		0,
		uintptr(^uint32(0)),
		uintptr(^uint32(0)),
		uintptr(unsafe.Pointer(ov)),
	)
	if r1 == 0 {
		if callErr != syscall.Errno(0) {
			return callErr
		}
		return syscall.EINVAL
	}
	return nil
}
