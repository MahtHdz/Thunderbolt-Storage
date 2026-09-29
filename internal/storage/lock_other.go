//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly && !windows

package storage

import (
	"context"
	"fmt"
)

func (s *Store) acquireObjectLock(context.Context, string) (func(), error) {
	return nil, fmt.Errorf("cross-process object locking is unsupported on this platform")
}

func (s *Store) acquireObjectReadLock(context.Context, string) (func(), error) {
	return nil, fmt.Errorf("cross-process object locking is unsupported on this platform")
}

func (s *Store) tryAcquireObjectLock(string) (func(), bool, error) {
	return nil, false, fmt.Errorf("cross-process object locking is unsupported on this platform")
}
