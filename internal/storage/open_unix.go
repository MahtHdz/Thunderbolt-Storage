//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package storage

import (
	"os"
	"syscall"
)

// O_NONBLOCK prevents a raced FIFO replacement from blocking before fstat.
// O_NOFOLLOW rejects a symlink substituted between validation and open.
func openRegular(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	return validateRegularHandle(f)
}
