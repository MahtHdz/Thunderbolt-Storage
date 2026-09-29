//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package storage

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

const directorySyncSupported = true

func replaceFile(tempPath, targetPath string) error {
	if err := os.Rename(tempPath, targetPath); err != nil {
		return err
	}
	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()

	if err := dir.Sync(); err != nil {
		if errors.Is(err, syscall.EROFS) || errors.Is(err, syscall.EINVAL) {
			return nil
		}
		return fmt.Errorf("fsync directory: %w", err)
	}
	return nil
}
