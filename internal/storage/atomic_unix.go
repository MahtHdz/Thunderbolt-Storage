//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package storage

import (
	"fmt"
	"os"
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
		return fmt.Errorf("fsync directory: %w", err)
	}
	return nil
}
