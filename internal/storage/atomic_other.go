//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly && !windows

package storage

import "os"

const directorySyncSupported = false

func replaceFile(tempPath, targetPath string) error {
	return os.Rename(tempPath, targetPath)
}

func syncDir(string) error { return nil }
