//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package storage

import "os"

func openRegular(path string) (*os.File, error) {
	if _, err := regularFileExists(path); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return validateRegularHandle(f)
}
