//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly && !windows

package storage

import (
	"fmt"
	"os"
)

func validateBasePermissions(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat storage base directory %q: %w", path, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("storage base path %q is not a directory", path)
	}
	return nil
}
