//go:build windows

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
	// Windows authorization is governed by ACLs, not POSIX mode bits.
	return nil
}
