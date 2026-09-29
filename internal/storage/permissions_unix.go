//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

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
	if st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: %q has mode %04o; expected owner-only access such as 0700", ErrInsecurePermissions, path, st.Mode().Perm())
	}
	return nil
}
