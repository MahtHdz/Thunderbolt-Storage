package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CanonicalDestination resolves existing parents, including aliases above a
// not-yet-created directory. The final component is deliberately not followed.
func CanonicalDestination(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("destination path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := resolveMissing(filepath.Dir(abs))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

func resolveMissing(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	// A dangling symlink is not a missing directory we may create.
	if st, e := os.Lstat(path); e == nil && st.Mode()&os.ModeSymlink != 0 {
		return "", err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return "", err
	}
	resolved, err = resolveMissing(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, filepath.Base(path)), nil
}

// DestinationKey conservatively folds case on every platform. This also
// catches aliases on case-insensitive volumes mounted on Unix. It may reject
// distinct case-sensitive paths; safety is preferable to batch data loss.
func DestinationKey(path string) (string, error) {
	canonical, err := CanonicalDestination(path)
	return strings.ToLower(canonical), err
}

func (s *Store) destination(path string) (string, error) {
	canonical, err := CanonicalDestination(path)
	if err != nil {
		return "", err
	}
	base := strings.ToLower(filepath.Clean(s.baseDir))
	key := strings.ToLower(canonical)
	if key == base || strings.HasPrefix(key, base+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q", ErrUnsafeDestination, canonical)
	}
	if _, err := regularFileExists(canonical); err != nil {
		return "", err
	}
	return canonical, nil
}

// durableMkdirAll synchronizes each ancestor, including directories another
// process may have just created. Syncing only the leaf leaves its name unsafe.
func durableMkdirAll(path string, sync func(string) error) error {
	return durableMkdirTo(path, "", sync)
}

// stop is an already durable store root. Empty means the volume root.
func durableMkdirTo(path, stop string, sync func(string) error) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	for {
		if err := sync(canonical); err != nil {
			return fmt.Errorf("sync directory %q: %w", canonical, err)
		}
		parent := filepath.Dir(canonical)
		if canonical == stop || parent == canonical {
			return nil
		}
		canonical = parent
	}
}
