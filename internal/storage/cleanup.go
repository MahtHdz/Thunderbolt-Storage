package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (s *Store) downloadPrefix() string {
	sum := sha256.Sum256([]byte(s.baseDir))
	return tempPrefix + "download-" + hex.EncodeToString(sum[:]) + "-"
}

func (s *Store) CleanupTemps(ctx context.Context, olderThan time.Duration) (CleanupReport, error) {
	return s.cleanup(ctx, s.objectsDir, tempPrefix, olderThan, true)
}

// CleanupDownloads scans only the specified directory and only this store's
// download fragments. The object's read lock protects an active download.
func (s *Store) CleanupDownloads(ctx context.Context, directory string, olderThan time.Duration) (CleanupReport, error) {
	if strings.TrimSpace(directory) == "" {
		return CleanupReport{}, fmt.Errorf("download directory is empty")
	}
	dir, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return CleanupReport{}, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return CleanupReport{}, err
	}
	return s.cleanup(ctx, dir, s.downloadPrefix(), olderThan, false)
}

func (s *Store) cleanup(ctx context.Context, root, prefix string, age time.Duration, recursive bool) (CleanupReport, error) {
	report := CleanupReport{}
	if age <= 0 {
		return report, fmt.Errorf("older-than must be greater than zero")
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	cutoff := time.Now().Add(-age)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if os.IsNotExist(walkErr) && path != root {
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			if !recursive && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasPrefix(d.Name(), prefix) {
			return nil
		}
		report.Scanned++
		hash, ok := hashFromTempName(tempPrefix + strings.TrimPrefix(d.Name(), prefix))
		if !ok || (recursive && filepath.Dir(path) != filepath.Dir(s.objectPathFromHash(hash))) {
			report.Skipped++
			return nil
		}
		unlock, locked, err := s.tryAcquireObjectLock(hash)
		if err != nil {
			return err
		}
		if !locked {
			report.Skipped++
			return nil
		}
		defer unlock()
		// Re-stat under the lock: the producer may have finished since WalkDir.
		st, err := os.Lstat(path)
		if os.IsNotExist(err) {
			report.Skipped++
			return nil
		}
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() || st.ModTime().After(cutoff) {
			report.Skipped++
			return nil
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		report.Removed++
		if err := s.ops.syncDir(filepath.Dir(path)); err != nil {
			return &CommitError{Path: path, Err: err}
		}
		return nil
	})
	return report, err
}

func hashFromTempName(name string) (string, bool) {
	if !strings.HasPrefix(name, tempPrefix) {
		return "", false
	}
	rest := strings.TrimPrefix(name, tempPrefix)
	if len(rest) < 66 || rest[64] != '-' {
		return "", false
	}
	h := rest[:64]
	if h != strings.ToLower(h) {
		return "", false
	}
	if _, err := hex.DecodeString(h); err != nil {
		return "", false
	}
	return h, true
}
