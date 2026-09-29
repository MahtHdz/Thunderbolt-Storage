package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
)

// Migrate atomically wraps a legacy raw object only when its bytes match a
// caller-supplied trusted SHA-256. Repeating an already completed migration is
// safe. Stop old binaries before migrating; they do not understand containers.
func (s *Store) Migrate(ctx context.Context, id, expectedSHA256 string) (ObjectInfo, error) {
	expected, err := hex.DecodeString(expectedSHA256)
	if err != nil || len(expected) != sha256.Size {
		return ObjectInfo{}, fmt.Errorf("expected SHA-256 must be 64 hexadecimal characters")
	}
	expectedSHA256 = hex.EncodeToString(expected)
	hash, err := keyHash(id)
	if err != nil {
		return ObjectInfo{}, err
	}
	unlock, err := s.acquireObjectLock(ctx, hash)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer unlock()
	target := s.objectPathFromHash(hash)
	if err := s.ensureInternalDir(filepath.Dir(target)); err != nil {
		return ObjectInfo{}, err
	}
	f, err := openRegular(target)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer f.Close()
	header, headerErr := readHeader(f, id)
	hasher := sha256.New()
	if headerErr == nil {
		n, err := copyWithContext(ctx, hasher, f)
		if err != nil {
			return ObjectInfo{}, err
		}
		if err := header.verify(n, hasher.Sum(nil)); err != nil {
			return ObjectInfo{}, err
		}
		if header.checksumString() != expectedSHA256 {
			return ObjectInfo{}, ErrIntegrity
		}
		st, err := f.Stat()
		if err != nil {
			return ObjectInfo{}, err
		}
		return ObjectInfo{ID: id, Size: n, SHA256: expectedSHA256, Verified: true, ModTime: st.ModTime()}, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ObjectInfo{}, err
	}
	if _, err := copyWithContext(ctx, hasher, f); err != nil {
		return ObjectInfo{}, err
	}
	if hex.EncodeToString(hasher.Sum(nil)) != expectedSHA256 {
		return ObjectInfo{}, ErrIntegrity
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ObjectInfo{}, err
	}
	// Close before publication on Windows; writeObject needs the source only
	// during its staging callback, so a reader closes it at EOF.
	return s.writeObject(ctx, id, hash, target, &closeAtEOF{ReadCloser: f}, true)
}

type closeAtEOF struct{ io.ReadCloser }

func (r *closeAtEOF) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err == io.EOF {
		if e := r.Close(); e != nil {
			return n, e
		}
	}
	return n, err
}
