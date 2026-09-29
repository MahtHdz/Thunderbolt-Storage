package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	objectsDirName = "objects"
	locksDirName   = "locks"
	tempPrefix     = ".storage-tmp-"
)

// Options is immutable after construction. Zero MaxObjectBytes means unlimited.
type Options struct {
	MaxObjectBytes    int64
	RequireDurability bool
}

// Per-store filesystem boundaries allow fault injection without global hooks.
type fileOps struct {
	syncDir  func(string) error
	syncFile func(*os.File) error
	commit   func(string, string, bool) error
}

type Store struct {
	baseDir, objectsDir, locksDir string
	options                       Options
	ops                           fileOps
}

func New(baseDir string) (*Store, error) { return NewWithOptions(baseDir, Options{}) }
func NewWithOptions(baseDir string, options Options) (*Store, error) {
	if strings.TrimSpace(baseDir) == "" {
		return nil, fmt.Errorf("storage base directory is empty")
	}
	if options.RequireDurability && !directorySyncSupported {
		return nil, fmt.Errorf("directory durability barriers are unsupported on this platform")
	}
	if options.MaxObjectBytes < 0 {
		return nil, fmt.Errorf("max object bytes must not be negative")
	}
	abs, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, err
	}
	if err := durableMkdirAll(abs, syncDir); err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	if err := validateBasePermissions(canonical); err != nil {
		return nil, err
	}
	s := &Store{baseDir: canonical, objectsDir: filepath.Join(canonical, objectsDirName), locksDir: filepath.Join(canonical, locksDirName), options: options,
		ops: fileOps{syncDir: syncDir, syncFile: (*os.File).Sync, commit: commitFile}}
	for _, dir := range []string{s.objectsDir, s.locksDir} {
		if err := s.ensureInternalDir(dir); err != nil {
			return nil, err
		}
	}
	return s, nil
}
func (s *Store) BaseDir() string { return s.baseDir }

// CalculateObjectPath returns the internal sharded container path for an object
// ID under the specified store base directory without inspecting or modifying
// the filesystem.
func CalculateObjectPath(baseDir, id string) (string, error) {
	if strings.TrimSpace(baseDir) == "" {
		return "", fmt.Errorf("storage base directory is empty")
	}
	abs, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}
	if st, err := os.Stat(abs); err == nil && !st.IsDir() {
		return "", fmt.Errorf("storage base path %q is not a directory", baseDir)
	}
	h, err := keyHash(id)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	return filepath.Join(abs, objectsDirName, h[:2], h[2:4], h), nil
}
func ValidateID(id string) error {
	if id == "" || len(id) > MaxIDBytes || strings.IndexByte(id, 0) >= 0 {
		return fmt.Errorf("%w: must contain 1-%d bytes and no NUL", ErrInvalidID, MaxIDBytes)
	}
	return nil
}
func keyHash(id string) (string, error) {
	if err := ValidateID(id); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:]), nil
}
func (s *Store) ObjectPath(id string) (string, error) {
	h, err := keyHash(id)
	if err != nil {
		return "", err
	}
	return s.objectPathFromHash(h), nil
}
func (s *Store) objectPathFromHash(h string) string {
	return filepath.Join(s.objectsDir, h[:2], h[2:4], h)
}
func (s *Store) lockPathFromHash(h string) string {
	return filepath.Join(s.locksDir, h[:2], h[2:3]+".lock")
}

// The store and its parents must not be concurrently modified by outsiders.
// Reject preexisting internal symlinks instead of silently traversing them.
func (s *Store) ensureInternalDir(dir string) error {
	rel, err := filepath.Rel(s.baseDir, dir)
	if err != nil {
		return err
	}
	current := s.baseDir
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		st, err := os.Lstat(current)
		if err == nil && (!st.IsDir() || st.Mode()&os.ModeSymlink != 0) {
			return fmt.Errorf("invalid internal directory %q", current)
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return durableMkdirTo(dir, s.baseDir, s.ops.syncDir)
}

func (s *Store) PutFile(ctx context.Context, id, sourcePath string, mode WriteMode) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if err := ValidateID(id); err != nil {
		return ObjectInfo{}, err
	}
	src, err := openRegular(sourcePath)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("open source %q: %w", sourcePath, err)
	}
	defer src.Close()
	return s.Put(ctx, id, src, mode)
}

// Put cannot interrupt an arbitrary Reader's blocked Read. Callers must use
// readers whose own cancellation/deadlines unblock I/O. No goroutines are leaked.
func (s *Store) Put(ctx context.Context, id string, src io.Reader, mode WriteMode) (ObjectInfo, error) {
	h, err := keyHash(id)
	if err != nil {
		return ObjectInfo{}, err
	}
	if src == nil {
		return ObjectInfo{}, fmt.Errorf("source reader is nil")
	}
	if mode > UpdateOnly {
		return ObjectInfo{}, fmt.Errorf("invalid write mode %d", mode)
	}
	unlock, err := s.acquireObjectLock(ctx, h)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer unlock()
	target := s.objectPathFromHash(h)
	if err := s.ensureInternalDir(filepath.Dir(target)); err != nil {
		return ObjectInfo{}, err
	}
	exists, err := regularFileExists(target)
	if err != nil {
		return ObjectInfo{}, err
	}
	if mode == CreateOnly && exists {
		return ObjectInfo{}, fmt.Errorf("%w: %q", ErrAlreadyExists, id)
	}
	if mode == UpdateOnly && !exists {
		return ObjectInfo{}, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return s.writeObject(ctx, id, h, target, src, mode != CreateOnly)
}

func (s *Store) writeObject(ctx context.Context, id, h, target string, src io.Reader, overwrite bool) (ObjectInfo, error) {
	return s.publish(ctx, target, tempPrefix+h+"-", overwrite, func(f *os.File) (ObjectInfo, error) {
		if _, err := f.Write(make([]byte, headerSize)); err != nil {
			return ObjectInfo{}, err
		}
		hasher := sha256.New()
		reader := src
		if s.options.MaxObjectBytes > 0 {
			reader = io.LimitReader(src, s.options.MaxObjectBytes)
		}
		n, err := copyWithContext(ctx, io.MultiWriter(f, hasher), reader)
		if err != nil {
			return ObjectInfo{}, err
		}
		if s.options.MaxObjectBytes > 0 && n == s.options.MaxObjectBytes {
			var extra [1]byte
			count, e := io.ReadFull(&contextReader{ctx: ctx, r: src}, extra[:])
			if count != 0 {
				return ObjectInfo{}, ErrTooLarge
			}
			if e != nil && !errors.Is(e, io.EOF) {
				return ObjectInfo{}, e
			}
		}
		if err := writeHeader(f, id, n, hasher.Sum(nil)); err != nil {
			return ObjectInfo{}, err
		}
		return ObjectInfo{ID: id, Size: n, SHA256: hex.EncodeToString(hasher.Sum(nil)), Verified: true}, nil
	})
}

func commitFile(temp, target string, overwrite bool) error {
	if overwrite {
		return replaceFile(temp, target)
	}
	if err := commitNoReplace(temp, target); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%w: %s", ErrAlreadyExists, target)
		}
		return err
	}
	return nil
}

func (s *Store) publish(ctx context.Context, target, prefix string, overwrite bool, write func(*os.File) (ObjectInfo, error)) (ObjectInfo, error) {
	f, err := os.CreateTemp(filepath.Dir(target), prefix)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	info, err := write(f)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if err := s.ops.syncFile(f); err != nil {
		return ObjectInfo{}, err
	}
	st, err := f.Stat()
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.ModTime.IsZero() {
		info.ModTime = st.ModTime()
	}
	if err := f.Close(); err != nil {
		return ObjectInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	// Recheck immediately before committing. Parents must remain trusted.
	if _, err := regularFileExists(target); err != nil {
		return ObjectInfo{}, err
	}
	if err := s.ops.commit(f.Name(), target, overwrite); err != nil {
		return ObjectInfo{}, err
	}
	if err := s.ops.syncDir(filepath.Dir(target)); err != nil {
		return info, &CommitError{Path: target, Err: err}
	}
	return info, nil
}

func (s *Store) openObject(id, hash string) (*os.File, objectHeader, error) {
	path := s.objectPathFromHash(hash)
	// Validate existing internal parents without creating missing objects.
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotFound
		}
		return nil, objectHeader{}, err
	}
	if parent != filepath.Dir(path) {
		return nil, objectHeader{}, fmt.Errorf("internal directory contains symlink")
	}
	f, err := openRegular(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotFound
		}
		return nil, objectHeader{}, err
	}
	header, err := readHeader(f, id)
	if err != nil {
		f.Close()
		return nil, header, err
	}
	return f, header, nil
}

func (s *Store) GetFile(ctx context.Context, id, destinationPath string, overwrite bool) (ObjectInfo, error) {
	h, err := keyHash(id)
	if err != nil {
		return ObjectInfo{}, err
	}
	dest, err := s.destination(destinationPath)
	if err != nil {
		return ObjectInfo{}, err
	}
	unlock, err := s.acquireObjectReadLock(ctx, h)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer unlock()
	src, header, err := s.openObject(id, h)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer src.Close()
	if err := durableMkdirAll(filepath.Dir(dest), s.ops.syncDir); err != nil {
		return ObjectInfo{}, err
	}
	return s.publish(ctx, dest, s.downloadPrefix()+h+"-", overwrite, func(f *os.File) (ObjectInfo, error) {
		hasher := sha256.New()
		n, err := copyWithContext(ctx, io.MultiWriter(f, hasher), src)
		if err != nil {
			return ObjectInfo{}, err
		}
		if err := header.verify(n, hasher.Sum(nil)); err != nil {
			return ObjectInfo{}, err
		}
		st, err := src.Stat()
		if err != nil {
			return ObjectInfo{}, err
		}
		return ObjectInfo{ID: id, Size: n, SHA256: header.checksumString(), Verified: true, ModTime: st.ModTime()}, nil
	})
}

func (s *Store) Delete(ctx context.Context, id string, missingOK bool) error {
	h, err := keyHash(id)
	if err != nil {
		return err
	}
	unlock, err := s.acquireObjectLock(ctx, h)
	if err != nil {
		return err
	}
	defer unlock()
	target := s.objectPathFromHash(h)
	if err := s.ensureInternalDir(filepath.Dir(target)); err != nil {
		return err
	}
	exists, err := regularFileExists(target)
	if err != nil {
		return err
	}
	if !exists {
		if missingOK {
			return nil
		}
		return ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(target); err != nil {
		return err
	}
	if err := s.ops.syncDir(filepath.Dir(target)); err != nil {
		return &CommitError{Path: target, Err: err}
	}
	return nil
}

func (s *Store) Stat(ctx context.Context, id string, checksum bool) (ObjectInfo, error) {
	h, err := keyHash(id)
	if err != nil {
		return ObjectInfo{}, err
	}
	unlock, err := s.acquireObjectReadLock(ctx, h)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer unlock()
	f, header, err := s.openObject(id, h)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return ObjectInfo{}, err
	}
	info := ObjectInfo{ID: id, Size: header.size, SHA256: header.checksumString(), ModTime: st.ModTime()}
	if checksum {
		hasher := sha256.New()
		n, err := copyWithContext(ctx, hasher, f)
		if err != nil {
			return ObjectInfo{}, err
		}
		if err := header.verify(n, hasher.Sum(nil)); err != nil {
			return ObjectInfo{}, err
		}
		info.Verified = true
	}
	return info, nil
}

func regularFileExists(path string) (bool, error) {
	st, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() {
		return false, fmt.Errorf("%w: %q", ErrNotRegularFile, path)
	}
	return true, nil
}
func validateRegularHandle(f *os.File) (*os.File, error) {
	st, err := f.Stat()
	if err == nil && !st.Mode().IsRegular() {
		err = fmt.Errorf("%w: %q", ErrNotRegularFile, f.Name())
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

const copyBufferSize = 1024 * 1024

var copyBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, copyBufferSize)
		return &b
	},
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(bufPtr)
	return io.CopyBuffer(dst, &contextReader{ctx: ctx, r: src}, *bufPtr)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
