package storage

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound            = errors.New("object not found")
	ErrAlreadyExists       = errors.New("object already exists")
	ErrInvalidID           = errors.New("invalid object id")
	ErrInsecurePermissions = errors.New("insecure storage permissions")
	ErrNotRegularFile      = errors.New("not a regular file")
	ErrIntegrity           = errors.New("object integrity check failed")
	ErrTooLarge            = errors.New("object exceeds size limit")
	ErrUnsafeDestination   = errors.New("destination is inside the store")
	ErrCommitUncertain     = errors.New("state changed but durability is uncertain")
)

// CommitError means publication/deletion succeeded but its durability barrier
// failed. Inspect the object before retrying; a failure is not a rollback.
type CommitError struct {
	Path string
	Err  error
}

func (e *CommitError) Error() string {
	return fmt.Sprintf("%v: %q: %v", ErrCommitUncertain, e.Path, e.Err)
}
func (e *CommitError) Unwrap() []error { return []error{ErrCommitUncertain, e.Err} }
