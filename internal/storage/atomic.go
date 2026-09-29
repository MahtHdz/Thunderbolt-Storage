package storage

import "os"

// commitNoReplace publishes a fully-written same-directory temporary file
// without overwriting an existing target. It uses a hard link so existence
// checking and publication are one filesystem operation. The store therefore
// expects a local filesystem that supports hard links for create-only writes.
func commitNoReplace(tempPath, targetPath string) error {
	if err := os.Link(tempPath, targetPath); err != nil {
		return err
	}
	// Publication already succeeded. Failure to remove the temporary hard-link
	// name must not be reported as a failed create; a later cleanup pass can
	// reclaim the extra name.
	_ = os.Remove(tempPath)
	return nil
}
