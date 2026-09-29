//go:build windows

package storage

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

const moveFileWriteThrough = 0x00000008

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procReplaceFileW = kernel32.NewProc("ReplaceFileW")
	procMoveFileExW  = kernel32.NewProc("MoveFileExW")
)

const directorySyncSupported = false

func replaceFile(tempPath, targetPath string) error {
	_, statErr := os.Stat(targetPath)
	if statErr == nil {
		return replaceExistingFileWindows(tempPath, targetPath)
	}
	if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	return moveNewFileWindows(tempPath, targetPath)
}

func replaceExistingFileWindows(tempPath, targetPath string) error {
	target, err := syscall.UTF16PtrFromString(targetPath)
	if err != nil {
		return err
	}
	temp, err := syscall.UTF16PtrFromString(tempPath)
	if err != nil {
		return err
	}

	r1, _, callErr := procReplaceFileW.Call(
		uintptr(unsafe.Pointer(target)),
		uintptr(unsafe.Pointer(temp)),
		0,
		0,
		0,
		0,
	)
	if r1 == 0 {
		if callErr != syscall.Errno(0) {
			return callErr
		}
		return syscall.EINVAL
	}
	return nil
}

func moveNewFileWindows(tempPath, targetPath string) error {
	temp, err := syscall.UTF16PtrFromString(tempPath)
	if err != nil {
		return err
	}
	target, err := syscall.UTF16PtrFromString(targetPath)
	if err != nil {
		return err
	}

	r1, _, callErr := procMoveFileExW.Call(
		uintptr(unsafe.Pointer(temp)),
		uintptr(unsafe.Pointer(target)),
		uintptr(moveFileWriteThrough),
	)
	if r1 == 0 {
		if callErr != syscall.Errno(0) {
			return callErr
		}
		return syscall.EINVAL
	}
	return nil
}

// Windows does not provide a directory fsync equivalent through os.File.
// File data is flushed before replacement; MoveFileEx uses WRITE_THROUGH when
// publishing a previously-nonexistent destination.
func syncDir(string) error { return nil }
