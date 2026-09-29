//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package main

import (
	"os"
	"syscall"
)

func terminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
