//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly && !windows

package main

import "os"

func terminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
