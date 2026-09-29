package storage

import "time"

const MaxIDBytes = 16 * 1024

type WriteMode uint8

const (
	Upsert WriteMode = iota
	CreateOnly
	UpdateOnly
)

func (m WriteMode) String() string {
	switch m {
	case Upsert:
		return "upsert"
	case CreateOnly:
		return "create"
	case UpdateOnly:
		return "update"
	default:
		return "unknown"
	}
}

type ObjectInfo struct {
	Verified  bool      `json:"verified"`
	ID        string    `json:"id"`
	Size      int64     `json:"size"`
	SHA256    string    `json:"sha256,omitempty"`
	ModTime   time.Time `json:"modTime"`
	StorePath string    `json:"storePath,omitempty"`
}

type CleanupReport struct {
	Scanned int `json:"scanned"`
	Removed int `json:"removed"`
	Skipped int `json:"skipped"`
}
