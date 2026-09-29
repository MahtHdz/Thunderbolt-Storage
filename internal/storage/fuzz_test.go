package storage

import (
	"path/filepath"
	"strings"
	"testing"
)

func FuzzObjectPath(f *testing.F) {
	for _, id := range []string{"id", "../../etc/passwd", "", "hello\x00", "日本語"} {
		f.Add(id)
	}
	f.Fuzz(func(t *testing.T, id string) {
		s := &Store{objectsDir: filepath.Join(string(filepath.Separator), "objects")}
		p, err := s.ObjectPath(id)
		if ValidateID(id) != nil {
			if err == nil {
				t.Fatal("invalid ID accepted")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(s.objectsDir, p)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Fatalf("escaped: %q", p)
		}
		if len(filepath.Base(p)) != 64 {
			t.Fatal(p)
		}
	})
}
func FuzzTempName(f *testing.F) {
	f.Add(tempPrefix + strings.Repeat("a", 64) + "-123")
	f.Add("")
	f.Fuzz(func(t *testing.T, name string) {
		if h, ok := hashFromTempName(name); ok {
			if len(h) != 64 || !strings.HasPrefix(name, tempPrefix+h+"-") {
				t.Fatal(name)
			}
		}
	})
}
