package storage

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	for _, size := range []int{1024, 1024 * 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			s, err := New(filepath.Join(b.TempDir(), "store"))
			if err != nil {
				b.Fatal(err)
			}
			payload := bytes.Repeat([]byte("x"), size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.Put(context.Background(), "id", bytes.NewReader(payload), Upsert); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
