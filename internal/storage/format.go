package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

const headerSize = 80
const objectMagic = "TBSTORE1"

type objectHeader struct {
	size     int64
	checksum [32]byte
}

func writeHeader(f *os.File, id string, size int64, checksum []byte) error {
	var b [headerSize]byte
	copy(b[:8], objectMagic)
	binary.BigEndian.PutUint64(b[8:16], uint64(size))
	copy(b[16:48], checksum)
	key := sha256.Sum256([]byte(id))
	copy(b[48:], key[:])
	_, err := f.WriteAt(b[:], 0)
	return err
}

func readHeader(f *os.File, id string) (objectHeader, error) {
	var h objectHeader
	var b [headerSize]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return h, fmt.Errorf("%w: missing/truncated header: %v", ErrIntegrity, err)
	}
	key := sha256.Sum256([]byte(id))
	if string(b[:8]) != objectMagic || !bytes.Equal(b[48:], key[:]) {
		return h, fmt.Errorf("%w: invalid format or object ID (legacy raw files require migrate)", ErrIntegrity)
	}
	h.size = int64(binary.BigEndian.Uint64(b[8:16]))
	copy(h.checksum[:], b[16:48])
	st, err := f.Stat()
	if err != nil {
		return h, err
	}
	if h.size < 0 || st.Size() < headerSize || h.size != st.Size()-headerSize {
		return h, fmt.Errorf("%w: payload length mismatch", ErrIntegrity)
	}
	return h, nil
}

func (h objectHeader) verify(size int64, sum []byte) error {
	if size != h.size || !bytes.Equal(sum, h.checksum[:]) {
		return fmt.Errorf("%w: payload checksum/size mismatch", ErrIntegrity)
	}
	return nil
}
func (h objectHeader) checksumString() string { return hex.EncodeToString(h.checksum[:]) }
