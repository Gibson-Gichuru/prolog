package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type Store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a new log.Store from a given file, using the current
// size of the file as the log's size. It returns a pointer to the new
// Store and an error, if any.
func newStore(file *os.File) (*Store, error) {

	fi, err := os.Stat(file.Name())

	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &Store{
		File: file,
		size: size,
		buf:  bufio.NewWriter(file),
	}, nil
}

// Append writes the record to the log, first writing the length of the record
// encoded in `lenWidth` bytes, then the record itself. It returns the number of
// bytes written, the position of the record, and any error.
func (s *Store) Append(p []byte) (n uint64, pos uint64, err error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)

	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read retrieves a record from the log at the given position. It first reads
// the length of the record, then reads the record itself. It returns the
// record as a byte slice and any error encountered.
func (s *Store) Read(pos uint64) ([]byte, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)

	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))

	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt reads from the log at the given offset, and writes the result into
// p. It returns the number of bytes read and any error encountered.
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close flushes the buffer and closes the underlying file. It is safe to
// call multiple times. It returns any error encountered during the close
// operation.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
