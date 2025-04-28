package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}

// newSegment creates a new segment for the log, initializing its store and index.
// It opens or creates the store and index files in the specified directory,
// using the base offset for naming. The store file is opened in append mode,
// while the index file is opened for reading and writing. The segment's next
// offset is set based on the last entry in the index or defaults to the base
// offset if the index is empty. It returns a pointer to the new segment and
// an error, if any.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {

	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil

}

// Append adds a new record to the segment. It marshals the record using protobuf,
// appends it to the store, and writes the offset and position to the index.
// It returns the offset of the appended record and any error encountered.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)

	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)

	if err != nil {
		return 0, err
	}

	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++

	return cur, nil
}

// Read retrieves a record from the segment at the given offset. It
// returns an error if the offset is out of bounds or if there is an
// error reading from the store or index.
func (s *segment) Read(offset uint64) (*api.Record, error) {

	_, pos, err := s.index.Read(int64(offset - s.baseOffset))

	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)

	if err != nil {
		return nil, err
	}

	reccord := &api.Record{}

	err = proto.Unmarshal(p, reccord)

	return reccord, err
}

// IsMaxed checks if the segment is at maximum capacity. A segment is at maximum
// capacity when either the store file has reached its maximum size or the index
// has reached its maximum size.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment's store and index, then removes the files from disk.
// It returns any error encountered during the removal process.
func (s *segment) Remove() error {

	if err := s.store.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close flushes the index's memory map, synchronizes the underlying file,
// truncates it to the correct size, and closes it. It also flushes the buffer
// and closes the underlying store file. It is safe to call multiple times. It
// returns any error encountered during the close operation.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j == 0 {
		return (j / k) * k
	}

	return ((j - k + 1) / k) * k
}
