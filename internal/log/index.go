package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	endWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates a new index from a given file, using the current
// size of the file as the index's size. It truncates the file to the
// given maximum index size, maps it into memory, and returns a pointer
// to the new index and an error, if any.
func newIndex(file *os.File, c Config) (*index, error) {
	idx := &index{
		file: file,
	}

	fi, err := os.Stat(file.Name())

	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	if err = os.Truncate(
		file.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Close flushes the index's memory map, synchronizes the underlying file,
// truncates it to the correct size, and closes it. It is safe to call
// multiple times. It returns any error encountered during the close
// operation.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// Read retrieves the entry's offset and position from the index at the given entry number `in`.
// If `in` is -1, it returns the last entry's offset and position. It returns an error if the
// index is empty or if the entry number is out of bounds.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {

	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / endWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * endWidth

	if i.size < pos+endWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+endWidth])
	return out, pos, nil
}

// Write appends the given `off` and `pos` to the index.
//
// It returns an error if the index is full.
func (i *index) Write(off uint32, pos uint64) error {

	if uint64(len(i.mmap)) < i.size+endWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+endWidth], pos)
	i.size += endWidth
	return nil
}

// Name returns the name of the file associated with the index.
func (i *index) Name() string {
	return i.file.Name()
}
