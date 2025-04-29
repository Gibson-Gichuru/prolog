package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

type origiinReader struct {
	*store
	off int64
}

// NewLog returns a new Log with the given directory and config.
// It will create a new segment if none exists, and set up the log ready for use.
// If the config.MaxStoreBytes or config.MaxIndexBytes are zero then they will
// be set to 1024.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

// setup creates new segments up to the last one existing in the directory. If there are no segments, it creates a new one at the initial offset.
func (l *Log) setup() error {

	files, err := os.ReadDir(l.Dir)

	if err != nil {
		return err
	}

	var baseOffsets []uint64

	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		off, _ := strconv.ParseUint(offStr, 10, 0)

		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}

	if l.segments == nil {
		if err := l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

// Append adds a new record to the current segment. If the segment is at maximum
// capacity, it will create a new one at the next offset. It returns the offset of
// the appended record and any error encountered.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)

	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, err
}

// Read retrieves a record from the log at the given offset. It
// returns an error if the offset is out of bounds or if there is an
// error reading from the store or index.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment

	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, api.ErrorOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

// Close closes all segments in the log. It is safe to call multiple times.
// It returns any error encountered during the close operation.
func (l *Log) Close() error {

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove removes all the files associated with the log from disk. It first calls Close
// to ensure that all in-memory data is flushed to disk. It returns any error encountered
// during the removal process.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

// Reset reinitializes the log by closing all current segments and setting up new segments.
// It ensures that all in-memory data is flushed to disk before reinitialization.
// It returns any error encountered during the close or setup operation.
func (l *Log) Reset() error {
	if err := l.Close(); err != nil {
		return err
	}

	return l.setup()
}

// LowestOffset returns the lowest offset in the log. If the log is empty, it
// returns 0 and nil.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the highest offset in the log. If the log is empty, it
// returns 0 and nil.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments)-1].nextOffset

	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// Truncate removes all segments that have an offset lower than the given lowest.
// It then sets the log's segments to the remaining segments.
// It returns any error encountered during the removal process.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment

	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {

	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))

	for i, s := range l.segments {
		readers[i] = &origiinReader{
			store: s.store,
			off:   0,
		}
	}

	return io.MultiReader(readers...)
}

// Read reads up to len(p) bytes from the log starting at the current offset
// and stores them in p. It returns the number of bytes read (0 <= n <= len(p))
// and any error encountered during the read operation. The current offset is
// incremented by the number of bytes read. If the end of the log is reached,
// Read returns io.EOF.
func (o *origiinReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// newSegment creates a new segment for the log starting at the given offset.
// It initializes the segment with the log's directory and configuration, and
// appends it to the log's list of segments. It also sets the newly created
// segment as the active segment. Returns an error if the segment creation fails.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)

	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}
