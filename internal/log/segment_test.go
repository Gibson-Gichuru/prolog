package log

import (
	"io"
	"os"
	"testing"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"github.com/stretchr/testify/require"
)

// TestSegment exercises the Segment type.
//
// It creates a new segment and writes some records to it. It verifies that the
// next offset is correct and that the record is successfully written and read
// back. It also verifies that maxing out the index or store causes the
// Append method to return an error. It also verifies that removing the segment
// works correctly.
func TestSegment(t *testing.T) {

	dir, _ := os.MkdirTemp("", "segment_test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}

	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = endWidth * 3

	s, err := newSegment(dir, 16, c)

	require.NoError(t, err)

	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)

		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)

	require.Equal(t, io.EOF, err)

	// maxed index

	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	require.True(t, s.IsMaxed())

	err = s.Remove()

	require.NoError(t, err)

	s, err = newSegment(dir, 16, c)

	require.NoError(t, err)

	require.False(t, s.IsMaxed())
}
