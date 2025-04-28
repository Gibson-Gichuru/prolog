package log

import (
	"io"
	"os"
	"testing"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestLog exercises the Log type, performing various tests such as
// appending and reading a record, reading a record out of bounds, initializing
// with existing segments, and truncating the log.
func TestLog(t *testing.T) {
	for scenarial, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds":  testAppendRead,
		"offset out of bounds returns error": testReadOutOfBounds,
		"init with existing segments":        testInitExisting,
		"reader":                             testReader,
		"truncate":                           testTruncate,
	} {
		t.Run(scenarial, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "log_test")

			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

// testAppendRead tests that appending a record and reading it back
// works as expected.
func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{Value: []byte("hello world")}

	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)

	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

// testReadOutOfBounds tests that reading a record out of bounds
// returns an error.
func testReadOutOfBounds(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)

}

// testInitExisting tests initializing a log with existing segments.
// It appends records to the log, closes it, and then verifies that the
// lowest and highest offsets are correct. It then reopens the log with
// the existing segments and verifies that the offsets remain correct.
func testInitExisting(t *testing.T, log *Log) {
	append := &api.Record{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	require.NoError(t, log.Close())

	off, err := log.LowestOffset()

	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

}

// testReader tests that the Reader method returns a reader that
// can be read from like a normal reader. It appends a record to the
// log, then reads from the log using the Reader method and verifies
// that the record is read back correctly.
func testReader(t *testing.T, log *Log) {
	append := &api.Record{Value: []byte("hello world")}

	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()

	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}

	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
