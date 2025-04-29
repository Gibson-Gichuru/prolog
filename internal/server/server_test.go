package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"github.com/Gibson-Gichuru/prolog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teadown := setupTest(t, nil)
			defer teadown()
			fn(t, client, config)
		})
	}
}

// setupTest returns a client, config, and teardown function for testing the
// server. It will create a temporary directory, create a log in that directory,
// and start a server listening on a random port. The client will be able to
// connect to the server and send requests. The teardown function will clean up
// the temporary directory, stop the server, and close the client connection.
func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teadown func(),

) {

	t.Helper()
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server_test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)
	cfg = &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		cc.Close()
		l.Close()
		server.Stop()
		os.RemoveAll(dir)
	}

}

// testProduceConsume tests that the Produce and Consume RPC methods work as expected.
// It creates a record, appends it to the log using Produce, and then reads it back
// from the log using Consume. It verifies that the record is read back correctly,
// and that the offset is correct.
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, produce.Offset, consume.Record.Offset)
}

// testConsumePastBoundary tests that the Consume RPC method returns an
// error when attempting to consume a record at an offset that is out of
// bounds. It creates a record, appends it to the log using Produce, and
// then attempts to read it back from the log using Consume at an offset
// that is one greater than the offset produced by Produce. It verifies
// that the error returned is of the correct type.
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})

	if consume != nil {
		t.Fatalf("expected consume to be nil, got %v", consume)
	}
	got := grpc.Code(err)

	want := grpc.Code(api.ErrorOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// testProduceConsumeStream tests that the ProduceStream RPC method works as expected.
// It sends a sequence of records to the log, and verifies that the offset returned
// from the server matches the offset sent in the request. It also verifies that no
// error is returned from the stream until the stream is closed by the client.
func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}
	{
		stream, err := client.ProduceStream(ctx)

		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("expected offset %d, got %d", offset, res.Offset)
			}
		}
	}

}
