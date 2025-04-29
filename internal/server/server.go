package server

import (
	"context"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// newgrpcServer returns a new gRPC server that wraps the given CommitLog.
// It wraps the CommitLog in a gRPC server that implements the
// Produce RPC method of the Log service.
func newgrpcServer(Config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: Config,
	}
	return srv, nil
}

// Produce appends a record to the log and returns the offset.
// It returns an error if it cannot append the record.
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{
		Offset: offset,
	}, nil
}

// Consume retrieves a record from the log at the specified offset
// provided in the ConsumeRequest. It returns a ConsumeResponse
// containing the record, or an error if the record cannot be read.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{
		Record: record,
	}, nil
}

// ProduceStream streams records to the log. It takes a stream of ProduceRequest
// messages and for each message appends the given record to the log and sends
// a ProduceResponse message containing the offset. It returns an error if it
// cannot append the record or if the stream is closed.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {

	for {
		req, err := stream.Recv()

		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)

		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream streams records from the log starting at the given offset.
// It takes a ConsumeRequest and a stream, and continuously sends ConsumeResponse
// messages back to the client for each record read from the log. The stream
// terminates when the context is done or an error occurs while reading or sending
// records. If the offset is out of range, it will continue to attempt to read the
// next available record.
func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil

		default:
			res, err := s.Consume(stream.Context(), req)

			switch err.(type) {
			case nil:
			case api.ErrorOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// NewGRPCServer returns a new gRPC server that wraps the given CommitLog.
// It registers the server with the gRPC API and returns the gRPC server and
// an error if any.
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()

	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)

	return gsrv, nil
}
