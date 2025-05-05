package log

import (
	"context"
	"sync"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

// init initializes the replicator's logger and server map if they are nil.
// It also initializes the close channel if it is nil.
func (r *Replicator) init() {

	if r.logger == nil {
		r.logger = zap.L().Named("Replicator")
	}

	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}

	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Join adds a server to the replicator's list of servers and starts a 
// replication process to the given address. It initializes the replicator 
// if not already initialized and checks if the replicator is closed or 
// if the server is already present. If neither condition is true, it 
// creates a new channel for the server and starts a goroutine to handle 
// replication. Returns nil if the replicator is closed or if the server 
// is already in the list.
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()

	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		return nil
	}

	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])

	return nil
}

// Leave removes a server from the replicator's list of servers and closes the
// channel used to signal that the server should stop replicating. It returns
// an error if the server is not found in the list of servers. The function is
// safe to call multiple times and returns nil after successfully removing the
// server.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.servers[name]; !ok {
		return nil
	}

	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// Close shuts down the replicator, marking it as closed and closing the
// channel used for signaling. It ensures that the replicator is not
// already closed before proceeding. The function is safe to call
// multiple times and returns nil after successfully closing.
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}

	r.closed = true

	close(r.close)

	return nil
}

// replicate establishes a gRPC client connection to the given address and
// starts a consume stream to receive log records. It listens for log records
// and produces them to the local server. The function continues running until
// the replicator is closed or the leave channel is signaled. Any errors
// encountered during the process are logged with the provided address.
func (r *Replicator) replicate(addr string, leave chan struct{}) {

	cc, err := grpc.NewClient(addr, r.DialOptions...)

	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}

	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()

	stream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)

	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)

	go func() {
		for {
			recv, err := stream.Recv()

			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}

			records <- recv.Record
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return

		case record := <-records:

			_, err := r.LocalServer.Produce(
				ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)

			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}

}

// logError logs the given error at error level with the given message and the
// name and rpc address of the given member.
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
