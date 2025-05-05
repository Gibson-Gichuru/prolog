package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"github.com/Gibson-Gichuru/prolog/internal/auth"
	"github.com/Gibson-Gichuru/prolog/internal/discovery"
	"github.com/Gibson-Gichuru/prolog/internal/log"
	"github.com/Gibson-Gichuru/prolog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

// RPCAddr returns the address that the agent will expose its RPC server on, in
// the format "host:port". It returns an error if the BindAddr in the agent's
// Config is not a valid host:port address.
func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

type Agent struct {
	Config
	log        *log.Log
	server     *grpc.Server
	membership *discovery.MemberShip
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

// New returns a new Agent with the given configuration. It sets up the agent's
// logger, log, server, and membership, and returns an error if any of the
// setup steps fail.
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setups := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, setup := range setups {
		if err := setup(); err != nil {
			return nil, err
		}
	}

	return a, nil
}

// setupLogger sets up the global logger to be a development logger. It
// returns any error encountered while creating the logger.
func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()

	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)

	return nil
}

// setupLog sets up the agent's Log. It creates a new log in the DataDir
// specified in the agent's Config, using the default log config. It returns an
// error if the log cannot be created.
func (a *Agent) setupLog() error {
	var err error

	a.log, err = log.NewLog(
		a.Config.DataDir,
		log.Config{},
	)

	return err
}

// setupServer sets up the agent's gRPC server. It creates a new server with a
// configuration based on the agent's Log and ACL configuration. It then
// starts listening on the address specified in the agent's Config, and
// returns an error if any of the setup steps fail.
func (a *Agent) setupServer() error {

	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption

	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error

	a.server, err = server.NewGRPCServer(serverConfig, opts...)

	if err != nil {
		return err
	}

	rpcAddr, err := a.RPCAddr()

	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", rpcAddr)

	if err != nil {
		return err
	}

	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

// setupMembership sets up the agent's membership and replication components.
// It establishes a gRPC client connection using the provided RPC address and 
// peer TLS configuration, if available. The function creates a LogClient and 
// initializes the replicator with the dial options and local server. It also 
// configures the membership with the agent's node name, bind address, and 
// starting join addresses, returning any error encountered during the setup process.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()

	if err != nil {
		return err
	}

	var opts []grpc.DialOption

	if a.Config.PeerTLSConfig != nil {
		opts = append(
			opts,
			grpc.WithTransportCredentials(
				credentials.NewTLS(a.Config.PeerTLSConfig),
			),
		)
	}

	conn, err := grpc.NewClient(rpcAddr, opts...)

	if err != nil {
		return err
	}

	client := api.NewLogClient(conn)

	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}

	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})

	return err

}

// Shutdown shuts down the agent. It stops the replicator, leaves the cluster,
// shuts down the gRPC server, and closes the log. It returns an error if any
// of the shutdown steps fail. Shutdown is safe to call multiple times and will
// not return an error if the agent is already shut down.
func (a *Agent) Shutdown() error {

	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true

	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}
