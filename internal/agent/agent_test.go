package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/Gibson-Gichuru/prolog/api/v1"
	"github.com/Gibson-Gichuru/prolog/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	serverConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			Server:        true,
			ServerAddress: "127.0.0.1",
		},
	)

	require.NoError(t, err)

	peerConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootCLientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: "127.0.0.1",
		},
	)

	require.NoError(t, err)

	var agents []*Agent

	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAdd := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string

		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := New(
			Config{
				NodeName:        fmt.Sprintf("%d", i),
				StartJoinAddrs:  startJoinAddrs,
				BindAddr:        bindAdd,
				RPCPort:         rpcPort,
				DataDir:         dataDir,
				ServerTLSConfig: serverConfig,
				PeerTLSConfig:   peerConfig,
				ACLModelFile:    config.ACLModelFile,
				ACLPolicyFile:   config.ACLPolicyFile,
			},
		)

		require.NoError(t, err)

		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()

	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerConfig)

	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)

	require.NoError(t, err)

	consumerReponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)

	require.NoError(t, err)
	require.Equal(t, consumerReponse.Record.Value, []byte("hello world"))

	time.Sleep(3 * time.Second)
	followerClient := client(t, agents[1], peerConfig)

	consumerReponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)

	require.NoError(t, err)
	require.Equal(t, consumerReponse.Record.Value, []byte("hello world"))
}

func client(t *testing.T, agent *Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.NewClient(
		rpcAddr,
		opts...,
	)

	require.NoError(t, err)
	client := api.NewLogClient(conn)

	return client
}
