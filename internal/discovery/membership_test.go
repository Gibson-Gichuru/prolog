package discovery

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
)

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

// Join sends a join event with the given id and addr to the joins channel if it is not nil.
// This function is part of the handler implementation for managing membership events.
func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

// Leave sends a leave event with the given id to the leaves channel if it is not nil.
// This function is part of the handler implementation for managing membership events.
func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}

	return nil
}

// TestMembership exercises the MemberShip type. It creates a cluster of 3 nodes,
// verifies that each node sees the other two, and then verifies that a node
// leaving the cluster is properly removed from the other two nodes' membership
// lists.
func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)

	m, _ = setupMember(t, m)

	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members()) &&
			0 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members()) &&
			serf.StatusLeft == m[0].Members()[2].Status &&
			1 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)
}

// setupMember returns a new MemberShip and a handler that will be passed to it.
// It also takes a slice of existing MemberShips and will have the new one join
// the cluster if not empty. It returns the new MemberShip and the handler.
func setupMember(t *testing.T, members []*MemberShip) ([]*MemberShip, *handler) {

	id := len(members)

	port, err := dynaport()

	require.NoError(t, err)

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	tags := map[string]string{
		"rpc_addr": addr,
	}

	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}

	h := &handler{}

	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}

	m, err := New(h, c)

	require.NoError(t, err)

	members = append(members, m)

	return members, h

}

// dynaport returns an available port number. It does this by listening on
// an available port and then immediately closing the listener. The port
// number is returned as the first return value, and any error encountered is
// returned as the second.
func dynaport() (int, error) {
	l, err := net.Listen("tcp", ":0")

	if err != nil {
		return 0, err
	}

	defer l.Close()

	add := l.Addr().(*net.TCPAddr)

	return add.Port, nil
}
