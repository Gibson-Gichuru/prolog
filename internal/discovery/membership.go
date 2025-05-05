package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type MemberShip struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// New returns a new MemberShip for the given handler and config. It sets up a
// Serf instance with the given config and begins listening for events on the
// event channel. It also joins the cluster if a list of addresses is given.
func New(hander Handler, config Config) (*MemberShip, error) {

	c := &MemberShip{
		Config:  config,
		handler: hander,
		logger:  zap.L().Named("membership"),
	}

	if err := c.setupSerf(); err != nil {
		return nil, err
	}

	return c, nil
}

// Members returns a slice of serf.Member representing the current
// members of the cluster. It queries the underlying Serf instance
// to retrieve the list of all members currently part of the cluster.
func (m *MemberShip) Members() []serf.Member {
	return m.serf.Members()
}

// Leave gracefully exits the current node from the cluster.
// It delegates the leave operation to the underlying Serf instance.
// Returns any error encountered during the leave process.
func (m *MemberShip) Leave() error {
	return m.serf.Leave()
}

// setupSerf sets up a Serf instance, starts it, and begins listening for events on the event channel.
// It binds to the address given in the configuration and tags the node with the given tags.
// It also joins the cluster if a list of addresses is given.
func (m *MemberShip) setupSerf() (err error) {

	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)

	if err != nil {
		return err
	}

	config := serf.DefaultConfig()

	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	m.events = make(chan serf.Event)

	config.EventCh = m.events

	config.Tags = m.Tags

	config.NodeName = m.NodeName
	m.serf, err = serf.Create(config)

	if err != nil {
		return err
	}

	go m.eventHandler()

	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)

		if err != nil {
			return err
		}
	}

	return
}

// eventHandler listens for events on the serf event channel and calls the
// corresponding handler methods when a member joins or leaves the cluster.
func (m *MemberShip) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}

				m.handleJoin(member)
			}

		case serf.EventMemberLeave, serf.EventMemberFailed:

			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}

				m.handleLeave(member)
			}
		}

	}
}

// isLocal returns true if the given member is the local node, false otherwise.
func (m *MemberShip) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// handleJoin calls the Join method of the handler for the given member. If the
// handler returns an error, it is logged at error level with the given message
// and the name and rpc address of the given member.
func (m *MemberShip) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rcp_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

// handleLeave calls the Leave method of the handler for the given member.
// If the handler returns an error, it is logged at error level with the given
// message and the name and rpc address of the given member.
func (m *MemberShip) handleLeave(member serf.Member) {

	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// logError logs the given error at error level with the given message and the
// name and rpc address of the given member.
func (m *MemberShip) logError(err error, msg string, member serf.Member) {

	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
