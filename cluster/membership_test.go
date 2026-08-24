package cluster

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestJoinAndLeaveRequireLeadership(t *testing.T) {
	h := startThreeNodeCluster(t, nil)
	leader := h.waitLeader()
	follower := harnessFollower(t, h, leader)

	require.ErrorIs(t, follower.Join("x", "x-addr", "grpc://x", false), ErrNotLeader)
	require.ErrorIs(t, follower.Leave("n1"), ErrNotLeader)
}

func TestJoinRejectsIncompleteRequests(t *testing.T) {
	h := harnessNew(t)
	leader := h.start("n1", func(c *Config) { c.Bootstrap = true })
	h.waitLeader()

	require.Error(t, leader.Join("", "addr", "", false))
	require.Error(t, leader.Join("id", "", "", false))
	require.Error(t, leader.Leave(""))
}

func TestLeaveUnknownNodeSucceeds(t *testing.T) {
	h := harnessNew(t)
	leader := h.start("n1", func(c *Config) { c.Bootstrap = true })
	h.waitLeader()

	require.NoError(t, leader.Leave("never-existed"))
	servers, err := leader.Configuration()
	require.NoError(t, err)
	require.Len(t, servers, 1)
}

func TestGRPCAddrResolution(t *testing.T) {
	h := startThreeNodeCluster(t, nil)
	leader := h.waitLeader()

	require.Equal(t, leader.grpcAddr, leader.GRPCAddrForID(leader.id))
	for _, node := range h.live() {
		require.Equal(t, node.grpcAddr, leader.GRPCAddrForID(node.id),
			"leader should resolve %s through discovery", node.id)
		require.Equal(t, leader.grpcAddr, node.LeaderGRPCAddr())
	}
	require.Empty(t, leader.GRPCAddrForID("unknown"))
	require.Empty(t, leader.GRPCAddrForID(""))
}

// Join records the gRPC address from the request so the leader can be dialled
// even for a node discovery has not gossiped.
func TestJoinRecordsGRPCAddr(t *testing.T) {
	h := harnessNew(t)
	leader := h.start("n1", func(c *Config) { c.Bootstrap = true })
	h.waitLeader()

	require.NoError(t, leader.Join("phantom", "phantom-addr", "grpc://phantom", true))
	require.Equal(t, "grpc://phantom", leader.GRPCAddrForID("phantom"))

	require.NoError(t, leader.Leave("phantom"))
	require.Empty(t, leader.GRPCAddrForID("phantom"), "a departed node's address must be forgotten")
}

// A failed-node eviction that fires while the node is alive again must be
// dropped, even if the cancel and the timer race.
func TestEvictFailedMemberSkipsRevivedNode(t *testing.T) {
	h := startThreeNodeCluster(t, nil)
	leader := h.waitLeader()
	victim := harnessFollower(t, h, leader)

	h.bus.fail(victim.id)
	harnessWaitFor(t, "the grace timer to be armed", func() bool {
		return leader.timers.armed() == 1
	})
	h.bus.revive(victim.id)

	// Fire the callback directly: liveness is re-checked at expiry, not when
	// the timer was armed.
	leader.evictFailedMember(victim.id)

	servers, err := leader.Configuration()
	require.NoError(t, err)
	_, found := findServer(servers, victim.id)
	require.True(t, found, "a revived node must survive an expiring grace timer")
}

// A negative grace period means "never evict on a suspected failure".
func TestNegativeGracePeriodDisablesFailureEviction(t *testing.T) {
	h := startThreeNodeCluster(t, func(c *Config) { c.FailureGracePeriod = -1 })
	leader := h.waitLeader()
	victim := harnessFollower(t, h, leader)

	require.NoError(t, h.stop(victim.id))
	h.bus.fail(victim.id)

	require.Never(t, func() bool { return leader.timers.armed() > 0 }, 200*time.Millisecond, 20*time.Millisecond)
	servers, err := leader.Configuration()
	require.NoError(t, err)
	require.Len(t, servers, 3)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "missing node id",
			cfg:  Config{},
		},
		{
			name: "bootstrap expect one is ambiguous",
			cfg:  Config{NodeID: "n1", BootstrapExpect: 1},
		},
		{
			name: "bootstrap expect without discovery",
			cfg:  Config{NodeID: "n1", BootstrapExpect: 3},
		},
		{
			name: "bootstrap and bootstrap expect conflict",
			cfg:  Config{NodeID: "n1", BootstrapExpect: 3, SerfEnabled: true, Bootstrap: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.cfg.validate())
		})
	}

	valid := Config{NodeID: "n1", Bootstrap: true}
	require.NoError(t, valid.validate())
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{NodeID: "n1"}
	cfg.applyDefaults()

	require.Equal(t, defaultApplyTimeout, cfg.Timeout)
	require.Equal(t, defaultFailureGracePeriod, cfg.FailureGracePeriod)
	require.Equal(t, defaultPromotionInterval, cfg.PromotionInterval)
	require.NotNil(t, cfg.after)

	// A negative grace period is a deliberate "never evict", not an unset value.
	disabled := Config{NodeID: "n1", FailureGracePeriod: -1}
	disabled.applyDefaults()
	require.Equal(t, -1, int(disabled.FailureGracePeriod))
}

func TestFindServerAndCountVoters(t *testing.T) {
	servers := []raft.Server{
		{ID: "n1", Suffrage: raft.Voter},
		{ID: "n2", Suffrage: raft.Nonvoter},
		{ID: "n3", Suffrage: raft.Voter},
	}
	require.Equal(t, 2, countVoters(servers))

	server, found := findServer(servers, "n2")
	require.True(t, found)
	require.Equal(t, raft.Nonvoter, server.Suffrage)

	_, found = findServer(servers, "n9")
	require.False(t, found)
}

func TestTLSConfigRejectsIncompleteSettings(t *testing.T) {
	_, err := (&TLSConfig{CertFile: "cert.pem"}).ClientConfig()
	require.Error(t, err, "cert without key must be rejected")

	_, err = (&TLSConfig{}).ServerConfig()
	require.Error(t, err, "server side needs a certificate")

	client, err := (&TLSConfig{}).ClientConfig()
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), client.MinVersion)

	client, err = (&TLSConfig{MinVersion: tls.VersionTLS13}).ClientConfig()
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), client.MinVersion)

	_, err = (&TLSConfig{MinVersion: tls.VersionTLS11}).ClientConfig()
	require.ErrorContains(t, err, "below TLS 1.2")
}

func TestErrNotLeaderIsMatchable(t *testing.T) {
	wrapped := errors.Join(errors.New("context"), ErrNotLeader)
	require.ErrorIs(t, wrapped, ErrNotLeader)
}

// Removing the last voter is already impossible — raft's checkConfiguration
// rejects any change leaving zero voters. What this covers is the diagnostic:
// the refusal must name the node and the rule rather than dumping raft's
// configuration struct, because this path is reached from `cluster leave` and
// from gossip-driven eviction, where the message is all an operator sees.
func TestRemoveServerRefusesTheLastVoterWithAClearError(t *testing.T) {
	h := harnessNew(t)
	leader := h.start("n1", func(c *Config) { c.Bootstrap = true })
	h.waitLeader()

	require.NoError(t, leader.Raft().AddNonvoter("n2", "n2", 0, leader.cfg.Timeout).Error())
	require.NoError(t, leader.Raft().AddNonvoter("n3", "n3", 0, leader.cfg.Timeout).Error())

	servers, err := leader.Configuration()
	require.NoError(t, err)
	require.Len(t, servers, 3, "a plain length check would not catch this")
	require.Equal(t, 1, countVoters(servers))

	err = leader.removeServer("n1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "last voter")
	require.Contains(t, err.Error(), "n1")

	// The voter is still there, and non-voters remain removable: the guard is
	// about electability, not about configuration size.
	after, err := leader.Configuration()
	require.NoError(t, err)
	require.Equal(t, 1, countVoters(after))
	require.NoError(t, leader.removeServer("n3"))
}
