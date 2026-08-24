package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/teddymalhan/pallasdb/cluster/discovery"
	"github.com/teddymalhan/pallasdb/db"
	pbv1 "github.com/teddymalhan/pallasdb/pb/v1"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultApplyTimeout       = 10 * time.Second
	defaultFailureGracePeriod = 30 * time.Second
	defaultPromotionInterval  = time.Second
)

// Config holds all parameters needed to start a cluster node.
type Config struct {
	NodeID    string
	GRPCAddr  string        // gRPC address advertised through discovery
	RaftAddr  string        // TCP address for Raft transport, e.g. ":7001"
	RaftDir   string        // directory for BoltDB log store and snapshots
	Bootstrap bool          // true only when starting the very first node of a fresh cluster
	JoinAddr  string        // gRPC address of an existing node; empty if bootstrapping
	Timeout   time.Duration // Raft apply timeout (default 10s)

	// BootstrapExpect, when >= 2, replaces Bootstrap: the node waits until that
	// many tagged members are visible through discovery and then exactly one of
	// them — the lowest node ID — bootstraps with the full server list. This is
	// what makes a symmetric N-node start produce one cluster instead of N.
	BootstrapExpect int

	// NonVoter keeps this node out of the voting set permanently, making it a
	// read-only replica.
	NonVoter bool

	// FailureGracePeriod is how long a member reported failed by gossip may stay
	// unreachable before the leader evicts it from the configuration. A flapping
	// node that returns within the window is never removed. Zero selects the
	// 30s default; a negative value disables eviction on failure.
	FailureGracePeriod time.Duration

	// PromotionInterval is the poll interval of the self-promotion and
	// bootstrap-expect loops. Zero selects the 1s default.
	PromotionInterval time.Duration

	// LeaveOnShutdown removes this node from the Raft configuration during
	// Shutdown instead of leaving a dead server behind.
	LeaveOnShutdown bool

	// RaftTLS secures the Raft transport. Nil means plaintext TCP.
	RaftTLS *TLSConfig
	// JoinTLS secures the node-to-node join/leave gRPC client. Nil means
	// insecure credentials.
	JoinTLS *TLSConfig

	SerfEnabled       bool
	SerfAddr          string
	SerfAdvertiseAddr string
	SerfJoinAddrs     []string
	SerfEventBuffer   int
	// SerfEncryptKey is the gossip encryption key: 16, 24, or 32 bytes.
	SerfEncryptKey []byte

	// Test seams. They stay unexported so the public surface only describes
	// real deployments; the in-process harness in this package sets them.
	discovery   Discovery
	transport   raft.Transport
	logStore    raft.LogStore
	stableStore raft.StableStore
	snapStore   raft.SnapshotStore
	tuneRaft    func(*raft.Config)
	logOutput   io.Writer
	joinRPC     JoinFunc
	leaveRPC    LeaveFunc
	after       afterFunc
}

func (c *Config) applyDefaults() {
	if c.Timeout == 0 {
		c.Timeout = defaultApplyTimeout
	}
	if c.FailureGracePeriod == 0 {
		c.FailureGracePeriod = defaultFailureGracePeriod
	}
	if c.PromotionInterval <= 0 {
		c.PromotionInterval = defaultPromotionInterval
	}
	if c.logOutput == nil {
		c.logOutput = os.Stderr
	}
	if c.after == nil {
		c.after = realAfterFunc
	}
}

func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("cluster: node id is required")
	}
	if c.BootstrapExpect == 1 {
		return fmt.Errorf("cluster: bootstrap expect 1 is ambiguous, use Bootstrap instead")
	}
	if c.BootstrapExpect >= 2 && !c.SerfEnabled && c.discovery == nil {
		return fmt.Errorf("cluster: bootstrap expect requires discovery to be enabled")
	}
	if c.BootstrapExpect >= 2 && c.Bootstrap {
		return fmt.Errorf("cluster: bootstrap and bootstrap expect are mutually exclusive")
	}
	return nil
}

// Node wraps a raft.Raft instance with its supporting infrastructure.
type Node struct {
	raft      *raft.Raft
	fsm       *FSM
	transport raft.Transport
	cfg       Config
	discovery Discovery

	joinRPC  JoinFunc
	leaveRPC LeaveFunc
	after    afterFunc

	fenceCh  chan raft.Observation
	fenceObs *raft.Observer

	closers []func() error

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeErr  error

	mu             sync.Mutex
	closed         bool
	grpcAddrs      map[string]string
	pendingRemoval map[string]stopper
	evicted        map[string]struct{}
}

// Open initializes all Raft infrastructure and starts the node.
func Open(kvStore *db.KV, cfg Config) (*Node, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	node := &Node{
		cfg:            cfg,
		after:          cfg.after,
		grpcAddrs:      map[string]string{},
		pendingRemoval: map[string]stopper{},
		evicted:        map[string]struct{}{},
	}
	node.ctx, node.cancel = context.WithCancel(context.Background())

	// Anything opened past this point is registered so a failure unwinds
	// cleanly instead of leaking the transport or the Bolt file.
	fail := func(err error) (*Node, error) {
		node.cancel()
		return nil, errors.Join(err, node.closeAll())
	}

	dialOpt, err := dialOption(cfg.JoinTLS)
	if err != nil {
		return fail(err)
	}
	node.joinRPC = cfg.joinRPC
	if node.joinRPC == nil {
		node.joinRPC = newJoinFunc(dialOpt)
	}
	node.leaveRPC = cfg.leaveRPC
	if node.leaveRPC == nil {
		node.leaveRPC = newLeaveFunc(dialOpt)
	}

	transport, err := node.openTransport()
	if err != nil {
		return fail(err)
	}
	node.transport = transport

	logStore, stableStore, snapStore, err := node.openStores()
	if err != nil {
		return fail(err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.LogOutput = cfg.logOutput
	if cfg.tuneRaft != nil {
		cfg.tuneRaft(rc)
	}

	// fsm.go reopens the store from these options after a snapshot restore, so
	// carry the compaction setting across: a restored node must keep compacting.
	fsmKVOpts := append(kvStore.Options.CacheKVOpts(), db.WithAutoCompact(kvStore.Options.AutoCompact))
	node.fsm = NewFSM(kvStore, kvStore.Options.Dirpath, fsmKVOpts...)

	r, err := raft.NewRaft(rc, node.fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return fail(fmt.Errorf("new raft: %w", err))
	}
	node.raft = r

	// Raft never routes a term's no-op entry to an FSM, so the leader has to
	// fence its applied index explicitly or linearizable reads stall for the
	// rest of the term. See fenceAppliedIndex. Registration happens here,
	// before this node can win an election, so the first one is never missed.
	node.watchLeadership()
	node.wg.Add(1)
	go node.leadershipFenceLoop(node.ctx)

	hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		return fail(fmt.Errorf("check raft state: %w", err))
	}
	if cfg.Bootstrap && !hasState {
		configuration := raft.Configuration{Servers: []raft.Server{{
			ID:      rc.LocalID,
			Address: transport.LocalAddr(),
		}}}
		if err := r.BootstrapCluster(configuration).Error(); err != nil {
			return fail(fmt.Errorf("bootstrap: %w", err))
		}
	}

	if err := node.startDiscovery(); err != nil {
		return fail(err)
	}

	if cfg.BootstrapExpect >= 2 && !hasState {
		node.wg.Add(1)
		go node.bootstrapExpectLoop(node.ctx)
	}

	if cfg.JoinAddr != "" && !hasState {
		ctx, cancel := context.WithTimeout(node.ctx, cfg.Timeout)
		err := node.joinRPC(ctx, cfg.JoinAddr, JoinRequest{
			NodeID:   cfg.NodeID,
			RaftAddr: string(transport.LocalAddr()),
			GRPCAddr: cfg.GRPCAddr,
			NonVoter: true,
		})
		cancel()
		if err != nil {
			return fail(fmt.Errorf("join cluster: %w", err))
		}
	}

	// Gossip and the Raft configuration drift apart whenever an event lands
	// while the leader is mid-election, so reconciliation runs continuously.
	// The same loop drives this node from non-voter to voter once it catches up.
	if cfg.JoinAddr != "" || node.discovery != nil {
		node.wg.Add(1)
		go node.membershipLoop(node.ctx)
	}

	return node, nil
}

func (n *Node) openTransport() (raft.Transport, error) {
	if n.cfg.transport != nil {
		return n.cfg.transport, nil
	}
	addr, err := net.ResolveTCPAddr("tcp", n.cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	if n.cfg.RaftTLS != nil {
		stream, err := newTLSStreamLayer(n.cfg.RaftAddr, addr, n.cfg.RaftTLS)
		if err != nil {
			return nil, fmt.Errorf("new tls transport: %w", err)
		}
		transport := raft.NewNetworkTransport(stream, 3, 10*time.Second, n.cfg.logOutput)
		n.closers = append(n.closers, transport.Close)
		return transport, nil
	}
	transport, err := raft.NewTCPTransport(n.cfg.RaftAddr, addr, 3, 10*time.Second, n.cfg.logOutput)
	if err != nil {
		return nil, fmt.Errorf("new tcp transport: %w", err)
	}
	n.closers = append(n.closers, transport.Close)
	return transport, nil
}

func (n *Node) openStores() (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	if n.cfg.logStore != nil && n.cfg.stableStore != nil && n.cfg.snapStore != nil {
		return n.cfg.logStore, n.cfg.stableStore, n.cfg.snapStore, nil
	}

	if err := os.MkdirAll(n.cfg.RaftDir, 0o750); err != nil {
		return nil, nil, nil, fmt.Errorf("mkdir raft dir: %w", err)
	}
	boltStore, err := openBoltStoreWithTimeout(filepath.Join(n.cfg.RaftDir, "raft.db"), 5*time.Second)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open bolt store: %w", err)
	}
	n.closers = append(n.closers, boltStore.Close)

	snapStore, err := raft.NewFileSnapshotStore(n.cfg.RaftDir, 2, n.cfg.logOutput)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new snapshot store: %w", err)
	}
	return boltStore, boltStore, snapStore, nil
}

func (n *Node) startDiscovery() error {
	if n.cfg.discovery != nil {
		n.discovery = n.cfg.discovery
	} else if n.cfg.SerfEnabled {
		disc, err := discovery.NewSerfDiscovery(discovery.Config{
			NodeID:            n.cfg.NodeID,
			GRPCAddr:          n.cfg.GRPCAddr,
			RaftAddr:          n.cfg.RaftAddr,
			SerfAddr:          n.cfg.SerfAddr,
			SerfAdvertiseAddr: n.cfg.SerfAdvertiseAddr,
			JoinAddrs:         n.cfg.SerfJoinAddrs,
			EventBuffer:       n.cfg.SerfEventBuffer,
			EncryptKey:        n.cfg.SerfEncryptKey,
		})
		if err != nil {
			return fmt.Errorf("create serf discovery: %w", err)
		}
		if err := disc.Start(); err != nil {
			return fmt.Errorf("start serf discovery: %w", err)
		}
		n.discovery = disc
	}
	if n.discovery == nil {
		return nil
	}
	n.wg.Add(1)
	go n.handleDiscoveryEvents(n.discovery.Events())
	return nil
}

// Raft returns the underlying raft.Raft instance.
func (n *Node) Raft() *raft.Raft { return n.raft }

// FSM returns the FSM wrapping the KV store.
func (n *Node) FSM() *FSM { return n.fsm }

// NodeID returns this node's Raft server ID.
func (n *Node) NodeID() string { return n.cfg.NodeID }

// DiscoveryMembers returns the current known Serf discovery members.
func (n *Node) DiscoveryMembers() []discovery.NodeInfo {
	if n.discovery == nil {
		return nil
	}
	return n.discovery.Members()
}

// Shutdown stops discovery, Raft, and the log store, releasing every resource
// even when one of the steps fails.
//
// A leader always hands leadership to another voter first so the cluster is not
// left leaderless. When LeaveOnShutdown is set the node also removes itself
// from the configuration.
func (n *Node) Shutdown() error {
	n.closeOnce.Do(func() { n.closeErr = n.shutdown() })
	return n.closeErr
}

func (n *Node) shutdown() error {
	// Stop the membership loop first: a node that is leaving must not race its
	// own departure by asking the leader to add it back.
	n.cancel()

	var errs []error
	if n.raft != nil {
		if n.cfg.LeaveOnShutdown {
			// Announce the departure both ways: out of the Raft configuration
			// so no dead voter is left behind, and over gossip so the other
			// members treat this as intentional rather than a failure.
			if err := n.LeaveSelf(); err != nil {
				errs = append(errs, fmt.Errorf("leave cluster: %w", err))
			}
			if n.discovery != nil {
				if err := n.discovery.Leave(); err != nil {
					errs = append(errs, fmt.Errorf("leave discovery: %w", err))
				}
			}
		} else if err := n.transferLeadership(); err != nil {
			errs = append(errs, err)
		}
	}

	n.mu.Lock()
	n.closed = true
	for nodeID, timer := range n.pendingRemoval {
		timer.Stop()
		delete(n.pendingRemoval, nodeID)
	}
	n.mu.Unlock()

	if n.discovery != nil {
		if err := n.discovery.Shutdown(); err != nil {
			errs = append(errs, fmt.Errorf("shutdown discovery: %w", err))
		}
	}

	// Let every canceled loop leave while Raft is still able to resolve work
	// already submitted to it. In particular, leadershipFenceLoop may be inside
	// Barrier().Error(); shutting Raft down first can strand that future forever.
	n.wg.Wait()
	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			errs = append(errs, fmt.Errorf("shutdown raft: %w", err))
		}
	}
	errs = append(errs, n.closeAll())
	return errors.Join(errs...)
}

// closeAll releases every registered resource, newest first.
func (n *Node) closeAll() error {
	var errs []error
	for i := len(n.closers) - 1; i >= 0; i-- {
		if err := n.closers[i](); err != nil {
			errs = append(errs, err)
		}
	}
	n.closers = nil
	return errors.Join(errs...)
}

func dialOption(cfg *TLSConfig) (grpc.DialOption, error) {
	if cfg == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	creds, err := cfg.GRPCCredentials()
	if err != nil {
		return nil, fmt.Errorf("join client tls: %w", err)
	}
	return grpc.WithTransportCredentials(creds), nil
}

func newJoinFunc(dialOpt grpc.DialOption) JoinFunc {
	return func(ctx context.Context, leaderGRPCAddr string, req JoinRequest) error {
		return withClusterClient(leaderGRPCAddr, dialOpt, func(client pbv1.ClusterServiceClient) error {
			_, err := client.Join(ctx, &pbv1.JoinRequest{
				NodeId:   req.NodeID,
				RaftAddr: req.RaftAddr,
				GrpcAddr: req.GRPCAddr,
				NonVoter: req.NonVoter,
			})
			if err != nil {
				return fmt.Errorf("call join grpc: %w", err)
			}
			return nil
		})
	}
}

func newLeaveFunc(dialOpt grpc.DialOption) LeaveFunc {
	return func(ctx context.Context, leaderGRPCAddr, nodeID string) error {
		return withClusterClient(leaderGRPCAddr, dialOpt, func(client pbv1.ClusterServiceClient) error {
			if _, err := client.Leave(ctx, &pbv1.LeaveRequest{NodeId: nodeID}); err != nil {
				return fmt.Errorf("call leave grpc: %w", err)
			}
			return nil
		})
	}
}

func withClusterClient(addr string, dialOpt grpc.DialOption, fn func(pbv1.ClusterServiceClient) error) error {
	conn, err := grpc.NewClient(addr, dialOpt)
	if err != nil {
		return fmt.Errorf("create cluster grpc client: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return fn(pbv1.NewClusterServiceClient(conn))
}

// openBoltStoreWithTimeout opens a BoltStore with a lock timeout so that a
// previous crashed instance does not block startup indefinitely.
func openBoltStoreWithTimeout(path string, timeout time.Duration) (*raftboltdb.BoltStore, error) {
	return raftboltdb.New(raftboltdb.Options{
		Path: path,
		BoltOptions: &bolt.Options{
			Timeout: timeout,
		},
	})
}
