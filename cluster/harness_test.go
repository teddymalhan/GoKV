package cluster

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/teddymalhan/pallasdb/cluster/discovery"
	"github.com/teddymalhan/pallasdb/db"
)

// ---------------------------------------------------------------------------
// Gossip bus: an in-process stand-in for Serf.
// ---------------------------------------------------------------------------

// harnessBus broadcasts membership events between in-process nodes so tests can
// drive joins, graceful leaves, and suspected failures without a network.
type harnessBus struct {
	mu      sync.Mutex
	members map[string]discovery.NodeInfo
	order   []string
	subs    map[string]*harnessDiscovery
}

func harnessNewBus() *harnessBus {
	return &harnessBus{
		members: map[string]discovery.NodeInfo{},
		subs:    map[string]*harnessDiscovery{},
	}
}

// subscribe registers a member and returns its Discovery view. Existing members
// are announced to the newcomer and the newcomer to every existing member, the
// same way Serf converges after a gossip join.
func (b *harnessBus) subscribe(info discovery.NodeInfo) *harnessDiscovery {
	info.Status = discovery.MemberStatusAlive
	sub := &harnessDiscovery{bus: b, nodeID: info.NodeID, events: make(chan discovery.Event, 256)}

	b.mu.Lock()
	existing := make([]discovery.NodeInfo, 0, len(b.order))
	for _, id := range b.order {
		existing = append(existing, b.members[id])
	}
	if _, dup := b.members[info.NodeID]; !dup {
		b.order = append(b.order, info.NodeID)
	}
	b.members[info.NodeID] = info
	b.subs[info.NodeID] = sub
	b.mu.Unlock()

	for _, member := range existing {
		sub.deliver(discovery.Event{Type: discovery.EventMemberJoin, Member: member})
	}
	b.broadcast(discovery.EventMemberJoin, info, info.NodeID)
	return sub
}

// setStatus updates a member's status and announces the matching event.
func (b *harnessBus) setStatus(nodeID string, status discovery.MemberStatus, eventType discovery.EventType) {
	b.mu.Lock()
	info, ok := b.members[nodeID]
	if ok {
		info.Status = status
		b.members[nodeID] = info
	}
	b.mu.Unlock()
	if !ok {
		return
	}
	b.broadcast(eventType, info, "")
}

// leave marks a member as having left gracefully.
func (b *harnessBus) leave(nodeID string) {
	b.setStatus(nodeID, discovery.MemberStatusLeft, discovery.EventMemberLeave)
}

// fail marks a member as unreachable, the way Serf reports a suspected failure.
func (b *harnessBus) fail(nodeID string) {
	b.setStatus(nodeID, discovery.MemberStatusFailed, discovery.EventMemberFailed)
}

// revive marks a failed member alive again, simulating a flap.
func (b *harnessBus) revive(nodeID string) {
	b.setStatus(nodeID, discovery.MemberStatusAlive, discovery.EventMemberJoin)
}

func (b *harnessBus) broadcast(eventType discovery.EventType, member discovery.NodeInfo, skip string) {
	b.mu.Lock()
	targets := make([]*harnessDiscovery, 0, len(b.subs))
	for id, sub := range b.subs {
		if id == skip {
			continue
		}
		targets = append(targets, sub)
	}
	b.mu.Unlock()

	for _, sub := range targets {
		sub.deliver(discovery.Event{Type: eventType, Member: member})
	}
}

func (b *harnessBus) snapshot() []discovery.NodeInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	members := make([]discovery.NodeInfo, 0, len(b.order))
	for _, id := range b.order {
		members = append(members, b.members[id])
	}
	return members
}

func (b *harnessBus) unsubscribe(nodeID string) {
	b.mu.Lock()
	delete(b.subs, nodeID)
	b.mu.Unlock()
}

// harnessDiscovery is the per-node Discovery implementation backed by the bus.
type harnessDiscovery struct {
	bus    *harnessBus
	nodeID string
	events chan discovery.Event

	mu     sync.Mutex
	closed bool
}

func (d *harnessDiscovery) deliver(event discovery.Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	select {
	case d.events <- event:
	default: // a full buffer means the test outran the node; drop like Serf does
	}
}

func (d *harnessDiscovery) Events() <-chan discovery.Event { return d.events }

func (d *harnessDiscovery) Members() []discovery.NodeInfo { return d.bus.snapshot() }

// Leave announces a graceful departure, matching SerfDiscovery.Leave.
func (d *harnessDiscovery) Leave() error {
	d.bus.leave(d.nodeID)
	return nil
}

func (d *harnessDiscovery) Shutdown() error {
	d.bus.unsubscribe(d.nodeID)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	close(d.events)
	return nil
}

// ---------------------------------------------------------------------------
// Deterministic timers for the failure grace period.
// ---------------------------------------------------------------------------

// harnessTimers replaces time.AfterFunc so the failure grace period elapses
// exactly when a test says so, never on wall-clock time.
type harnessTimers struct {
	mu      sync.Mutex
	pending []*harnessTimer
}

type harnessTimer struct {
	timers  *harnessTimers
	delay   time.Duration
	fn      func()
	stopped bool
}

func (h *harnessTimers) after(d time.Duration, fn func()) stopper {
	timer := &harnessTimer{timers: h, delay: d, fn: fn}
	h.mu.Lock()
	h.pending = append(h.pending, timer)
	h.mu.Unlock()
	return timer
}

func (t *harnessTimer) Stop() bool {
	t.timers.mu.Lock()
	defer t.timers.mu.Unlock()
	already := t.stopped
	t.stopped = true
	return !already
}

// armed counts timers that are still waiting to fire.
func (h *harnessTimers) armed() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, timer := range h.pending {
		if !timer.stopped {
			count++
		}
	}
	return count
}

// elapse fires every armed timer, as if their grace period had expired, and
// reports how many ran.
func (h *harnessTimers) elapse() int {
	h.mu.Lock()
	pending := h.pending
	h.pending = nil
	h.mu.Unlock()

	fired := 0
	for _, timer := range pending {
		h.mu.Lock()
		stopped := timer.stopped
		h.mu.Unlock()
		if stopped {
			continue
		}
		fired++
		timer.fn()
	}
	return fired
}

// ---------------------------------------------------------------------------
// Cluster harness.
// ---------------------------------------------------------------------------

type harnessNode struct {
	*Node
	id        string
	grpcAddr  string
	raftAddr  raft.ServerAddress
	transport *raft.InmemTransport
	timers    *harnessTimers
	stopped   bool
}

type harness struct {
	t   *testing.T
	bus *harnessBus

	// root is the single parent directory every node's data dir lives under.
	// It exists so the stores can be closed before the directory is removed:
	// see harnessNew for why that ordering matters.
	root string

	mu    sync.Mutex
	nodes map[string]*harnessNode
}

func harnessNew(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, bus: harnessNewBus(), nodes: map[string]*harnessNode{}}
	// The shared root must be created *before* stopAll is registered. Go runs
	// t.Cleanup functions last-registered first, so with this ordering stopAll
	// (which shuts every node down and closes every store, releasing the WAL
	// file handles) runs before t.TempDir removes the root. On Windows a store
	// that is still open pins its kv_log, so the reverse order would make
	// RemoveAll fail with "file in use" and fail every harness test.
	h.root = t.TempDir()
	t.Cleanup(h.stopAll)
	return h
}

// start opens a node wired to the in-process transport mesh and gossip bus.
// mutate may adjust the Config before Open.
func (h *harness) start(id string, mutate func(*Config)) *harnessNode {
	h.t.Helper()

	addr, transport := raft.NewInmemTransport(raft.ServerAddress(id))
	h.connectMesh(addr, transport)

	grpcAddr := "grpc://" + id
	timers := &harnessTimers{}
	disc := h.bus.subscribe(discovery.NodeInfo{
		NodeID:   id,
		GRPCAddr: grpcAddr,
		RaftAddr: string(addr),
	})

	store, err := db.NewKV(filepath.Join(h.root, "kv-"+id))
	require.NoError(h.t, err)

	logStore := raft.NewInmemStore()
	cfg := Config{
		NodeID:            id,
		GRPCAddr:          grpcAddr,
		RaftAddr:          string(addr),
		Timeout:           5 * time.Second,
		PromotionInterval: 10 * time.Millisecond,

		discovery:   disc,
		transport:   transport,
		logStore:    logStore,
		stableStore: logStore,
		snapStore:   raft.NewInmemSnapshotStore(),
		logOutput:   io.Discard,
		after:       timers.after,
		tuneRaft:    harnessTuneRaft,
		joinRPC:     h.joinRPC,
		leaveRPC:    h.leaveRPC,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	node, err := Open(store, cfg)
	if err != nil {
		_ = store.Close()
		require.NoError(h.t, err)
	}

	hn := &harnessNode{
		Node:      node,
		id:        id,
		grpcAddr:  grpcAddr,
		raftAddr:  addr,
		transport: transport,
		timers:    timers,
	}
	h.mu.Lock()
	h.nodes[id] = hn
	h.mu.Unlock()
	return hn
}

// harnessTuneRaft shrinks every Raft timer so elections settle in milliseconds.
func harnessTuneRaft(rc *raft.Config) {
	rc.HeartbeatTimeout = 50 * time.Millisecond
	rc.ElectionTimeout = 50 * time.Millisecond
	rc.LeaderLeaseTimeout = 50 * time.Millisecond
	rc.CommitTimeout = 5 * time.Millisecond
	rc.SnapshotInterval = time.Hour
	rc.SnapshotThreshold = 1 << 20
	rc.LogLevel = "ERROR"
}

// connectMesh wires a new transport to every transport already in the harness.
func (h *harness) connectMesh(addr raft.ServerAddress, transport *raft.InmemTransport) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, peer := range h.nodes {
		transport.Connect(peer.raftAddr, peer.transport)
		peer.transport.Connect(addr, transport)
	}
}

// joinRPC delivers a JoinRequest to the node listening on leaderGRPCAddr.
func (h *harness) joinRPC(_ context.Context, leaderGRPCAddr string, req JoinRequest) error {
	target := h.byGRPCAddr(leaderGRPCAddr)
	if target == nil {
		return fmt.Errorf("harness: no node at %s", leaderGRPCAddr)
	}
	return target.Join(req.NodeID, req.RaftAddr, req.GRPCAddr, req.NonVoter)
}

// leaveRPC delivers a Leave request to the node listening on leaderGRPCAddr.
func (h *harness) leaveRPC(_ context.Context, leaderGRPCAddr, nodeID string) error {
	target := h.byGRPCAddr(leaderGRPCAddr)
	if target == nil {
		return fmt.Errorf("harness: no node at %s", leaderGRPCAddr)
	}
	return target.Leave(nodeID)
}

func (h *harness) byGRPCAddr(addr string) *harnessNode {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, node := range h.nodes {
		if node.grpcAddr == addr && !node.stopped {
			return node
		}
	}
	return nil
}

func (h *harness) node(id string) *harnessNode {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[id]
}

func (h *harness) live() []*harnessNode {
	h.mu.Lock()
	defer h.mu.Unlock()
	live := make([]*harnessNode, 0, len(h.nodes))
	for _, node := range h.nodes {
		if !node.stopped {
			live = append(live, node)
		}
	}
	return live
}

// stop shuts a node down and cuts it out of the transport mesh so surviving
// nodes do not keep replicating to a dead consumer.
func (h *harness) stop(id string) error {
	h.mu.Lock()
	node := h.nodes[id]
	if node == nil || node.stopped {
		h.mu.Unlock()
		return nil
	}
	node.stopped = true
	peers := make([]*harnessNode, 0, len(h.nodes))
	for _, peer := range h.nodes {
		if peer.id != id {
			peers = append(peers, peer)
		}
	}
	h.mu.Unlock()

	err := node.Shutdown()
	for _, peer := range peers {
		peer.transport.Disconnect(node.raftAddr)
	}
	node.transport.DisconnectAll()
	_ = node.transport.Close()
	_ = node.FSM().Close()
	return err
}

func (h *harness) stopAll() {
	h.mu.Lock()
	ids := make([]string, 0, len(h.nodes))
	for id := range h.nodes {
		ids = append(ids, id)
	}
	h.mu.Unlock()
	for _, id := range ids {
		_ = h.stop(id)
	}
}

// leader returns the single node that currently believes it leads, or nil when
// there is none or more than one.
func (h *harness) leader() *harnessNode {
	var leader *harnessNode
	for _, node := range h.live() {
		if node.IsLeader() {
			if leader != nil {
				return nil // still settling; caller retries
			}
			leader = node
		}
	}
	return leader
}

// waitLeader blocks until exactly one live node leads.
func (h *harness) waitLeader() *harnessNode {
	h.t.Helper()
	var leader *harnessNode
	harnessWaitFor(h.t, "a single leader", func() bool {
		leader = h.leader()
		return leader != nil
	})
	return leader
}

// waitVoters blocks until every live node sees exactly the given voter set.
func (h *harness) waitVoters(ids ...string) {
	h.t.Helper()
	harnessWaitFor(h.t, fmt.Sprintf("voters %v on every node", ids), func() bool {
		for _, node := range h.live() {
			if !harnessSameVoters(node.Node, ids) {
				return false
			}
		}
		return true
	})
}

func harnessSameVoters(node *Node, ids []string) bool {
	servers, err := node.Configuration()
	if err != nil {
		return false
	}
	voters := map[string]bool{}
	for _, server := range servers {
		if server.Suffrage == raft.Voter {
			voters[string(server.ID)] = true
		}
	}
	if len(voters) != len(ids) {
		return false
	}
	for _, id := range ids {
		if !voters[id] {
			return false
		}
	}
	return true
}

// harnessSuffrage reports a node's suffrage in another node's view of the
// configuration. The bool is false when the node is absent.
func harnessSuffrage(t *testing.T, viewer *Node, nodeID string) (raft.ServerSuffrage, bool) {
	t.Helper()
	servers, err := viewer.Configuration()
	require.NoError(t, err)
	server, found := findServer(servers, nodeID)
	return server.Suffrage, found
}

// harnessPut commits a write through the leader and returns once it is applied
// on the leader's own FSM.
func harnessPut(t *testing.T, leader *harnessNode, key, value string) {
	t.Helper()
	payload, err := EncodeCommand(Command{
		Op:   OpPut,
		Key:  []byte(key),
		Val:  []byte(value),
		Mode: int(db.ModeUpsert),
	})
	require.NoError(t, err)

	future := leader.Raft().Apply(payload, 5*time.Second)
	require.NoError(t, future.Error())
	result, ok := future.Response().(*FSMResult)
	require.True(t, ok, "apply response should be *FSMResult")
	require.NoError(t, result.Err)
}

// harnessRequireValue waits for key to hold value on every listed node.
func harnessRequireValue(t *testing.T, key, value string, nodes ...*harnessNode) {
	t.Helper()
	harnessWaitFor(t, fmt.Sprintf("%q replicated to every node", key), func() bool {
		for _, node := range nodes {
			got, found, err := node.FSM().Get([]byte(key))
			if err != nil || !found || string(got) != value {
				return false
			}
		}
		return true
	})
}

// harnessWaitFor polls cond until it holds or the test fails.
func harnessWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
