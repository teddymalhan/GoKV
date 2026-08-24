package client_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/teddymalhan/pallasdb/client"
	pbv1 "github.com/teddymalhan/pallasdb/pb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubServer is an in-process PallasDB server that records what the client sent
// and replays scripted responses.
type stubServer struct {
	pbv1.UnimplementedKVServiceServer
	pbv1.UnimplementedSQLServiceServer
	pbv1.UnimplementedClusterServiceServer

	addr string

	// unavailable makes every KV/SQL call fail with codes.Unavailable until it
	// is cleared, which is how a follower behaves.
	unavailable atomic.Bool
	// leaderAddr is what GetLeader advertises.
	leaderAddr atomic.Value // string

	store map[string]string

	lastRange   atomic.Value // *pbv1.RangeRequest
	lastGet     atomic.Value // *pbv1.GetRequest
	lastPut     atomic.Value // *pbv1.PutRequest
	lastQuery   atomic.Value // *pbv1.QueryRequest
	putCalls    atomic.Int32
	getCalls    atomic.Int32
	queryCalls  atomic.Int32
	rangeCalls  atomic.Int32
	leaderCalls atomic.Int32

	// rangeScript, when set, replaces the store-backed Range implementation.
	rangeScript func(stream grpc.ServerStreamingServer[pbv1.RangeResponse]) error
	// queryScript, when set, replaces the default Query implementation.
	queryScript func(stream grpc.ServerStreamingServer[pbv1.QueryResponse]) error
}

func (s *stubServer) unavailableErr() error {
	return status.Error(codes.Unavailable, "not leader")
}

func (s *stubServer) Get(_ context.Context, req *pbv1.GetRequest) (*pbv1.GetResponse, error) {
	s.getCalls.Add(1)
	s.lastGet.Store(req)
	if s.unavailable.Load() {
		return nil, s.unavailableErr()
	}
	value, ok := s.store[string(req.GetKey())]
	if !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &pbv1.GetResponse{Value: []byte(value)}, nil
}

func (s *stubServer) Put(_ context.Context, req *pbv1.PutRequest) (*pbv1.PutResponse, error) {
	s.putCalls.Add(1)
	s.lastPut.Store(req)
	if s.unavailable.Load() {
		return nil, s.unavailableErr()
	}
	key := string(req.GetKey())
	_, exists := s.store[key]
	switch req.GetMode() {
	case pbv1.PutMode_PUT_MODE_INSERT:
		if exists {
			return &pbv1.PutResponse{Updated: false}, nil
		}
	case pbv1.PutMode_PUT_MODE_UPDATE:
		if !exists {
			return &pbv1.PutResponse{Updated: false}, nil
		}
	}
	s.store[key] = string(req.GetValue())
	return &pbv1.PutResponse{Updated: true}, nil
}

func (s *stubServer) Delete(_ context.Context, req *pbv1.DeleteRequest) (*pbv1.DeleteResponse, error) {
	if s.unavailable.Load() {
		return nil, s.unavailableErr()
	}
	key := string(req.GetKey())
	if _, ok := s.store[key]; !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	delete(s.store, key)
	return &pbv1.DeleteResponse{Deleted: true}, nil
}

func (s *stubServer) Range(req *pbv1.RangeRequest, stream grpc.ServerStreamingServer[pbv1.RangeResponse]) error {
	s.rangeCalls.Add(1)
	s.lastRange.Store(req)
	if s.unavailable.Load() {
		return s.unavailableErr()
	}
	if s.rangeScript != nil {
		return s.rangeScript(stream)
	}

	keys := sortedKeys(s.store)
	if req.GetDescending() {
		reverse(keys)
	}
	var sent uint64
	for _, key := range keys {
		if string(req.GetStart()) != "" && key < string(req.GetStart()) {
			continue
		}
		if string(req.GetStop()) != "" && key > string(req.GetStop()) {
			continue
		}
		if req.GetLimit() > 0 && sent >= req.GetLimit() {
			break
		}
		resp := &pbv1.RangeResponse{Key: []byte(key)}
		if !req.GetKeysOnly() {
			resp.Value = []byte(s.store[key])
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		sent++
	}
	return nil
}

func (s *stubServer) Query(req *pbv1.QueryRequest, stream grpc.ServerStreamingServer[pbv1.QueryResponse]) error {
	s.queryCalls.Add(1)
	s.lastQuery.Store(req)
	if s.unavailable.Load() {
		return s.unavailableErr()
	}
	if s.queryScript != nil {
		return s.queryScript(stream)
	}
	return sendSelectResult(stream)
}

func (s *stubServer) GetLeader(context.Context, *pbv1.GetLeaderRequest) (*pbv1.GetLeaderResponse, error) {
	s.leaderCalls.Add(1)
	addr, _ := s.leaderAddr.Load().(string)
	if addr == "" {
		return &pbv1.GetLeaderResponse{}, nil
	}
	return &pbv1.GetLeaderResponse{NodeId: "leader", GrpcAddr: addr, RaftAddr: "raft:" + addr}, nil
}

func sendSelectResult(stream grpc.ServerStreamingServer[pbv1.QueryResponse]) error {
	header := &pbv1.QueryResponse{Columns: []*pbv1.ColumnDescriptor{
		{Name: "id", Type: pbv1.ValueType_VALUE_TYPE_INT64},
		{Name: "name", Type: pbv1.ValueType_VALUE_TYPE_STRING},
	}}
	if err := stream.Send(header); err != nil {
		return err
	}
	rows := []struct {
		id   int64
		name string
	}{{1, "alpha"}, {2, "bravo"}}
	for _, row := range rows {
		msg := &pbv1.QueryResponse{Values: []*pbv1.Value{
			{Value: &pbv1.Value_Int64Value{Int64Value: row.id}},
			{Value: &pbv1.Value_StringValue{StringValue: row.name}},
		}}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func reverse(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// startStub brings up a stub server on a loopback port and returns it.
func startStub(t *testing.T) *stubServer {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	stub := &stubServer{addr: lis.Addr().String(), store: map[string]string{}}
	srv := grpc.NewServer()
	pbv1.RegisterKVServiceServer(srv, stub)
	pbv1.RegisterSQLServiceServer(srv, stub)
	pbv1.RegisterClusterServiceServer(srv, stub)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return stub
}

func dial(t *testing.T, stub *stubServer, opts ...client.Option) *client.Client {
	t.Helper()

	c, err := client.New(stub.addr, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func testContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNewRequiresAddress(t *testing.T) {
	_, err := client.New("")
	require.Error(t, err)
}

func TestGetPutDelete(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)
	ctx := testContext(t)

	updated, err := c.Put(ctx, []byte("alpha"), []byte("bravo"), client.PutUpsert)
	require.NoError(t, err)
	require.True(t, updated)

	value, err := c.Get(ctx, []byte("alpha"), client.ConsistencyDefault)
	require.NoError(t, err)
	require.Equal(t, "bravo", string(value))

	deleted, err := c.Delete(ctx, []byte("alpha"))
	require.NoError(t, err)
	require.True(t, deleted)

	_, err = c.Get(ctx, []byte("alpha"), client.ConsistencyDefault)
	require.ErrorIs(t, err, client.ErrNotFound)

	_, err = c.Delete(ctx, []byte("alpha"))
	require.ErrorIs(t, err, client.ErrNotFound)
}

func TestPutModeAndConsistencyReachTheWire(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)
	ctx := testContext(t)

	_, err := c.Put(ctx, []byte("k"), []byte("v"), client.PutInsert)
	require.NoError(t, err)
	require.Equal(t, pbv1.PutMode_PUT_MODE_INSERT, stub.lastPut.Load().(*pbv1.PutRequest).GetMode())

	_, err = c.Put(ctx, []byte("k"), []byte("v2"), client.PutUpdate)
	require.NoError(t, err)
	require.Equal(t, pbv1.PutMode_PUT_MODE_UPDATE, stub.lastPut.Load().(*pbv1.PutRequest).GetMode())

	_, err = c.Get(ctx, []byte("k"), client.ConsistencyStale)
	require.NoError(t, err)
	require.Equal(t, pbv1.Consistency_CONSISTENCY_STALE, stub.lastGet.Load().(*pbv1.GetRequest).GetConsistency())

	_, err = c.Get(ctx, []byte("k"), client.ConsistencyLinearizable)
	require.NoError(t, err)
	require.Equal(t, pbv1.Consistency_CONSISTENCY_LINEARIZABLE, stub.lastGet.Load().(*pbv1.GetRequest).GetConsistency())
}

func TestRangeStreamsEntries(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)
	ctx := testContext(t)

	for _, key := range []string{"a", "b", "c", "d"} {
		_, err := c.Put(ctx, []byte(key), []byte("v-"+key), client.PutUpsert)
		require.NoError(t, err)
	}

	entries, err := c.RangeSlice(ctx, client.RangeRequest{Start: []byte("a"), Stop: []byte("c")})
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, "a", string(entries[0].Key))
	require.Equal(t, "v-a", string(entries[0].Value))
	require.Equal(t, "c", string(entries[2].Key))
}

func TestRangeHonoursLimitKeysOnlyAndConsistency(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)
	ctx := testContext(t)

	for _, key := range []string{"a", "b", "c", "d"} {
		_, err := c.Put(ctx, []byte(key), []byte("v-"+key), client.PutUpsert)
		require.NoError(t, err)
	}

	entries, err := c.RangeSlice(ctx, client.RangeRequest{
		Limit:       2,
		KeysOnly:    true,
		Consistency: client.ConsistencyStale,
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "a", string(entries[0].Key))
	require.Empty(t, entries[0].Value)

	sent := stub.lastRange.Load().(*pbv1.RangeRequest)
	require.Equal(t, uint64(2), sent.GetLimit())
	require.True(t, sent.GetKeysOnly())
	require.Equal(t, pbv1.Consistency_CONSISTENCY_STALE, sent.GetConsistency())
	require.Empty(t, sent.GetStart())
	require.Empty(t, sent.GetStop())
}

func TestRangeDescending(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)
	ctx := testContext(t)

	for _, key := range []string{"a", "b", "c"} {
		_, err := c.Put(ctx, []byte(key), []byte(key), client.PutUpsert)
		require.NoError(t, err)
	}

	entries, err := c.RangeSlice(ctx, client.RangeRequest{Descending: true})
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, "c", string(entries[0].Key))
	require.Equal(t, "a", string(entries[2].Key))
}

func TestRangeCallbackErrorAbandonsStream(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)
	ctx := testContext(t)

	for _, key := range []string{"a", "b", "c"} {
		_, err := c.Put(ctx, []byte(key), []byte(key), client.PutUpsert)
		require.NoError(t, err)
	}

	sentinel := errors.New("stop")
	seen := 0
	err := c.Range(ctx, client.RangeRequest{}, func(client.KeyValue) error {
		seen++
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, seen)

	seen = 0
	err = c.Range(ctx, client.RangeRequest{}, func(client.KeyValue) error {
		seen++
		if seen == 2 {
			return io.EOF
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, seen)
}

func TestQueryStreamsHeaderThenRows(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)
	ctx := testContext(t)

	result, err := c.Query(ctx, "SELECT id, name FROM t", client.ConsistencyDefault)
	require.NoError(t, err)
	require.Equal(t, []client.Column{
		{Name: "id", Type: client.ValueTypeInt64},
		{Name: "name", Type: client.ValueTypeString},
	}, result.Columns)
	require.Len(t, result.Rows, 2)
	require.Equal(t, client.Value{Type: client.ValueTypeInt64, Int: 1}, result.Rows[0][0])
	require.Equal(t, "alpha", result.Rows[0][1].Str)
	require.Zero(t, result.RowsAffected)

	sent := stub.lastQuery.Load().(*pbv1.QueryRequest)
	require.Equal(t, "SELECT id, name FROM t", sent.GetStatement())
}

func TestQueryReportsRowsAffected(t *testing.T) {
	stub := startStub(t)
	stub.queryScript = func(stream grpc.ServerStreamingServer[pbv1.QueryResponse]) error {
		if err := stream.Send(&pbv1.QueryResponse{}); err != nil {
			return err
		}
		return stream.Send(&pbv1.QueryResponse{RowsAffected: 3})
	}
	c := dial(t, stub)

	result, err := c.Query(testContext(t), "INSERT INTO t (id) VALUES (1)", client.ConsistencyDefault)
	require.NoError(t, err)
	require.Empty(t, result.Columns)
	require.Empty(t, result.Rows)
	require.Equal(t, uint64(3), result.RowsAffected)
}

func TestQueryRejectsEmptyStatement(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)

	_, err := c.Query(testContext(t), "", client.ConsistencyDefault)
	require.Error(t, err)
	require.Zero(t, stub.queryCalls.Load())
}

func TestQueryStreamCallbacksSeeEveryRow(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)

	var columns []client.Column
	var rows [][]client.Value
	affected, err := c.QueryStream(testContext(t), "SELECT *", client.ConsistencyStale, client.QueryHandler{
		OnColumns: func(cols []client.Column) error { columns = cols; return nil },
		OnRow:     func(row []client.Value) error { rows = append(rows, row); return nil },
	})
	require.NoError(t, err)
	require.Zero(t, affected)
	require.Len(t, columns, 2)
	require.Len(t, rows, 2)
	require.Equal(t, pbv1.Consistency_CONSISTENCY_STALE, stub.lastQuery.Load().(*pbv1.QueryRequest).GetConsistency())
}

func TestLeaderRedirectRetriesAgainstLeader(t *testing.T) {
	leader := startStub(t)
	follower := startStub(t)
	follower.unavailable.Store(true)
	follower.leaderAddr.Store(leader.addr)

	c := dial(t, follower)
	ctx := testContext(t)

	updated, err := c.Put(ctx, []byte("alpha"), []byte("bravo"), client.PutUpsert)
	require.NoError(t, err)
	require.True(t, updated)
	require.Equal(t, "bravo", leader.store["alpha"])
	require.Equal(t, int32(1), follower.putCalls.Load(), "follower is tried exactly once")
	require.Equal(t, int32(1), leader.putCalls.Load())
	require.Equal(t, int32(1), follower.leaderCalls.Load())

	// The leader connection is now the active one: no further lookups.
	value, err := c.Get(ctx, []byte("alpha"), client.ConsistencyDefault)
	require.NoError(t, err)
	require.Equal(t, "bravo", string(value))
	require.Zero(t, follower.getCalls.Load())
	require.Equal(t, int32(1), follower.leaderCalls.Load())
}

func TestLeaderRedirectAppliesToStreams(t *testing.T) {
	leader := startStub(t)
	leader.store["a"] = "1"
	follower := startStub(t)
	follower.unavailable.Store(true)
	follower.leaderAddr.Store(leader.addr)

	c := dial(t, follower)
	ctx := testContext(t)

	entries, err := c.RangeSlice(ctx, client.RangeRequest{})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "a", string(entries[0].Key))

	result, err := c.Query(ctx, "SELECT 1", client.ConsistencyDefault)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
}

func TestLeaderRedirectIsBounded(t *testing.T) {
	// A follower that names itself leader must not spin forever.
	follower := startStub(t)
	follower.unavailable.Store(true)
	follower.leaderAddr.Store(follower.addr)

	c := dial(t, follower)

	_, err := c.Put(testContext(t), []byte("k"), []byte("v"), client.PutUpsert)
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, int32(1), follower.putCalls.Load(), "self-redirect is not retried")
}

func TestLeaderRedirectDisabled(t *testing.T) {
	leader := startStub(t)
	follower := startStub(t)
	follower.unavailable.Store(true)
	follower.leaderAddr.Store(leader.addr)

	c := dial(t, follower, client.WithMaxRedirects(0))

	_, err := c.Put(testContext(t), []byte("k"), []byte("v"), client.PutUpsert)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, follower.leaderCalls.Load())
	require.Zero(t, leader.putCalls.Load())
}

func TestLeaderRedirectWithoutLeaderSurfacesOriginalError(t *testing.T) {
	follower := startStub(t)
	follower.unavailable.Store(true) // GetLeader returns an empty grpc_addr

	c := dial(t, follower)

	_, err := c.Get(testContext(t), []byte("k"), client.ConsistencyLinearizable)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, int32(1), follower.getCalls.Load())
}

func TestStreamFailureAfterFirstRowIsNotReplayed(t *testing.T) {
	leader := startStub(t)
	leader.store["a"] = "1"

	flaky := startStub(t)
	flaky.leaderAddr.Store(leader.addr)
	flaky.rangeScript = func(stream grpc.ServerStreamingServer[pbv1.RangeResponse]) error {
		if err := stream.Send(&pbv1.RangeResponse{Key: []byte("partial"), Value: []byte("row")}); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "leadership lost mid-stream")
	}

	c := dial(t, flaky)

	var seen []string
	err := c.Range(testContext(t), client.RangeRequest{}, func(kv client.KeyValue) error {
		seen = append(seen, string(kv.Key))
		return nil
	})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, []string{"partial"}, seen, "delivered rows are never replayed")
	require.Zero(t, leader.rangeCalls.Load())
	require.Zero(t, flaky.leaderCalls.Load())
}

func TestClosedClientRejectsCalls(t *testing.T) {
	stub := startStub(t)
	c, err := client.New(stub.addr)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "Close is idempotent")

	_, err = c.Get(context.Background(), []byte("k"), client.ConsistencyDefault)
	require.ErrorIs(t, err, client.ErrClosed)
}

func TestContextCancellationIsHonoured(t *testing.T) {
	stub := startStub(t)
	c := dial(t, stub)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Get(ctx, []byte("k"), client.ConsistencyDefault)
	require.Error(t, err)
	require.Equal(t, codes.Canceled, status.Code(err))
}

func TestParseHelpers(t *testing.T) {
	mode, err := client.ParsePutMode("insert")
	require.NoError(t, err)
	require.Equal(t, client.PutInsert, mode)
	_, err = client.ParsePutMode("nope")
	require.Error(t, err)

	consistency, err := client.ParseConsistency("stale")
	require.NoError(t, err)
	require.Equal(t, client.ConsistencyStale, consistency)
	_, err = client.ParseConsistency("nope")
	require.Error(t, err)
}

func TestValueRendering(t *testing.T) {
	require.Equal(t, "7", client.Value{Type: client.ValueTypeInt64, Int: 7}.String())
	require.Equal(t, "hi", client.Value{Type: client.ValueTypeString, Str: "hi"}.String())
	require.Equal(t, "NULL", client.Value{}.String())
	require.Equal(t, int64(7), client.Value{Type: client.ValueTypeInt64, Int: 7}.Any())
	require.Nil(t, client.Value{}.Any())
}

// A server bounds a scan by a deadline as well as a row count and reports the
// former as an error mid-stream. Callers that must see the whole keyspace need
// to tell that apart from a scan that genuinely failed.
func TestPartialScanIsDistinguishableAndResumable(t *testing.T) {
	stub := startStub(t)
	stub.store = map[string]string{"a": "1", "b": "2", "c": "3"}
	stub.rangeScript = func(stream grpc.ServerStreamingServer[pbv1.RangeResponse]) error {
		if err := stream.Send(&pbv1.RangeResponse{Key: []byte("a"), Value: []byte("1")}); err != nil {
			return err
		}
		return status.Error(codes.DeadlineExceeded, "range scan deadline exceeded")
	}

	c := dial(t, stub)
	ctx := testContext(t)
	var got []string
	err := c.Range(ctx, client.RangeRequest{}, func(kv client.KeyValue) error {
		got = append(got, string(kv.Key))
		return nil
	})
	require.Error(t, err)
	require.True(t, client.IsPartialScan(err), "a truncated scan must be recognisable")
	require.Equal(t, []string{"a"}, got, "rows delivered before the cut stand")
	// Resuming from the last key received completes the scan.
	stub.rangeScript = nil
	got = nil
	require.NoError(t, c.Range(ctx, client.RangeRequest{Start: []byte("a")}, func(kv client.KeyValue) error {
		got = append(got, string(kv.Key))
		return nil
	}))
	require.Equal(t, []string{"a", "b", "c"}, got)
}

func TestIsPartialScanRejectsOtherFailures(t *testing.T) {
	require.False(t, client.IsPartialScan(nil))
	require.False(t, client.IsPartialScan(errors.New("boom")))
	require.False(t, client.IsPartialScan(status.Error(codes.Unavailable, "not leader")))
	require.True(t, client.IsPartialScan(status.Error(codes.DeadlineExceeded, "too slow")))
}
