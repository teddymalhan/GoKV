package main

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	pbv1 "github.com/teddymalhan/pallasdb/pb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeServer is a minimal in-process PallasDB server: enough of the KV and SQL
// services for the remote CLI commands to be driven end to end without the real
// storage engine or a running process.
type fakeServer struct {
	pbv1.UnimplementedKVServiceServer
	pbv1.UnimplementedSQLServiceServer
	pbv1.UnimplementedClusterServiceServer

	addr string

	mu         sync.Mutex
	store      map[string]string
	statements []string
	// rangeCap truncates every scan, mimicking the server's row cap. 0 is
	// uncapped.
	rangeCap uint64
	// deadlineAfter ends the FIRST scan with DeadlineExceeded once it has sent
	// this many rows, mimicking the server's scan deadline elapsing mid-stream.
	// 0 disables it.
	deadlineAfter uint64
	deadlineFired bool
	// rangeErr fails every scan before a single row is sent.
	rangeErr error
}

// firedDeadline reports whether a scan was cut short by the simulated deadline.
func (f *fakeServer) firedDeadline() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadlineFired
}

func startFakeServer(t *testing.T) *fakeServer {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	fake := &fakeServer{addr: lis.Addr().String(), store: map[string]string{}}
	srv := grpc.NewServer()
	pbv1.RegisterKVServiceServer(srv, fake)
	pbv1.RegisterSQLServiceServer(srv, fake)
	pbv1.RegisterClusterServiceServer(srv, fake)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return fake
}

func (f *fakeServer) seed(entries map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, value := range entries {
		f.store[key] = value
	}
}

func (f *fakeServer) snapshot() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.store))
	for key, value := range f.store {
		out[key] = value
	}
	return out
}

func (f *fakeServer) lastStatement() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.statements) == 0 {
		return ""
	}
	return f.statements[len(f.statements)-1]
}

func (f *fakeServer) Get(_ context.Context, req *pbv1.GetRequest) (*pbv1.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value, ok := f.store[string(req.GetKey())]
	if !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &pbv1.GetResponse{Value: []byte(value)}, nil
}

func (f *fakeServer) Put(_ context.Context, req *pbv1.PutRequest) (*pbv1.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(req.GetKey())
	_, exists := f.store[key]
	if req.GetMode() == pbv1.PutMode_PUT_MODE_INSERT && exists {
		return &pbv1.PutResponse{Updated: false}, nil
	}
	f.store[key] = string(req.GetValue())
	return &pbv1.PutResponse{Updated: true}, nil
}

func (f *fakeServer) Delete(_ context.Context, req *pbv1.DeleteRequest) (*pbv1.DeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(req.GetKey())
	if _, ok := f.store[key]; !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	delete(f.store, key)
	return &pbv1.DeleteResponse{Deleted: true}, nil
}

func (f *fakeServer) Range(req *pbv1.RangeRequest, stream grpc.ServerStreamingServer[pbv1.RangeResponse]) error {
	f.mu.Lock()
	keys := make([]string, 0, len(f.store))
	for key := range f.store {
		keys = append(keys, key)
	}
	values := make(map[string]string, len(f.store))
	for key, value := range f.store {
		values[key] = value
	}
	rowCap, deadlineAfter, rangeErr := f.rangeCap, f.deadlineAfter, f.rangeErr
	if deadlineAfter > 0 && !f.deadlineFired {
		f.deadlineFired = true
	} else {
		deadlineAfter = 0
	}
	f.mu.Unlock()
	if rangeErr != nil {
		return rangeErr
	}

	sort.Strings(keys)
	if req.GetDescending() {
		// The real server cannot seek to the last key, so a descending scan
		// must be anchored by an explicit start.
		if len(req.GetStart()) == 0 {
			return status.Error(codes.InvalidArgument, "descending range requires a start key")
		}
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}

	// The real server caps a scan and truncates silently: the wire format has
	// no "there is more" flag. Clients are expected to resume from the last key.
	limit := req.GetLimit()
	if rowCap > 0 && (limit == 0 || limit > rowCap) {
		limit = rowCap
	}

	// start is where the scan begins and stop where it ends, so a descending
	// scan has start as its upper bound. Both bounds are inclusive.
	lower, upper := string(req.GetStart()), string(req.GetStop())
	if req.GetDescending() {
		lower, upper = upper, lower
	}

	var sent uint64
	for _, key := range keys {
		if lower != "" && key < lower {
			continue
		}
		if upper != "" && key > upper {
			continue
		}
		if limit > 0 && sent >= limit {
			break
		}
		if deadlineAfter > 0 && sent >= deadlineAfter {
			// The scan deadline elapsed mid-stream: rows already sent stand,
			// and the client is expected to resume from the last one.
			return status.Error(codes.DeadlineExceeded, "range scan deadline exceeded")
		}
		resp := &pbv1.RangeResponse{Key: []byte(key)}
		if !req.GetKeysOnly() {
			resp.Value = []byte(values[key])
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		sent++
	}
	return nil
}

// Query answers SELECT with two fixed rows and anything else with a rows
// affected count, which is all the CLI rendering paths need.
func (f *fakeServer) Query(req *pbv1.QueryRequest, stream grpc.ServerStreamingServer[pbv1.QueryResponse]) error {
	f.mu.Lock()
	f.statements = append(f.statements, req.GetStatement())
	f.mu.Unlock()

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(req.GetStatement())), "SELECT") {
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
			err := stream.Send(&pbv1.QueryResponse{Values: []*pbv1.Value{
				{Value: &pbv1.Value_Int64Value{Int64Value: row.id}},
				{Value: &pbv1.Value_StringValue{StringValue: row.name}},
			}})
			if err != nil {
				return err
			}
		}
		return nil
	}

	if strings.Contains(strings.ToUpper(req.GetStatement()), "BOOM") {
		return status.Error(codes.InvalidArgument, "syntax error near BOOM")
	}
	if err := stream.Send(&pbv1.QueryResponse{}); err != nil {
		return err
	}
	return stream.Send(&pbv1.QueryResponse{RowsAffected: 2})
}
