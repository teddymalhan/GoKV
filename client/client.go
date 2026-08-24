// Package client is a thin, dependency-light Go client for a running PallasDB
// server. It wraps the generated pb/v1 gRPC stubs so that callers never have to
// hand-roll a connection, a request message, or the leader-redirect dance.
//
// A Client is safe for concurrent use, holds no global state, and takes a
// context.Context on every method.
//
// # Leader redirection
//
// A clustered PallasDB node that is not the Raft leader answers writes (and
// linearizable reads) with codes.Unavailable. When that happens the Client asks
// the node it just talked to for the current leader, dials the leader's
// advertised gRPC address, and retries the call once. Redirects are bounded by
// [WithMaxRedirects] and are only attempted for streaming calls that have not
// yet delivered a row, so a failure mid-stream is never silently replayed.
//
//	c, err := client.New("127.0.0.1:50051")
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	if _, err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
//		return err
//	}
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pbv1 "github.com/teddymalhan/pallasdb/pb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ErrNotFound is returned by Get and Delete when the key does not exist.
var ErrNotFound = errors.New("pallasdb: key not found")

// ErrNoLeader is returned when a redirect was required but the cluster could
// not name a leader with a reachable gRPC address.
var ErrNoLeader = errors.New("pallasdb: no leader available")

// ErrClosed is returned by every method once Close has been called.
var ErrClosed = errors.New("pallasdb: client is closed")

// DefaultMaxRedirects bounds how many times a single call may be re-sent to a
// newly discovered leader before the original error is surfaced.
const DefaultMaxRedirects = 1

// defaultLeaderLookupTimeout bounds the auxiliary GetLeader call made while
// resolving a redirect, so a wedged follower cannot consume the caller's whole
// deadline before the retry is even attempted.
const defaultLeaderLookupTimeout = 5 * time.Second

// Option configures a Client.
type Option func(*settings)

type settings struct {
	dialOptions         []grpc.DialOption
	maxRedirects        int
	leaderLookupTimeout time.Duration
}

// WithDialOptions appends gRPC dial options. They are applied after the
// client's defaults (insecure transport credentials), so passing transport
// credentials here replaces the default.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(s *settings) { s.dialOptions = append(s.dialOptions, opts...) }
}

// WithMaxRedirects bounds leader-redirect retries per call. Zero disables
// redirection entirely; negative values are ignored.
func WithMaxRedirects(n int) Option {
	return func(s *settings) {
		if n >= 0 {
			s.maxRedirects = n
		}
	}
}

// WithLeaderLookupTimeout bounds the GetLeader call issued while resolving a
// redirect. Non-positive values are ignored.
func WithLeaderLookupTimeout(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.leaderLookupTimeout = d
		}
	}
}

// Client talks to a PallasDB server. Create one with [New] and release it with
// [Client.Close].
type Client struct {
	settings settings
	seedAddr string

	mu     sync.Mutex
	conns  map[string]*conn
	active *conn
	closed bool
}

// conn bundles a dialled address with the service stubs riding on it.
type conn struct {
	addr    string
	cc      *grpc.ClientConn
	kv      pbv1.KVServiceClient
	sql     pbv1.SQLServiceClient
	cluster pbv1.ClusterServiceClient
}

func newConn(addr string, dialOpts []grpc.DialOption) (*conn, error) {
	cc, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &conn{
		addr:    addr,
		cc:      cc,
		kv:      pbv1.NewKVServiceClient(cc),
		sql:     pbv1.NewSQLServiceClient(cc),
		cluster: pbv1.NewClusterServiceClient(cc),
	}, nil
}

// New creates a Client for the given "host:port" target. The connection is
// lazy: no network I/O happens until the first call.
func New(addr string, opts ...Option) (*Client, error) {
	if addr == "" {
		return nil, errors.New("pallasdb: server address is required")
	}

	s := settings{
		maxRedirects:        DefaultMaxRedirects,
		leaderLookupTimeout: defaultLeaderLookupTimeout,
	}
	for _, opt := range opts {
		opt(&s)
	}
	dialOpts := append(
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		s.dialOptions...,
	)
	s.dialOptions = dialOpts

	seed, err := newConn(addr, dialOpts)
	if err != nil {
		return nil, err
	}
	return &Client{
		settings: s,
		seedAddr: addr,
		conns:    map[string]*conn{addr: seed},
		active:   seed,
	}, nil
}

// Address reports the seed address the Client was created with.
func (c *Client) Address() string { return c.seedAddr }

// Close releases every connection the Client opened, including connections
// created by leader redirection. Close is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conns := make([]*conn, 0, len(c.conns))
	for _, cn := range c.conns {
		conns = append(conns, cn)
	}
	c.conns = nil
	c.active = nil
	c.mu.Unlock()

	var errs []error
	for _, cn := range conns {
		if err := cn.cc.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Client) current() (*conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	return c.active, nil
}

// connFor returns a cached connection for addr, dialling one if needed.
func (c *Client) connFor(addr string) (*conn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if cn, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		return cn, nil
	}
	c.mu.Unlock()

	cn, err := newConn(addr, c.settings.dialOptions)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = cn.cc.Close()
		return nil, ErrClosed
	}
	// Another goroutine may have dialled the same address concurrently.
	if existing, ok := c.conns[addr]; ok {
		_ = cn.cc.Close()
		return existing, nil
	}
	c.conns[addr] = cn
	return cn, nil
}

// noRetry marks an error as ineligible for leader redirection, which is how a
// streaming call reports "I already handed rows to the caller".
type noRetry struct{ err error }

func (e noRetry) Error() string { return e.err.Error() }
func (e noRetry) Unwrap() error { return e.err }

func isRedirectable(err error) bool {
	if err == nil {
		return false
	}
	var stop noRetry
	if errors.As(err, &stop) {
		return false
	}
	return status.Code(err) == codes.Unavailable
}

func unwrapNoRetry(err error) error {
	var stop noRetry
	if errors.As(err, &stop) {
		return stop.err
	}
	return err
}

// call runs fn against the active connection and, on codes.Unavailable, retries
// it against the current leader up to maxRedirects times.
func (c *Client) call(ctx context.Context, fn func(context.Context, *conn) error) error {
	cn, err := c.current()
	if err != nil {
		return err
	}

	callErr := fn(ctx, cn)
	for attempt := 0; attempt < c.settings.maxRedirects && isRedirectable(callErr); attempt++ {
		next, redirectErr := c.resolveLeader(ctx, cn)
		if redirectErr != nil || next == cn {
			break
		}
		cn = next
		callErr = fn(ctx, cn)
	}
	return unwrapNoRetry(callErr)
}

// resolveLeader asks from (falling back to the seed connection) who the leader
// is and returns a connection to the leader's advertised gRPC address.
func (c *Client) resolveLeader(ctx context.Context, from *conn) (*conn, error) {
	lookupCtx := ctx
	if c.settings.leaderLookupTimeout > 0 {
		var cancel context.CancelFunc
		lookupCtx, cancel = context.WithTimeout(ctx, c.settings.leaderLookupTimeout)
		defer cancel()
	}

	candidates := []*conn{from}
	if from.addr != c.seedAddr {
		if seed, err := c.connFor(c.seedAddr); err == nil {
			candidates = append(candidates, seed)
		}
	}

	var lastErr = ErrNoLeader
	for _, candidate := range candidates {
		resp, err := candidate.cluster.GetLeader(lookupCtx, &pbv1.GetLeaderRequest{})
		if err != nil {
			lastErr = err
			continue
		}
		addr := resp.GetGrpcAddr()
		if addr == "" {
			lastErr = ErrNoLeader
			continue
		}
		leader, err := c.connFor(addr)
		if err != nil {
			lastErr = err
			continue
		}
		c.setActive(leader)
		return leader, nil
	}
	return nil, lastErr
}

func (c *Client) setActive(cn *conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.active = cn
	}
}
