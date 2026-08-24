package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/credentials"
)

// TLSConfig describes a certificate-based TLS setup. The field names mirror
// grpcapi.TLSConfig so a single set of CLI flags can configure the gRPC server,
// the Raft transport, and the node-to-node join client.
//
// ClientCAFile plays two roles: it is the pool used to verify peer certificates
// when this node accepts a connection, and the root pool used when this node
// dials a peer. Cluster traffic is symmetric, so both sides present CertFile.
type TLSConfig struct {
	CertFile          string
	KeyFile           string
	ClientCAFile      string
	RequireClientCert bool
	MinVersion        uint16
}

func (c *TLSConfig) minVersion() uint16 {
	if c.MinVersion == 0 {
		return tls.VersionTLS12
	}
	return c.MinVersion
}

func (c *TLSConfig) certificates() ([]tls.Certificate, error) {
	if c.CertFile == "" && c.KeyFile == "" {
		return nil, nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, fmt.Errorf("tls: cert file and key file must be set together")
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load key pair: %w", err)
	}
	return []tls.Certificate{cert}, nil
}

func (c *TLSConfig) caPool() (*x509.CertPool, error) {
	if c.ClientCAFile == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("tls: read ca file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tls: no certificates found in %s", c.ClientCAFile)
	}
	return pool, nil
}

// ServerConfig builds the tls.Config used when accepting peer connections.
func (c *TLSConfig) ServerConfig() (*tls.Config, error) {
	certs, err := c.certificates()
	if err != nil {
		return nil, err
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("tls: server side requires a certificate and key")
	}
	pool, err := c.caPool()
	if err != nil {
		return nil, err
	}
	clientAuth := tls.NoClientCert
	if c.RequireClientCert {
		if pool == nil {
			return nil, fmt.Errorf("tls: require client cert needs a client ca file")
		}
		clientAuth = tls.RequireAndVerifyClientCert
	} else if pool != nil {
		clientAuth = tls.VerifyClientCertIfGiven
	}
	return &tls.Config{
		Certificates: certs,
		ClientCAs:    pool,
		ClientAuth:   clientAuth,
		MinVersion:   c.minVersion(),
	}, nil
}

// ClientConfig builds the tls.Config used when dialing a peer.
func (c *TLSConfig) ClientConfig() (*tls.Config, error) {
	certs, err := c.certificates()
	if err != nil {
		return nil, err
	}
	pool, err := c.caPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: certs,
		RootCAs:      pool,
		MinVersion:   c.minVersion(),
	}, nil
}

// GRPCCredentials builds transport credentials for the node-to-node client.
func (c *TLSConfig) GRPCCredentials() (credentials.TransportCredentials, error) {
	cfg, err := c.ClientConfig()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}

// tlsStreamLayer is a raft.StreamLayer that wraps TCP in TLS on both the
// accept and the dial side.
type tlsStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
	client    *tls.Config
}

// newTLSStreamLayer listens on bindAddr with server TLS and dials peers with
// client TLS. advertise may be nil, in which case the bound address is used.
func newTLSStreamLayer(bindAddr string, advertise net.Addr, cfg *TLSConfig) (*tlsStreamLayer, error) {
	serverCfg, err := cfg.ServerConfig()
	if err != nil {
		return nil, err
	}
	clientCfg, err := cfg.ClientConfig()
	if err != nil {
		return nil, err
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("tls: listen on %s: %w", bindAddr, err)
	}
	if advertise != nil {
		if tcpAddr, ok := advertise.(*net.TCPAddr); ok && tcpAddr.IP.IsUnspecified() {
			_ = ln.Close()
			return nil, fmt.Errorf("tls: advertise address %s is not routable", advertise)
		}
	}
	return &tlsStreamLayer{
		listener:  tls.NewListener(ln, serverCfg),
		advertise: advertise,
		client:    clientCfg,
	}, nil
}

// Dial opens a TLS connection to a peer. The server name is derived from the
// dial address by crypto/tls when ClientConfig leaves it empty.
func (l *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return (&tls.Dialer{NetDialer: &net.Dialer{Timeout: timeout}, Config: l.client}).DialContext(context.Background(), "tcp", string(address))
}

func (l *tlsStreamLayer) Accept() (net.Conn, error) { return l.listener.Accept() }

func (l *tlsStreamLayer) Close() error { return l.listener.Close() }

func (l *tlsStreamLayer) Addr() net.Addr {
	if l.advertise != nil {
		return l.advertise
	}
	return l.listener.Addr()
}
