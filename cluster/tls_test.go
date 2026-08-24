package cluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// tlstestPKI writes a CA plus one leaf certificate valid for 127.0.0.1 and
// returns a TLSConfig pointing at them.
func tlstestPKI(t *testing.T, requireClientCert bool) *TLSConfig {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pallasdb-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "pallasdb-node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)

	certFile := filepath.Join(dir, "node.pem")
	keyFile := filepath.Join(dir, "node-key.pem")
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER}), 0o600))
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))

	return &TLSConfig{
		CertFile:          certFile,
		KeyFile:           keyFile,
		ClientCAFile:      caFile,
		RequireClientCert: requireClientCert,
	}
}

// The Raft transport must actually negotiate TLS, not merely compile against a
// TLS type: bytes have to cross an encrypted, mutually authenticated link.
func TestTLSStreamLayerRoundTrip(t *testing.T) {
	cfg := tlstestPKI(t, true)
	layer, err := newTLSStreamLayer("127.0.0.1:0", nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = layer.Close() })

	accepted := make(chan error, 1)
	go func() {
		conn, err := layer.Accept()
		if err != nil {
			accepted <- err
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 4)
		if _, err := conn.Read(buf); err != nil {
			accepted <- err
			return
		}
		_, err = conn.Write(buf)
		accepted <- err
	}()

	conn, err := layer.Dial(raft.ServerAddress(layer.Addr().String()), 5*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tlsConn, ok := conn.(*tls.Conn)
	require.True(t, ok, "the raft transport must be handed a TLS connection")
	require.True(t, tlsConn.ConnectionState().HandshakeComplete)
	require.GreaterOrEqual(t, tlsConn.ConnectionState().Version, uint16(tls.VersionTLS12))

	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	echo := make([]byte, 4)
	_, err = conn.Read(echo)
	require.NoError(t, err)
	require.Equal(t, "ping", string(echo))
	require.NoError(t, <-accepted)
}

// With mTLS required, a peer that presents no certificate must be rejected.
func TestTLSStreamLayerRejectsUnauthenticatedPeer(t *testing.T) {
	cfg := tlstestPKI(t, true)
	layer, err := newTLSStreamLayer("127.0.0.1:0", nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = layer.Close() })

	go func() {
		conn, err := layer.Accept()
		if err == nil {
			buf := make([]byte, 1)
			_, _ = conn.Read(buf) // drives the handshake so it can fail
			_ = conn.Close()
		}
	}()

	anonymous := &TLSConfig{ClientCAFile: cfg.ClientCAFile}
	clientCfg, err := anonymous.ClientConfig()
	require.NoError(t, err)

	conn, err := (&tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: clientCfg}).DialContext(context.Background(), "tcp", layer.Addr().String())
	if err == nil {
		defer func() { _ = conn.Close() }()
		_, err = conn.Write([]byte("x"))
		if err == nil {
			_, err = conn.Read(make([]byte, 1))
		}
	}
	require.Error(t, err, "a peer without a client certificate must not be accepted")
}

// The advertised address is what peers dial, so it must win over the bound one.
func TestTLSStreamLayerUsesAdvertiseAddr(t *testing.T) {
	cfg := tlstestPKI(t, false)
	advertise := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7123}
	layer, err := newTLSStreamLayer("127.0.0.1:0", advertise, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = layer.Close() })

	require.Equal(t, "127.0.0.1:7123", layer.Addr().String())
}

// An unroutable advertise address would publish "0.0.0.0" to every peer.
func TestTLSStreamLayerRejectsUnspecifiedAdvertiseAddr(t *testing.T) {
	cfg := tlstestPKI(t, false)
	_, err := newTLSStreamLayer("127.0.0.1:0", &net.TCPAddr{IP: net.IPv4zero, Port: 7124}, cfg)
	require.ErrorContains(t, err, "not routable")
}

func TestTLSConfigRequiresCAForClientCerts(t *testing.T) {
	cfg := tlstestPKI(t, true)
	cfg.ClientCAFile = ""
	_, err := cfg.ServerConfig()
	require.ErrorContains(t, err, "client ca file")
}

func TestGRPCCredentialsBuildFromTLSConfig(t *testing.T) {
	creds, err := tlstestPKI(t, false).GRPCCredentials()
	require.NoError(t, err)
	require.Equal(t, "tls", creds.Info().SecurityProtocol)
}
