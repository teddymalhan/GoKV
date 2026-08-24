package grpcapi

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// TLSConfig describes the server's transport security. The same field names are
// used by cluster.TLSConfig for the Raft transport so a single set of CLI flags
// configures both.
type TLSConfig struct {
	// CertFile and KeyFile hold the server's PEM-encoded certificate chain and
	// private key. Both are required.
	CertFile string
	KeyFile  string
	// ClientCAFile is the PEM bundle used to verify client certificates.
	// Setting it makes client certificates verifiable; combine with
	// RequireClientCert for mutual TLS.
	ClientCAFile string
	// RequireClientCert rejects connections that do not present a certificate
	// signed by ClientCAFile.
	RequireClientCert bool
	// MinVersion is a crypto/tls version constant. It defaults to TLS 1.2 and
	// may not be set lower.
	MinVersion uint16
}

// Credentials builds transport credentials from the configuration.
func (c TLSConfig) Credentials() (credentials.TransportCredentials, error) {
	cfg, err := c.serverTLSConfig()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}

func (c TLSConfig) serverTLSConfig() (*tls.Config, error) {
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, errors.New("tls: cert file and key file are required")
	}
	minVersion := c.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}
	if minVersion < tls.VersionTLS12 {
		return nil, fmt.Errorf("tls: min version %#x is below TLS 1.2", minVersion)
	}

	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load key pair: %w", err)
	}
	// #nosec G402 -- minVersion is validated to be >= TLS 1.2 above.
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVersion,
		ClientAuth:   tls.NoClientCert,
	}

	if c.ClientCAFile != "" {
		pem, err := os.ReadFile(c.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls: client CA %s contains no certificates", c.ClientCAFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	if c.RequireClientCert {
		if cfg.ClientCAs == nil {
			return nil, errors.New("tls: client CA file is required when client certificates are required")
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
