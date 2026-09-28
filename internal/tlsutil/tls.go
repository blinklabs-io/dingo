// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
)

// ServerConfig applies the minimum TLS version policy for server connections.
func ServerConfig(config *tls.Config) *tls.Config {
	if config == nil {
		return &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	floor := uint16(tls.VersionTLS12)
	// Encrypted Client Hello is a TLS 1.3-only extension; Go requires
	// MinVersion == VersionTLS13 whenever ECH keys are configured, whether
	// supplied statically or via the dynamic callback (which takes priority
	// over the static field when both are set).
	if len(config.EncryptedClientHelloKeys) > 0 ||
		config.GetEncryptedClientHelloKeys != nil {
		floor = tls.VersionTLS13
	}
	if config.MinVersion < floor {
		config.MinVersion = floor
	}
	// Keep an explicit MaxVersion from making the raised floor unusable.
	if config.MaxVersion != 0 && config.MaxVersion < config.MinVersion {
		config.MaxVersion = config.MinVersion
	}
	// GetConfigForClient, when set, supersedes this config for the
	// connection's handshake. Wrap it so any config it selects also gets the
	// same floor applied, or a per-client override could reintroduce TLS 1.0/1.1.
	// The config it returns "may not be subsequently modified" (crypto/tls
	// docs) and may be a shared/cached object returned to concurrent
	// connections, so clone before mutating rather than editing it in place.
	if orig := config.GetConfigForClient; orig != nil {
		config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			selected, err := orig(hello)
			if err != nil || selected == nil {
				return selected, err
			}
			return ServerConfig(selected.Clone()), nil
		}
	}
	return config
}

// ConfigureServerTLS loads the certificate/key pair at certFilePath/
// keyFilePath and installs it on server's TLSConfig (creating one via
// ServerConfig if server.TLSConfig is nil), applying the shared minimum
// TLS version floor. It is the single keypair-loading implementation
// shared by every built-in API provider (Blockfrost, Mesh, UTxORPC),
// replacing what was previously duplicated tls.LoadX509KeyPair-plus-floor
// logic per provider. Callers must still call server.ServeTLS (not
// Serve) after this returns successfully -- this only prepares the
// config, it does not decide whether the caller wants a TLS listener at
// all (see EffectiveTLS.Enabled in package apiconfig).
func ConfigureServerTLS(
	server *http.Server,
	certFilePath, keyFilePath string,
) error {
	cert, err := tls.LoadX509KeyPair(certFilePath, keyFilePath)
	if err != nil {
		return fmt.Errorf("loading TLS keypair: %w", err)
	}
	server.TLSConfig = ServerConfig(server.TLSConfig)
	// Assign a fresh single-element slice rather than appending: every
	// caller loads exactly one keypair, and appending to whatever
	// Certificates already held could silently accumulate a stale entry
	// (from a reused/reinitialized *http.Server or a pre-populated
	// TLSConfig) alongside the new one, letting Go's SNI cert selection
	// pick the wrong certificate.
	server.TLSConfig.Certificates = []tls.Certificate{cert}
	return nil
}

// LoadClientCAPool reads a PEM-encoded CA bundle from path and returns an
// x509.CertPool built from it, for use as a tls.Config's ClientCAs — the
// trust anchor against which a server verifies client (mTLS) certificates.
// Returns an error if the file can't be read or contains no valid PEM
// certificates, rather than silently returning an empty (trust-nobody) pool.
func LoadClientCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading client CA file %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf(
			"no valid PEM certificates found in client CA file %q",
			path,
		)
	}
	return pool, nil
}
