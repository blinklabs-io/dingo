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

import "crypto/tls"

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
