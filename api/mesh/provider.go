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

package mesh

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/plugin"
)

// defaultProviderPort is the TCP port the Mesh listener uses when the
// node's configuration does not set one.
const defaultProviderPort uint = 8080

// ProviderConfig's TLS and Auth fields are documented in ARCHITECTURE.md's
// "API security" section. Composition (node.go) merges the top-level
// api.tls/api.auth defaults into these fields before this provider ever
// decodes them, so from this package's point of view they are always
// already-resolved-for-this-provider settings, identical in shape to a
// provider that set every field inline.
type ProviderConfig struct {
	Port uint                 `yaml:"port"`
	TLS  apiconfig.TLSPolicy  `yaml:"tls"`
	Auth apiconfig.AuthPolicy `yaml:"auth"`
}

// providerDefaults returns the configuration the plugin host applies
// before decoding the node's own settings over it. RegisterProvider
// hands this to plugin.Register, so it is the single definition of the
// Mesh provider's defaults.
func providerDefaults() ProviderConfig {
	return ProviderConfig{Port: defaultProviderPort}
}

type ProviderDependencies struct {
	Logger              *slog.Logger
	LedgerState         MeshLedgerState
	Database            MeshDatabase
	Chain               MeshChain
	Mempool             MeshMempool
	Host                string
	Network             string
	NetworkMagic        uint32
	GenesisHash         string
	GenesisStartTimeSec int64
	CORSAllowedOrigins  []string
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityAPIMesh,
			Name:        "builtin",
			Description: "built-in Mesh-compatible Rosetta HTTP API",
		},
		providerDefaults,
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Server, plugin.Instance, error) {
			tls, err := cfg.TLS.Resolve("plugins.api.mesh.config.tls")
			if err != nil {
				return nil, nil, fmt.Errorf("mesh: %w", err)
			}
			auth, err := cfg.Auth.Resolve("plugins.api.mesh.config.auth")
			if err != nil {
				return nil, nil, fmt.Errorf("mesh: %w", err)
			}
			server, err := NewServer(ServerConfig{
				Logger: deps.Logger, LedgerState: deps.LedgerState,
				Database: deps.Database, Chain: deps.Chain, Mempool: deps.Mempool,
				ListenAddress: net.JoinHostPort(
					deps.Host,
					strconv.FormatUint(uint64(cfg.Port), 10),
				),
				Network: deps.Network, NetworkMagic: deps.NetworkMagic,
				GenesisHash: deps.GenesisHash, GenesisStartTimeSec: deps.GenesisStartTimeSec,
				CORSAllowedOrigins: deps.CORSAllowedOrigins,
				TLS:                tls,
				Auth:               auth,
			})
			if err != nil {
				return nil, nil, err
			}
			return server, server, nil
		},
	)
}
