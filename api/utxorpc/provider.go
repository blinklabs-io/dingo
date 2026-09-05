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

package utxorpc

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/plugin"
)

// ProviderConfig's TLS and Auth fields are documented in ARCHITECTURE.md's
// "API security" section. Composition (node.go) merges the top-level
// api.tls/api.auth defaults into these fields before this provider ever
// decodes them, so from this package's point of view they are always
// already-resolved-for-this-provider settings, identical in shape to a
// provider that set every field inline.
type ProviderConfig struct {
	Port uint `yaml:"port"`
	// Host overrides the shared API bind address (apiBindAddr) for this
	// listener alone. Unset means "use the shared default", which is
	// loopback -- see ARCHITECTURE.md's "API security" section.
	Host string               `yaml:"host"`
	TLS  apiconfig.TLSPolicy  `yaml:"tls"`
	Auth apiconfig.AuthPolicy `yaml:"auth"`
}

type ProviderDependencies struct {
	Logger             *slog.Logger
	EventBus           UtxorpcEventBus
	LedgerState        UtxorpcLedgerState
	Mempool            UtxorpcMempool
	Host               string
	CORSAllowedOrigins []string
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityAPIUtxorpc,
			Name:        "builtin",
			Description: "built-in UTxO RPC Connect server",
		},
		func() ProviderConfig { return ProviderConfig{Port: 9090} },
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Utxorpc, plugin.Instance, error) {
			tls, err := cfg.TLS.Resolve("plugins.api.utxorpc.config.tls")
			if err != nil {
				return nil, nil, fmt.Errorf("utxorpc: %w", err)
			}
			auth, err := cfg.Auth.Resolve("plugins.api.utxorpc.config.auth")
			if err != nil {
				return nil, nil, fmt.Errorf("utxorpc: %w", err)
			}
			server := NewUtxorpc(UtxorpcConfig{
				Logger: deps.Logger, EventBus: deps.EventBus,
				LedgerState: deps.LedgerState, Mempool: deps.Mempool,
				Host: apiconfig.ListenHost(cfg.Host, deps.Host),
				Port: cfg.Port,
				TLS:  tls, Auth: auth,
				CORSAllowedOrigins: deps.CORSAllowedOrigins,
			})
			return server, server, nil
		},
	)
}
