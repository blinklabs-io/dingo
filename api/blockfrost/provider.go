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

package blockfrost

import (
	"context"
	"log/slog"
	"net"
	"strconv"

	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/blinklabs-io/dingo/plugin"
)

type ProviderConfig struct {
	Port uint `yaml:"port"`
}

// ProviderDependencies carries resolved, instance-owned settings from
// composition (internal/config.ResolveAPISecurity merges the shared api:
// policy with this provider's own plugins.api.blockfrost.config.tls/auth
// overrides before Run passes the result here) -- this package does not
// perform that merge itself.
type ProviderDependencies struct {
	Node               BlockfrostNode
	Logger             *slog.Logger
	Host               string
	CORSAllowedOrigins []string
	TLSCertFilePath    string
	TLSKeyFilePath     string
	AuthMode           apiauth.Mode
	AuthTokenFilePath  string
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityAPIBlockfrost,
			Name:        "builtin",
			Description: "built-in Blockfrost-compatible HTTP API",
		},
		func() ProviderConfig { return ProviderConfig{Port: 3000} },
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Blockfrost, plugin.Instance, error) {
			server := New(BlockfrostConfig{
				ListenAddress: net.JoinHostPort(
					deps.Host,
					strconv.FormatUint(uint64(cfg.Port), 10),
				),
				CORSAllowedOrigins: deps.CORSAllowedOrigins,
				TLSCertFilePath:    deps.TLSCertFilePath,
				TLSKeyFilePath:     deps.TLSKeyFilePath,
				Auth: apiauth.Policy{
					Mode:          deps.AuthMode,
					TokenFilePath: deps.AuthTokenFilePath,
				},
			}, deps.Node, deps.Logger)
			return server, server, nil
		},
	)
}
