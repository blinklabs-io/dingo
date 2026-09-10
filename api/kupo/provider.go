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

package kupo

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/plugin"
)

// ProviderConfig is the plugin configuration decoded by plugin.Host.
type ProviderConfig struct {
	Port uint                 `yaml:"port"`
	TLS  apiconfig.TLSPolicy  `yaml:"tls"`
	Auth apiconfig.AuthPolicy `yaml:"auth"`
}

// ProviderDependencies are supplied by node composition.
type ProviderDependencies struct {
	Node               KupoNode
	Logger             *slog.Logger
	Host               string
	CORSAllowedOrigins []string
}

// RegisterProvider registers the built-in Kupo-compatible provider.
func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityAPIKupo,
			Name:        "builtin",
			Description: "built-in Kupo-compatible HTTP API",
		},
		func() ProviderConfig { return ProviderConfig{Port: 1442} },
		func(
			_ context.Context,
			cfg ProviderConfig,
			deps ProviderDependencies,
		) (*Server, plugin.Instance, error) {
			tls, err := cfg.TLS.Resolve("plugins.api.kupo.config.tls")
			if err != nil {
				return nil, nil, fmt.Errorf("kupo: %w", err)
			}
			auth, err := cfg.Auth.Resolve("plugins.api.kupo.config.auth")
			if err != nil {
				return nil, nil, fmt.Errorf("kupo: %w", err)
			}
			server := New(Config{
				ListenAddress: net.JoinHostPort(
					deps.Host,
					strconv.FormatUint(uint64(cfg.Port), 10),
				),
				CORSAllowedOrigins: deps.CORSAllowedOrigins,
				TLS:                tls,
				Auth:               auth,
			}, deps.Node, deps.Logger)
			return server, server, nil
		},
	)
}
