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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/blinklabs-io/dingo/plugin"
)

// RegisterProvider registers the built-in MCP provider with the plugin host.
func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityAPIMcp,
			Name:        "builtin",
			Description: "built-in Model Context Protocol (MCP) server for SQLite and Cardano introspection",
		},
		func() ProviderConfig {
			return DefaultProviderConfig()
		},
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Server, plugin.Instance, error) {
			tls, err := cfg.TLS.Resolve("plugins.api.mcp.config.tls")
			if err != nil {
				return nil, nil, fmt.Errorf("mcp: %w", err)
			}
			listenHost := deps.Host
			if cfg.Host != "" {
				listenHost = cfg.Host
			}
			listenAddress := net.JoinHostPort(
				listenHost,
				strconv.FormatUint(uint64(cfg.Port), 10),
			)
			server, err := NewServer(cfg, deps, tls, listenAddress)
			if err != nil {
				return nil, nil, fmt.Errorf("mcp: %w", err)
			}
			return server, server, nil
		},
	)
}
