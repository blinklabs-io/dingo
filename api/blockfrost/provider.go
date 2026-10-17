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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/plugin"
)

type ProviderConfig struct {
	Port                            uint                `yaml:"port"`
	Host                            string              `yaml:"host"`
	AuthToken                       string              `yaml:"authToken"`
	MaxConcurrentMutations          uint                `yaml:"maxConcurrentMutations"`
	MaxConcurrentMutationsPerClient uint                `yaml:"maxConcurrentMutationsPerClient"`
	TLS                             apiconfig.TLSPolicy `yaml:"tls"`
}

type ProviderDependencies struct {
	Node               BlockfrostNode
	Logger             *slog.Logger
	Host               string
	CORSAllowedOrigins []string
}

// DefaultProviderConfig returns the secure defaults for the built-in
// Blockfrost provider.
func DefaultProviderConfig() ProviderConfig {
	return ProviderConfig{
		Port:                            3000,
		Host:                            defaultListenHost,
		MaxConcurrentMutations:          defaultMaxConcurrentMutations,
		MaxConcurrentMutationsPerClient: defaultMaxConcurrentMutationsPerClient,
	}
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityAPIBlockfrost,
			Name:        "builtin",
			Description: "built-in Blockfrost-compatible HTTP API",
		},
		DefaultProviderConfig,
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Blockfrost, plugin.Instance, error) {
			tls, err := cfg.TLS.Resolve("plugins.api.blockfrost.config.tls")
			if err != nil {
				return nil, nil, fmt.Errorf("blockfrost: %w", err)
			}
			listenHost := cfg.Host
			if listenHost == "" {
				listenHost = deps.Host
			}
			listenHost = normalizeListenHost(listenHost)
			if cfg.MaxConcurrentMutations == 0 {
				return nil, nil, errors.New(
					"blockfrost: maxConcurrentMutations must be positive",
				)
			}
			if cfg.MaxConcurrentMutationsPerClient == 0 ||
				cfg.MaxConcurrentMutationsPerClient > cfg.MaxConcurrentMutations {
				return nil, nil, errors.New(
					"blockfrost: maxConcurrentMutationsPerClient must be positive and no greater than maxConcurrentMutations",
				)
			}
			if err := validateListenSecurity(listenHost, cfg.AuthToken, tls); err != nil {
				return nil, nil, fmt.Errorf("blockfrost: %w", err)
			}
			server := New(BlockfrostConfig{
				ListenAddress: net.JoinHostPort(
					listenHost,
					strconv.FormatUint(uint64(cfg.Port), 10),
				),
				AuthToken:                       cfg.AuthToken,
				MaxConcurrentMutations:          cfg.MaxConcurrentMutations,
				MaxConcurrentMutationsPerClient: cfg.MaxConcurrentMutationsPerClient,
				CORSAllowedOrigins:              deps.CORSAllowedOrigins,
				TLS:                             tls,
			}, deps.Node, deps.Logger)
			return server, server, nil
		},
	)
}

// normalizeListenHost strips one enclosing bracket pair so net.JoinHostPort,
// which adds its own brackets to IPv6 literals, does not double them.
func normalizeListenHost(host string) string {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return host[1 : len(host)-1]
	}
	return host
}

func validateListenSecurity(
	host string,
	authToken string,
	tls apiconfig.EffectiveTLS,
) error {
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	if authToken == "" || strings.EqualFold(host, "localhost") ||
		(ip != nil && ip.IsLoopback()) || tls.Enabled {
		return nil
	}
	return fmt.Errorf(
		"non-loopback Blockfrost mutation access on %q requires server TLS",
		host,
	)
}
