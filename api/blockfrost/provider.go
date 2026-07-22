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

	"github.com/blinklabs-io/dingo/plugin"
)

type ProviderConfig struct {
	Port uint `yaml:"port"`
}

type ProviderDependencies struct {
	Node               BlockfrostNode
	Logger             *slog.Logger
	Host               string
	CORSAllowedOrigins []string
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{Capability: plugin.CapabilityAPIBlockfrost, Name: "builtin", Description: "built-in Blockfrost-compatible HTTP API"},
		func() ProviderConfig { return ProviderConfig{Port: 3000} },
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Blockfrost, plugin.Instance, error) {
			server := New(BlockfrostConfig{
				ListenAddress:      net.JoinHostPort(deps.Host, strconv.FormatUint(uint64(cfg.Port), 10)),
				CORSAllowedOrigins: deps.CORSAllowedOrigins,
			}, deps.Node, deps.Logger)
			return server, server, nil
		},
	)
}
