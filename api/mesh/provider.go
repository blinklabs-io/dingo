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
	"log/slog"
	"net"
	"strconv"

	"github.com/blinklabs-io/dingo/plugin"
)

type ProviderConfig struct {
	Port uint `yaml:"port"`
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
		plugin.Descriptor{Capability: plugin.CapabilityAPIMesh, Name: "builtin", Description: "built-in Mesh-compatible Rosetta HTTP API"},
		func() ProviderConfig { return ProviderConfig{Port: 8080} },
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Server, plugin.Instance, error) {
			server, err := NewServer(ServerConfig{
				Logger: deps.Logger, LedgerState: deps.LedgerState,
				Database: deps.Database, Chain: deps.Chain, Mempool: deps.Mempool,
				ListenAddress: net.JoinHostPort(deps.Host, strconv.FormatUint(uint64(cfg.Port), 10)),
				Network:       deps.Network, NetworkMagic: deps.NetworkMagic,
				GenesisHash: deps.GenesisHash, GenesisStartTimeSec: deps.GenesisStartTimeSec,
				CORSAllowedOrigins: deps.CORSAllowedOrigins,
			})
			if err != nil {
				return nil, nil, err
			}
			return server, server, nil
		},
	)
}
