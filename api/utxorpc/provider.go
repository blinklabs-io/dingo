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
	"log/slog"

	"github.com/blinklabs-io/dingo/plugin"
)

type ProviderConfig struct {
	Port uint `yaml:"port"`
}

type ProviderDependencies struct {
	Logger             *slog.Logger
	EventBus           UtxorpcEventBus
	LedgerState        UtxorpcLedgerState
	Mempool            UtxorpcMempool
	Host               string
	TLSCertFilePath    string
	TLSKeyFilePath     string
	CORSAllowedOrigins []string
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{Capability: plugin.CapabilityAPIUtxorpc, Name: "builtin", Description: "built-in UTxO RPC Connect server"},
		func() ProviderConfig { return ProviderConfig{Port: 9090} },
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (*Utxorpc, plugin.Instance, error) {
			server := NewUtxorpc(UtxorpcConfig{
				Logger: deps.Logger, EventBus: deps.EventBus,
				LedgerState: deps.LedgerState, Mempool: deps.Mempool,
				Host: deps.Host, Port: cfg.Port,
				TlsCertFilePath:    deps.TLSCertFilePath,
				TlsKeyFilePath:     deps.TLSKeyFilePath,
				CORSAllowedOrigins: deps.CORSAllowedOrigins,
			})
			return server, server, nil
		},
	)
}
