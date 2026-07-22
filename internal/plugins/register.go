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

// Package plugins contains application-composition registration for all
// providers compiled into this binary.
package plugins

import (
	"fmt"

	"github.com/blinklabs-io/dingo/api/blockfrost"
	"github.com/blinklabs-io/dingo/api/mesh"
	"github.com/blinklabs-io/dingo/api/utxorpc"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/plugin"
)

// NewHost constructs a host and explicitly registers all built-in providers.
func NewHost() (*plugin.Host, error) {
	host := plugin.NewHost()
	if err := badger.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register badger provider: %w", err)
	}
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register sqlite provider: %w", err)
	}
	if err := mempool.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register mempool provider: %w", err)
	}
	if err := blockfrost.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register blockfrost provider: %w", err)
	}
	if err := mesh.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register mesh provider: %w", err)
	}
	if err := utxorpc.RegisterProvider(host); err != nil {
		return nil, fmt.Errorf("register utxorpc provider: %w", err)
	}
	if err := registerExtra(host); err != nil {
		return nil, err
	}
	return host, nil
}
