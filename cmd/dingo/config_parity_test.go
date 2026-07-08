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

package main

import (
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/config"
)

// TestChainsyncStrategyWhitelistParity guards against drift between the
// chainsync.strategy values internal/config.Validate accepts and those
// chainsync.ParseHeaderSyncStrategy actually accepts. internal/config
// cannot import chainsync without pulling node subsystems into the
// config package, so this parity check lives in cmd/dingo, which can
// import both.
func TestChainsyncStrategyWhitelistParity(t *testing.T) {
	for _, strategy := range config.AcceptedChainsyncStrategies {
		if _, err := chainsync.ParseHeaderSyncStrategy(strategy); err != nil {
			t.Errorf(
				"config accepts chainsync.strategy %q but "+
					"ParseHeaderSyncStrategy rejects it: %v",
				strategy, err,
			)
		}
	}
}

// TestMithrilBackendWhitelistParity guards against drift between the
// mithril.backend values internal/config.Validate accepts and those
// resolveMithrilBackend actually accepts.
func TestMithrilBackendWhitelistParity(t *testing.T) {
	for _, backend := range config.AcceptedMithrilBackends {
		if _, err := resolveMithrilBackend(backend); err != nil {
			t.Errorf(
				"config accepts mithril.backend %q but "+
					"resolveMithrilBackend rejects it: %v",
				backend, err,
			)
		}
	}
}
