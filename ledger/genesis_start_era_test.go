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

package ledger

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGenesisStartEraHonoursScheduledDijkstraFork pins the era a fresh
// database starts in when the Shelley genesis protocol major is Dijkstra's.
// A scheduled Dijkstra hard fork keeps the epochs before it in Conway, as a
// reference node does, so the genesis major alone must not pick the era.
func TestGenesisStartEraHonoursScheduledDijkstraFork(t *testing.T) {
	t.Parallel()

	epochOne := uint64(1)
	epochZero := uint64(0)
	tests := []struct {
		name         string
		experimental bool
		dijkstraAt   *uint64
		wantEra      eras.EraDesc
	}{
		{"fork scheduled at epoch 1", true, &epochOne, eras.ConwayEraDesc},
		{"fork scheduled at epoch 0", true, &epochZero, eras.DijkstraEraDesc},
		{"no fork scheduled", true, nil, eras.DijkstraEraDesc},
		{
			"schedule ignored without experimental forks",
			false, &epochOne, eras.DijkstraEraDesc,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := cardano.NewCardanoNodeConfigFromFile(
				"../config/cardano/preview/config.json",
			)
			require.NoError(t, err)
			cfg.ShelleyGenesis().ProtocolParameters.ProtocolVersion.Major =
				gdijkstra.MinProtocolVersionDijkstra
			cfg.ShelleyGenesis().ProtocolParameters.ProtocolVersion.Minor = 0
			cfg.ExperimentalHardForksEnabled = &tc.experimental
			cfg.TestDijkstraHardForkAtEpoch = tc.dijkstraAt

			db := newTestDB(t)
			cm, err := chain.NewManager(context.Background(), db, nil)
			require.NoError(t, err)
			ls, err := NewLedgerState(LedgerStateConfig{
				Database:              db,
				ChainManager:          cm,
				CardanoNodeConfig:     cfg,
				Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
				PromRegistry:          prometheus.NewRegistry(),
				ManualBlockProcessing: true,
				EnableDijkstra:        true,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ls.Close()) })

			txn := db.Transaction(context.Background(), true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return ls.setEpochCache(context.Background(), txn, nil)
			}))

			assert.Equal(t, uint64(0), ls.currentEpoch.EpochId)
			assert.Equal(t, tc.wantEra.Id, ls.currentEpoch.EraId)
			assert.Equal(t, tc.wantEra.Id, ls.currentEra.Id)
		})
	}
}
