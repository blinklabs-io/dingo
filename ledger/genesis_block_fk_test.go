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
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
)

// TestCreateGenesisBlockFileBackedNoFKError drives the real genesis sync path
// (createGenesisBlock -> database.SetGenesisTransaction -> UtxoLedgerToModel ->
// metadata SetGenesisTransaction) on a file-backed SQLite store, where
// foreign_keys=ON is enforced.
//
// Genesis UTxOs are unspent/unreferenced, so the utxo columns spent_at_tx_id /
// referenced_by_tx_id / collateral_by_tx_id (FKs to transaction(hash)) must be
// stored as SQL NULL. If they are bound as an empty blob, the FK fails with
// "FOREIGN KEY constraint failed (787)". This is the failure reported for
// BURSA_SYNC=genesis on preview/devnet.
//
// It also re-runs createGenesisBlock to confirm idempotency.
func TestCreateGenesisBlockFileBackedNoFKError(t *testing.T) {
	networks := []struct {
		name       string
		configPath string
	}{
		{name: "preview", configPath: "preview/config.json"},
		{name: "devnet", configPath: "devnet/config.json"},
	}

	for _, nw := range networks {
		t.Run(nw.name, func(t *testing.T) {
			// File-backed store: enables foreign_keys(1) + WAL, the
			// production configuration that matches the reported failure.
			db, err := database.New(&database.Config{
				BlobPlugin:     "badger",
				MetadataPlugin: "sqlite",
				DataDir:        t.TempDir(),
			})
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() }) //nolint:errcheck

			nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
				nw.configPath,
				nw.name,
				cardano.EmbeddedConfigFS,
			)
			require.NoError(t, err)

			ls := &LedgerState{
				db: db,
				config: LedgerStateConfig{
					Database:          db,
					CardanoNodeConfig: nodeCfg,
					Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
				},
			}

			// First run: must not hit the FK 787 error.
			require.NoError(
				t,
				ls.createGenesisBlock(),
				"createGenesisBlock should not fail with FK constraint",
			)

			store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
			require.True(t, ok, "metadata store should be sqlite")

			// At least one genesis UTxO should have been created so the
			// assertions below are meaningful.
			var utxoCount int64
			require.NoError(
				t,
				store.DB().Model(&models.Utxo{}).Count(&utxoCount).Error,
			)
			require.Greater(t, utxoCount, int64(0), "genesis UTxOs created")

			// The hash FK columns must be stored as NULL, never empty blobs.
			var nonNull int64
			require.NoError(
				t,
				store.DB().Model(&models.Utxo{}).
					Where("spent_at_tx_id IS NOT NULL OR referenced_by_tx_id IS NOT NULL OR collateral_by_tx_id IS NOT NULL").
					Count(&nonNull).Error,
			)
			require.Equal(
				t,
				int64(0),
				nonNull,
				"genesis UTxOs must store NULL hash FKs, not empty blobs",
			)

			// Second run: idempotent, still no error.
			require.NoError(
				t,
				ls.createGenesisBlock(),
				"re-running createGenesisBlock must remain idempotent",
			)
		})
	}
}
