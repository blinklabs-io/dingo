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

package ledgerstate

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// importLedgerStateForTest runs ImportLedgerState through the ledger's
// import LedgerDB over cfg.UtxoSet, as the Mithril import builds it.
func importLedgerStateForTest(ctx context.Context, cfg ImportConfig) error {
	return ImportLedgerState(ctx, withImportLedgerDB(cfg))
}

func withImportLedgerDB(cfg ImportConfig) ImportConfig {
	if cfg.LedgerDB != nil || cfg.Database == nil {
		return cfg
	}
	if w, ok := cfg.UtxoSet.(ledger.UtxoSetWriter); ok {
		cfg.LedgerDB = ledger.NewImportLedgerDB(cfg.Database, w, cfg.Logger)
	}
	return cfg
}

// importTargetForTest gives cfg the LedgerDB set of cfg.Database in this test,
// marked for an import of cfg's snapshot as LoadUtxoSet leaves it, and the
// import LedgerDB over it, for a test that runs single import phases.
func importTargetForTest(t *testing.T, cfg ImportConfig) ImportConfig {
	t.Helper()
	set, ok := cfg.UtxoSet.(*utxoset.Set)
	if cfg.UtxoSet == nil || ok {
		if !ok {
			set = importSetFor(t, cfg.Database)
		}
		_, pending, err := set.ImportPending()
		require.NoError(t, err)
		if !pending {
			var point ocommon.Point
			if cfg.State != nil && cfg.State.Tip != nil {
				point = ocommon.Point{
					Slot: cfg.State.Tip.Slot, Hash: cfg.State.Tip.BlockHash,
				}
			}
			require.NoError(t, set.LoadUtxoSet(
				point,
				func(func([]byte, uint32, []byte) error) error { return nil },
			))
		}
		cfg.UtxoSet = set
	}
	return withImportLedgerDB(cfg)
}

// ledgerDBFor returns the import LedgerDB over db's set in this test, which
// reads what the import phases wrote.
func ledgerDBFor(t *testing.T, db *database.Database) ImportLedgerDB {
	t.Helper()
	return importTargetForTest(t, ImportConfig{Database: db}).LedgerDB
}

// importedBlockCounts reads the imported per-pool block counts of epoch from
// the set; known is false when none were imported for it.
func importedBlockCounts(
	t *testing.T,
	cfg ImportConfig,
	epoch uint64,
) (map[string]uint64, uint64, bool) {
	t.Helper()
	set, ok := cfg.UtxoSet.(*utxoset.Set)
	require.True(t, ok)
	rows, err := set.ImportedBlocks()
	require.NoError(t, err)
	e, known := rows[epoch]
	if !known {
		return nil, 0, false
	}
	return e.Pools, e.Total, true
}

// storedPParamsForTest returns the protocol parameters db's set holds for
// era at the latest epoch at or before epoch, the last written of that
// epoch's, or none.
func storedPParamsForTest(
	t *testing.T,
	db *database.Database,
	epoch uint64,
	era uint,
	_ ledgerdb.Txn,
) ([]models.PParams, error) {
	t.Helper()
	rows, err := ledgerDBFor(t, db).ListPParamsForEra(era, nil)
	if err != nil {
		return nil, err
	}
	var latest *models.PParams
	for i := range rows {
		if rows[i].Epoch <= epoch &&
			(latest == nil || rows[i].Epoch >= latest.Epoch) {
			latest = &rows[i]
		}
	}
	if latest == nil {
		return nil, nil
	}
	return []models.PParams{*latest}, nil
}
