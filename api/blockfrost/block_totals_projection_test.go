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
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type blockTotalsLegacyStore struct {
	metadata.MetadataStore
	hydratedReads atomic.Uint64
}

func (s *blockTotalsLegacyStore) GetTransactionsByBlockHash(
	hash []byte, txn types.Txn,
) ([]models.Transaction, error) {
	s.hydratedReads.Add(1)
	return s.MetadataStore.GetTransactionsByBlockHash(hash, txn)
}

type blockTotalsProjectionStore struct {
	*blockTotalsLegacyStore
	projection metadata.BlockTransactionTotalsStore
	failure    error
	reads      atomic.Uint64
}

func (s *blockTotalsProjectionStore) GetBlockTransactionTotals(
	hash []byte, txn types.Txn,
) (models.BlockTransactionTotals, error) {
	s.reads.Add(1)
	if s.failure != nil {
		return models.BlockTransactionTotals{}, s.failure
	}
	return s.projection.GetBlockTransactionTotals(hash, txn)
}

func newBlockTotalsProjectionAdapter(
	t *testing.T,
	projected bool,
) (*NodeAdapter, *sql.DB, *blockTotalsLegacyStore, *blockTotalsProjectionStore) {
	t.Helper()
	var legacy *blockTotalsLegacyStore
	var projection *blockTotalsProjectionStore
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t, dbtest.Options{Config: &database.Config{DataDir: t.TempDir()}},
		func(store metadata.MetadataStore) metadata.MetadataStore {
			legacy = &blockTotalsLegacyStore{MetadataStore: store}
			if !projected {
				return legacy
			}
			capability, ok := store.(metadata.BlockTransactionTotalsStore)
			require.True(t, ok)
			projection = &blockTotalsProjectionStore{
				blockTotalsLegacyStore: legacy, projection: capability,
			}
			return projection
		},
	)
	require.NoError(t, err)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database: db, ChainManager: cm,
	})
	require.NoError(t, err)
	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return adapter, raw, legacy, projection
}

func TestNodeAdapterBlockTotalsProjectionAndLegacyProvider(t *testing.T) {
	t.Parallel()
	for _, projected := range []bool{false, true} {
		name := "legacy"
		if projected {
			name = "projection"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, raw, legacy, projection := newBlockTotalsProjectionAdapter(t, projected)
			hash := fill32(0xa5)
			insertAdapterTransaction(t, raw, &models.Transaction{
				Hash: fill32(1), BlockHash: hash, Valid: true, Fee: 7,
				Outputs: []models.Utxo{{TxId: fill32(1), Amount: 11}},
			})
			output, fees, err := adapter.blockOutputAndFees(t.Context(), hash)
			require.NoError(t, err)
			require.Equal(t, "11", output)
			require.Equal(t, "7", fees)
			if projected {
				require.Equal(t, uint64(1), projection.reads.Load())
				require.Zero(t, legacy.hydratedReads.Load())
				failure := errors.New("amount projection unavailable")
				projection.failure = failure
				_, _, err := adapter.blockOutputAndFees(t.Context(), hash)
				require.ErrorIs(t, err, failure)
				require.Zero(t, legacy.hydratedReads.Load())
			} else {
				require.Equal(t, uint64(1), legacy.hydratedReads.Load())
			}
		})
	}
}

func TestNodeAdapterBlockTotalsCancellation(t *testing.T) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := adapter.blockOutputAndFees(ctx, fill32(0xa7))
	require.ErrorIs(t, err, context.Canceled)
}

func TestNodeAdapterBlockOutputAndFeesRetainsOutputHistory(t *testing.T) {
	t.Parallel()

	for _, projected := range []bool{false, true} {
		name := "legacy"
		if projected {
			name = "projection"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, raw, legacy, projection := newBlockTotalsProjectionAdapter(t, projected)
			hash := fill32(0xb5)
			for i, valid := range []bool{true, false, false} {
				id := fill32(byte(i + 1))
				tx := &models.Transaction{
					Hash: id, BlockHash: hash, Valid: valid, Fee: 7,
					Outputs: []models.Utxo{{TxId: id, Amount: 11}},
				}
				if i < 2 {
					tx.CollateralReturn = &models.Utxo{
						TxId: fill32(byte(i + 21)), Amount: 19,
					}
				}
				insertAdapterTransaction(t, raw, tx)
			}
			insertAdapterTransaction(t, raw, &models.Transaction{
				Hash: fill32(40), BlockHash: fill32(0xc5), Valid: true, Fee: 99,
				Outputs: []models.Utxo{{TxId: fill32(40), Amount: 99}},
			})
			// Block totals retain produced outputs after they have been spent.
			_, err := raw.Exec(`UPDATE utxo SET deleted_slot = 100`)
			require.NoError(t, err)
			output, fees, err := adapter.blockOutputAndFees(t.Context(), hash)
			require.NoError(t, err)
			require.Equal(t, "41", output)
			require.Equal(t, "21", fees)
			// Legacy nullable validity is decoded as false by the hydrated path.
			_, err = raw.Exec(`UPDATE applied_transaction SET valid = NULL
WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?)`, fill32(2))
			require.NoError(t, err)
			output, fees, err = adapter.blockOutputAndFees(t.Context(), hash)
			require.NoError(t, err)
			require.Equal(t, "41", output)
			require.Equal(t, "21", fees)
			if projected {
				require.Equal(t, uint64(2), projection.reads.Load())
				require.Zero(t, legacy.hydratedReads.Load())
			} else {
				require.Equal(t, uint64(2), legacy.hydratedReads.Load())
			}
		})
	}
}
