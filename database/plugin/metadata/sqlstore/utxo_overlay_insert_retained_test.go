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

//nolint:gosec // SQL INTEGER mappings preserve the unsigned domain API.
package sqlstore

import (
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestSQLiteOverlayInsertRetainsClosureHistoryThroughBarriers(t *testing.T) {
	t.Parallel()
	for _, barrier := range []bool{false, true} {
		t.Run(fmt.Sprint(barrier), func(t *testing.T) {
			t.Parallel()
			seed, transactions := plainOverlayChain(t, 128)
			states := make([]string, 2)
			for i, limit := range []int{0, 2047} {
				store, metrics := newOverlayInsertStore(t, false, limit)
				store.storageMode = types.StorageModeAPI
				require.NoError(t, store.SetTransaction(seed,
					ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
					0, nil, true, nil, 0))
				txn := store.Transaction(t.Context())
				t.Cleanup(func() { _ = txn.Rollback() })
				acc := store.NewLeiosClosureBatchAccumulator()
				enabled, err := store.EnableLeiosClosureUtxoOverlay(
					acc,
					txn,
					transactions,
				)
				require.NoError(t, err)
				require.True(t, enabled)
				for index, transaction := range transactions {
					slot := uint64(99)
					require.NoError(t, store.SetTransactionLeiosClosureBatched(
						transaction,
						ocommon.Point{Slot: 100, Hash: overlayTxID(0x53, 0)},
						uint32(index), nil, true, &slot, acc, txn, 0))
				}
				overlay := acc.(*transactionBatchAccumulator).overlay
				if barrier {
					last := transactions[len(transactions)-1]
					stored, err := store.GetTransactionByHash(
						last.Hash().Bytes(),
						txn,
					)
					require.NoError(t, err)
					require.NotNil(t, stored)
					output, err := store.GetUtxo(last.Hash().Bytes(), 0, txn)
					require.NoError(t, err)
					require.NotNil(t, output)
					require.Positive(
						t,
						overlay.stats.BarrierFlushes,
						"transaction read must flush retained output rows before the UTxO assertion",
					)
				} else {
					require.Zero(t, overlay.stats.BarrierFlushes)
				}
				require.NoError(t, store.FlushBatch(acc, txn))
				require.NoError(t, txn.Commit())
				wantStatements := uint64(2)
				if limit != 0 {
					wantStatements = 1
				}
				require.Equal(
					t,
					wantStatements,
					overlayInsertStatementCount(t, metrics),
				)
				states[i] = retainedOverlayAPIState(t, store)
			}
			require.Equal(t, states[0], states[1])
		})
	}
}
