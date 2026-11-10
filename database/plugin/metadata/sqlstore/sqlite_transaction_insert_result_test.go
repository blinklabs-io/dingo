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

package sqlstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteTransactionInsertKeepsConflictIdentity(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"unbatched", "batch", "closure"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			store := newAPIModeSQLiteStore(t, nil)
			_, err := store.writeDB.Exec(
				`CREATE TABLE transaction_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id INTEGER)`,
			)
			require.NoError(t, err)
			_, err = store.writeDB.Exec(
				`INSERT INTO transaction_audit(id) VALUES(1000)`,
			)
			require.NoError(t, err)
			_, err = store.writeDB.Exec(
				`CREATE TRIGGER audit_transaction AFTER INSERT ON "transaction" BEGIN INSERT INTO transaction_audit(transaction_id) VALUES(new.id); END`,
			)
			require.NoError(t, err)
			insert := store.insertTransactionUnbatched
			var acc *transactionBatchAccumulator
			if mode != "unbatched" {
				factory := store.NewBatchAccumulator
				if mode == "closure" {
					factory = store.NewLeiosClosureBatchAccumulator
				}
				acc = factory().(*transactionBatchAccumulator)
				defer acc.Reset()
				insert = func(ctx context.Context, db queryer, args ...any) (int64, bool, error) {
					id, fresh, err := acc.insertTransaction(ctx, db, args...)
					return int64(id), fresh, err
				}
			}
			txn := store.Transaction(t.Context())
			t.Cleanup(func() { _ = txn.Rollback() })
			db, ctx, err := store.dbFromTxn(txn)
			require.NoError(t, err)
			args := []any{
				[]byte{1},
				[]byte{11},
				nil,
				1,
				0,
				"3",
				"4",
				"5",
				6,
				true,
			}
			id, fresh, err := insert(ctx, db, args...)
			require.NoError(t, err)
			require.True(t, fresh)
			require.Positive(t, id)
			var auditID int64
			require.NoError(
				t,
				db.QueryRowContext(ctx, `SELECT transaction_id FROM transaction_audit WHERE id=1001`).
					Scan(&auditID),
			)
			require.Equal(
				t,
				id,
				auditID,
				"a trigger's inserted row must not replace the transaction's identity",
			)
			other := append([]any(nil), args...)
			other[0] = []byte{2}
			otherID, fresh, err := insert(ctx, db, other...)
			require.NoError(t, err)
			require.True(t, fresh)
			require.NotEqual(t, id, otherID)
			replay := append([]any(nil), args...)
			replay[1] = []byte{21}
			replay[3] = 7
			replay[6] = "8"
			replay[8] = 9
			replayID, fresh, err := insert(ctx, db, replay...)
			require.NoError(t, err)
			require.False(
				t,
				fresh,
				"an ignored hash conflict must not reuse the preceding insert result",
			)
			require.Equal(t, id, replayID)
			var blockHash []byte
			var slot, index uint64
			var collateral string
			require.NoError(
				t,
				db.QueryRowContext(ctx, `SELECT block_hash,slot,collateral_fee,block_index FROM applied_transaction WHERE tx_id=?`, id).
					Scan(&blockHash, &slot, &collateral, &index),
			)
			require.Equal(t, []byte{21}, blockHash)
			require.Equal(t, uint64(7), slot)
			require.Equal(t, "8", collateral)
			require.Equal(t, uint64(9), index)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			other[0] = []byte{3}
			_, fresh, err = insert(canceled, db, other...)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, fresh)
			require.NoError(t, txn.Rollback())
			var count int
			require.NoError(
				t,
				store.readDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "transaction"`).
					Scan(&count),
			)
			require.Zero(
				t,
				count,
				"rollback must discard inserted and replayed rows",
			)
			if acc != nil {
				acc.Reset()
				txn = store.Transaction(t.Context())
				db, ctx, err = store.dbFromTxn(txn)
				require.NoError(t, err)
				_, fresh, err = insert(ctx, db, other...)
				require.NoError(t, err)
				require.True(t, fresh)
				require.NoError(t, txn.Rollback())
			}
		})
	}
}
