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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestBatchedPoolOpCertSequencesPreserveEveryObservation(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	pool := lcommon.PoolKeyHash{1}
	acc := store.NewBatchAccumulator()
	txn := store.Transaction(t.Context())

	require.NoError(t, store.UpdatePoolOpCertSequenceBatched(pool, 2, 10, acc, txn))
	require.NoError(t, store.UpdatePoolOpCertSequenceBatched(pool, 3, 20, acc, txn))
	require.NoError(t, store.UpdatePoolOpCertSequenceBatched(pool, 3, 30, acc, txn))
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	sequence, found, err := store.LatestPoolOpCertSequence(pool, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), sequence)
	var count int
	require.NoError(t, store.readDB.QueryRow(
		"SELECT COUNT(*) FROM pool_opcert_sequence WHERE pool_key_hash = ?",
		pool.Bytes(),
	).Scan(&count))
	require.Equal(t, 3, count)
}

func TestBatchedPoolOpCertSequenceRejectsStoredConflict(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	pool := lcommon.PoolKeyHash{2}
	require.NoError(t, store.UpdatePoolOpCertSequence(pool, 4, 10, nil))

	acc := store.NewBatchAccumulator()
	txn := store.Transaction(t.Context())
	require.NoError(t, store.UpdatePoolOpCertSequenceBatched(pool, 5, 10, acc, txn))
	err := store.FlushBatch(acc, txn)
	require.ErrorContains(t, err, "opcert counter conflict")
	require.NoError(t, txn.Rollback())

	sequence, found, err := store.LatestPoolOpCertSequence(pool, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(4), sequence)
}

func TestBatchedPoolOpCertSequenceRejectsQueuedConflict(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	pool := lcommon.PoolKeyHash{3}
	acc := store.NewBatchAccumulator()
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })

	require.NoError(t, store.UpdatePoolOpCertSequenceBatched(pool, 6, 10, acc, txn))
	err := store.UpdatePoolOpCertSequenceBatched(pool, 7, 10, acc, txn)
	require.ErrorContains(t, err, "opcert counter conflict")
}
