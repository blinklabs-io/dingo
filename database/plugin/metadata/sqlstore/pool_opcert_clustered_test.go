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

func TestPoolOpCertSequenceIsClusteredByPoolCredentialAndSlot(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.Equal(t,
		[]string{"pool_key_id", "slot", "sequence"},
		tableColumnNames(t, store, "pool_opcert_sequence"),
		"the pool key is named by id and the primary key is the row")
	require.Equal(t,
		[]string{
			"idx_pool_opcert_sequence_pool_sequence",
			"idx_pool_opcert_sequence_slot",
		},
		tableIndexNames(t, store, "pool_opcert_sequence"))
	require.Equal(t, []string{"pool_key_id", "sequence"},
		indexColumnNames(t, store, "idx_pool_opcert_sequence_pool_sequence"))
	require.Equal(t, 1, dictionaryRows(t, store, `
SELECT COUNT(*) FROM sqlite_master
WHERE name = 'pool_opcert_sequence' AND sql LIKE '%WITHOUT ROWID%'`))
}

// TestPoolOpCertSequenceHoldsEachPoolKeyOnce writes the counters of two pools
// through both writers and checks the dictionary holds each pool key once, a
// key a utxo already named included, and every reader returns the hash.
func TestPoolOpCertSequenceHoldsEachPoolKeyOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	poolA, poolB := lcommon.PoolKeyHash{0xa1}, lcommon.PoolKeyHash{0xb2}
	// The same hash as a payment credential is one dictionary row.
	importCredentialUtxos(t, store,
		credentialUtxo(1, poolA.Bytes(), nil, nil, 0))
	for slot, sequence := range []uint64{1, 1, 2} {
		require.NoError(t, store.UpdatePoolOpCertSequence(
			poolA, sequence, uint64(10*(slot+1)), nil))
	}
	require.NoError(t, store.UpdatePoolOpCertSequence(poolB, 5, 15, nil))

	require.Equal(
		t,
		2,
		dictionaryRows(t, store, `SELECT COUNT(*) FROM credential`),
	)
	require.Equal(t, 4, dictionaryRows(t, store,
		`SELECT COUNT(*) FROM pool_opcert_sequence`))
	latest, err := store.LatestPoolOpCertSequences(nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{
		string(poolA.Bytes()): 2, string(poolB.Bytes()): 5,
	}, latest)
	rows, err := store.GetPoolBlockIssuersInSlotRange(0, 100, nil)
	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, poolA.Bytes(), rows[0].PoolKeyHash)
	require.Equal(t, uint64(10), rows[0].Slot)
	require.Equal(t, poolB.Bytes(), rows[1].PoolKeyHash)
	require.Equal(t, uint64(15), rows[1].Slot)

	// A conflicting counter at a recorded (pool, slot) is still refused.
	err = store.UpdatePoolOpCertSequence(poolA, 9, 10, nil)
	require.ErrorContains(t, err, "opcert counter conflict")
}

// TestPoolOpCertSequenceReadersUseTheClusteredKey pins the plans of the
// per-pool readers: the pool hash is resolved once through the dictionary and
// the rows are read by the primary key or the (pool, sequence) index.
func TestPoolOpCertSequenceReadersUseTheClusteredKey(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	pool := lcommon.PoolKeyHash{9}
	for slot := uint64(1); slot <= 20; slot++ {
		require.NoError(
			t,
			store.UpdatePoolOpCertSequence(pool, slot, slot*10, nil),
		)
	}
	plan := opCertQueryPlan(
		t, store, poolOpCertSequenceBySlotSQL, pool.Bytes(), int64(100),
	)
	require.Contains(
		t,
		plan,
		"USING PRIMARY KEY (pool_key_id=? AND slot=?)",
		plan,
	)
	plan = opCertQueryPlan(
		t, store, PoolOpCertSequenceAtOrBeforeSQL, pool.Bytes(), int64(100),
	)
	require.Contains(
		t,
		plan,
		"USING PRIMARY KEY (pool_key_id=? AND slot<?)",
		plan,
	)
	plan = opCertQueryPlan(
		t, store, latestPoolOpCertSequenceAtOrBeforeSQL,
		pool.Bytes(), int64(100),
	)
	require.Contains(t, plan, "idx_pool_opcert_sequence_pool_sequence", plan)
	require.NotContains(t, plan, "SCAN pool_opcert_sequence", plan)
}
