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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// TestCatchUpMergeRetiresAStalePoolInTheSnapshotsEpoch requires a merge to
// schedule the retirement of a pool the snapshot does not list in the epoch
// that holds the snapshot's point, not in a later epoch the metadata store
// holds a row for.
func TestCatchUpMergeRetiresAStalePoolInTheSnapshotsEpoch(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	applySlotsWithNonces(t, set, 1, 4)
	pool := bytes.Repeat([]byte{0x61}, 28)
	tip := slotPoint(5)
	require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
		Pools: []ledger.PoolChange{{Slot: 5, State: ledger.PoolState{Pool: pool}}},
	}))
	require.NoError(t, set.LoadUtxoSetCatchUp(slotPoint(6), catchUpFeed(0xa1)))
	db := ledgerAt(t, slotPoint(6))
	for _, e := range []struct{ start, id uint64 }{{0, 3}, {10, 4}} {
		require.NoError(t, db.SetEpoch(
			e.start, e.id, nil, nil, nil, nil, 6, 1000, 10, nil,
		))
	}

	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))

	st, err := set.PoolState(pool)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.NotNil(t, st.RetiringEpoch, "the stale pool is scheduled to retire")
	require.Equal(t, uint64(3), *st.RetiringEpoch)
}
