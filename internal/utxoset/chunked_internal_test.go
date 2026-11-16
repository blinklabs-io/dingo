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

package utxoset

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func chunkTestPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
}

// A chunked batch that stopped before its last transaction leaves outputs
// and journal records past a tip that never moved, and its failure mark.
// Reconcile has to undo them, though the tip is already the point before the
// batch, so the ledger replays onto the set it expects.
func TestReconcileRewindsAChunkedBatchStoppedBeforeItsTip(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	for slot := uint64(1); slot <= 3; slot++ {
		tip := chunkTestPoint(slot)
		_, err := set.store.Apply(tip, []badgerutxo.Output{{
			Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{byte(slot)}, 32), 0),
			Cbor: []byte{0x80},
		}}, nil)
		require.NoError(t, err)
	}
	// What a chunk leaves: changes and their journal records, no tip.
	var changes []badgerutxo.Change
	for slot := uint64(4); slot <= 5; slot++ {
		changes = append(changes, badgerutxo.Change{
			Point: chunkTestPoint(slot),
			Added: []badgerutxo.Output{{
				Ref: badgerutxo.NewRef(
					bytes.Repeat([]byte{byte(slot)}, 32), 0,
				),
				Cbor: []byte{0x80},
			}},
		})
	}
	_, err = set.store.ApplyBatch(nil, changes)
	require.NoError(t, err)
	require.NoError(t, set.store.MarkFailed(4))
	n, err := set.Count()
	require.NoError(t, err)
	require.EqualValues(t, 5, n)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: chunkTestPoint(5)}, nil))
	var resumedFrom *ocommon.Point
	resume := func(_ context.Context, p ocommon.Point) (bool, error) {
		resumedFrom = &p
		return true, nil
	}
	require.NoError(t, set.Reconcile(t.Context(), dbadapter.New(db), resume))
	require.NotNil(t, resumedFrom, "the ledger did not resume")
	require.EqualValues(t, 3, resumedFrom.Slot)
	n, err = set.Count()
	require.NoError(t, err)
	require.EqualValues(t, 3, n, "outputs of the unfinished batch remain")
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 3, tip.Slot)
}
