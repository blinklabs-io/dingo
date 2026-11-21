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

package dingo

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// pendingImportNode is a node whose database drained the nonces family and
// whose import into the set wrote the SQL nonce rows, then stopped.
func pendingImportNode(t *testing.T) *Node {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	tip := ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)}
	require.NoError(t, set.LoadUtxoSet(tip,
		func(func([]byte, uint32, []byte) error) error { return nil }))
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))
	began, err := db.BeginDrainedImport(t.Context())
	require.NoError(t, err)
	require.True(t, began)
	require.NoError(t, db.SetBlockNonce(
		tip.Hash, tip.Slot, bytes.Repeat([]byte{2}, 32), true, nil,
	))
	db.EndDrainedImport(t.Context())
	n := &Node{db: db, utxoSet: set}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return n
}

func countNonceRows(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	var n int
	require.NoError(t, raw.QueryRow(`SELECT count(*) FROM block_nonce`).Scan(&n))
	return n
}

// TestFinishDrainedImportMovesAFinishedImportIntoTheSet requires a start that
// finds an import whose SQL phases completed to load the set, delete the rows
// again and clear the marker, keeping the latch.
func TestFinishDrainedImportMovesAFinishedImportIntoTheSet(t *testing.T) {
	t.Parallel()
	n := pendingImportNode(t)
	require.Equal(t, 1, countNonceRows(t, n.db))

	require.NoError(t, n.finishDrainedImport(t.Context()))

	require.Zero(t, countNonceRows(t, n.db), "the SQL rows are deleted again")
	_, pending, err := n.db.DrainedImportPending(t.Context())
	require.NoError(t, err)
	require.False(t, pending)
	loaded, err := n.utxoSet.NoncesLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	_, ok, err := n.utxoSet.LatestBlockNonce()
	require.NoError(t, err)
	require.True(t, ok, "the set holds the imported nonce")
	mask, err := n.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyNonces))
}

// TestFinishDrainedImportLeavesAnUnfinishedImportToItsOwner requires an import
// still writing SQL rows to be refused, and a re-import the node began to be
// run again through the set's Mithril source.
func TestFinishDrainedImportLeavesAnUnfinishedImportToItsOwner(t *testing.T) {
	t.Parallel()
	t.Run("a Mithril sync's import", func(t *testing.T) {
		t.Parallel()
		n := pendingImportNode(t)
		require.NoError(t, n.db.SetSyncState("sync_status", "in_progress", nil))
		err := n.finishDrainedImport(t.Context())
		require.ErrorIs(t, err, database.ErrDrainedImportPending)
		require.Equal(t, 1, countNonceRows(t, n.db), "nothing is deleted")
	})
	t.Run("the node's own re-import", func(t *testing.T) {
		t.Parallel()
		n := pendingImportNode(t)
		require.NoError(t, n.db.SetSyncState("sync_status", "in_progress", nil))
		require.NoError(t, n.db.SetSyncState(database.LedgerReimportSyncKey, "1", nil))
		var asked []ocommon.Point
		n.utxoSet.SetReimporter(func(
			_ context.Context, tip ocommon.Point,
		) (ocommon.Point, error) {
			asked = append(asked, tip)
			return tip, nil
		})
		require.NoError(t, n.finishDrainedImport(t.Context()))
		require.Len(t, asked, 1)
		require.Equal(t, uint64(10), asked[0].Slot)
	})
}

// TestFinishDrainedImportResumesAReimportPastItsImportStep requires a start
// that finds the node's re-import marks still set after the import finished to
// complete the run from where it stopped: record the Mithril anchor and clear
// the marks when the swap mark names the ledger's tip, and run the re-import
// again when the swap did not happen or the set at the tip is one the run
// never imported into.
func TestFinishDrainedImportResumesAReimportPastItsImportStep(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		setSlot   uint64
		swapped   bool
		wantAsked int
		wantDone  int
	}{
		{name: "the set holds the imported state", setSlot: 10, swapped: true, wantDone: 1},
		{name: "the set is at the tip with no swap recorded", setSlot: 10, wantAsked: 1},
		{name: "the swap did not happen", setSlot: 7, wantAsked: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := pendingImportNode(t)
			require.NoError(t, n.finishDrainedImport(t.Context()))
			_, pending, err := n.db.DrainedImportPending(t.Context())
			require.NoError(t, err)
			require.False(t, pending, "the import step has finished")
			if tc.setSlot != 10 {
				other := ocommon.Point{Slot: tc.setSlot, Hash: bytes.Repeat([]byte{9}, 32)}
				require.NoError(t, n.utxoSet.LoadUtxoSet(other,
					func(func([]byte, uint32, []byte) error) error { return nil }))
			}
			require.NoError(t, n.db.SetSyncState("sync_status", "in_progress", nil))
			require.NoError(t, n.db.SetSyncState(database.LedgerReimportSyncKey, "1", nil))
			if tc.swapped {
				require.NoError(t, n.db.SetSyncState(
					database.LedgerReimportSwappedSyncKey,
					database.LedgerReimportPointMark(ocommon.Point{
						Slot: 10, Hash: bytes.Repeat([]byte{1}, 32),
					}),
					nil,
				))
			}
			var asked []ocommon.Point
			n.utxoSet.SetReimporter(func(
				_ context.Context, tip ocommon.Point,
			) (ocommon.Point, error) {
				asked = append(asked, tip)
				return tip, nil
			})
			var finished []ocommon.Point
			n.finishLedgerReimport = func(_ context.Context, p ocommon.Point) error {
				finished = append(finished, p)
				return nil
			}

			require.NoError(t, n.finishDrainedImport(t.Context()))

			require.Len(t, asked, tc.wantAsked)
			require.Len(t, finished, tc.wantDone)
			if tc.wantDone == 1 {
				require.Equal(t, uint64(10), finished[0].Slot)
			}
		})
	}
}
