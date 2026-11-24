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
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// pendingImportNode is a node whose LedgerDB set an import began to write at
// slot 10 and did not finish.
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
	n := &Node{db: db, utxoSet: set}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return n
}

// TestFinishPendingImportLeavesAnUnfinishedImportToItsOwner requires a set an
// import stopped writing to be refused, so nothing serves a partial ledger
// state, and a re-import the node began to be run again through the set's
// Mithril source.
func TestFinishPendingImportLeavesAnUnfinishedImportToItsOwner(t *testing.T) {
	t.Parallel()
	t.Run("a Mithril sync's import", func(t *testing.T) {
		t.Parallel()
		n := pendingImportNode(t)
		require.NoError(t, n.db.SetChainState("sync_status", "in_progress", nil))
		err := n.finishPendingImport(t.Context())
		require.ErrorIs(t, err, badgerutxo.ErrImportPending)
	})
	t.Run("a finished import", func(t *testing.T) {
		t.Parallel()
		n := pendingImportNode(t)
		require.NoError(t, n.utxoSet.FinishImport())
		require.NoError(t, n.finishPendingImport(t.Context()))
	})
	t.Run("the node's own re-import", func(t *testing.T) {
		t.Parallel()
		n := pendingImportNode(t)
		require.NoError(t, n.db.SetChainState("sync_status", "in_progress", nil))
		require.NoError(t, n.db.SetChainState(database.LedgerReimportSyncKey, "1", nil))
		var asked []ocommon.Point
		n.utxoSet.SetReimporter(func(
			_ context.Context, tip ocommon.Point,
		) (ocommon.Point, error) {
			asked = append(asked, tip)
			return tip, nil
		})
		n.finishLedgerReimport = func(context.Context, ocommon.Point) error {
			return nil
		}
		require.NoError(t, n.finishPendingImport(t.Context()))
		require.Len(t, asked, 1)
		require.Equal(t, uint64(10), asked[0].Slot)
	})
}

// TestFinishPendingImportResumesAReimportPastItsImportStep requires a start
// that finds the node's re-import marks still set after the import finished to
// complete the run from where it stopped: record the Mithril anchor and clear
// the marks when the swap mark names the ledger's tip, and run the re-import
// again when the swap did not happen or the set at the tip is one the run
// never imported into.
func TestFinishPendingImportResumesAReimportPastItsImportStep(t *testing.T) {
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
			require.NoError(t, n.utxoSet.FinishImport())
			if tc.setSlot != 10 {
				other := ocommon.Point{Slot: tc.setSlot, Hash: bytes.Repeat([]byte{9}, 32)}
				require.NoError(t, n.utxoSet.LoadUtxoSet(other,
					func(func([]byte, uint32, []byte) error) error { return nil }))
			}
			require.NoError(t, n.db.SetChainState("sync_status", "in_progress", nil))
			require.NoError(t, n.db.SetChainState(database.LedgerReimportSyncKey, "1", nil))
			if tc.swapped {
				require.NoError(t, n.db.SetChainState(
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

			require.NoError(t, n.finishPendingImport(t.Context()))

			require.Len(t, asked, tc.wantAsked)
			require.Len(t, finished, tc.wantDone)
			if tc.wantDone == 1 {
				require.Equal(t, uint64(10), finished[0].Slot)
			}
		})
	}
}
