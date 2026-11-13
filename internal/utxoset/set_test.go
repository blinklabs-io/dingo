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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The metadata store here is empty, so a rebuild empties the set and a kept
// set still holds the output applied to it.
func TestReconcileKeepsASetAtTheLedgerTipAndResumesOrRebuildsOneBehind(
	t *testing.T,
) {
	t.Parallel()
	tip := ocommon.NewPoint(5, bytes.Repeat([]byte{5}, 32))
	behind := ocommon.NewPoint(4, bytes.Repeat([]byte{4}, 32))
	for _, tc := range []struct {
		name       string
		setTip     ocommon.Point
		resumes    bool
		wantResume bool
		kept       bool
	}{
		{"at the tip", tip, true, false, true},
		{"behind, resumed", behind, true, true, true},
		{"behind, not resumable", behind, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(
				t,
				&database.Config{DataDir: t.TempDir()},
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
			require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))

			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			require.NoError(
				t,
				set.ApplyUtxoChanges(&tc.setTip, []ledger.UtxoEvent{{
					Added: []ledger.UtxoOutput{{
						Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{1}, 32)},
						Cbor: []byte{0x80},
					}},
				}}),
			)
			var resumedFrom *ocommon.Point
			resume := func(_ context.Context, p ocommon.Point) (bool, error) {
				resumedFrom = &p
				return tc.resumes, nil
			}
			require.NoError(
				t,
				set.Reconcile(t.Context(), dbadapter.New(db), resume),
			)
			if tc.wantResume {
				require.NotNil(t, resumedFrom)
				require.Equal(t, tc.setTip, *resumedFrom)
			} else {
				require.Nil(t, resumedFrom)
			}
			n, err := set.Count()
			require.NoError(t, err)
			if tc.kept {
				require.Equal(t, uint64(1), n)
			} else {
				require.Zero(t, n)
			}
		})
	}
}

// TestSetPrunesItsJournalBehindTheHistoryWindow applies enough commits to
// trigger a prune and requires a rollback within the window to work and one
// past it to be refused.
func TestSetPrunesItsJournalBehindTheHistoryWindow(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(
		utxoset.Config{Dir: t.TempDir(), HistorySlots: 100},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	for slot := uint64(1); slot <= 1200; slot++ {
		tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{{
			Point: tip,
			Added: []ledger.UtxoOutput{{
				Ref: models.UtxoId{
					Hash: bytes.Repeat([]byte{byte(slot)}, 32),
					Idx:  uint32(slot),
				},
				Cbor: []byte{0x80},
			}},
		}}))
	}
	_, err = set.Rollback(ocommon.NewPoint(500, bytes.Repeat([]byte{1}, 32)))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	back := uint64(1150)
	_, err = set.Rollback(
		ocommon.NewPoint(back, bytes.Repeat([]byte{byte(back)}, 32)),
	)
	require.NoError(t, err)
	n, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(1150), n)
}

// TestReconcileWithoutAMetadataUtxoSetRefusesToRebuild: when the set is the
// only copy, a set that cannot be resumed is an error, not an empty rebuild.
func TestReconcileWithoutAMetadataUtxoSetRefusesToRebuild(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	tip := ocommon.NewPoint(5, bytes.Repeat([]byte{5}, 32))
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	behind := ocommon.NewPoint(4, bytes.Repeat([]byte{4}, 32))
	require.NoError(t, set.ApplyUtxoChanges(&behind, []ledger.UtxoEvent{{
		Point: behind,
		Added: []ledger.UtxoOutput{{
			Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{9}, 32), Idx: 0},
			Cbor: []byte{0x80},
		}},
	}}))
	err = set.Reconcile(
		t.Context(),
		dbadapter.New(db),
		func(context.Context, ocommon.Point) (bool, error) { return false, nil },
	)
	require.Error(t, err)
	n, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(1), n, "the refused rebuild must not clear the set")
	setTip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, behind.Slot, setTip.Slot, "the set is left as it was")
}
