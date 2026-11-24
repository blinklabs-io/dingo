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
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// A kept set still holds the output applied to it.
func TestReconcileKeepsASetAtTheLedgerTipAndResumesOneBehind(
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
			require.NoError(t, set.StartEmptyForTest())
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
	// The set prunes only up to a recorded immutable tip; one far ahead
	// leaves the history window as the bound.
	set.UpdateAnchor(ocommon.NewPoint(1100, bytes.Repeat([]byte{0x44}, 32)))
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

func TestUtxoSetAtRejectsPointOutsideJournalLineage(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	tip := ocommon.NewPoint(10, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, set.ApplyUtxoChanges(&tip, nil))

	view, err := set.UtxoSetAt(
		ocommon.NewPoint(tip.Slot, bytes.Repeat([]byte{2}, 32)),
	)
	require.Nil(t, view)
	require.ErrorIs(t, err, ledger.ErrPointNotOnChain)
	require.ErrorIs(t, err, badgerutxo.ErrPointNotInJournal)
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

// TestRebuildReloadsPParamUpdates requires a rebuild from a v0.94.2 metadata
// store to replace the proposals the set held with the store's, which it does
// only if the rebuild clears the loaded marker.
func TestRebuildReloadsPParamUpdates(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	tip := ocommon.NewPoint(5, bytes.Repeat([]byte{5}, 32))
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))
	stored := bytes.Repeat([]byte{0xa1}, 28)
	archive := testutil.NewPayloadArchive()
	db.Metadata().(types.PayloadResolverSetter).SetPayloadResolver(archive)
	txn := db.Transaction(t.Context(), true)
	defer txn.Rollback() //nolint:errcheck
	txn.Metadata().(types.PayloadLocatorSetter).SetPayloadLocator(archive)
	require.NoError(t, db.SetPParamUpdate(stored, []byte{0xa0}, 3, 0, txn))
	require.NoError(t, txn.Commit())
	ldb := dbadapter.New(db)

	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	require.NoError(t, set.LoadLedgerState(t.Context(), ldb))
	behind := ocommon.NewPoint(4, bytes.Repeat([]byte{4}, 32))
	stale := models.PParamUpdate{
		GenesisHash: bytes.Repeat([]byte{0xb2}, 28),
		Cbor:        []byte{0xa1, 0x03, 0x01},
		AddedSlot:   4,
		Epoch:       7,
	}
	require.NoError(t, set.ApplyLedgerChanges(&behind, ledger.LedgerChanges{
		PParamUpdates: []ledger.PParamUpdateChange{{Update: stale}},
	}))
	updates := func() []models.PParamUpdate {
		t.Helper()
		var out []models.PParamUpdate
		require.NoError(t, set.PParamUpdates(func(u models.PParamUpdate) error {
			out = append(out, u)
			return nil
		}))
		return out
	}
	storedRow := models.PParamUpdate{
		GenesisHash: stored, Cbor: []byte{0xa0}, AddedSlot: 3, Epoch: 0,
	}
	require.Equal(t, []models.PParamUpdate{storedRow, stale}, updates())

	require.NoError(t, set.ReconcileReleased(t.Context(), ldb))
	require.Equal(t, []models.PParamUpdate{storedRow}, updates())
}

// TestSetLoadsGovernanceRowsWithTheStoresRowIDs loads a set from a metadata
// store holding proposals and votes and requires every row to carry the ID
// the store gave it, a vote's proposal ID included.
func TestSetLoadsGovernanceRowsWithTheStoresRowIDs(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	store := dbadapter.New(db)
	ctx := t.Context()
	proposals := make([]*models.GovernanceProposal, 0, 3)
	for i := range 3 {
		p := &models.GovernanceProposal{
			TxHash:      bytes.Repeat([]byte{byte(0x40 + i)}, 32),
			ActionIndex: uint32(i),
			ActionType:  1, ExpiresEpoch: 9, AddedSlot: uint64(10 + i),
			AnchorHash:    bytes.Repeat([]byte{1}, 32),
			ReturnAddress: bytes.Repeat([]byte{0xe0}, 29),
		}
		require.NoError(t, store.SetGovernanceProposal(ctx, p, nil))
		require.NotZero(t, p.ID)
		proposals = append(proposals, p)
	}
	votes := make([]*models.GovernanceVote, 0, 2)
	for i, voter := range []byte{0xd1, 0xd2} {
		v := &models.GovernanceVote{
			ProposalID:      proposals[2].ID,
			VoterType:       models.VoterTypeDRep,
			VoterCredential: bytes.Repeat([]byte{voter}, 28),
			Vote:            models.VoteYes, AddedSlot: uint64(20 + i),
		}
		require.NoError(t, store.SetGovernanceVote(ctx, v, nil))
		require.NotZero(t, v.ID)
		votes = append(votes, v)
	}

	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	require.NoError(t, set.LoadLedgerState(ctx, store))

	for _, want := range proposals {
		got, err := set.ProposalAt(want.TxHash, want.ActionIndex, nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, want.ID, got.ID)
	}
	got, err := set.VotesAt(proposals[2].TxHash, proposals[2].ActionIndex, nil)
	require.NoError(t, err)
	require.Len(t, got, len(votes))
	ids := map[uint]uint{}
	for _, v := range got {
		ids[v.ID] = v.ProposalID
	}
	for _, want := range votes {
		require.Equal(t, proposals[2].ID, ids[want.ID])
	}
}

// A live restore or truncate closes the set and opens it again on the same
// registry, so Close must release the metrics Open registered.
func TestSetReopensOnTheSameRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	dir := t.TempDir()
	for range 2 {
		set, err := utxoset.Open(utxoset.Config{Dir: dir, Registerer: reg})
		require.NoError(t, err)
		require.NoError(t, set.Close())
	}
}
