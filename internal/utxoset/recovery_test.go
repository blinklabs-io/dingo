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
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func slotPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
}

// applySlots applies one output per slot in [1, last], each its own commit.
func applySlots(t *testing.T, set *utxoset.Set, last uint64) {
	t.Helper()
	for slot := uint64(1); slot <= last; slot++ {
		tip := slotPoint(slot)
		require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{{
			Point: tip,
			Added: []ledger.UtxoOutput{{
				Ref: models.UtxoId{
					Hash: bytes.Repeat([]byte{byte(slot)}, 32),
				},
				Cbor: []byte{0x80},
			}},
		}}))
	}
}

func openRecoverySet(
	t *testing.T,
	cfg utxoset.Config,
) *utxoset.Set {
	t.Helper()
	cfg.Dir = t.TempDir()
	set, err := utxoset.Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	return set
}

func ledgerAt(t *testing.T, tip ocommon.Point) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))
	return db
}

func neverResume(context.Context, ocommon.Point) (bool, error) {
	return false, nil
}

// A set ahead of the ledger, as after an offline truncate or a restore, is
// rolled back from its journal when the ledger tip is a point the journal
// holds, and left alone with an error when the set is the only copy and the
// tip is not one.
func TestReconcileRollsASetAheadOfTheLedgerBack(t *testing.T) {
	t.Parallel()
	const applied = 8
	for _, tc := range []struct {
		name      string
		cfg       utxoset.Config
		ledgerTip ocommon.Point
		prune     uint64
		wantSlot  uint64
		wantCount uint64
		wantErr   string
	}{
		{
			name:      "only, journal point",
			cfg:       utxoset.Config{Only: true},
			ledgerTip: slotPoint(5),
			wantSlot:  5, wantCount: 5,
		},
		{
			name:      "set reads, journal point",
			cfg:       utxoset.Config{Authoritative: true},
			ledgerTip: slotPoint(3),
			wantSlot:  3, wantCount: 3,
		},
		{
			name: "only, tip on another fork",
			cfg:  utxoset.Config{Only: true},
			ledgerTip: ocommon.NewPoint(
				5, bytes.Repeat([]byte{0xee}, 32),
			),
			wantSlot: applied, wantCount: applied,
			wantErr: "ahead of",
		},
		{
			name:      "only, below the journal",
			cfg:       utxoset.Config{Only: true},
			ledgerTip: slotPoint(2),
			prune:     6,
			wantSlot:  applied, wantCount: applied,
			wantErr: "journal does not reach",
		},
		{
			name:      "not authoritative, rebuilt from the store",
			cfg:       utxoset.Config{},
			ledgerTip: slotPoint(5),
			wantSlot:  5, wantCount: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openRecoverySet(t, tc.cfg)
			applySlots(t, set, applied)
			if tc.prune != 0 {
				require.NoError(t, set.PruneHistory(tc.prune))
			}
			db := ledgerAt(t, tc.ledgerTip)
			err := set.Reconcile(t.Context(), dbadapter.New(db), neverResume)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.ErrorContains(t, err, "resync")
			} else {
				require.NoError(t, err)
			}
			tip, ok, err := set.Tip()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.wantSlot, tip.Slot)
			n, err := set.Count()
			require.NoError(t, err)
			require.Equal(t, tc.wantCount, n)
		})
	}
}

// A failure mark rewinds the set to before the failed commit and has the
// ledger resume from there, even when the set's tip is already at the ledger
// tip, and the mark is gone once the set is back in step.
func TestReconcileRewindsASetThatFailedAnUpdate(t *testing.T) {
	t.Parallel()
	const applied = 5
	for _, tc := range []struct {
		name       string
		markSlot   uint64
		wantResume *uint64
		wantTip    uint64
	}{
		{"set took the failed commit", 4, new(uint64(3)), 3},
		{"set never took it", applied + 3, nil, applied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openRecoverySet(t, utxoset.Config{Only: true})
			applySlots(t, set, applied)
			require.NoError(t, set.MarkUtxoSetFailed(tc.markSlot))
			db := ledgerAt(t, slotPoint(applied))
			var resumedFrom *ocommon.Point
			resume := func(_ context.Context, p ocommon.Point) (bool, error) {
				resumedFrom = &p
				return true, nil
			}
			require.NoError(t, set.Reconcile(
				t.Context(), dbadapter.New(db), resume,
			))
			if tc.wantResume == nil {
				require.Nil(t, resumedFrom)
			} else {
				require.NotNil(t, resumedFrom, "the ledger did not resume")
				require.Equal(t, *tc.wantResume, resumedFrom.Slot)
			}
			tip, _, err := set.Tip()
			require.NoError(t, err)
			require.Equal(t, tc.wantTip, tip.Slot)
			_, marked, err := set.FailedAt()
			require.NoError(t, err)
			require.False(t, marked, "the failure mark outlived the resume")
		})
	}
}

// A failure mark the journal cannot rewind stops a set that is the only copy
// with a resync error, and keeps the mark.
func TestReconcileRefusesAFailedSetItCannotRewind(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{Only: true})
	applySlots(t, set, 6)
	require.NoError(t, set.PruneHistory(5))
	require.NoError(t, set.MarkUtxoSetFailed(2))
	db := ledgerAt(t, slotPoint(6))
	err := set.Reconcile(t.Context(), dbadapter.New(db), neverResume)
	require.ErrorContains(t, err, "resync")
	slot, marked, err := set.FailedAt()
	require.NoError(t, err)
	require.True(t, marked)
	require.Equal(t, uint64(2), slot)
}
