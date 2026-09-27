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

package ledger

import (
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// A Mithril bootstrap anchored mid-epoch seeds the imported epoch's own
// RewardAdaPots row with ImportedEpochFees (the fees collected up to and
// including the anchor block) and a CapturedSlot at the anchor. The node's
// locally stored transactions for that epoch only cover slots after the
// anchor -- plus, once the historical backfill (#4061) has run, slots at or
// before it too. saveRewardAdaPotsForEpoch must sum the local fees strictly
// after the anchor and add the imported amount, not sum the whole epoch:
// summing the whole epoch either silently drops the pre-anchor fees (the
// defect in dingo #3975) or double-counts them once backfill has stored
// pre-anchor transactions locally.
func TestSaveRewardAdaPotsForEpochUsesImportedPreAnchorFees(t *testing.T) {
	t.Parallel()
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		endedEpoch           = uint64(5)
		epochStartSlot       = uint64(1000)
		epochLengthInSlots   = uint(100) // slots [1000, 1099]
		anchorSlot           = uint64(1050)
		importedPreAnchor    = uint64(1_000_000)
		postAnchorFee        = uint64(500_000)
		preAnchorBackfillFee = uint64(300_000)
		newEpochBoundarySlot = uint64(1100)
	)

	// Simulates seedImportedRewardBasis's write for the anchor epoch: the
	// pots row this epoch's own boundary would have produced, had the node
	// been running, carrying the pre-anchor fee pot the import derived from
	// State.Fees - snapshots.Fee.
	importedFees := types.Uint64(importedPreAnchor)
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:             endedEpoch,
		CapturedSlot:      anchorSlot,
		ImportedEpochFees: &importedFees,
	}, nil))

	// A transaction at the anchor slot itself: excluded, because the
	// imported amount already accounts for fees up to and including the
	// anchor block. Sum range is (CapturedSlot, epochEnd].
	rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
		[]byte("pre-anchor-backfill-tx"), []byte("pre-anchor-block"),
		anchorSlot, strconv.FormatUint(preAnchorBackfillFee, 10),
	)
	// A transaction after the anchor: included.
	rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (2, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
		[]byte("post-anchor-tx"), []byte("post-anchor-block"),
		anchorSlot+30, strconv.FormatUint(postAnchorFee, 10),
	)

	ended := models.Epoch{
		EpochId:       endedEpoch,
		StartSlot:     epochStartSlot,
		LengthInSlots: epochLengthInSlots,
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.saveRewardAdaPotsForEpoch(
			txn, endedEpoch+1, ended, newEpochBoundarySlot,
		)
	}))

	pots, err := meta.GetRewardAdaPots(endedEpoch+1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(
		t,
		importedPreAnchor+postAnchorFee,
		uint64(pots.Fees),
		"fees for the epoch after an imported anchor epoch must be the "+
			"imported pre-anchor amount plus only the post-anchor local sum",
	)
}

// The imported pots row's CapturedSlot is the anchor block's slot, which can
// be any slot of its epoch, including the first and the last. The anchor
// block's own fees are part of ImportedEpochFees at both ends, so the local
// sum must exclude the anchor slot and still add the imported amount.
func TestSaveRewardAdaPotsForEpochImportedAnchorAtEpochEdges(t *testing.T) {
	t.Parallel()
	const (
		endedEpoch         = uint64(5)
		epochStartSlot     = uint64(1000)
		epochLengthInSlots = uint(100) // slots [1000, 1099]
		epochEndSlot       = uint64(1099)
		importedPreAnchor  = uint64(1_000_000)
		anchorBlockFee     = uint64(300_000)
		laterFee           = uint64(500_000)
	)
	tests := []struct {
		name       string
		anchorSlot uint64
		laterSlot  uint64
		want       uint64
	}{
		{
			name:       "anchor at first slot",
			anchorSlot: epochStartSlot,
			laterSlot:  epochStartSlot + 1,
			want:       importedPreAnchor + laterFee,
		},
		{
			name:       "anchor at last slot",
			anchorSlot: epochEndSlot,
			want:       importedPreAnchor,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, db := newRewardCalculationTestLedger(t)
			meta := db.Metadata()
			importedFees := types.Uint64(importedPreAnchor)
			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:             endedEpoch,
				CapturedSlot:      tc.anchorSlot,
				ImportedEpochFees: &importedFees,
			}, nil))
			// A backfilled copy of the anchor block's transaction, already
			// counted in ImportedEpochFees.
			rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
				[]byte("anchor-tx"), []byte("anchor-block"),
				tc.anchorSlot, strconv.FormatUint(anchorBlockFee, 10),
			)
			if tc.laterSlot != 0 {
				rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (2, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
					[]byte("later-tx"), []byte("later-block"),
					tc.laterSlot, strconv.FormatUint(laterFee, 10),
				)
			}
			ended := models.Epoch{
				EpochId:       endedEpoch,
				StartSlot:     epochStartSlot,
				LengthInSlots: epochLengthInSlots,
			}
			txn := db.Transaction(true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return ls.saveRewardAdaPotsForEpoch(
					txn, endedEpoch+1, ended, epochEndSlot+1,
				)
			}))
			pots, err := meta.GetRewardAdaPots(endedEpoch+1, nil)
			require.NoError(t, err)
			require.NotNil(t, pots)
			require.Equal(t, tc.want, uint64(pots.Fees))
		})
	}
}
