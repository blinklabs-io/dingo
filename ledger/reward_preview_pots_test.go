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
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
)

// Preview's on-chain ADA pots, as reported by Koios
// (https://preview.koios.rest/api/v1/totals). Preview declares
// TestShelleyHardForkAtEpoch: 0, so epoch 0 is already Alonzo and every epoch
// boundary from 0->1 onward runs cardano-ledger's NEWEPOCH monetary expansion.
// Preview's genesis decentralisationParam is 1, so eta is 1 by definition and
// no stake rewards are distributed in these epochs: the whole reward pot is
// split between the treasury tax and the reserves refund.
const (
	previewGenesisReserves = uint64(15_000_000_000_000_000)
	previewMaxSupply       = uint64(45_000_000_000_000_000)

	// Epoch 0's fee pot is empty: nothing was collected before epoch 0.
	previewEpoch1Treasury = uint64(9_000_000_000_000)
	previewEpoch1Reserves = uint64(14_991_000_000_000_000)

	// Epoch 0 collected 437793 lovelace in fees, which the 1->2 boundary
	// folds into the reward pot.
	previewEpoch1Fees     = uint64(437_793)
	previewEpoch2Treasury = uint64(17_994_600_087_558)
	previewEpoch2Reserves = uint64(14_982_005_400_350_235)

	previewEpochLength = uint64(86_400)
)

// newPreviewRewardPotsTestLedger builds a LedgerState configured with
// Preview's Shelley genesis and seeds the epoch rows, protocol parameters and
// empty mark snapshots that the delayed reward calculation reads for the first
// two boundaries.
func newPreviewRewardPotsTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"epochLength": 86400,
		"maxLovelaceSupply": 45000000000000000,
		"securityParam": 432,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	ls := &LedgerState{
		db:         db,
		currentEra: eras.AlonzoEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	// Preview's genesis protocol parameters: rho 0.003, tau 0.2, d 1.
	pparams := &alonzo.AlonzoProtocolParameters{
		NOpt:             150,
		A0:               rewardCalcRat(3, 10),
		Rho:              rewardCalcRat(3, 1_000),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(1, 1),
		ProtocolMajor:    6,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	meta := db.Metadata()
	for _, epoch := range []uint64{0, 1} {
		startSlot := epoch * previewEpochLength
		require.NoError(t, meta.SetEpoch(
			startSlot,
			epoch,
			nil,
			nil,
			nil,
			nil,
			eras.AlonzoEraDesc.Id,
			1,
			uint(previewEpochLength),
			nil,
		))
		require.NoError(t, db.SetPParams(
			pparamsCbor,
			startSlot,
			epoch,
			eras.AlonzoEraDesc.Id,
			nil,
		))
		// Preview has no stake delegated to non-overlay pools in these
		// epochs, so the mark snapshot is empty. Epoch 0's is seeded at
		// startup by snapshot.Manager.CaptureGenesisSnapshot.
		require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch:           epoch,
			SnapshotType:    "mark",
			CapturedSlot:    startSlot,
			BoundarySlot:    startSlot,
			ProtocolVersion: 6,
		}, nil))
	}
	return ls, db
}

// TestApplyStakeRewardsPreviewEpoch1Pots pins the 0->1 boundary. cardano-ledger
// applies monetary expansion and the treasury tax at the first boundary of a
// network whose epoch 0 is already Shelley-era, with an empty fee pot and no
// distribution. Skipping that round leaves the treasury at 0 and the reserves
// at their genesis value, which is what dingo #3381 observed on Preview.
func TestApplyStakeRewardsPreviewEpoch1Pots(t *testing.T) {
	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(0, previewGenesisReserves, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Treasury:     0,
		Reserves:     types.Uint64(previewGenesisReserves),
		Fees:         0,
		CapturedSlot: 0,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 1, previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch1Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch1Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewEpoch2Pots pins the 1->2 boundary against the
// same Koios reference. It is seeded with the epoch-1 pots the previous
// boundary must produce, so it isolates the epoch-2 arithmetic from the
// epoch-1 seeding defect.
func TestApplyStakeRewardsPreviewEpoch2Pots(t *testing.T) {
	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(
		previewEpoch1Treasury, previewEpoch1Reserves, previewEpochLength, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        1,
		Treasury:     types.Uint64(previewEpoch1Treasury),
		Reserves:     types.Uint64(previewEpoch1Reserves),
		Fees:         types.Uint64(previewEpoch1Fees),
		CapturedSlot: previewEpochLength,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 2, 2*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch2Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch2Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewGenesisToEpoch2 chains both boundaries the way a
// genesis replay does: the 0->1 round, the epoch-1 ADA pots capture that
// records its result, then the 1->2 round that reads it back. Preview's epoch 0
// carries exactly two transactions, at slots 60 and 320, whose fees (200000 and
// 237793) are the 437793 the 1->2 boundary folds into the reward pot.
//
// This is the unit-level counterpart of dingo #3381's reproduction: the
// epoch-2 treasury and reserves must equal the Koios Preview reference values.
func TestApplyStakeRewardsPreviewGenesisToEpoch2(t *testing.T) {
	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(0, previewGenesisReserves, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Treasury:     0,
		Reserves:     types.Uint64(previewGenesisReserves),
		Fees:         0,
		CapturedSlot: 0,
	}, nil))

	// Preview's two epoch-0 transactions.
	_, err := rewardCalcSQLDB(t, db).Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES
    (1, ?, ?, 60, 5, '200000', '0', '0', 0, TRUE),
    (2, ?, ?, 320, 5, '237793', '0', '0', 0, TRUE)`,
		[]byte("preview-tx-0"), []byte("preview-block-0"),
		[]byte("preview-tx-1"), []byte("preview-block-1"),
	)
	require.NoError(t, err)

	epoch0, err := meta.GetEpoch(0, nil)
	require.NoError(t, err)
	require.NotNil(t, epoch0)

	// Boundary into epoch 1: apply the reward round, then capture the epoch-1
	// ADA pots the way processEpochRollover does.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := ls.applyStakeRewards(
			txn, 1, previewEpochLength,
		); err != nil {
			return err
		}
		return ls.saveRewardAdaPotsForEpoch(
			txn, 1, *epoch0, previewEpochLength,
		)
	}))

	pots1, err := meta.GetRewardAdaPots(1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots1)
	require.Equal(t, previewEpoch1Treasury, uint64(pots1.Treasury))
	require.Equal(t, previewEpoch1Reserves, uint64(pots1.Reserves))
	require.Equal(t, previewEpoch1Fees, uint64(pots1.Fees))

	// Boundary into epoch 2, reading the row the previous boundary wrote.
	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 2, 2*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch2Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch2Reserves, uint64(state.Reserves))
}
