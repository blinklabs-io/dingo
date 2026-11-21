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
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type rebuildLiveState struct {
	Epochs   []models.Epoch
	Tip      ochainsync.Tip
	Treasury uint64
	Reserves uint64
	Proposal *models.GovernanceProposal
}

func (f *treasuryRolloverFixture) liveState(
	t *testing.T,
	proposal *models.GovernanceProposal,
) rebuildLiveState {
	t.Helper()
	epochs, err := f.db.GetEpochs(nil)
	require.NoError(t, err)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	treasury, reserves, _ := networkState(t, f.db)
	return rebuildLiveState{
		Epochs:   epochs,
		Tip:      tip,
		Treasury: treasury,
		Reserves: reserves,
		Proposal: f.proposal(t, proposal),
	}
}

func (f *treasuryRolloverFixture) boundaryRow(
	t *testing.T,
	epoch models.Epoch,
) (*models.GovernanceBoundarySnapshot, error) {
	t.Helper()
	return f.db.GetGovernanceBoundarySnapshotAtSlot(
		context.Background(), epoch.StartSlot, nil,
	)
}

// rebuildScenario is two live boundaries that ratify and then enact a
// treasury withdrawal, with their rows stored by the live rollover.
type rebuildScenario struct {
	f          *treasuryRolloverFixture
	withdrawal *models.GovernanceProposal
	epochs     []models.Epoch
	prev       []models.Epoch
	rows       []*models.GovernanceBoundarySnapshot
}

func newRebuildScenario(t *testing.T) *rebuildScenario {
	t.Helper()
	const ada = uint64(1_000_000)
	f := newTreasuryRolloverFixture(t, 100*ada)
	destination, destinationReturn, _ := f.rewardAddress(t, 0x91)
	require.NoError(t, f.db.Metadata().AddNetworkDonation(
		f.currentEpoch.StartSlot+10, f.currentEpoch.EpochId, 50*ada, nil,
	))
	withdrawal := f.addProposal(
		t, 0x92, f.currentEpoch.StartSlot+20,
		map[*lcommon.Address]uint64{destination: 120 * ada},
		destinationReturn, 1*ada, false,
	)
	// One DRep stays active and one passes its expiry across the boundaries;
	// each holds a delegator whose reward balance is its voting power.
	for i, expiry := range []uint64{
		f.currentEpoch.EpochId + 20,
		f.currentEpoch.EpochId + 1,
	} {
		credential := repeatByte(28, byte(0xd1+i))
		require.NoError(t, f.db.Metadata().CreateDrep(nil, &models.Drep{
			Credential:        credential,
			AddedSlot:         1,
			Active:            true,
			LastActivityEpoch: f.currentEpoch.EpochId,
			ExpiryEpoch:       expiry,
		}))
		require.NoError(t, f.db.CreateAccount(
			context.Background(), nil, &models.Account{
				StakingKey: repeatByte(28, byte(0xe1+i)),
				Drep:       credential,
				DrepType:   0,
				AddedSlot:  1,
				Reward:     types.Uint64((30 - 10*uint64(i)) * ada),
				Active:     true,
			},
		))
	}
	first := f.rollover(t, f.currentEpoch, f.currentPParams)
	second := f.rollover(t, first.NewCurrentEpoch, first.NewCurrentPParams)
	sc := &rebuildScenario{
		f:          f,
		withdrawal: withdrawal,
		epochs:     []models.Epoch{first.NewCurrentEpoch, second.NewCurrentEpoch},
		prev:       []models.Epoch{f.currentEpoch, first.NewCurrentEpoch},
	}
	for i, epoch := range sc.epochs {
		row, err := f.boundaryRow(t, epoch)
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, epoch.EpochId, row.Epoch)
		sc.rows = append(sc.rows, row)
		require.NoError(t, f.db.BlockCreate(models.Block{
			Slot:     epoch.StartSlot - 1,
			Hash:     sc.blockHash(i),
			PrevHash: repeatByte(32, byte(0xa0+i)),
			Number:   epoch.StartSlot,
			Type:     6,
			Cbor:     []byte{0x80},
		}, nil))
		require.NoError(t, f.db.SetBlockNonce(
			sc.blockHash(i), epoch.StartSlot-1, repeatByte(32, 0x11), false, nil,
		))
	}
	require.NotEmpty(t, sc.rows[0].Ratified, "the first boundary ratifies")
	require.NotEmpty(t, sc.rows[0].DRepDistribution)
	require.NoError(t, f.db.DeleteGovernanceBoundarySnapshotsAfterSlot(
		context.Background(), 0, nil,
	))
	for _, epoch := range sc.epochs {
		_, err := f.boundaryRow(t, epoch)
		require.ErrorIs(
			t, err, models.ErrGovernanceBoundarySnapshotNotRetained,
		)
	}
	require.NoError(t, f.db.SetTip(ochainsync.Tip{Point: ocommon.NewPoint(
		second.NewCurrentEpoch.StartSlot+5, repeatByte(32, 0xaa),
	)}, nil))
	f.ls.currentEpoch = second.NewCurrentEpoch
	return sc
}

func (sc *rebuildScenario) blockHash(i int) []byte {
	return repeatByte(32, byte(0xb0+i))
}

func (sc *rebuildScenario) point(i int) ocommon.Point {
	return ocommon.NewPoint(sc.epochs[i].StartSlot-1, sc.blockHash(i))
}

func (sc *rebuildScenario) rebuild(i int) error {
	return sc.f.ls.rebuildGovernanceBoundarySnapshotAt(
		context.Background(), sc.epochs[i], sc.prev[i], sc.point(i),
	)
}

// TestRebuildGovernanceBoundarySnapshotsMatchesLive deletes the rows of two
// live boundaries and checks that the rebuilt rows equal the stored ones, that
// the live ledger state stays as it was, and that a second pass changes
// nothing.
func TestRebuildGovernanceBoundarySnapshotsMatchesLive(t *testing.T) {
	t.Parallel()

	sc := newRebuildScenario(t)
	before := sc.f.liveState(t, sc.withdrawal)
	for pass := range 2 {
		for _, i := range []int{1, 0} {
			require.NoError(t, sc.rebuild(i))
		}
		for i, epoch := range sc.epochs {
			got, err := sc.f.boundaryRow(t, epoch)
			require.NoError(t, err)
			require.Equal(
				t, sc.rows[i], got, "epoch %d pass %d", epoch.EpochId, pass,
			)
		}
		require.Equal(t, before, sc.f.liveState(t, sc.withdrawal))
	}
}

// journalMissSet is a live UTxO set whose journal no longer reaches any
// rollback target.
type journalMissSet struct {
	*memUtxoSet
}

func (journalMissSet) PreviewUtxoSetRollback(
	ocommon.Point,
) (UtxoEvent, error) {
	return UtxoEvent{}, ErrUtxoAsOfUnavailable
}

func TestRebuildGovernanceBoundarySnapshotJournalMissKeepsSentinel(
	t *testing.T,
) {
	t.Parallel()

	sc := newRebuildScenario(t)
	before := sc.f.liveState(t, sc.withdrawal)
	sc.f.ls.config.UtxoSet = journalMissSet{&memUtxoSet{}}
	err := sc.rebuild(1)
	require.ErrorIs(t, err, errGovernanceBoundaryUnreachable)
	require.ErrorIs(t, err, ErrUtxoAsOfUnavailable)
	_, err = sc.f.boundaryRow(t, sc.epochs[1])
	require.ErrorIs(t, err, models.ErrGovernanceBoundarySnapshotNotRetained)
	require.Equal(t, before, sc.f.liveState(t, sc.withdrawal))
	require.False(t, sc.f.ls.utxoSetFailed.Load())
}

func TestRebuildGovernanceBoundarySnapshotsSkipsPendingBoundary(
	t *testing.T,
) {
	t.Parallel()

	sc := newRebuildScenario(t)
	rec := pendingRatificationRecord{
		Epoch:        sc.epochs[1].EpochId,
		BoundarySlot: sc.epochs[1].StartSlot,
		ID:           1,
	}
	raw, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, sc.f.db.SetSyncState(
		pendingRatificationSyncKey, string(raw), nil,
	))
	require.NoError(t, sc.f.ls.rebuildGovernanceBoundarySnapshots(
		context.Background(),
	))
	_, err = sc.f.boundaryRow(t, sc.epochs[1])
	require.ErrorIs(t, err, models.ErrGovernanceBoundarySnapshotNotRetained)
	got, err := sc.f.db.GetSyncState(pendingRatificationSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, string(raw), got)
	require.False(t, errors.Is(err, errGovernanceBoundaryUnreachable))
}
