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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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
	epochs, err := f.ls.ledgerDB().GetEpochs(nil)
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
	return f.ls.GovernanceBoundarySnapshotAtSlot(epoch.StartSlot)
}

// memBoundarySnapshots keeps the boundary snapshots of a fixture in place of
// the live UTxO set's rows, so a scenario can drop them to model a database
// that holds none.
type memBoundarySnapshots struct {
	mu   sync.Mutex
	rows map[uint64]models.GovernanceBoundarySnapshot
}

func (m *memBoundarySnapshots) GovernanceBoundarySnapshotAt(
	epoch uint64,
	_ *uint64,
) (*models.GovernanceBoundarySnapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[epoch]
	if !ok {
		return nil, false, nil
	}
	return &row, true, nil
}

func (m *memBoundarySnapshots) save(s *models.GovernanceBoundarySnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rows == nil {
		m.rows = map[uint64]models.GovernanceBoundarySnapshot{}
	}
	m.rows[s.Epoch] = *s
}

func (m *memBoundarySnapshots) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = nil
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

// rebuildOptions varies how newRebuildScenarioWith builds the two live
// boundaries.
type rebuildOptions struct {
	// journalGap leaves the first boundary's preceding block out of the
	// set's journal, which then starts after it.
	journalGap bool
	// protocolMajor sets the protocol version the boundaries run under.
	protocolMajor uint
	// delegatorInactivity enables CIP-0163 with delegatorWindow epochs of
	// inactivity allowed.
	delegatorInactivity bool
	delegatorWindow     uint64
	// prepare runs before the first live boundary.
	prepare func(
		t *testing.T, f *treasuryRolloverFixture, withdrawal *models.GovernanceProposal,
	)
	// afterBoundary runs once the live boundary i has stored its row and
	// before the next one runs.
	afterBoundary func(
		t *testing.T, f *treasuryRolloverFixture, i int, epoch models.Epoch,
	)
}

func newRebuildScenario(t *testing.T) *rebuildScenario {
	t.Helper()
	return newRebuildScenarioWith(t, rebuildOptions{})
}

func newRebuildScenarioWith(
	t *testing.T,
	opts rebuildOptions,
) *rebuildScenario {
	t.Helper()
	const ada = uint64(1_000_000)
	f := newTreasuryRolloverFixture(t, 100*ada)
	rows := &memBoundarySnapshots{}
	f.ls.boundarySnapshotSeam = rows
	if opts.protocolMajor != 0 {
		f.currentPParams.ProtocolVersion.Major = opts.protocolMajor
	}
	if opts.delegatorInactivity {
		f.ls.config.DelegatorInactivityEnabled = true
		f.ls.config.DelegatorInactivity = opts.delegatorWindow
	}
	destination, destinationReturn, _ := f.rewardAddress(t, 0x91)
	require.NoError(t, seedDB(t, f.db).AddNetworkDonation(
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
		require.NoError(t, seedCreateDrep(t, f.db, context.Background(), nil, &models.Drep{
			Credential:        credential,
			AddedSlot:         1,
			Active:            true,
			LastActivityEpoch: f.currentEpoch.EpochId,
			ExpiryEpoch:       expiry,
		}))
		require.NoError(t, seedCreateAccount(
			t, f.db, context.Background(), nil, &models.Account{
				StakingKey: repeatByte(28, byte(0xe1+i)),
				Drep:       credential,
				DrepType:   0,
				AddedSlot:  1,
				Reward:     types.Uint64((30 - 10*uint64(i)) * ada),
				Active:     true,
			},
		))
	}
	if opts.prepare != nil {
		opts.prepare(t, f, withdrawal)
	}
	// The replay of a boundary rolls the set back to the block before it, so
	// the set's journal must hold that block.
	journalLastBlock := func(i int, boundary models.Epoch) {
		slot := boundary.StartSlot + uint64(boundary.LengthInSlots) - 1
		commitTestLedgerChanges(t, f.ls, ochainsync.Tip{
			Point: ocommon.NewPoint(slot, repeatByte(32, byte(0xb0+i))),
		}, LedgerChanges{Nonces: []models.BlockNonce{{
			Hash: repeatByte(32, byte(0xb0+i)), Slot: slot,
			Nonce: repeatByte(32, 0x11),
		}}})
	}
	if !opts.journalGap {
		journalLastBlock(0, f.currentEpoch)
	}
	first := f.rollover(t, f.currentEpoch, f.currentPParams)
	if opts.afterBoundary != nil {
		opts.afterBoundary(t, f, 0, first.NewCurrentEpoch)
	}
	journalLastBlock(1, first.NewCurrentEpoch)
	second := f.rollover(t, first.NewCurrentEpoch, first.NewCurrentPParams)
	if opts.afterBoundary != nil {
		opts.afterBoundary(t, f, 1, second.NewCurrentEpoch)
	}
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
	}
	require.NotEmpty(t, sc.rows[0].Ratified, "the first boundary ratifies")
	require.NotEmpty(t, sc.rows[0].DRepDistribution)
	rows.clear()
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

func TestRebuildGovernanceBoundarySnapshotJournalMissKeepsSentinel(
	t *testing.T,
) {
	t.Parallel()

	sc := newRebuildScenarioWith(t, rebuildOptions{journalGap: true})
	before := sc.f.liveState(t, sc.withdrawal)
	err := sc.rebuild(0)
	require.ErrorIs(t, err, errGovernanceBoundaryUnreachable)
	require.ErrorIs(t, err, ErrUtxoAsOfUnavailable)
	_, err = sc.f.boundaryRow(t, sc.epochs[0])
	require.ErrorIs(t, err, models.ErrGovernanceBoundarySnapshotNotRetained)
	require.Equal(t, before, sc.f.liveState(t, sc.withdrawal))
	require.False(t, sc.f.ls.utxoSetFailed.Load())
}

// TestRebuildGovernanceBoundarySnapshotsSkipsPendingBoundary leaves the row of
// a boundary whose RATIFY job is still pending to that job, while the pass
// still rebuilds the older boundary. The control run shows the stored chain
// lets the pass rebuild that same boundary when nothing is pending, so the
// absent row comes from the skip.
func TestRebuildGovernanceBoundarySnapshotsSkipsPendingBoundary(
	t *testing.T,
) {
	t.Parallel()

	control := newRebuildScenario(t)
	require.NoError(t, control.f.ls.rebuildGovernanceBoundarySnapshots(
		context.Background(),
	))
	for i, epoch := range control.epochs {
		got, err := control.f.boundaryRow(t, epoch)
		require.NoError(t, err)
		require.Equal(t, control.rows[i], got, "control epoch %d", epoch.EpochId)
	}

	sc := newRebuildScenario(t)
	rec := pendingRatificationRecord{
		Epoch:        sc.epochs[1].EpochId,
		BoundarySlot: sc.epochs[1].StartSlot,
		ID:           1,
	}
	raw, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, seedDB(t, sc.f.db).SetSyncState(
		pendingRatificationSyncKey, string(raw), nil,
	))
	before := sc.f.liveState(t, sc.withdrawal)
	require.NoError(t, sc.f.ls.rebuildGovernanceBoundarySnapshots(
		context.Background(),
	))
	_, err = sc.f.boundaryRow(t, sc.epochs[1])
	require.ErrorIs(t, err, models.ErrGovernanceBoundarySnapshotNotRetained)
	older, err := sc.f.boundaryRow(t, sc.epochs[0])
	require.NoError(t, err)
	require.Equal(t, sc.rows[0], older)
	got, err := sc.f.ls.ledgerDB().GetSyncState(pendingRatificationSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, string(raw), got)
	require.Equal(t, before, sc.f.liveState(t, sc.withdrawal))
}

// liveSetOutput is a live output staked to a credential, with the outputs
// the ledger reads its stake from.
func liveSetOutput(
	t *testing.T,
	id byte,
	stakingKey []byte,
	amount uint64,
) (models.UtxoId, []byte) {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		repeatByte(lcommon.AddressHashSize, 0x37), stakingKey,
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr, OutputAmount: amount,
	})
	require.NoError(t, err)
	return models.UtxoId{Hash: repeatByte(32, id)}, encoded
}

func applyLiveSetChange(
	t *testing.T,
	f *treasuryRolloverFixture,
	slot uint64,
	evt UtxoEvent,
) {
	t.Helper()
	evt.Point = ocommon.Point{Slot: slot, Hash: repeatByte(32, byte(slot))}
	require.NoError(t, f.ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{evt}))
}

// liveSetState is what a rebuild must leave alone in the live UTxO set: its
// outputs and what its journal would restore at each given point.
type liveSetState struct {
	Outputs map[string]string
	At      []map[string]string
}

func readLiveSetState(
	t *testing.T,
	f *treasuryRolloverFixture,
	points ...ocommon.Point,
) liveSetState {
	t.Helper()
	collect := func(it UtxoSetIterator) map[string]string {
		out := map[string]string{}
		require.NoError(t, it.EachUtxo(
			func(txID []byte, index uint32, c []byte) error {
				out[fmt.Sprintf("%x#%d", txID, index)] = hex.EncodeToString(c)
				return nil
			},
		))
		return out
	}
	set := f.ls.config.UtxoSet
	state := liveSetState{Outputs: collect(set.(UtxoSetIterator))}
	for _, point := range points {
		view, err := set.(UtxoSetHistory).UtxoSetAt(point)
		require.NoError(t, err)
		state.At = append(state.At, collect(view))
		view.Close()
	}
	return state
}

// TestRebuildGovernanceBoundarySnapshotRestoresSpentLiveOutput spends, after
// the first boundary, an output that boundary's DRep stake counts. The live
// UTxO set no longer holds it at the tip, so the rebuilt first row only
// matches the stored one when the replay restores it from the set's journal.
func TestRebuildGovernanceBoundarySnapshotRestoresSpentLiveOutput(
	t *testing.T,
) {
	t.Parallel()

	const ada = uint64(1_000_000)
	stakeKey := repeatByte(28, 0xe1)
	var spentID models.UtxoId
	sc := newRebuildScenarioWith(t, rebuildOptions{
		prepare: func(
			t *testing.T, f *treasuryRolloverFixture, _ *models.GovernanceProposal,
		) {
			id, encoded := liveSetOutput(t, 0x61, stakeKey, 500*ada)
			spentID = id
			applyLiveSetChange(
				t, f, f.currentEpoch.StartSlot+90,
				UtxoEvent{Added: []UtxoOutput{{Ref: id, Cbor: encoded}}},
			)
		},
		afterBoundary: func(
			t *testing.T, f *treasuryRolloverFixture, i int, epoch models.Epoch,
		) {
			if i != 0 {
				return
			}
			applyLiveSetChange(
				t, f, epoch.StartSlot+50,
				UtxoEvent{Removed: []models.UtxoId{spentID}},
			)
		},
	})
	_, held, err := sc.f.ls.config.UtxoSet.(UtxoSetReader).Get(
		spentID.Hash, spentID.Idx,
	)
	require.NoError(t, err)
	require.False(t, held, "the output is spent at the tip")
	require.NotEqual(
		t, sc.rows[0].DRepDistribution, sc.rows[1].DRepDistribution,
		"the spend changes the DRep stake between the boundaries",
	)

	points := []ocommon.Point{sc.point(0), sc.point(1)}
	before := sc.f.liveState(t, sc.withdrawal)
	setBefore := readLiveSetState(t, sc.f, points...)
	require.Contains(t, setBefore.At[0], fmt.Sprintf("%x#0", spentID.Hash))
	for _, i := range []int{1, 0} {
		require.NoError(t, sc.rebuild(i))
	}
	for i, epoch := range sc.epochs {
		got, err := sc.f.boundaryRow(t, epoch)
		require.NoError(t, err)
		require.Equal(t, sc.rows[i], got, "epoch %d", epoch.EpochId)
	}
	require.Equal(t, before, sc.f.liveState(t, sc.withdrawal))
	require.Equal(t, setBefore, readLiveSetState(t, sc.f, points...))
	require.Empty(t, sc.f.ls.utxoSetBatches)
	require.False(t, sc.f.ls.utxoSetFailed.Load())
}

func drepDistributionByCredential(
	snapshot *models.GovernanceBoundarySnapshot,
) map[string]uint64 {
	out := map[string]uint64{}
	for _, entry := range snapshot.DRepDistribution {
		out[hex.EncodeToString(entry.Credential)] = entry.Stake
	}
	return out
}

type drepScenarioOptions struct {
	// moveDelegation moves a delegator between DReps after the first
	// boundary with a vote delegation certificate.
	moveDelegation bool
	// renewLapsed renews a lapsed DRep's activity after the first boundary.
	renewLapsed bool
}

var (
	drepScenarioFirst  = repeatByte(28, 0xd1)
	drepScenarioSecond = repeatByte(28, 0xd2)
	drepScenarioLapsed = repeatByte(28, 0xd3)
	drepScenarioMoved  = repeatByte(28, 0xe3)
)

func drepOfCredential(credential []byte) lcommon.Drep {
	return lcommon.Drep{
		Type:       lcommon.DrepTypeAddrKeyHash,
		Credential: credential,
	}
}

// newDRepRebuildScenario builds two live boundaries where the first
// boundary's treasury withdrawal ratifies only while the lapsed DRep, which
// voted against it, stays out of the active stake. Its delegations and
// expiry come from certificates and an expiry history, as a live chain
// records them, so rolling the tables back restores the first boundary's
// view of them.
func newDRepRebuildScenario(
	t *testing.T,
	opts drepScenarioOptions,
) *rebuildScenario {
	t.Helper()
	const ada = uint64(1_000_000)
	lapsedKey := repeatByte(28, 0xe4)
	return newRebuildScenarioWith(t, rebuildOptions{
		prepare: func(
			t *testing.T,
			f *treasuryRolloverFixture,
			withdrawal *models.GovernanceProposal,
		) {
			start := f.currentEpoch.StartSlot
			// The withdrawal needs 70% of the active DRep stake that voted
			// or abstained against it.
			f.currentPParams.DRepVotingThresholds.TreasuryWithdrawal =
				cbor.Rat{Rat: big.NewRat(7, 10)}
			seedStakeCertAt(t, f.db, drepScenarioMoved, true, start+60, 2*ada)
			seedVoteDelegationAt(
				t, f.db, drepScenarioMoved,
				drepOfCredential(drepScenarioFirst), start+70,
			)
			require.NoError(t, seedDB(t, f.db).AddAccountRewardByCredential(
				context.Background(), 0, drepScenarioMoved, 40*ada, start+75,
				repeatByte(32, 0xc9), nil,
			))
			require.NoError(t, seedCreateDrep(t, f.db, context.Background(), nil, &models.Drep{
				Credential:        drepScenarioLapsed,
				AddedSlot:         1,
				Active:            true,
				LastActivityEpoch: f.currentEpoch.EpochId - 1,
				ExpiryEpoch:       f.currentEpoch.EpochId,
			}))
			require.NoError(t, seedDB(t, f.db).UpdateDRepActivity(
				context.Background(), 0, drepScenarioLapsed,
				f.currentEpoch.EpochId-1, 1, start+20, nil,
			))
			require.NoError(t, seedCreateAccount(
				t, f.db, context.Background(), nil, &models.Account{
					StakingKey: lapsedKey,
					Drep:       drepScenarioLapsed,
					DrepType:   0,
					AddedSlot:  1,
					Reward:     types.Uint64(25 * ada),
					Active:     true,
				},
			))
			addDRepVote(
				t, f, withdrawal, drepScenarioFirst, models.VoteYes, start+30,
			)
			addDRepVote(
				t, f, withdrawal, drepScenarioLapsed, models.VoteNo, start+30,
			)
		},
		afterBoundary: func(
			t *testing.T, f *treasuryRolloverFixture, i int, epoch models.Epoch,
		) {
			if i != 0 {
				return
			}
			if opts.moveDelegation {
				seedVoteDelegationAt(
					t, f.db, drepScenarioMoved,
					drepOfCredential(drepScenarioSecond), epoch.StartSlot+30,
				)
			}
			if opts.renewLapsed {
				require.NoError(t, seedDB(t, f.db).UpdateDRepActivity(
					context.Background(), 0, drepScenarioLapsed,
					epoch.EpochId, 10, epoch.StartSlot+40, nil,
				))
			}
		},
	})
}

func requireRebuiltRowsMatchLive(t *testing.T, sc *rebuildScenario) {
	t.Helper()
	before := sc.f.liveState(t, sc.withdrawal)
	for _, i := range []int{1, 0} {
		require.NoError(t, sc.rebuild(i))
	}
	for i, epoch := range sc.epochs {
		got, err := sc.f.boundaryRow(t, epoch)
		require.NoError(t, err)
		require.Equal(t, sc.rows[i], got, "epoch %d", epoch.EpochId)
	}
	require.Equal(t, before, sc.f.liveState(t, sc.withdrawal))
}

// TestRebuildGovernanceBoundarySnapshotDRepDelegationMoves moves a delegator
// to another DRep after the first boundary. The first row must keep the
// delegator's stake with the first DRep.
func TestRebuildGovernanceBoundarySnapshotDRepDelegationMoves(t *testing.T) {
	t.Parallel()

	const ada = uint64(1_000_000)
	sc := newDRepRebuildScenario(t, drepScenarioOptions{moveDelegation: true})
	first := drepDistributionByCredential(sc.rows[0])
	second := drepDistributionByCredential(sc.rows[1])
	require.Equal(t, 70*ada, first[hex.EncodeToString(drepScenarioFirst)])
	require.Equal(t, 20*ada, first[hex.EncodeToString(drepScenarioSecond)])
	require.Equal(t, 30*ada, second[hex.EncodeToString(drepScenarioFirst)])
	require.Equal(t, 60*ada, second[hex.EncodeToString(drepScenarioSecond)])
	requireRebuiltRowsMatchLive(t, sc)
}

// TestRebuildGovernanceBoundarySnapshotDRepExpiryRenewed renews a lapsed
// DRep's activity after the first boundary. That DRep voted against the
// withdrawal, so the first boundary ratifies it only while the DRep is still
// lapsed; a rebuild that kept the renewed expiry would not ratify it.
func TestRebuildGovernanceBoundarySnapshotDRepExpiryRenewed(t *testing.T) {
	t.Parallel()

	sc := newDRepRebuildScenario(t, drepScenarioOptions{renewLapsed: true})
	require.NotEmpty(t, sc.rows[0].Ratified)
	requireRebuiltRowsMatchLive(t, sc)
}

func TestRebuildGovernanceBoundarySnapshotDijkstraOrdering(t *testing.T) {
	t.Parallel()

	sc := newRebuildScenarioWith(t, rebuildOptions{
		protocolMajor: lcommon.ProtocolVersionDijkstra,
	})
	require.NotEmpty(t, sc.rows[0].Ratified)
	requireRebuiltRowsMatchLive(t, sc)
}

func addDRepVote(
	t *testing.T,
	f *treasuryRolloverFixture,
	proposal *models.GovernanceProposal,
	credential []byte,
	vote uint8,
	slot uint64,
) {
	t.Helper()
	require.NoError(t, seedDB(t, f.db).SetGovernanceVote(
		context.Background(), &models.GovernanceVote{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeDRep,
			VoterCredential: credential,
			Vote:            vote,
			AddedSlot:       slot,
		}, nil,
	))
}

// delegatorInactivityScenario runs two boundaries where a second treasury
// withdrawal, proposed after the first boundary, is voted for by the one DRep
// that holds stake. CIP-0163 activates at the first boundary with no window,
// so every delegator's stake has lapsed by the second boundary and the
// second withdrawal ratifies only without the gate.
func delegatorInactivityScenario(
	t *testing.T,
	delegatorInactivity bool,
) (*rebuildScenario, *models.GovernanceProposal) {
	t.Helper()
	const ada = uint64(1_000_000)
	var late *models.GovernanceProposal
	sc := newRebuildScenarioWith(t, rebuildOptions{
		delegatorInactivity: delegatorInactivity,
		prepare: func(
			t *testing.T,
			f *treasuryRolloverFixture,
			withdrawal *models.GovernanceProposal,
		) {
			f.currentPParams.DRepVotingThresholds.TreasuryWithdrawal =
				cbor.Rat{Rat: big.NewRat(1, 2)}
			addDRepVote(
				t, f, withdrawal, repeatByte(28, 0xd1), models.VoteYes,
				f.currentEpoch.StartSlot+30,
			)
		},
		afterBoundary: func(
			t *testing.T, f *treasuryRolloverFixture, i int, epoch models.Epoch,
		) {
			if i != 0 {
				return
			}
			destination, destinationReturn, _ := f.rewardAddress(t, 0x95)
			late = f.addProposal(
				t, 0x96, epoch.StartSlot+20,
				map[*lcommon.Address]uint64{destination: 1 * ada},
				destinationReturn, 1*ada, false,
			)
			addDRepVote(
				t, f, late, repeatByte(28, 0xd1), models.VoteYes,
				epoch.StartSlot+30,
			)
		},
	})
	return sc, late
}

func ratifiedTxHashes(row *models.GovernanceBoundarySnapshot) [][]byte {
	var out [][]byte
	for _, ref := range row.Ratified {
		out = append(out, ref.TxHash)
	}
	return out
}

// TestRebuildGovernanceBoundarySnapshotDelegatorInactivity checks that a
// replayed boundary runs CIP-0163 as the live one did: the activation at the
// first boundary gates the delegators' stake out of the second boundary's
// RATIFY.
func TestRebuildGovernanceBoundarySnapshotDelegatorInactivity(t *testing.T) {
	t.Parallel()

	ungated, late := delegatorInactivityScenario(t, false)
	require.Contains(t, ratifiedTxHashes(ungated.rows[1]), late.TxHash)

	sc, late := delegatorInactivityScenario(t, true)
	require.NotEmpty(t, sc.rows[0].Ratified)
	require.NotContains(t, ratifiedTxHashes(sc.rows[1]), late.TxHash)
	requireRebuiltRowsMatchLive(t, sc)
}
