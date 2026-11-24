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

package ledger_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// serveLedgerOnly makes the harness's set the ledger's only copy of its
// state, as a node's startup does.
func serveLedgerOnly(t *testing.T, h *p5Harness) {
	t.Helper()
	h.set.ServeLedgerOnly()
}

// releasedFactsRun builds the harness over a set that takes over the reward
// input facts a v0.94.2 database left in its metadata store, with the rest of
// the harness's state written to the set.
// It then writes nonces, network state, summaries, op-cert counters and the
// committee, runs drive, rolls back across the boundary, and renders what the
// ledger answers after each step. The ledger holds a Leios certificate
// validator, as a node always does.
func releasedFactsRun(
	t *testing.T,
	drive func(h *p5Harness) func() []string,
) (map[string][]string, *p5Harness) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &p5Harness{set: set}
	h.scenario = ledger.NewSeededBoundaryEpochStateScenarioForTest(
		t, set, nil, func(db *database.Database) {
			seedRewardInputFacts(t, db.Metadata())
		},
	)
	require.NoError(t, set.Accounts(
		func(a *models.Account) error {
			h.refs = append(h.refs, models.NewStakeCredentialRef(
				a.CredentialTag, a.StakingKey,
			))
			if h.pool == nil {
				h.pool = bytes.Clone(a.Pool)
			}
			return nil
		}))
	h.store = dbadapter.New(h.scenario.DB)
	require.Len(t, h.refs, 2, "the harness has two accounts")
	s := h.scenario
	s.ConfigureLeiosValidator()
	s.InstallDrainedSources()
	cur := s.CurrentPoint().Slot
	s.WriteNonces([]models.BlockNonce{{
		Slot: cur + 1, Hash: bytes.Repeat([]byte{byte(cur + 1)}, 32),
		Nonce:        bytes.Repeat([]byte{byte(cur + 2)}, 32),
		IsCheckpoint: true,
	}})
	s.SetNetworkState(500, 8_000, cur+1)
	s.Donate(11)
	s.SaveSummary(models.EpochSummary{
		Epoch: 1, TotalActiveStake: 7, TotalPoolCount: 2,
		EpochNonce: bytes.Repeat([]byte{3}, 32),
	})
	s.Committee()
	out := map[string][]string{}
	render := drive(h)
	read := func() []string {
		lines := append([]string{}, s.ReadNonces()...)
		lines = append(lines, s.ReadNetwork()...)
		lines = append(lines, s.ReadEpochs()...)
		return append(lines, render()...)
	}
	out["applied"] = read()
	s.RollbackBeforeBoundary()
	out["rolled back"] = read()
	return out, h
}

// The pool scenario: a registration, a VRF key rotation, a retirement and its
// reap at the boundary, a rollback and a replay.
func TestEveryGraduatedFamilyDrainedTogetherServesPoolHistory(
	t *testing.T,
) {
	t.Parallel()
	var slots ledger.PoolSlots
	var refunds []models.PoolRetirementRefund
	var tip ochainsync.Tip
	var err error
	apply := func(h *p5Harness) {
		slots = h.scenario.Pools(func() {
			epochs, err := h.scenario.LedgerDB().GetEpochs(nil)
			require.NoError(t, err)
			var cur models.Epoch
			for _, e := range epochs {
				if e.EpochId > cur.EpochId {
					cur = e
				}
			}
			refunds, err = h.scenario.LedgerDB().GetPoolsRetiringAtEpoch(
				t.Context(), cur.EpochId+1, cur.StartSlot+23, nil,
			)
			require.NoError(t, err)
		})
	}
	out, h := releasedFactsRun(
		t,
		func(h *p5Harness) func() []string {
			// The op-cert write keeps the pool row's counter in step in
			// the metadata store, which holds no row of the family.
			ops := h.scenario.OpCerts()
			apply(h)
			require.Len(t, refunds, 1)
			first := true
			return func() []string {
				initial := first
				if first {
					tip, err = h.scenario.DB.GetTip(nil)
					require.NoError(t, err)
					requireDrainedPoolAnswers(
						t, h.scenario.LedgerDB(), slots,
					)
				}
				first = false
				lines := readPools(
					t,
					h.scenario.LedgerDB(),
					h.scenario.DB,
					slots,
				)
				lines = append(lines, renderOpCerts(
					t,
					h.scenario.LedgerDB(),
					[][]byte{ops.Pool, ops.Other},
					[]uint64{
						ops.Raised,
						ops.Repeated,
						ops.Lowered,
						ops.Batched,
					},
				)...)
				// The counters observed at one slot are what a snapshot
				// import's anchor exports.
				for _, slot := range []uint64{
					ops.Base, ops.Raised, ops.Repeated, ops.Lowered,
					ops.Batched, ops.Batched + 1,
				} {
					got, err := h.scenario.AnchorOpCertCounters(slot)
					require.NoError(t, err, "anchor counters at %d", slot)
					if initial && slot == ops.Repeated {
						require.Len(t, got, 2, "both pools observed at %d", slot)
					}
					for _, c := range got {
						lines = append(lines, fmt.Sprintf(
							"anchor %d %x %d", slot, c.PoolKeyHash,
							c.Sequence,
						))
					}
					lines = append(lines, fmt.Sprintf(
						"anchor %d count %d", slot, len(got),
					))
				}
				return lines
			}
		},
	)
	// The replay applies no blocks, so the tip stays at the rollback
	// target while the metadata store's epoch reads follow the tip the
	// first pass left.
	apply(h)
	require.NoError(t, h.scenario.DB.SetTip(tip, nil))
	out["replay"] = readPools(
		t, h.scenario.LedgerDB(), h.scenario.DB, slots,
	)
	for _, name := range []string{"applied", "rolled back", "replay"} {
		require.NotEmpty(t, out[name], name)
	}
	require.NotEqual(
		t, out["applied"], out["rolled back"],
		"the rollback undoes the pool certificates",
	)
}

// newRewardBoundaryRun is newRewardBlocksRun over a Mithril anchor with the
// blocks and fees after it and phase-2-invalid transactions, whose collateral
// the metadata store and the set both consume, so the live stake the two hold
// agrees. Without an anchor the store counts the overlay slots by pparams the
// harness does not give it.
func newRewardBoundaryRun(t *testing.T) *rewardBlocksRun {
	t.Helper()
	r := newRewardBlocksRun(t, "set", nil, true)
	s := r.scenario
	s.Collateral(0xc0, 1_000)
	s.Block(9, s.PoolB)
	s.Fee(9, 200)
	s.InvalidTotal(9, 300)
	s.Block(10, s.PoolA)
	s.Fee(10, 50)
	s.InvalidInputs(10, 0xc0, 400)
	s.Block(11, s.PoolB)
	s.Fee(11, 25)
	stakeUp(r)
	s.ConfigureLeiosValidator()
	return r
}

// The boundary scenario: the reward capture of the epoch that closes, the
// block counts and fees the round reads, a rollback across the boundary and a
// replay, with a Leios certificate validator configured.
func TestBoundaryRewardCaptureSurvivesRollbackAndReplay(
	t *testing.T,
) {
	t.Parallel()
	r := newRewardBoundaryRun(t)
	s := r.scenario
	s.InstallDrainedSources()
	out := map[string]string{}
	read := func(name string) {
		inputs := s.Inputs(6)
		require.NoError(t, inputs.Err, name)
		rawIn, err := json.Marshal(inputs)
		require.NoError(t, err, name)
		out[name] = string(rawIn) + fmt.Sprint(r.read(t, 5), r.read(t, 4))
	}
	read("before the boundary")
	s.Boundary()
	read("boundary")
	s.RollbackBeforeBoundary()
	read("rollback")
	s.Block(11, s.PoolB)
	s.Fee(11, 25)
	s.Boundary()
	read("replay")
	require.Equal(t, out["boundary"], out["replay"])
	require.NotEqual(t, out["boundary"], out["rollback"])
	require.NotEqual(t, out["before the boundary"], out["boundary"])
}

// seedRewardInputFacts stores, as a v0.94.2 database holds them, the reward
// input rows a harness block run does not write: pool and stake inputs, and
// an imported basis failure, in the epoch the harness's boundary ends.
func seedRewardInputFacts(
	t *testing.T,
	store metadata.MetadataStore,
) {
	t.Helper()
	epoch := ledger.BoundaryScenarioEndedEpochForTest
	slot := uint64(1)
	// The set takes over the reward inputs of the epochs the store holds a
	// row of, which are the harness's own.
	length := ledger.BoundaryScenarioEpochLengthForTest
	for e := range epoch + 1 {
		require.NoError(t, store.SetEpoch(
			e*uint64(length), e, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1000, length, nil,
		))
	}
	pool := bytes.Repeat([]byte{0x61}, 28)
	other := bytes.Repeat([]byte{0x62}, 28)
	blocks := uint64(5)
	require.NoError(t, store.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch: epoch, PoolKeyHash: pool, RewardAccount: bytes.Repeat([]byte{1}, 29),
			Margin: &types.Rat{Rat: big.NewRat(1, 20)}, Pledge: 3,
			DelegatedStake: 100, OwnerStake: 3, Cost: 340, DelegatorCount: 2,
			BlocksProduced: &blocks, TotalBlocksInEpoch: &blocks,
			CapturedSlot: slot, BoundarySlot: slot,
		},
		{
			Epoch: epoch, PoolKeyHash: other, Pledge: 1, DelegatedStake: 50,
			Cost: 170, DelegatorCount: 1, CapturedSlot: slot, BoundarySlot: slot,
		},
	}, nil))
	var stakes []*models.RewardStakeInput
	for i, p := range [][]byte{pool, pool, other} {
		stakes = append(stakes, &models.RewardStakeInput{
			Epoch: epoch, PoolKeyHash: p, CredentialTag: uint8(i % 2),
			StakingKey: bytes.Repeat([]byte{byte(0x70 + i)}, 28),
			Stake:      types.Uint64(10 * (i + 1)), Owner: i == 0, Registered: true,
			CapturedSlot: slot, BoundarySlot: slot,
		})
	}
	require.NoError(t, store.SaveRewardStakeInputs(stakes, nil))
	// The set keeps a basis failure at its snapshot's captured slot, so the
	// two share one here as an import writes them.
	excluded := types.Uint64(4)
	require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: epoch, SnapshotType: models.PoolStakeSnapshotTypeMark,
		TotalActiveStake: 150, ExcludedActiveStake: &excluded,
		TotalPoolCount: 2, TotalDelegators: 3, CapturedSlot: slot,
		BoundarySlot: slot, EpochNonce: bytes.Repeat([]byte{9}, 32),
		ProtocolVersion: 10, Authoritative: true, CalculationVersion: 3,
	}, nil))
	require.NoError(t, store.SaveRewardSeedFailure(
		epoch, models.PoolStakeSnapshotTypeMark, "basis missing", slot, nil,
	))
	require.NoError(t, store.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{PoolKeyHash: pool, Epoch: epoch, BlocksProduced: 4, CapturedSlot: slot},
			{PoolKeyHash: other, Epoch: epoch, BlocksProduced: 2, CapturedSlot: slot},
		}, nil,
	))
	require.NoError(t, store.SaveImportedEpochBlockTotal(epoch, 6, slot, nil))
	perf := &types.Rat{Rat: big.NewRat(9, 10)}
	require.NoError(t, store.SaveRewardPoolOutputs([]*models.RewardPoolOutput{{
		Epoch: epoch, PoolKeyHash: pool, ApparentPerformance: perf,
		OptimalReward: 90, TotalReward: 80, LeaderReward: 30,
		LeaderRewardDeficit: 1, MemberRewardTotal: 50, OwnerStake: 3,
		Undistributed: 2, Unspendable: 4, CapturedSlot: slot,
		BoundarySlot: slot,
	}, {
		Epoch: epoch, PoolKeyHash: other, OptimalReward: 5, TotalReward: 4,
		CapturedSlot: slot, BoundarySlot: slot,
	}}, nil))
	require.NoError(t, store.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{
			{
				Epoch: epoch, PoolKeyHash: pool, CredentialTag: 0,
				StakingKey: bytes.Repeat([]byte{0x70}, 28),
				RewardType: "leader", Amount: 30, Spendable: true,
				CapturedSlot: slot, BoundarySlot: slot,
			},
			{
				Epoch: epoch, PoolKeyHash: pool, CredentialTag: 1,
				StakingKey: bytes.Repeat([]byte{0x71}, 28),
				RewardType: "member", Amount: 20, Guarded: true,
				CapturedSlot: slot, BoundarySlot: slot,
			},
		}, nil,
	))
	require.NoError(t, store.AddAppliedRewardCreditRound(
		models.RewardCreditRound{SnapshotEpoch: epoch, BoundarySlot: slot}, nil,
	))
	for i := range 2 {
		require.NoError(t, store.CreateDrep(nil, &models.Drep{
			Credential: bytes.Repeat([]byte{byte(0x50 + i)}, 28),
			AddedSlot:  slot, Active: i == 0, LastActivityEpoch: epoch,
			ExpiryEpoch: epoch + 20, CredentialTag: uint8(i),
		}))
	}
	u := func(v uint64) *uint64 { return &v }
	for i, p := range []models.GovernanceProposal{
		{
			RatifiedEpoch: u(epoch), RatifiedSlot: u(slot),
			EnactedEpoch: u(epoch + 1), EnactedSlot: u(slot + 5),
		},
		{
			ExpiredEpoch: u(epoch), ExpiredSlot: u(slot),
			DroppedEpoch: u(epoch + 1), DroppedSlot: u(slot + 7),
		},
		{DeletedSlot: u(slot + 2)},
	} {
		p.TxHash = bytes.Repeat([]byte{byte(0x40 + i)}, 32)
		p.ActionIndex = uint32(i)
		p.ActionType = 1
		p.ProposedEpoch, p.ExpiresEpoch = epoch, epoch+6
		p.Deposit, p.AddedSlot = 100, slot
		p.AnchorURL, p.AnchorHash = "https://a", bytes.Repeat([]byte{1}, 32)
		p.ReturnAddress = bytes.Repeat([]byte{2}, 29)
		require.NoError(t, store.SetGovernanceProposal(&p, nil))
	}
	proof := bytes.Repeat([]byte{5}, 48)
	regEpoch := epoch
	require.NoError(t, store.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch: epoch, SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash: pool, TotalStake: 900, DelegatorCount: 3,
				CapturedSlot: slot, VrfKeyHash: bytes.Repeat([]byte{6}, 32),
				LeiosKeyPublic: bytes.Repeat([]byte{7}, 96), LeiosKeyPossessionProof: proof,
				LeiosKeyRegistrationEpoch: &regEpoch, CalculationVersion: 3,
				RewardAccountAutoVote: 1, RewardAccountAutoVoteResolved: true,
			},
			{
				Epoch: epoch, SnapshotType: models.PoolStakeSnapshotTypeActive,
				PoolKeyHash: other, TotalStake: 5, StakeDenominator: 40,
				DelegatorCount: 1, CapturedSlot: slot,
			},
		}, nil,
	))
}
