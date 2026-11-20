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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// graduatedTables maps every family of statedrain.Graduated to its tables.
func graduatedTables() map[nodesettings.Family][]string {
	out := map[nodesettings.Family][]string{}
	for _, e := range statedrain.Graduated(drainTestSource{}) {
		for _, table := range e.Tables {
			out[e.Family] = append(out[e.Family], table.Name)
		}
	}
	return out
}

// drainEveryGraduatedFamily drains the whole of statedrain.Graduated in its
// own order with the statement guard armed.
func drainEveryGraduatedFamily(t *testing.T, h *p5Harness) {
	t.Helper()
	src := drainTestSource{set: h.set, db: h.scenario.DB}
	h.drainErr = statedrain.Run(t.Context(), statedrain.Env{
		Store: drainStore{h.scenario.DB, h.scenario.DrainFamilies},
		Set:   src, Guard: h.guard,
	}, statedrain.Graduated(src))
}

// allGraduatedRun writes nonces, network state, summaries, op-cert counters
// and the committee, drains every graduated family together when drain is
// set, runs drive, rolls back across the boundary, and renders what the
// ledger answers after each step. The ledger holds a Leios certificate
// validator, as a node always does.
func allGraduatedRun(
	t *testing.T,
	drain bool,
	drive func(h *p5Harness) func() []string,
) (map[string][]string, *p5Harness) {
	t.Helper()
	h := newSeededP5Harness(t, nil, nil, func(db *database.Database) {
		seedRewardInputFacts(t, db.Metadata())
	})
	s := h.scenario
	s.ConfigureLeiosValidator()
	// The harness wrote its two nonces to the metadata store only.
	fixture, err := h.store.GetBlockNoncesInSlotRange(0, math.MaxInt64, nil)
	require.NoError(t, err)
	s.StageNoncesInSet(fixture)
	// So is the stake of the harness's two stake credentials.
	s.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	s.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
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
	if drain {
		s.InstallDrainedSources()
		drainEveryGraduatedFamily(t, h)
		require.NoError(t, h.drainErr)
	}
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

// requireEveryGraduatedFamilyDrained requires the latch bit of each graduated
// family and every one of its tables empty, with no statement having touched
// a drained table.
func requireEveryGraduatedFamilyDrained(t *testing.T, h *p5Harness) {
	t.Helper()
	tables := graduatedTables()
	require.Equal(t, []string{"epoch"}, tables[nodesettings.FamilyEpochs])
	require.Equal(
		t, []string{"epoch_summary"}, tables[nodesettings.FamilySummaries],
	)
	families := make([]nodesettings.Family, 0, len(tables))
	for _, e := range statedrain.Graduated(drainTestSource{}) {
		families = append(families, e.Family)
	}
	h.requireDrained(t, families, tables)
}

// With every graduated family drained together, each scenario reads as an
// undrained run does across a boundary, a rollback and the writes of every
// family, with the statement guard armed over every graduated table.
func TestEveryGraduatedFamilyDrainedTogetherMatchesAnUndrainedRun(
	t *testing.T,
) {
	t.Parallel()
	for _, d := range p5Drives {
		t.Run(d.name, func(t *testing.T) {
			t.Parallel()
			drive := func(drained bool) func(*p5Harness) func() []string {
				return func(h *p5Harness) func() []string {
					touched := d.drive(t, h)
					return func() []string {
						return accountDRepReads(t, h.reads(drained), h, touched)
					}
				}
			}
			undrained, plain := allGraduatedRun(t, false, drive(false))
			require.NoError(t, plain.drainErr)
			drained, h := allGraduatedRun(t, true, drive(true))
			requireEveryGraduatedFamilyDrained(t, h)
			for name, want := range undrained {
				require.NotEmpty(t, want, name)
				requireSameLines(t, name, want, drained[name])
			}
			require.NotEqual(
				t, undrained["applied"], undrained["rolled back"],
				"the rollback must change the reads",
			)
		})
	}
}

// The pool scenario: a registration, a VRF key rotation, a retirement and its
// reap at the boundary, a rollback and a replay.
func TestEveryGraduatedFamilyDrainedTogetherServesPoolHistory(
	t *testing.T,
) {
	t.Parallel()
	run := func(t *testing.T, drained bool) (map[string][]string, *p5Harness) {
		var slots ledger.PoolSlots
		var refunds []models.PoolRetirementRefund
		var tip ochainsync.Tip
		var err error
		apply := func(h *p5Harness) {
			slots = h.scenario.Pools(func() {
				epochs, err := h.store.GetEpochs(nil)
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
		out, h := allGraduatedRun(
			t,
			drained,
			func(h *p5Harness) func() []string {
				// The op-cert write keeps the pool row's counter in step in
				// the metadata store, which a drained family has no row for.
				ops := h.scenario.OpCerts()
				apply(h)
				require.Len(t, refunds, 1)
				first := true
				return func() []string {
					initial := first
					if first {
						tip, err = h.scenario.DB.GetTip(nil)
						require.NoError(t, err)
						if drained {
							requireDrainedPoolAnswers(
								t, h.scenario.LedgerDB(), slots,
							)
						}
					}
					first = false
					lines := readPools(
						t,
						h.reads(drained),
						h.scenario.DB,
						slots,
					)
					lines = append(lines, renderOpCerts(
						t,
						h.reads(drained),
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
			t, h.reads(drained), h.scenario.DB, slots,
		)
		return out, h
	}
	undrained, _ := run(t, false)
	drained, h := run(t, true)
	requireEveryGraduatedFamilyDrained(t, h)
	for name, want := range undrained {
		require.NotEmpty(t, want, name)
		requireSameLines(t, name, want, drained[name])
	}
}

// newRewardBoundaryRun is newRewardBlocksRun over a Mithril anchor with the
// blocks and fees after it and phase-2-invalid transactions, whose collateral
// the metadata store and the set both consume, so the live stake the two hold
// agrees. Without an anchor the store counts the overlay slots by pparams the
// harness does not give it.
func newRewardBoundaryRun(
	t *testing.T,
	guard *statedrain.TripGuard,
) *rewardBlocksRun {
	t.Helper()
	r := newRewardBlocksRun(t, "set", guard, true)
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
// replay, with every graduated family drained and a Leios certificate
// validator configured.
func TestEveryGraduatedFamilyDrainedTogetherCapturesTheSameRewardInputs(
	t *testing.T,
) {
	t.Parallel()
	run := func(t *testing.T, drain bool) (map[string]string, *statedrain.TripGuard, *rewardBlocksRun) {
		guard := statedrain.NewTripGuard()
		r := newRewardBoundaryRun(t, guard)
		s := r.scenario
		fixture, err := dbadapter.New(s.DB).GetBlockNoncesInSlotRange(
			0, math.MaxInt64, nil,
		)
		require.NoError(t, err)
		s.StageNoncesInSet(fixture)
		if drain {
			s.InstallDrainedSources()
			src := rewardBlocksDrainSource{r: r}
			require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
				Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
				Guard: guard,
			}, statedrain.Graduated(src)))
		}
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
		s.WaitDeferred()
		read("boundary")
		s.RollbackBeforeBoundary()
		s.WaitDeferred()
		read("rollback")
		s.Block(11, s.PoolB)
		s.Fee(11, 25)
		s.Boundary()
		s.WaitDeferred()
		read("replay")
		return out, guard, r
	}
	undrained, _, plain := run(t, false)
	drained, guard, r := run(t, true)
	require.Empty(t, guard.Trips(), "a drained table was touched")
	// Control: the guard is armed, so a read around the ledger trips it.
	require.Error(t, r.scenario.DB.Metadata().IterateAccounts(
		nil, func(*models.Account) error { return nil },
	))
	require.NotEmpty(t, guard.Trips(), "the guard did not see a drained read")
	rawPlain, err := dbtest.RawSQLiteMetadata(t, plain.scenario.DB)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, r.scenario.DB)
	require.NoError(t, err)
	mask, err := r.scenario.DB.DrainedFamilies(t.Context())
	require.NoError(t, err)
	for family, tables := range graduatedTables() {
		require.True(t, mask.Has(family), "latch bit of %s", family)
		for _, table := range tables {
			require.Zero(t, tableRows(t, raw, table), "drained %s", table)
		}
	}
	for _, table := range []string{
		"account", "pool", "reward_pool_input", "reward_stake_input",
		"reward_snapshot", "reward_ada_pots", "pool_opcert_sequence",
	} {
		require.Positive(
			t, tableRows(t, rawPlain, table), "undrained %s", table,
		)
	}
	require.Equal(t, undrained, drained)
	require.Equal(t, drained["boundary"], drained["replay"])
	require.NotEqual(t, drained["boundary"], drained["rollback"])
}

// seedRewardInputFacts stores the reward input rows a harness block run does
// not write: pool and stake inputs, and an imported basis failure.
func seedRewardInputFacts(
	t *testing.T,
	store metadata.MetadataStore,
) {
	t.Helper()
	epochs, err := store.GetEpochs(nil)
	require.NoError(t, err)
	require.NotEmpty(t, epochs)
	var epoch uint64
	for _, e := range epochs {
		epoch = max(epoch, e.EpochId)
	}
	slot := epochs[0].StartSlot + 1
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
