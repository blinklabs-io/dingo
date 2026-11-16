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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
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
// own order, verified, with the statement guard armed.
func drainEveryGraduatedFamily(t *testing.T, h *p5Harness) {
	t.Helper()
	src := drainTestSource{set: h.set, db: h.scenario.DB}
	h.drainErr = statedrain.Run(t.Context(), statedrain.Env{
		Store: drainStore{h.scenario.DB, h.scenario.DrainFamilies},
		Set:   src, Guard: h.guard, VerifyBeforeDrain: true,
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
	h := newP5Harness(t, nil, nil)
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
					return append(lines, renderOpCerts(
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
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, guard, true)
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
				Guard: guard, VerifyBeforeDrain: true,
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
