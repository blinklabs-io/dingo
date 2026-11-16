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
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

var rewardBlockReadNames = []string{
	"CountPoolBlocksInSlotRange", "GetPoolBlockIssuersInSlotRange",
	"MergeImportedPoolBlockCounts", "SumTransactionFeesInSlotRange",
	"GetRewardAdaPots", "CountEpochPoolBlocks",
}

type rewardBlocksRun struct {
	scenario ledger.RewardBlocksScenario
	set      *utxoset.Set
	imported bool
}

func newRewardBlocksRun(
	t *testing.T,
	reads string,
	guard *statedrain.TripGuard,
	imported bool,
) *rewardBlocksRun {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	var g metadata.StatementGuard
	if guard != nil {
		g = guard
	}
	scenario := ledger.NewRewardBlocksScenarioForTest(
		t, set, reads, g, ledger.RewardBlocksOptions{Imported: imported},
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	return &rewardBlocksRun{scenario: scenario, set: set, imported: imported}
}

// write commits blocks and transactions in epochs 3, 4 and 5. TPraos
// reserves the first slot of each two-slot epoch, so the blocks at slots 6,
// 8 and 10 are overlay blocks no round counts. A Mithril anchor at slot 8
// leaves nothing to write at or below it.
func (r *rewardBlocksRun) write() {
	s := r.scenario
	s.Collateral(0xc0, 1_000)
	if !r.imported {
		s.Block(6, s.PoolA)
		s.Block(7, s.PoolA)
		s.Fee(7, 100)
		s.Block(8, s.PoolB)
	}
	s.Block(9, s.PoolB)
	s.Fee(9, 200)
	s.InvalidTotal(9, 300)
	s.Block(10, s.PoolA)
	s.Fee(10, 50)
	s.InvalidInputs(10, 0xc0, 400)
	s.InvalidInputs(10, 0xee, 5)
	s.Block(11, s.PoolB)
	s.Fee(11, 25)
}

type rewardBlockAnswer struct {
	counts map[string]uint64
	total  uint64
	known  bool
	fees   uint64
}

func (r *rewardBlocksRun) read(t *testing.T, epoch uint64) rewardBlockAnswer {
	t.Helper()
	counts, total, known, err := r.scenario.Counts(epoch)
	require.NoError(t, err, "counts of epoch %d", epoch)
	fees, err := r.scenario.Fees(epoch)
	require.NoError(t, err, "fees of epoch %d", epoch)
	return rewardBlockAnswer{counts, total, known, fees}
}

func (r *rewardBlocksRun) expected(epoch uint64) rewardBlockAnswer {
	a, b, idle := string(r.scenario.PoolA), string(r.scenario.PoolB),
		string(r.scenario.PoolIdle)
	c := func(av, bv, iv uint64) map[string]uint64 {
		return map[string]uint64{a: av, b: bv, idle: iv}
	}
	if r.imported {
		return map[uint64]rewardBlockAnswer{
			3: {c(0, 2, 1), 3, true, 0},
			4: {c(3, 1, 0), 4, true, 1_500},
			5: {c(0, 1, 0), 1, true, 675},
		}[epoch]
	}
	return map[uint64]rewardBlockAnswer{
		3: {c(1, 0, 0), 1, true, 100},
		4: {c(0, 1, 0), 1, true, 500},
		5: {c(0, 1, 0), 1, true, 675},
	}[epoch]
}

// In set mode with the metadata store's reads of blocks, imported counts,
// fees and pots failing, the reward round's counts and fees are the ones the
// store would give: overlay slots left out, a collateral fee for a
// phase-2-invalid transaction, the imported counts and fees of a Mithril
// anchor merged with what the node applied after it.
func TestRewardBlockCountsAndFeesAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	for _, imported := range []bool{false, true} {
		t.Run(fmt.Sprintf("imported=%t", imported), func(t *testing.T) {
			t.Parallel()
			r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, imported)
			r.write()
			reached, _ := r.scenario.HideStoreReads(rewardBlockReadNames...)
			for _, epoch := range []uint64{3, 4, 5} {
				require.Equal(
					t, r.expected(epoch), r.read(t, epoch), "epoch %d", epoch,
				)
			}
			require.Zero(t, reached(), "a read reached the metadata store")
			verified, mismatches := r.scenario.Computed("reward_block_counts")
			require.Zero(t, verified, "set mode compares nothing")
			require.Zero(t, mismatches)
		})
	}
}

// A phase-2-invalid transaction consumes its collateral and creates its
// collateral return, and nothing else. After the scenario's invalid
// transactions the set holds the 400 and 5 lovelace returns and no longer the
// 1000 lovelace collateral output they consumed.
func TestInvalidTransactionsMoveTheSetsCredentialStake(t *testing.T) {
	t.Parallel()
	for _, imported := range []bool{false, true} {
		t.Run(fmt.Sprintf("imported=%t", imported), func(t *testing.T) {
			t.Parallel()
			r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, imported)
			r.write()
			stake, err := r.set.CredentialStake(
				0, bytes.Repeat([]byte{0xab}, 28),
			)
			require.NoError(t, err)
			require.Equal(t, uint64(405), stake)
		})
	}
}

// The boundary capture counts every block of the ended epoch, the slots TPraos
// reserves for the overlay schedule too, which the reward round does not. In
// set mode the counts come from the set and are the store's.
func TestEpochCaptureBlockCountsIncludeOverlaySlotsFromTheSet(t *testing.T) {
	t.Parallel()
	for _, imported := range []bool{false, true} {
		t.Run(fmt.Sprintf("imported=%t", imported), func(t *testing.T) {
			t.Parallel()
			expected := func(
				r *rewardBlocksRun,
				epoch uint64,
			) rewardBlockAnswer {
				a := string(r.scenario.PoolA)
				b := string(r.scenario.PoolB)
				idle := string(r.scenario.PoolIdle)
				c := func(av, bv, iv uint64) map[string]uint64 {
					return map[string]uint64{a: av, b: bv, idle: iv}
				}
				if imported {
					return map[uint64]rewardBlockAnswer{
						3: {counts: c(0, 2, 1), total: 3, known: true},
						4: {counts: c(3, 1, 0), total: 4, known: true},
						5: {counts: c(1, 1, 0), total: 2, known: true},
					}[epoch]
				}
				return map[uint64]rewardBlockAnswer{
					3: {counts: c(2, 0, 0), total: 2, known: true},
					4: {counts: c(0, 2, 0), total: 2, known: true},
					5: {counts: c(1, 1, 0), total: 2, known: true},
				}[epoch]
			}
			read := func(r *rewardBlocksRun, epoch uint64) rewardBlockAnswer {
				counts, total, known, err := r.scenario.EpochCounts(epoch)
				require.NoError(t, err, "epoch %d", epoch)
				return rewardBlockAnswer{
					counts: counts, total: total, known: known,
				}
			}
			reads := ledger.UtxoSetReadsOff
			store := newRewardBlocksRun(t, reads, nil, imported)
			store.write()
			for _, epoch := range []uint64{3, 4, 5} {
				require.Equal(
					t, expected(store, epoch), read(store, epoch),
					"store, epoch %d", epoch,
				)
			}
			r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, imported)
			r.write()
			reached, _ := r.scenario.HideStoreReads(rewardBlockReadNames...)
			for _, epoch := range []uint64{3, 4, 5} {
				require.Equal(
					t, expected(r, epoch), read(r, epoch),
					"set, epoch %d", epoch,
				)
			}
			require.Zero(t, reached(), "a read reached the metadata store")
		})
	}
}

// In verify mode the metadata store answers and the set's answer is compared
// with it: the same writes leave the two equal for every epoch.
func TestRewardBlockCountsAndFeesMatchTheStoreInVerifyMode(t *testing.T) {
	t.Parallel()
	for _, imported := range []bool{false, true} {
		t.Run(fmt.Sprintf("imported=%t", imported), func(t *testing.T) {
			t.Parallel()
			r := newRewardBlocksRun(t, ledger.UtxoSetReadsVerify, nil, imported)
			r.write()
			for _, epoch := range []uint64{3, 4, 5} {
				require.Equal(
					t, r.expected(epoch), r.read(t, epoch), "epoch %d", epoch,
				)
			}
			for _, result := range []string{
				"reward_block_counts", "reward_epoch_fees",
			} {
				verified, mismatches := r.scenario.Computed(result)
				require.Positive(t, verified, result)
				require.Zero(t, mismatches, result)
			}
		})
	}
}

// A write the set never saw is a difference verify mode reports, so the
// comparison is not vacuous.
func TestRewardBlockVerifyModeReportsADifference(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsVerify, nil, false)
	r.write()
	r.scenario.FeeAroundTheSet(11, 1)
	r.scenario.BlockAroundTheSet(11, r.scenario.PoolA)
	_ = r.read(t, 5)
	for _, result := range []string{
		"reward_block_counts", "reward_epoch_fees",
	} {
		_, mismatches := r.scenario.Computed(result)
		require.Positive(t, mismatches, result)
	}
}

// The boundary stores the fee total of the epoch it ends from the set, a
// rollback past the epoch's last block undoes that block's count and fee, and
// the replayed block and boundary end where the first pass did.
func TestRewardBlockFeesAcrossABoundaryRollbackAndReplay(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, false)
	s := r.scenario
	r.write()
	// The boundary itself reads the pots row it extends.
	reached, _ := s.HideStoreReads(rewardBlockReadNames[:4]...)

	s.Boundary()
	require.Equal(t, uint64(675), s.PotsFees(6))
	require.Equal(t, r.expected(5), r.read(t, 5))
	// The boundary opened epoch 6 with zero counts and fees, known to the
	// set, so the store is not asked.
	opened, err := s.Fees(6)
	require.NoError(t, err)
	require.Zero(t, opened)

	s.RollbackBeforeBoundary()
	after := r.read(t, 5)
	require.Equal(t, uint64(650), after.fees, "slot 11's fee is undone")
	require.Equal(t, uint64(0), after.counts[string(s.PoolB)])
	require.Zero(t, after.total, "slot 11's block is undone")

	s.Block(11, s.PoolB)
	s.Fee(11, 25)
	s.Boundary()
	require.Equal(t, uint64(675), s.PotsFees(6))
	require.Equal(t, r.expected(5), r.read(t, 5))
	require.Zero(t, reached(), "a read reached the metadata store")
}

// The same run with the family drained, its tables empty and the statement
// guard armed gives the same answers as the undrained run, across a boundary,
// a rollback and a replay, and the guard sees no statement on a drained table.
func TestRewardBlocksFeesDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, drain bool) (map[string]string, *statedrain.TripGuard, *rewardBlocksRun) {
		guard := statedrain.NewTripGuard()
		r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, guard, true)
		s := r.scenario
		out := map[string]string{}
		phase := func(name string, epochs ...uint64) {
			for _, epoch := range epochs {
				a := r.read(t, epoch)
				out[fmt.Sprintf("%s/%d", name, epoch)] = fmt.Sprintf("%+v", a)
			}
		}
		r.write()
		if drain {
			src := rewardBlocksDrainSource{r: r}
			var entries []statedrain.Entry
			for _, e := range statedrain.Graduated(src) {
				if e.Family == nodesettings.FamilyRewardBlocksFees {
					entries = append(entries, e)
				}
			}
			require.Len(t, entries, 1)
			require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
				Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
				Guard: guard, VerifyBeforeDrain: true,
			}, entries))
		}
		phase("before", 3, 4, 5)
		s.Boundary()
		out["pots"] = fmt.Sprint(s.PotsFees(6))
		phase("boundary", 4, 5)
		if drain {
			// The boundary pruned epoch 3, which the drained set no longer
			// holds and the emptied tables cannot answer.
			_, err := s.Fees(3)
			require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
		}
		s.RollbackBeforeBoundary()
		phase("rollback", 4, 5)
		s.Block(11, s.PoolB)
		s.Fee(11, 25)
		s.Boundary()
		out["pots replay"] = fmt.Sprint(s.PotsFees(6))
		phase("replay", 4, 5)
		return out, guard, r
	}
	undrained, _, plain := run(t, false)
	drained, guard, r := run(t, true)
	require.Empty(t, guard.Trips(), "a drained table was touched")
	rawPlain, err := dbtest.RawSQLiteMetadata(t, plain.scenario.DB)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, r.scenario.DB)
	require.NoError(t, err)
	for _, table := range []string{
		"imported_pool_block_count", "imported_epoch_block_total",
	} {
		require.Positive(
			t,
			tableRows(t, rawPlain, table),
			"undrained %s",
			table,
		)
		require.Zero(t, tableRows(t, raw, table), "drained %s", table)
	}
	mask, err := r.scenario.DB.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyRewardBlocksFees))
	require.Equal(t, undrained, drained)
	require.Equal(t, "675", drained["pots"])
	require.Equal(t, "675", drained["pots replay"])
}

// A set that differs from the metadata store refuses the drain and leaves the
// rows.
func TestRewardBlocksFeesVerifyMismatchRefusesTheDrain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		corrupt func(*rewardBlocksRun)
		want    string
	}{
		{"fees", func(r *rewardBlocksRun) {
			r.scenario.FeeAroundTheSet(11, 1)
		}, "fees"},
		{"blocks", func(r *rewardBlocksRun) {
			r.scenario.BlockAroundTheSet(11, r.scenario.PoolA)
		}, "block"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			guard := statedrain.NewTripGuard()
			r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, guard, true)
			r.write()
			tc.corrupt(r)
			src := rewardBlocksDrainSource{r: r}
			var entries []statedrain.Entry
			for _, e := range statedrain.Graduated(src) {
				if e.Family == nodesettings.FamilyRewardBlocksFees {
					entries = append(entries, e)
				}
			}
			err := statedrain.Run(t.Context(), statedrain.Env{
				Store: drainStore{r.scenario.DB, r.scenario.DrainFamilies},
				Set:   src, Guard: guard, VerifyBeforeDrain: true,
			}, entries)
			require.ErrorIs(t, err, statedrain.ErrGateRefused)
			require.ErrorContains(t, err, tc.want)
			mask, err := r.scenario.DB.DrainedFamilies(t.Context())
			require.NoError(t, err)
			require.False(t, mask.Has(nodesettings.FamilyRewardBlocksFees))
			raw, err := dbtest.RawSQLiteMetadata(t, r.scenario.DB)
			require.NoError(t, err)
			require.Positive(t, tableRows(t, raw, "imported_pool_block_count"))
		})
	}
}

// With the family drained and the set holding no rows for it, the reward
// round's fee read fails rather than answer from the emptied tables.
func TestRewardBlockFeesFailWhenTheDrainedSetHasNoRows(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, false)
	r.write()
	require.NoError(t, r.scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyRewardBlocksFees,
	))
	r.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardBlocksFees),
	)
	// Epoch 2 is outside the window the set loaded.
	_, err := r.scenario.Fees(2)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, known, err := r.scenario.Counts(2)
	require.NoError(t, err)
	require.False(t, known, "an epoch the set holds no counts for is unknown")
}

// rewardBlocksDrainSource is the statedrain.Source of a rewardBlocksRun.
type rewardBlocksDrainSource struct{ r *rewardBlocksRun }

func (s rewardBlocksDrainSource) Tip() (ocommon.Point, bool, error) {
	tip, err := s.r.scenario.DB.GetTip(nil)
	return tip.Point, true, err
}

func (s rewardBlocksDrainSource) Loaded(f nodesettings.Family) (bool, error) {
	return ledger.FamilyLoaded(f, s.r.set)
}

func (s rewardBlocksDrainSource) Verify(
	ctx context.Context,
	f nodesettings.Family,
) error {
	return ledger.VerifyFamily(ctx, f, dbadapter.New(s.r.scenario.DB), s.r.set)
}
