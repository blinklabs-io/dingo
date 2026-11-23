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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/statedrain"
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

// With the live set with the metadata store's reads of blocks, imported counts,
// fees and pots failing, the reward round's counts and fees are the ones the
// store would give: overlay slots left out, a collateral fee for a
// phase-2-invalid transaction, the imported counts and fees of a Mithril
// anchor merged with what the node applied after it.
func TestRewardBlockCountsAndFeesAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	for _, imported := range []bool{false, true} {
		t.Run(fmt.Sprintf("imported=%t", imported), func(t *testing.T) {
			t.Parallel()
			r := newRewardBlocksRun(t, "set", nil, imported)
			r.write()
			reached, _ := r.scenario.HideStoreReads(rewardBlockReadNames...)
			for _, epoch := range []uint64{3, 4, 5} {
				require.Equal(
					t, r.expected(epoch), r.read(t, epoch), "epoch %d", epoch,
				)
			}
			require.Zero(t, reached(), "a read reached the metadata store")
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
			r := newRewardBlocksRun(t, "set", nil, imported)
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
			reads := ""
			store := newRewardBlocksRun(t, reads, nil, imported)
			store.write()
			for _, epoch := range []uint64{3, 4, 5} {
				require.Equal(
					t, expected(store, epoch), read(store, epoch),
					"store, epoch %d", epoch,
				)
			}
			r := newRewardBlocksRun(t, "set", nil, imported)
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

// With the family drained and the set holding no rows for it, the reward
// round's fee read fails rather than answer from the emptied tables.
func TestRewardBlockFeesFailWhenTheDrainedSetHasNoRows(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, false)
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
