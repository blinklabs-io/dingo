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
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const (
	withdrawalResumStakeKey = 0x22
	withdrawalResumReward   = 5_000_000
)

type withdrawalResumStake struct {
	UtxoStake   uint64
	RewardStake uint64
	TotalStake  uint64
}

type withdrawalResumScenario struct {
	name string
	// spend makes the stake change spend C's 3 ADA output instead of
	// paying C 2 ADA.
	spend bool
	// withdrawFirst orders the withdrawal before the stake change.
	withdrawFirst bool
	// oneTx makes the stake change and the withdrawal one transaction.
	oneTx bool
}

func (s withdrawalResumScenario) wantUtxoStake() uint64 {
	if s.spend {
		return 0
	}
	return 5_000_000
}

var withdrawalResumScenarios = []withdrawalResumScenario{
	{name: "pay_then_withdraw"},
	{name: "spend_then_withdraw", spend: true},
	{name: "withdraw_then_pay", withdrawFirst: true},
	{name: "withdraw_then_spend", spend: true, withdrawFirst: true},
	{name: "pay_and_withdraw_one_tx", oneTx: true},
	{name: "spend_and_withdraw_one_tx", spend: true, oneTx: true},
}

// withdrawalResumBlocks returns two blocks. Block 1 registers credential C
// and pays it 3 ADA. Block 2 changes C's UTxO stake (pays C 2 ADA, or
// spends C's 3 ADA output) and withdraws 1 ADA from C, in the order and
// transaction split the scenario names.
func withdrawalResumBlocks(
	t *testing.T,
	scenario withdrawalResumScenario,
) []gledger.Block {
	t.Helper()
	seed := bytes.Repeat([]byte{0xAB}, 32)
	payStake := rowBatchingBaseAddress(t, 0x44, withdrawalResumStakeKey)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	register := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{
			map[uint]any{0: payOther, 1: uint64(1_000_000)},
			map[uint]any{0: payStake, 1: uint64(3_000_000)},
		},
		2: uint64(100_000),
		4: []any{[]any{
			uint64(0), rowBatchingCred(withdrawalResumStakeKey),
		}},
	})
	withdrawals := map[cbor.ByteString]uint64{
		cbor.NewByteString(
			rowBatchingRewardAccount(withdrawalResumStakeKey),
		): 1_000_000,
	}
	change := map[uint]any{
		0: []any{[]any{seed, uint64(1)}},
		1: []any{map[uint]any{0: payStake, 1: uint64(2_000_000)}},
		2: uint64(100_000),
	}
	if scenario.spend {
		change = map[uint]any{
			0: []any{[]any{register.hash, uint64(1)}},
			1: []any{map[uint]any{0: payOther, 1: uint64(2_900_000)}},
			2: uint64(100_000),
		}
	}
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, register)
	if scenario.oneTx {
		change[5] = withdrawals
		b2 := rowBatchingBlock(
			t, 20, 2, b1.Hash(), newRowBatchingTx(t, change),
		)
		return []gledger.Block{b1, b2}
	}
	withdraw := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(2)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_000_000)}},
		2: uint64(100_000),
		5: withdrawals,
	})
	txs := []rowBatchingTx{newRowBatchingTx(t, change), withdraw}
	if scenario.withdrawFirst {
		txs[0], txs[1] = txs[1], txs[0]
	}
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), txs...)
	return []gledger.Block{b1, b2}
}

// seedWithdrawalResumReward gives C a reward balance to withdraw. Nothing
// in a two-block scenario can earn one, so it is credited between the two
// blocks. The credit fails unless block 1 registered C.
func seedWithdrawalResumReward(t *testing.T, db *database.Database) {
	t.Helper()
	require.NoError(t, db.AddAccountRewardByCredential(
		t.Context(),
		0,
		rowBatchingKey(withdrawalResumStakeKey),
		withdrawalResumReward,
		10,
		bytes.Repeat([]byte{0xCD}, 32),
		nil,
	), "block 1 must register C")
}

func withdrawalResumStored(
	t *testing.T,
	db *database.Database,
) withdrawalResumStake {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	var got withdrawalResumStake
	require.NoError(t, raw.QueryRow(
		`SELECT CAST(utxo_stake AS INTEGER), CAST(reward_stake AS INTEGER),
		        CAST(total_stake AS INTEGER)
		 FROM reward_live_stake WHERE credential_tag = 0 AND staking_key = ?`,
		rowBatchingKey(withdrawalResumStakeKey),
	).Scan(&got.UtxoStake, &got.RewardStake, &got.TotalStake))
	return got
}

// withdrawalResumIndependent computes C's stake from the final live UTxO
// set and reward balance, without reading reward_live_stake.
func withdrawalResumIndependent(
	t *testing.T,
	db *database.Database,
) withdrawalResumStake {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	var got withdrawalResumStake
	require.NoError(t, raw.QueryRow(
		`SELECT COALESCE(SUM(CAST(amount AS INTEGER)), 0) FROM utxo
		 WHERE credential_tag = 0 AND staking_key = ? AND deleted_slot = 0`,
		rowBatchingKey(withdrawalResumStakeKey),
	).Scan(&got.UtxoStake))
	require.NoError(t, raw.QueryRow(
		`SELECT CAST(reward AS INTEGER) FROM account WHERE staking_key = ?`,
		rowBatchingKey(withdrawalResumStakeKey),
	).Scan(&got.RewardStake))
	got.TotalStake = got.UtxoStake + got.RewardStake
	return got
}

// runWithdrawalResumReplay applies the blocks through
// ProcessTrustedBlockBatches, one block per call, with the row-batching
// setting off or on. It returns the database and the batched write count.
func runWithdrawalResumReplay(
	t *testing.T,
	blocks []gledger.Block,
	batching bool,
) (*database.Database, int64, error) {
	t.Helper()
	h := newExternalIngestHarnessWith(
		t, true, t.TempDir(), types.StorageModeCore, 0,
	)
	h.ls.config.ApplyRowBatchingEnabled = batching
	batched := h.countBatchedWrites()
	for _, block := range blocks {
		require.NoError(t, h.cm.PrimaryChain().AddBlock(
			t.Context(), block, nil,
		))
	}
	for i, block := range blocks {
		ch := make(chan []gledger.Block, 1)
		ch <- []gledger.Block{block}
		close(ch)
		if err := h.ls.ProcessTrustedBlockBatches(t.Context(), ch); err != nil {
			return h.db, batched.Load(), err
		}
		if i == 0 {
			seedWithdrawalResumReward(t, h.db)
		}
	}
	return h.db, batched.Load(), nil
}

func runWithdrawalResumTrusted(
	t *testing.T,
	blocks []gledger.Block,
) (*database.Database, int64, error) {
	t.Helper()
	h := newExternalIngestHarness(t, false)
	batched := h.countBatchedWrites()
	overlays := h.countOverlays()
	expected := ocommon.Point{}
	for i, block := range blocks {
		before := overlays.Load()
		tip, err := h.applyPaused(t, expected, []gledger.Block{block})
		if err != nil {
			return h.db, batched.Load(), err
		}
		// Not require: p.run is called from a subtest with the parent's t.
		if overlays.Load() == before {
			return h.db, batched.Load(), errors.New(
				"the block did not apply through the batch UTxO overlay",
			)
		}
		expected = tip
		if i == 0 {
			seedWithdrawalResumReward(t, h.db)
		}
	}
	return h.db, batched.Load(), nil
}

// TestWithdrawalResumAfterSameBlockStakeChange applies a block that both
// changes credential C's UTxO stake and withdraws from C, in every order and
// transaction split, through the unbatched replay, the row-batching replay
// and ApplyTrustedBatch, and requires each path's reward_live_stake row for
// C to equal the value computed from the final UTxO set and reward balance.
func TestWithdrawalResumAfterSameBlockStakeChange(t *testing.T) {
	t.Parallel()
	for _, scenario := range withdrawalResumScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			blocks := withdrawalResumBlocks(t, scenario)
			wantUtxo := scenario.wantUtxoStake()
			want := withdrawalResumStake{
				UtxoStake:   wantUtxo,
				RewardStake: withdrawalResumReward - 1_000_000,
				TotalStake:  wantUtxo + withdrawalResumReward - 1_000_000,
			}

			type path struct {
				name string
				run  func() (*database.Database, int64, error)
				// batched is whether the path must write through the
				// row-batching accumulator.
				batched bool
			}
			paths := []path{
				{"unbatched_replay", func() (*database.Database, int64, error) {
					return runWithdrawalResumReplay(t, blocks, false)
				}, false},
				{
					"row_batching_replay",
					func() (*database.Database, int64, error) {
						return runWithdrawalResumReplay(t, blocks, true)
					},
					true,
				},
				{
					"apply_trusted_batch",
					func() (*database.Database, int64, error) {
						return runWithdrawalResumTrusted(t, blocks)
					},
					true,
				},
			}
			for _, p := range paths {
				t.Run(p.name, func(t *testing.T) {
					db, batched, err := p.run()
					if p.batched {
						require.Positive(t, batched,
							"the path must take the batched writer")
					} else {
						require.Zero(t, batched,
							"the reference must take the unbatched writer")
					}
					require.NoError(t, err, "applying the blocks failed")
					require.Equal(t, want, withdrawalResumIndependent(t, db),
						"final UTxO set and reward balance")
					require.Equal(t, want, withdrawalResumStored(t, db),
						"reward_live_stake for C diverged from the "+
							"independently computed stake")
				})
			}
		})
	}
}
