// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openTestDingoDB opens a fresh, schema-migrated Dingo metadata sqlite file
// and returns a read-write *DingoDB the test can seed directly, matching
// check_test.go's newTestDingoDataDir pattern but exposing the writable
// handle (dingo_db.go's own OpenDingoDB is read-only, which a seeding test
// cannot use).
func openTestDingoDB(t *testing.T) (*DingoDB, *testDB) {
	t.Helper()
	dir := t.TempDir()
	gdb := openTestSQLDB(t, dir, true)
	return &DingoDB{db: gdb.db, dialect: "sqlite"}, gdb
}

func testPoolKeyHash(t *testing.T, b byte) []byte {
	t.Helper()
	h := make([]byte, 28)
	h[0] = b
	return h
}

// TestGetPoolEpochDataMapAlignsRewardScheduleEpochs is a boundary test built
// directly from Dingo's actual snapshot/reward lifecycle layout — see
// ledger/snapshot/rotation.go's buildRewardStateInputs (which stamps
// BlocksProduced from evt.PreviousEpoch onto the *next* epoch's
// reward_pool_input row) and ledger/reward_calculation.go's
// stakeRewardEpochsForNewEpoch (epochs.snapshot = epochs.performance - 1,
// with reward_pool_output written at epochs.snapshot alongside the
// reward_pool_input row actually consumed for that computation) — rather
// than two structs with equal same-epoch fields.
//
// For Koios reporting epoch K=10, this seeds:
//   - reward_pool_input at epoch 9 (K-1, the "stake epoch"): DelegatedStake/
//     DelegatorCount — the go stake snapshot Praos actually used as epoch
//     10's active-stake/reward-calculation basis.
//   - reward_pool_output at epoch 9 (same stake epoch): MemberRewardTotal,
//     written alongside reward_pool_input in the same reward-application
//     event.
//   - reward_pool_input at epoch 9 (K-1) also carries Margin/FixedCost: a
//     mark snapshot records the pool parameters as of its own boundary, and
//     those are the ones in force for the epoch that snapshot is the basis
//     for. They therefore align with the stake epoch, not the param epoch.
//   - reward_pool_input at epoch 11 (K+1, the "param epoch"): BlocksProduced
//     alone, captured onto the boundary *after* epoch 10 — the row
//     describing epoch 10's ended-epoch block count.
//
// A decoy reward_pool_input row is also seeded at epoch 10 itself with
// deliberately wrong values in every field, so the test fails loudly if
// GetPoolEpochDataMap regresses to reading the naive same-numbered epoch.
func TestGetPoolEpochDataMapAlignsRewardScheduleEpochs(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	poolHash := testPoolKeyHash(t, 0x01)

	blocksAtStakeEpoch := uint64(
		999,
	) // decoy: must never surface as blocks_produced
	blocksAtParamEpoch := uint64(4) // real: epoch 10's actual block count

	// Decoy row at the naive "same epoch as Koios" (10). Every field here is
	// wrong; if the map ever reads from epoch 10 directly instead of 9/11,
	// these values leak into the result and the assertions below fail.
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          10,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(111),
		DelegatorCount: 111,
		Cost:           types.Uint64(111),
		Margin:         &types.Rat{Rat: big.NewRat(1, 2)},
		BlocksProduced: &blocksAtStakeEpoch,
	}).Error)

	// Stake epoch (9 = K-1): DelegatedStake/DelegatorCount are the real
	// values for Koios epoch 10.
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          9,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(5_000_000),
		DelegatorCount: 7,
		Cost:           types.Uint64(340_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
	}).Error)

	// reward_pool_output shares the stake epoch (9), not epoch 10 or 11.
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             9,
		PoolKeyHash:       poolHash,
		MemberRewardTotal: types.Uint64(123_456),
	}).Error)

	// Param epoch (11 = K+1): BlocksProduced is the real value describing
	// epoch 10. Its stake and pool parameters belong to a later epoch and
	// must not surface, so they are seeded as decoys.
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:       11,
		PoolKeyHash: poolHash,
		DelegatedStake: types.Uint64(
			222,
		), // irrelevant at this epoch; must not surface
		Cost:           types.Uint64(999_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 2)},
		BlocksProduced: &blocksAtParamEpoch,
	}).Error)

	koiosEpoch := uint64(10)
	stakeEpoch, ok := koiosStakeEpoch(koiosEpoch)
	require.True(t, ok)
	require.Equal(t, uint64(9), stakeEpoch)
	paramEpoch := koiosParamEpoch(koiosEpoch)
	require.Equal(t, uint64(11), paramEpoch)

	m, err := dingo.GetPoolEpochDataMap(
		context.Background(),
		stakeEpoch,
		paramEpoch,
	)
	require.NoError(t, err)

	key := hex.EncodeToString(poolHash)
	data, ok := m[key]
	require.True(t, ok)

	require.Equal(
		t,
		"5000000",
		data.DelegatedStake,
		"delegated_stake must come from the stake epoch (K-1), not K",
	)
	require.Equal(t, uint64(7), data.DelegatorCount)

	require.True(t, data.ParamsPresent)
	require.Equal(
		t,
		blocksAtParamEpoch,
		data.BlocksProduced,
		"blocks_produced must come from the param epoch (K+1), not K",
	)
	require.Equal(
		t,
		"340000000",
		data.FixedCost,
		"fixed_cost must come from the stake epoch (K-1), not the param epoch",
	)
	require.Equal(
		t,
		"1/10",
		data.Margin,
		"margin must come from the stake epoch (K-1), not the param epoch",
	)

	require.True(t, data.MemberRewardPresent)
	require.Equal(
		t,
		"123456",
		data.MemberRewardTotal,
		"member_rewards must come from reward_pool_output at the stake epoch (K-1)",
	)
}

// TestGetPoolEpochDataMapMissingParamEpochRow proves a pool with a stake
// -epoch row but no param-epoch row yet still gets an entry, with
// ParamsPresent left false rather than silently defaulting to a
// zero-value BlocksProduced that ComparePoolEpoch could mistake for a real
// (and wrong) value. FixedCost/Margin are stake-epoch fields and so are
// unaffected by the param-epoch row's absence (dingo #3484).
func TestGetPoolEpochDataMapMissingParamEpochRow(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	poolHash := testPoolKeyHash(t, 0x02)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          9,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(1_000),
		DelegatorCount: 2,
	}).Error)

	m, err := dingo.GetPoolEpochDataMap(context.Background(), 9, 11)
	require.NoError(t, err)

	key := hex.EncodeToString(poolHash)
	data, ok := m[key]
	require.True(t, ok)
	require.False(t, data.ParamsPresent)
	require.False(t, data.MemberRewardPresent)
	require.Equal(t, "1000", data.DelegatedStake)
}

// TestGetPoolEpochDataMapMissingStakeEpochRow proves a pool with a
// param-epoch (and/or output) row but no stake-epoch row yet still gets an
// entry, with StakePresent left false rather than silently defaulting to a
// zero-value DelegatedStake/DelegatorCount that ComparePoolEpoch could
// mistake for a real (and wrong) value — the mirror image of
// TestGetPoolEpochDataMapMissingParamEpochRow, guarding the bug where a
// freshly registered pool's param-epoch row lands before its stake-epoch row
// does.
func TestGetPoolEpochDataMapMissingStakeEpochRow(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	poolHash := testPoolKeyHash(t, 0x03)
	blocksAtParamEpoch := uint64(4)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          11,
		PoolKeyHash:    poolHash,
		Cost:           types.Uint64(340_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
		BlocksProduced: &blocksAtParamEpoch,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             9,
		PoolKeyHash:       poolHash,
		MemberRewardTotal: types.Uint64(123_456),
	}).Error)

	m, err := dingo.GetPoolEpochDataMap(context.Background(), 9, 11)
	require.NoError(t, err)

	key := hex.EncodeToString(poolHash)
	data, ok := m[key]
	require.True(t, ok)
	require.False(
		t,
		data.StakePresent,
		"no reward_pool_input row at the stake epoch yet",
	)
	require.Equal(t, "", data.DelegatedStake)
	require.Equal(t, uint64(0), data.DelegatorCount)

	// Fields from the other two queries are still recorded normally.
	require.True(t, data.ParamsPresent)
	require.Equal(t, blocksAtParamEpoch, data.BlocksProduced)
	require.True(t, data.MemberRewardPresent)
	require.Equal(t, "123456", data.MemberRewardTotal)
}

// TestGetEpochDataStakeEpochOffset confirms epoch_summary is read at the
// caller-supplied epoch verbatim (GetEpochData itself is offset-agnostic —
// check.go is responsible for passing koiosStakeEpoch(K), not K) and that a
// pool key hash round-trips correctly, guarding the low-level building block
// koiosStakeEpoch/koiosParamEpoch and check.go's checkEpoch rely on.
func TestGetEpochDataStakeEpochOffset(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            9,
		TotalActiveStake: types.Uint64(42_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch: 10,
		TotalActiveStake: types.Uint64(
			99_999_999,
		), // decoy: must not be read for K=10's stake
		SnapshotReady: true,
	}).Error)

	koiosEpoch := uint64(10)
	stakeEpoch, ok := koiosStakeEpoch(koiosEpoch)
	require.True(t, ok)

	data, err := dingo.GetEpochData(context.Background(), stakeEpoch)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, "42000000", data.TotalActiveStake)
}

// TestDingoDBGetRewardAccountOutputs is a symmetry check with
// DatabaseSource's equivalent test (source_test.go's
// TestDatabaseSourceGetRewardAccountOutputs): both RewardParitySource
// implementations must return the same committed reward_account_output rows
// for an epoch, since #3097's per-account parity check will read either one
// interchangeably.
func TestDingoDBGetRewardAccountOutputs(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck
	stakingKey := testPoolKeyHash(t, 0x11)
	poolKeyHash := testPoolKeyHash(t, 0x22)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch:       8,
		StakingKey:  stakingKey,
		PoolKeyHash: poolKeyHash,
		RewardType:  "member",
		Amount:      types.Uint64(55),
		Spendable:   true,
	}).Error)

	rows, err := dingo.GetRewardAccountOutputs(context.Background(), 8)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, types.Uint64(55), rows[0].Amount)
	require.True(t, rows[0].Spendable)

	rows, err = dingo.GetRewardAccountOutputs(context.Background(), 9)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestPoolKeyHashRoundTrip is a sanity check that testPoolKeyHash produces a
// valid pool ID, used implicitly by the boundary tests above via
// hex.EncodeToString matching GetPoolEpochDataMap's own key format.
func TestPoolKeyHashRoundTrip(t *testing.T) {
	h := testPoolKeyHash(t, 0x01)
	var pid lcommon.PoolId
	copy(pid[:], h)
	require.Len(t, pid[:], 28)
}

// TestGetPoolEpochDataMapTracksChangingPoolParams reproduces the preview
// pools that exposed dingo #3484. Both fields are constant for the great
// majority of pools, so a wrong epoch alignment is invisible until a pool
// actually changes its margin or cost; these two did, at preview epoch 13.
//
// Observed values, with Koios epoch 13 as the reporting epoch K:
//
//	pool1nk3uj… reward_pool_input.cost   epoch 12 = 411000000, epoch 13 = 412000000
//	            Koios pool_history       epoch 13 = 411000000, epoch 14 = 412000000
//	pool1z9nsz… reward_pool_input.margin epoch 12 = 1/20,      epoch 13 = 1/25
//	            Koios pool_history       epoch 13 = 0.05,      epoch 14 = 0.04
//
// Koios epoch 13's values are on Dingo's epoch-12 row — the stake epoch —
// while its block count is on the epoch-14 row. Reading cost and margin from
// the param epoch compared 412000000 against 411000000 and 1/25 against 0.05.
func TestGetPoolEpochDataMapTracksChangingPoolParams(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	const koiosEpoch = uint64(13)
	stakeEpoch, ok := koiosStakeEpoch(koiosEpoch)
	require.True(t, ok)
	paramEpoch := koiosParamEpoch(koiosEpoch)

	poolHash := testPoolKeyHash(t, 0x11)
	blocksAtParamEpoch := uint64(10)

	// Stake epoch (12): the parameters in force for Koios epoch 13.
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          stakeEpoch,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(5_000_000),
		DelegatorCount: 1,
		Cost:           types.Uint64(411_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 20)},
	}).Error)

	// The pool raised its cost and lowered its margin, so the next snapshot
	// carries different values. Koios places these at epoch 14, not 13.
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:       koiosEpoch,
		PoolKeyHash: poolHash,
		Cost:        types.Uint64(412_000_000),
		Margin:      &types.Rat{Rat: big.NewRat(1, 25)},
	}).Error)

	// Param epoch (14): owns blocks_produced for epoch 13 and nothing else.
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          paramEpoch,
		PoolKeyHash:    poolHash,
		Cost:           types.Uint64(412_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 25)},
		BlocksProduced: &blocksAtParamEpoch,
	}).Error)

	m, err := dingo.GetPoolEpochDataMap(
		context.Background(),
		stakeEpoch,
		paramEpoch,
	)
	require.NoError(t, err)

	data, ok := m[hex.EncodeToString(poolHash)]
	require.True(t, ok)
	require.Equal(
		t,
		"411000000",
		data.FixedCost,
		"the cost in force for epoch %d is on the stake-epoch row",
		koiosEpoch,
	)
	require.Equal(
		t,
		"1/20",
		data.Margin,
		"the margin in force for epoch %d is on the stake-epoch row",
		koiosEpoch,
	)
	require.Equal(
		t,
		blocksAtParamEpoch,
		data.BlocksProduced,
		"blocks_produced still comes from the param epoch",
	)
}

// TestGetPoolEpochDataMapSpendableMemberPresenceIsEpochWide pins what
// SpendableMemberRewardPresent means, which the two RewardParitySource
// implementations must not be able to disagree about. It reports whether the
// epoch's reward_account_output rows survive at all — cleanupOldSnapshots
// retains them without bound only in api storage mode — not whether any of
// them is a credited member reward. An epoch holding only a leader reward and a
// withheld member reward is a populated epoch whose spendable member total is
// genuinely zero, and reporting that as a pruned epoch would turn a correct
// zero into a dingo_db_missing ERROR.
func TestGetPoolEpochDataMapSpendableMemberPresenceIsEpochWide(t *testing.T) {
	const stakeEpoch, paramEpoch = uint64(9), uint64(11)
	db, gdb := openTestDingoDB(t)
	pool := testPoolKeyHash(t, 0x01)

	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          stakeEpoch,
		PoolKeyHash:    pool,
		DelegatedStake: types.Uint64(1_000),
		DelegatorCount: 1,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             stakeEpoch,
		PoolKeyHash:       pool,
		MemberRewardTotal: types.Uint64(77),
		Unspendable:       types.Uint64(77),
	}).Error)
	// A leader reward and a withheld member reward: rows exist, nothing was
	// credited to a member.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch:       stakeEpoch,
		PoolKeyHash: pool,
		StakingKey:  testPoolKeyHash(t, 0x02),
		RewardType:  "leader",
		Amount:      types.Uint64(500),
		Spendable:   true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch:       stakeEpoch,
		PoolKeyHash: pool,
		StakingKey:  testPoolKeyHash(t, 0x03),
		RewardType:  "member",
		Amount:      types.Uint64(77),
		Spendable:   false,
	}).Error)

	// A second pool in the same epoch with no reward_account_output rows of
	// its own. "Epoch-wide" means it is answerable too, at zero — a per-pool
	// presence rule would report it as pruned and turn a correct zero into a
	// dingo_db_missing ERROR.
	quiet := testPoolKeyHash(t, 0x04)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          stakeEpoch,
		PoolKeyHash:    quiet,
		DelegatedStake: types.Uint64(1_000),
		DelegatorCount: 1,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             stakeEpoch,
		PoolKeyHash:       quiet,
		MemberRewardTotal: types.Uint64(0),
	}).Error)

	m, err := db.GetPoolEpochDataMap(
		context.Background(), stakeEpoch, paramEpoch,
	)
	require.NoError(t, err)
	data := m[hex.EncodeToString(pool)]
	require.NotNil(t, data)
	assert.True(t, data.SpendableMemberRewardPresent,
		"rows exist for the epoch, so the sum is answerable")
	assert.Equal(t, "0", data.SpendableMemberRewardTotal,
		"nothing was credited to a member")
	assert.Equal(t, uint64(77), data.PoolUnspendable)

	quietData := m[hex.EncodeToString(quiet)]
	require.NotNil(t, quietData)
	assert.True(t, quietData.SpendableMemberRewardPresent,
		"presence is a property of the epoch, not of the pool")
	assert.Equal(t, "0", quietData.SpendableMemberRewardTotal)
}

// TestGetPoolEpochDataMapSpendableMemberAbsentWhenEpochPruned is the other
// side: with no reward_account_output rows for the epoch at all, the sum cannot
// be formed and presence must stay false so ComparePoolEpoch falls back to the
// pool total only where that is provably equal.
func TestGetPoolEpochDataMapSpendableMemberAbsentWhenEpochPruned(t *testing.T) {
	const stakeEpoch, paramEpoch = uint64(9), uint64(11)
	db, gdb := openTestDingoDB(t)
	pool := testPoolKeyHash(t, 0x01)

	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          stakeEpoch,
		PoolKeyHash:    pool,
		DelegatedStake: types.Uint64(1_000),
		DelegatorCount: 1,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             stakeEpoch,
		PoolKeyHash:       pool,
		MemberRewardTotal: types.Uint64(500),
	}).Error)

	m, err := db.GetPoolEpochDataMap(
		context.Background(), stakeEpoch, paramEpoch,
	)
	require.NoError(t, err)
	data := m[hex.EncodeToString(pool)]
	require.NotNil(t, data)
	assert.False(t, data.SpendableMemberRewardPresent)
	assert.Equal(t, "500", data.MemberRewardTotal)
}
