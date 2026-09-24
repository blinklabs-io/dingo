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
	"context"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	retentionNewEpoch            = uint64(4)
	retentionRewardSnapshotEpoch = uint64(1)
	retentionPerformanceEpoch    = uint64(2)
	retentionPotsEpoch           = uint64(3)
	retentionBoundarySlot        = uint64(400)
)

// seedRetentionRewardEpochs seeds the epoch rows, protocol parameters, and ADA
// pots that reward application reads before it reaches the retention skip, so a
// test that removes the skip fails inside the reward calculation rather than
// earlier on missing epoch metadata.
func seedRetentionRewardEpochs(t *testing.T, db *database.Database) {
	t.Helper()
	meta := db.Metadata()
	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)
	for _, epoch := range []struct {
		startSlot uint64
		id        uint64
	}{
		{0, retentionRewardSnapshotEpoch},
		{100, retentionPerformanceEpoch},
		{200, retentionPotsEpoch},
	} {
		require.NoError(t, meta.SetEpoch(
			epoch.startSlot, epoch.id, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1, 100, nil,
		), "set epoch %d", epoch.id)
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		retentionPerformanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        retentionPotsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
}

// TestApplyStakeRewardsSkipsPrunedStakeInputs covers the retention interaction
// tracked in dingo #4578. reward_ada_pots, reward_snapshot,
// reward_pool_input and reward_pool_output are retained for the life of the
// database while reward_stake_input is pruned to the rotation window, so an
// aged-out epoch presents complete-looking pots and snapshot rows over an empty
// credential set. Reward application must skip that epoch rather than hand
// validateRewardCalculatorInputs an unreconcilable snapshot, whose error would
// fail the whole epoch rollover.
func TestApplyStakeRewardsSkipsPrunedStakeInputs(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	poolKey := rewardCalcHash(0x11)
	rewardAccount := rewardCalcHash(0x22)

	seedRetentionRewardEpochs(t, db)

	// Snapshot and pool input survive retention, and the snapshot still claims
	// two delegators whose credential rows have aged out.
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            retentionRewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      retentionRewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	// reward_stake_input is deliberately absent: those rows aged out of the
	// retention window.

	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Pool:       poolKey,
		Active:     true,
	}))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "aged-out stake inputs must skip reward application, not error")

	// Nothing was credited and no outputs were persisted for the skipped epoch.
	account, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Zero(
		t,
		uint64(account.Reward),
		"skipped epoch must not credit rewards",
	)

	poolOutputs, err := meta.GetRewardPoolOutputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Empty(t, poolOutputs, "skipped epoch must not persist pool outputs")

	accountOutputs, err := meta.GetRewardAccountOutputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Empty(
		t,
		accountOutputs,
		"skipped epoch must not persist account outputs",
	)

	var deltas int64
	require.NoError(t, rewardCalcSQLDB(t, db).QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE added_slot = ?",
		retentionBoundarySlot,
	).Scan(&deltas))
	require.Zero(t, deltas, "skipped epoch must not record reward deltas")
}

// TestApplyStakeRewardsAcceptsZeroDelegatorSnapshot guards the skip predicate
// itself: an epoch that legitimately captured no delegators has an empty
// credential set too, and must still reconcile as a normal (non-pruned)
// snapshot rather than tripping the retention skip.
func TestApplyStakeRewardsAcceptsZeroDelegatorSnapshot(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	seedRetentionRewardEpochs(t, db)
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            retentionRewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 0,
		TotalPoolCount:   0,
		TotalDelegators:  0,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "an empty snapshot must reconcile normally, not error")
}

// seedPrunedStakeInputSnapshot seeds a mark snapshot and pool input that
// survive retention over an empty reward_stake_input credential set -- the
// same aged-out-epoch shape TestApplyStakeRewardsSkipsPrunedStakeInputs
// seeds -- so a test can drive the retention skip without duplicating the
// pool/account wiring at every call site.
func seedPrunedStakeInputSnapshot(
	t *testing.T,
	db *database.Database,
	poolKey, rewardAccount []byte,
) {
	t.Helper()
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            retentionRewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      retentionRewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Pool:       poolKey,
		Active:     true,
	}))
	// reward_stake_input is deliberately absent: those rows aged out of the
	// retention window.
}

// TestApplyStakeRewardsSkipsPrunedStakeInputsReportsLoudly proves the
// retention skip tracked in dingo #4578 is reported the same way its three
// sibling skips in calculateStakeRewardApplication are, through
// reportSkippedStakeRewards: counted, and logged with the permanent-shortfall
// consequence spelled out. Before this fix the retention skip was the one
// silent-by-comparison exception to what this file otherwise guards against
// (see TestSkippedStakeRewardsIsReportedLoudly and issue #3165) -- it logged
// inline at Warn with a bare reason and no metric increment, so monitoring
// built on the shared skippedStakeRewardRounds counter never saw this
// specific permanent-reward-loss condition.
func TestApplyStakeRewardsSkipsPrunedStakeInputsReportsLoudly(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	seedRetentionRewardEpochs(t, db)
	seedPrunedStakeInputSnapshot(
		t, db, rewardCalcHash(0x33), rewardCalcHash(0x44),
	)

	var logs bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "aged-out stake inputs must still skip, not error")

	out := logs.String()
	require.NotEmpty(t, out,
		"the retention skip must be visible at the default log level, "+
			"the same as its three sibling skips")
	assert.Contains(t, out, "level=WARN")
	assert.Contains(
		t,
		out,
		"reward stake inputs for the snapshot epoch are no longer retained",
	)
	assert.Contains(t, out, "reward_snapshot_epoch=1")
	assert.Contains(t, out, "snapshot_delegators=2")
	// The consequence, not just the event -- see reportSkippedStakeRewards.
	assert.Contains(t, out, "permanently")
	assert.Contains(t, out, "basis was never persisted")
}

// TestSkippedPrunedStakeInputsSuppressedDuringPrecompute proves the retention
// skip honors reportSkips like its three siblings in
// calculateStakeRewardApplication. The opportunistic precompute pass reads the
// same possibly-not-yet-retained inputs ahead of the real boundary and can
// miss while the round is still unapplied -- reportSkips exists precisely so
// that miss is not logged or counted as a skipped round the authoritative
// call goes on to apply moments later.
func TestSkippedPrunedStakeInputsSuppressedDuringPrecompute(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	seedRetentionRewardEpochs(t, db)
	seedPrunedStakeInputSnapshot(
		t, db, rewardCalcHash(0x55), rewardCalcHash(0x66),
	)

	var logs bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		retentionNewEpoch,
		retentionBoundarySlot,
		retentionBoundarySlot,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)

	assert.Empty(t, logs.String(),
		"an opportunistic precompute miss must stay silent, like its three "+
			"sibling skips, since the authoritative call still gets to apply "+
			"the round")
}

type retentionReconstructionFixture struct {
	ls                  *LedgerState
	db                  *database.Database
	ownerKey            []byte
	delegatorKey        []byte
	capturedStakeInputs []*models.RewardStakeInput
}

func seedRetentionReconstructionFixture(
	t *testing.T,
) retentionReconstructionFixture {
	t.Helper()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	poolHash := bytes.Repeat([]byte{0xd1}, 28)
	ownerKey := bytes.Repeat([]byte{0x71}, 28)
	delegatorKey := bytes.Repeat([]byte{0x72}, 28)

	pool := &models.Pool{
		PoolKeyHash:   poolHash,
		VrfKeyHash:    make([]byte, 32),
		Pledge:        5_000_000,
		Cost:          340_000_000,
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		RewardAccount: ownerKey,
	}
	reg := &models.PoolRegistration{
		PoolKeyHash:   poolHash,
		AddedSlot:     0,
		Pledge:        5_000_000,
		Cost:          340_000_000,
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:    make([]byte, 32),
		RewardAccount: ownerKey,
		Owners: []models.PoolRegistrationOwner{
			{KeyHash: append([]byte(nil), ownerKey...)},
		},
	}
	require.NoError(t, db.ImportPool(nil, pool, reg), "import pool")
	for i, key := range [][]byte{ownerKey, delegatorKey} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: key,
			Pool:       poolHash,
			AddedSlot:  0,
			Active:     true,
		}), "create account %d", i)
		require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
			TxId:       bytes.Repeat([]byte{byte(0x10 + i)}, 32),
			OutputIdx:  0,
			StakingKey: key,
			Amount:     types.Uint64(20_000_000),
			AddedSlot:  0,
		}), "create utxo %d", i)
	}
	require.NoError(t, db.AddAccountRewardByCredential(
		0, ownerKey, 2_000_000, 10, bytes.Repeat([]byte{0xa1}, 32), nil,
	))
	require.NoError(t, db.AddAccountRewardByCredential(
		0, delegatorKey, 3_000_000, 10, bytes.Repeat([]byte{0xa2}, 32), nil,
	))

	require.NoError(t, meta.SetEpoch(
		0, 0, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	mgr := snapshot.NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        retentionRewardSnapshotEpoch,
		BoundarySlot:    100,
		EpochNonce:      []byte{0x0a, 0x0b},
		ProtocolVersion: 7,
		SnapshotSlot:    99,
	}
	captureTxn := db.Transaction(true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), captureTxn, evt,
	))
	require.NoError(t, meta.SetEpoch(
		100, retentionRewardSnapshotEpoch, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 100, captureTxn.Metadata(),
	))
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), captureTxn, evt,
	))
	require.NoError(t, captureTxn.Commit())

	capturedStakeInputs, err := meta.GetRewardStakeInputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, capturedStakeInputs,
		"the legitimate capture must have produced real per-credential rows",
	)
	require.NoError(t, meta.DeleteRewardStakeInputBeforeEpoch(
		retentionRewardSnapshotEpoch+1, nil,
	))
	prunedStakeInputs, err := meta.GetRewardStakeInputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Empty(
		t, prunedStakeInputs,
		"reward_stake_input must actually be pruned for this test to be a "+
			"real reconstruction",
	)

	seedRetentionRewardEpochs(t, db)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolHash)
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID, i+1, 140+i, nil,
		))
	}

	return retentionReconstructionFixture{
		ls:                  ls,
		db:                  db,
		ownerKey:            ownerKey,
		delegatorKey:        delegatorKey,
		capturedStakeInputs: capturedStakeInputs,
	}
}

func TestAsyncPrecomputeReconstructsPrunedInputsInWritePhase(t *testing.T) {
	t.Parallel()

	fixture := seedRetentionReconstructionFixture(t)
	meta := fixture.db.Metadata()
	err := fixture.ls.precomputeStakeRewardsAfterEpochTransition(
		event.EpochTransitionEvent{
			PreviousEpoch: retentionPerformanceEpoch,
			NewEpoch:      retentionPotsEpoch,
			BoundarySlot:  200,
		},
	)
	require.NoError(t, err)

	rebuilt, err := meta.GetRewardStakeInputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Len(t, rebuilt, len(fixture.capturedStakeInputs))
	poolOutputs, err := meta.GetRewardPoolOutputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
}

func TestRewardPrecomputeCalculationCarriesReconstructedInputsWithoutWriting(
	t *testing.T,
) {
	t.Parallel()

	fixture := seedRetentionReconstructionFixture(t)
	meta := fixture.db.Metadata()
	readTxn := fixture.db.Transaction(false)
	require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
		app, ok, err := fixture.ls.precomputeStakeRewardsCalculate(
			txn,
			retentionNewEpoch,
			200,
			300,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, app)
		require.Len(
			t,
			app.reconstructedStakeInputs,
			len(fixture.capturedStakeInputs),
		)
		persisted, err := meta.GetRewardStakeInputs(
			retentionRewardSnapshotEpoch,
			txn.Metadata(),
		)
		require.NoError(t, err)
		require.Empty(t, persisted)
		return nil
	}))

	persisted, err := meta.GetRewardStakeInputs(
		retentionRewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, persisted)
}

func TestReconstructedRewardStakeTieBreaksAreDeterministic(t *testing.T) {
	t.Parallel()

	ls, _ := newRewardCalculationTestLedger(t)
	poolA := []byte{0x10}
	poolB := []byte{0x20}
	poolC := []byte{0x30}
	basePools := []*models.RewardPoolInput{
		{
			PoolKeyHash:    poolA,
			DelegatedStake: 2_000_001,
			DelegatorCount: 2,
		},
		{
			PoolKeyHash:    poolB,
			DelegatedStake: 4_000_000,
			OwnerStake:     2_000_001,
			DelegatorCount: 4,
		},
		{
			PoolKeyHash:    poolC,
			DelegatedStake: 2_000_002,
			DelegatorCount: 3,
		},
	}
	baseRows := []*models.RewardStakeInput{
		{PoolKeyHash: poolA, CredentialTag: 0, StakingKey: []byte{0x01}, Stake: 1_000_000},
		{PoolKeyHash: poolA, CredentialTag: 0, StakingKey: []byte{0x02}, Stake: 1_000_000},
		{PoolKeyHash: poolB, CredentialTag: 0, StakingKey: []byte{0x11}, Stake: 1_000_000, Owner: true},
		{PoolKeyHash: poolB, CredentialTag: 0, StakingKey: []byte{0x12}, Stake: 1_000_000, Owner: true},
		{PoolKeyHash: poolB, CredentialTag: 1, StakingKey: []byte{0x21}, Stake: 1_000_000},
		{PoolKeyHash: poolB, CredentialTag: 1, StakingKey: []byte{0x22}, Stake: 1_000_000},
		{PoolKeyHash: poolC, CredentialTag: 0, StakingKey: []byte{0x31}, Stake: 1},
		{PoolKeyHash: poolC, CredentialTag: 0, StakingKey: []byte{0x32}, Stake: 1},
		{PoolKeyHash: poolC, CredentialTag: 0, StakingKey: []byte{0x41}, Stake: 1_000_000},
		{PoolKeyHash: poolC, CredentialTag: 0, StakingKey: []byte{0x42}, Stake: 1_000_000},
	}
	rowOrders := [][]int{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		{1, 0, 3, 2, 5, 4, 7, 6, 9, 8},
		{6, 8, 2, 4, 0, 9, 7, 5, 3, 1},
	}
	poolOrders := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 2, 0}, {2, 0, 1}}

	type normalizedRow struct {
		pool  byte
		tag   uint8
		key   byte
		stake uint64
		owner bool
	}
	var expected []normalizedRow
	for permutation := range rowOrders {
		rows := make([]*models.RewardStakeInput, 0, len(baseRows))
		for _, index := range rowOrders[permutation] {
			row := *baseRows[index]
			row.PoolKeyHash = bytes.Clone(row.PoolKeyHash)
			row.StakingKey = bytes.Clone(row.StakingKey)
			rows = append(rows, &row)
		}
		pools := make([]*models.RewardPoolInput, 0, len(basePools))
		for _, index := range poolOrders[permutation] {
			pool := *basePools[index]
			pool.PoolKeyHash = bytes.Clone(pool.PoolKeyHash)
			pools = append(pools, &pool)
		}

		got := ls.reconcileRebuiltRewardStakeInputs(
			rows,
			pools,
			retentionRewardSnapshotEpoch,
		)
		normalized := make([]normalizedRow, 0, len(got))
		for _, row := range got {
			normalized = append(normalized, normalizedRow{
				pool:  row.PoolKeyHash[0],
				tag:   row.CredentialTag,
				key:   row.StakingKey[0],
				stake: uint64(row.Stake),
				owner: row.Owner,
			})
		}
		if permutation == 0 {
			expected = normalized
			continue
		}
		require.Equal(t, expected, normalized)
	}

	require.Equal(t, []normalizedRow{
		{pool: 0x10, tag: 0, key: 0x01, stake: 1_000_000},
		{pool: 0x10, tag: 0, key: 0x02, stake: 1_000_001},
		{pool: 0x20, tag: 0, key: 0x11, stake: 1_000_000, owner: true},
		{pool: 0x20, tag: 0, key: 0x12, stake: 1_000_001, owner: true},
		{pool: 0x20, tag: 1, key: 0x21, stake: 1_000_000},
		{pool: 0x20, tag: 1, key: 0x22, stake: 999_999},
		{pool: 0x30, tag: 0, key: 0x32, stake: 1},
		{pool: 0x30, tag: 0, key: 0x41, stake: 1_000_000},
		{pool: 0x30, tag: 0, key: 0x42, stake: 1_000_001},
	}, expected)
}

// TestApplyStakeRewardsReconstructsRetentionPrunedInputs is the positive
// control for dingo #4578's retention-vs-resume gap: reward_stake_input is
// aged out of retention (as it is for the other tests in this file), but this
// time the underlying certificate/UTxO/reward-delta history the historical
// CTE reconstructs from is real, matching the shape
// `dingo database truncate` + replay produces (the reward_snapshot/
// reward_pool_input rows a prior run captured survive the rollback, but the
// per-credential reward_stake_input rows they depended on had already aged
// out of the live run's retention window before the rollback ever happened).
// Unlike TestApplyStakeRewardsSkipsPrunedStakeInputs, the round here must
// actually apply -- crediting the owner and the delegator their share of the
// epoch's rewards -- rather than skip.
func TestApplyStakeRewardsReconstructsRetentionPrunedInputs(t *testing.T) {
	t.Parallel()

	fixture := seedRetentionReconstructionFixture(t)
	ls := fixture.ls
	db := fixture.db
	meta := db.Metadata()
	ownerKey := fixture.ownerKey
	delegatorKey := fixture.delegatorKey

	beforeOwner, err := db.GetAccountByCredential(0, ownerKey, false, nil)
	require.NoError(t, err)
	beforeDelegator, err := db.GetAccountByCredential(
		0, delegatorKey, false, nil,
	)
	require.NoError(t, err)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "a retention-pruned epoch with real underlying data must "+
		"reconstruct and apply, not skip")

	afterOwner, err := db.GetAccountByCredential(0, ownerKey, false, nil)
	require.NoError(t, err)
	afterDelegator, err := db.GetAccountByCredential(
		0, delegatorKey, false, nil,
	)
	require.NoError(t, err)
	poolOutputs, err := meta.GetRewardPoolOutputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Len(
		t, poolOutputs, 1,
		"the round must persist a pool output, proving it applied instead "+
			"of skipping",
	)
	assert.Positive(
		t, uint64(poolOutputs[0].TotalReward),
		"the reconstructed pool's block production must earn a nonzero "+
			"reward this round",
	)
	assert.Greater(
		t, uint64(afterOwner.Reward), uint64(beforeOwner.Reward),
		"the owner must be credited a share of this epoch's reward, not "+
			"skipped",
	)
	// The fixture's pool cost (340_000_000) exceeds the whole round's
	// reward pot, so cardano-ledger's formula correctly gives the entire
	// reward to the leader and nothing to members -- this is expected pool
	// economics, not evidence the delegator's credential was skipped.
	assert.Equal(
		t, uint64(beforeDelegator.Reward), uint64(afterDelegator.Reward),
		"the fixture's pool cost consumes the whole reward pot, so the "+
			"member share is legitimately zero this round",
	)
	// A zero-share member legitimately gets no account_reward_output row (only
	// spendable, nonzero credits are persisted there) -- reward_stake_input's
	// re-population, asserted below, is what proves the reconstruction
	// considered the delegator rather than dropping it.

	rebuiltStakeInputs, err := meta.GetRewardStakeInputs(
		retentionRewardSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	assert.Len(
		t, rebuiltStakeInputs, len(fixture.capturedStakeInputs),
		"the reconstruction should persist the same credential set the "+
			"original live capture held, self-healing the pruned rows",
	)
}
