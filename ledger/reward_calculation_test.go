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
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNegativeLeaderRewardApplicationRespectsExpiredAccountGuard(t *testing.T) {
	t.Parallel()
	credential, err := rewards.NewCredential(0, bytes.Repeat([]byte{0x41}, rewards.CredentialHashSize))
	require.NoError(t, err)
	negative := []rewards.NegativeLeaderReward{{
		Credential: credential,
		Amount:     10,
		Spendable:  true,
	}}
	require.ErrorIs(
		t,
		negativeLeaderRewardApplicationError(negative, nil),
		rewards.ErrNegativeLeaderReward,
	)
	guarded := map[string]struct{}{
		models.NewStakeCredentialRef(credential.Tag, credential.Hash[:]).MapKey(): {},
	}
	require.NoError(t, negativeLeaderRewardApplicationError(negative, guarded))
}

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

func TestApplyStakeRewardsHaltsWhenRequiredRewardBasisIsMissing(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	seedRetentionRewardEpochs(t, db)
	var fatalErr error
	ls.config.FatalErrorFunc = func(err error) {
		fatalErr = err
	}

	txn := db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			context.Background(),
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	})
	require.ErrorIs(t, err, errHaltLedgerPipeline)
	require.ErrorContains(t, err, "required stake reward basis unavailable")
	require.ErrorIs(t, fatalErr, errHaltLedgerPipeline)
	require.ErrorContains(t, fatalErr, "required stake reward basis unavailable")
}

// TestApplyStakeRewardsHaltsOnUnrecoverablePrunedStakeInputs covers a retention interaction:
// reward_ada_pots, reward_snapshot,
// reward_pool_input and reward_pool_output are retained for the life of the
// database while reward_stake_input is pruned to the rotation window, so an
// aged-out epoch presents complete-looking pots and snapshot rows over an empty
// credential set. Reward application must halt before committing a boundary
// that would permanently omit the reference node's reward update.
func TestApplyStakeRewardsHaltsOnUnrecoverablePrunedStakeInputs(t *testing.T) {
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

	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Pool:       poolKey,
			Active:     true,
		}),
	)

	txn := db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(),
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	})
	require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)
	require.ErrorIs(t, err, errHaltLedgerPipeline)

	// Nothing was credited and no outputs were persisted for the skipped epoch.
	account, err := db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
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

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(),
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "an empty snapshot must reconcile normally, not error")
}

// seedPrunedStakeInputSnapshot seeds a mark snapshot and pool input that
// survive retention over an empty reward_stake_input credential set -- the
// same aged-out-epoch shape TestApplyStakeRewardsHaltsOnUnrecoverablePrunedStakeInputs
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
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Pool:       poolKey,
			Active:     true,
		}),
	)
	// reward_stake_input is deliberately absent: those rows aged out of the
	// retention window.
}

// TestApplyStakeRewardsReportsUnavailablePrunedStakeInputs proves the
// permanent shortfall is reported before the ledger pipeline halts.
func TestApplyStakeRewardsReportsUnavailablePrunedStakeInputs(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	seedRetentionRewardEpochs(t, db)
	seedPrunedStakeInputSnapshot(
		t, db, rewardCalcHash(0x33), rewardCalcHash(0x44),
	)

	var logs bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	txn := db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(),
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	})
	require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)
	require.ErrorIs(t, err, errHaltLedgerPipeline)

	out := logs.String()
	require.NotEmpty(t, out,
		"the missing basis must be visible at the default log level")
	assert.Contains(t, out, "level=ERROR")
	assert.Contains(
		t,
		out,
		"reward stake inputs for the snapshot epoch are no longer retained",
	)
	assert.Contains(t, out, "reward_snapshot_epoch=1")
	assert.Contains(t, out, "snapshot_delegators=2")
	// The consequence, not just the event.
	assert.Contains(t, out, "permanently")
	assert.Contains(t, out, "missing basis")
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

	txn := db.Transaction(context.Background(), false)
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
	require.NoError(
		t,
		db.ImportPool(context.Background(), nil, pool, reg),
		"import pool",
	)
	for i, key := range [][]byte{ownerKey, delegatorKey} {
		require.NoError(
			t,
			db.CreateAccount(context.Background(), nil, &models.Account{
				StakingKey: key,
				Pool:       poolHash,
				AddedSlot:  0,
				Active:     true,
			}),
			"create account %d",
			i,
		)
		require.NoError(
			t,
			db.CreateUtxo(context.Background(), nil, &models.Utxo{
				TxId:       bytes.Repeat([]byte{byte(0x10 + i)}, 32),
				OutputIdx:  0,
				StakingKey: key,
				Amount:     types.Uint64(20_000_000),
				AddedSlot:  0,
			}),
			"create utxo %d",
			i,
		)
	}
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(),
		0, ownerKey, 2_000_000, 10, bytes.Repeat([]byte{0xa1}, 32), nil,
	))
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(),
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
	captureTxn := db.Transaction(context.Background(), true)
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
			context.Background(),
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
	readTxn := fixture.db.Transaction(context.Background(), false)
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
// control for retention-vs-resume gap: reward_stake_input is
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

	beforeOwner, err := db.GetAccountByCredential(
		context.Background(),
		0,
		ownerKey,
		false,
		nil,
	)
	require.NoError(t, err)
	beforeDelegator, err := db.GetAccountByCredential(
		context.Background(),
		0, delegatorKey, false, nil,
	)
	require.NoError(t, err)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(),
			txn, retentionNewEpoch, retentionBoundarySlot,
		)
	}), "a retention-pruned epoch with real underlying data must "+
		"reconstruct and apply, not skip")
	settleRewardCredits(t, ls)

	afterOwner, err := db.GetAccountByCredential(
		context.Background(),
		0,
		ownerKey,
		false,
		nil,
	)
	require.NoError(t, err)
	afterDelegator, err := db.GetAccountByCredential(
		context.Background(),
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

func TestApplyStakeRewardsUsesDelayedRewardState(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x11)
	rewardAccount := rewardCalcHash(0x22)
	member := rewardCalcHash(0x33)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

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

	require.NoError(
		t,
		meta.SetEpoch(
			0,
			1,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
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
			Epoch:                      rewardSnapshotEpoch,
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
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(
		t,
		db.ImportPool(
			context.Background(),
			nil,
			&pool,
			&models.PoolRegistration{
				PoolID:      pool.ID,
				PoolKeyHash: poolKey,
				AddedSlot:   0,
			},
		),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Pool:       poolKey,
			Active:     true,
		}),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: member,
			Pool:       poolKey,
			Active:     true,
		}),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		1,
		rewardAccount,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		2,
		member,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	rewardOwner, err := db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))

	rewardMember, err := db.GetAccountByCredential(
		context.Background(),
		0,
		member,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardMember)
	require.Equal(t, uint64(37_049), uint64(rewardMember.Reward))

	liveInputs, err := meta.GetLiveStakeInputsForPools(
		[][]byte{poolKey},
		0, // gate off
		nil,
	)
	require.NoError(t, err)
	require.Len(t, liveInputs, 2)
	liveStakeByKey := make(map[string]uint64, len(liveInputs))
	for _, input := range liveInputs {
		liveStakeByKey[string(input.StakingKey)] = uint64(input.Stake)
	}
	require.Equal(t, uint64(46_283), liveStakeByKey[string(rewardAccount)])
	require.Equal(t, uint64(37_049), liveStakeByKey[string(member)])

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(99_916_668), uint64(state.Reserves))
	require.Equal(t, uint64(0), uint64(state.Treasury))

	pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(1_000_000), uint64(pots.Rewards))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, uint64(83_333), uint64(poolOutputs[0].TotalReward))

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)

	var deltas int64
	require.NoError(t, rewardCalcSQLDB(t, db).QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE added_slot = ?",
		boundarySlot,
	).Scan(&deltas))
	require.Equal(t, int64(2), deltas)

	txn = db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	rewardOwner, err = db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))
	rewardMember, err = db.GetAccountByCredential(
		context.Background(),
		0,
		member,
		false,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(37_049), uint64(rewardMember.Reward))
	liveInputs, err = meta.GetLiveStakeInputsForPools(
		[][]byte{poolKey},
		0, // gate off
		nil,
	)
	require.NoError(t, err)
	require.Len(t, liveInputs, 2)
	liveStakeByKey = make(map[string]uint64, len(liveInputs))
	for _, input := range liveInputs {
		liveStakeByKey[string(input.StakingKey)] = uint64(input.Stake)
	}
	require.Equal(t, uint64(46_283), liveStakeByKey[string(rewardAccount)])
	require.Equal(t, uint64(37_049), liveStakeByKey[string(member)])
	require.NoError(t, rewardCalcSQLDB(t, db).QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE added_slot = ?",
		boundarySlot,
	).Scan(&deltas))
	require.Equal(t, int64(2), deltas)
}

// guardExpiredLeaderResult captures the post-application state a Task 10
// scenario run produces, so gate-on and gate-off runs can be compared.
type guardExpiredLeaderResult struct {
	reserves           uint64
	treasury           uint64
	leaderReward       uint64 // credited to the reward (leader) account
	memberReward       uint64 // credited to the member delegator
	leaderOutputAmount uint64 // persisted leader account output amount
	memberOutputAmount uint64 // persisted member account output amount
}

// applyGuardExpiredLeaderScenario seeds a single-pool reward application in
// which the pool's reward (leader) account is expired as of the reward's
// snapshot epoch, runs the application with the delegator-inactivity gate set
// to gateEnabled, and returns the resulting network state and reward balances.
//
// It is the shared harness for the Task 10 reward-crediting guard. newEpoch is
// 5, so stakeRewardEpochsForApplication maps it to snapshot epoch 2: an
// ExpirationEpoch of 1 is nonzero and strictly before 2 (expired), while an
// unset (0) expiration is active. This is the same snapshot epoch Task 8 uses
// to build the reward basis, so the guard and the basis agree on which snapshot
// an account is judged against.
func applyGuardExpiredLeaderScenario(
	t *testing.T,
	gateEnabled bool,
) guardExpiredLeaderResult {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	ls.config.DelegatorInactivityEnabled = gateEnabled
	meta := db.Metadata()

	const (
		newEpoch            = uint64(5)
		rewardSnapshotEpoch = uint64(2)
		performanceEpoch    = uint64(3)
		potsEpoch           = uint64(4)
		boundarySlot        = uint64(500)
	)
	poolKey := rewardCalcHash(0x11)
	rewardAccount := rewardCalcHash(0x22)
	member := rewardCalcHash(0x33)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

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

	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
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
			Epoch:                      rewardSnapshotEpoch,
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
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(
		t,
		db.ImportPool(
			context.Background(),
			nil,
			&pool,
			&models.PoolRegistration{
				PoolID:      pool.ID,
				PoolKeyHash: poolKey,
				AddedSlot:   0,
			},
		),
	)
	// The reward (leader) account is expired as of the snapshot epoch (2):
	// ExpirationEpoch 1 is nonzero and strictly before 2.
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:      rewardAccount,
			Pool:            poolKey,
			Active:          true,
			ExpirationEpoch: 1,
		}),
	)
	// The member delegator is active (unset expiration).
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:      member,
			Pool:            poolKey,
			Active:          true,
			ExpirationEpoch: 0,
		}),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		1,
		rewardAccount,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		2,
		member,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	rewardOwner, err := db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	rewardMember, err := db.GetAccountByCredential(
		context.Background(),
		0,
		member,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardMember)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)

	res := guardExpiredLeaderResult{
		reserves:     uint64(state.Reserves),
		treasury:     uint64(state.Treasury),
		leaderReward: uint64(rewardOwner.Reward),
		memberReward: uint64(rewardMember.Reward),
	}
	for _, output := range accountOutputs {
		switch string(output.StakingKey) {
		case string(rewardAccount):
			require.Equal(
				t,
				string(rewards.RewardTypeLeader),
				output.RewardType,
			)
			res.leaderOutputAmount = uint64(output.Amount)
		case string(member):
			require.Equal(
				t,
				string(rewards.RewardTypeMember),
				output.RewardType,
			)
			res.memberOutputAmount = uint64(output.Amount)
		}
	}
	return res
}

// TestStakeRewardEpochHelpersDivergeAtBootstrapRound pins the divergence that
// let the Byron guard in applyStakeRewards miss the round it exists to catch.
//
// The guard must resolve its epochs through the same helper as the path it
// guards (calculateStakeRewardApplication at :190,
// precomputedStakeRewardApplication at :670). Resolved through
// stakeRewardEpochsForNewEpoch instead, the guard reports nothing to guard at
// newEpoch == 2 while the application path resolves the bootstrap round
// against performance epoch 0 -- which is Byron on every network the Byron
// prefix affects, and therefore has no persisted pparams.
//
// This covers the helper contract only. The end-to-end rollover failure was
// reproduced against real database rows and has no unit-level fixture here.
func TestStakeRewardEpochHelpersDivergeAtBootstrapRound(t *testing.T) {
	t.Parallel()

	for _, newEpoch := range []uint64{1, 2} {
		_, ok := stakeRewardEpochsForNewEpoch(newEpoch)
		require.False(
			t,
			ok,
			"stakeRewardEpochsForNewEpoch must still report no round at "+
				"bootstrap epoch %d; the guard compensates by using the "+
				"application helper instead",
			newEpoch,
		)

		app, ok := stakeRewardEpochsForApplication(newEpoch)
		require.True(
			t,
			ok,
			"the application path acts on bootstrap round %d",
			newEpoch,
		)
		require.Equal(
			t,
			uint64(0),
			app.performance,
			"bootstrap round %d resolves against the performance epoch "+
				"with no pparams",
			newEpoch,
		)
		require.True(
			t,
			app.bootstrap,
			"newEpoch %d is a bootstrap round",
			newEpoch,
		)
	}

	// From epoch 3 up the two helpers agree, which is what made guarding on
	// either of them look equivalent.
	for newEpoch := uint64(3); newEpoch < 8; newEpoch++ {
		narrow, narrowOK := stakeRewardEpochsForNewEpoch(newEpoch)
		wide, wideOK := stakeRewardEpochsForApplication(newEpoch)
		require.Equal(t, narrowOK, wideOK, "newEpoch %d", newEpoch)
		require.Equal(t, narrow, wide, "newEpoch %d", newEpoch)
	}
}

// TestApplyStakeRewardsSkipsBootstrapRoundWithByronPerformanceEpoch drives the
// production guard at the one epoch where stakeRewardEpochsForApplication and
// stakeRewardEpochsForNewEpoch differ. Without the application helper in the
// guard, the bootstrap round reaches rewardParameters and fails because Byron
// legitimately has no persisted protocol parameters.
func TestApplyStakeRewardsSkipsBootstrapRoundWithByronPerformanceEpoch(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetEpoch(
		0,
		0,
		nil,
		nil,
		nil,
		nil,
		eras.ByronEraDesc.Id,
		1,
		21_600,
		nil,
	))
	require.NoError(t, meta.SetEpoch(
		21_600,
		1,
		nil,
		nil,
		nil,
		nil,
		eras.ByronEraDesc.Id,
		1,
		21_600,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        1,
		Reserves:     100_000_000,
		CapturedSlot: 21_600,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:        0,
		SnapshotType: "mark",
		CapturedSlot: 0,
		BoundarySlot: 0,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 2, 43_200)
	}))
	settleRewardCredits(t, ls)
}

// TestApplyStakeRewardsSkipsEpochOneRoundWithByronPerformanceEpoch is the
// negative case for the 0->1 bootstrap round. A network
// with a Byron prefix has no Shelley reward round at that boundary, so the
// Byron performance-epoch guard must suppress it and leave the slot-0 pots
// untouched -- even though the epoch 0 ADA pots row now exists.
func TestApplyStakeRewardsSkipsEpochOneRoundWithByronPerformanceEpoch(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetEpoch(
		0,
		0,
		nil,
		nil,
		nil,
		nil,
		eras.ByronEraDesc.Id,
		1,
		21_600,
		nil,
	))
	require.NoError(t, meta.SetNetworkState(0, 100_000_000, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Reserves:     100_000_000,
		CapturedSlot: 0,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:        0,
		SnapshotType: "mark",
		CapturedSlot: 0,
		BoundarySlot: 0,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 1, 21_600)
	}))
	settleRewardCredits(t, ls)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(0), uint64(state.Treasury))
	require.Equal(t, uint64(100_000_000), uint64(state.Reserves))
}

// TestApplyStakeRewardsGuardsExpiredRewardAccount is the Task 10 reward-crediting
// guard test: a pool reward (leader) account expired as of the reward snapshot
// epoch must not be credited, and its reward must be routed to undistributed ->
// reserves so the ADA pots reconcile exactly. Gate off is byte-identical to the
// pre-CIP behavior (both accounts credited).
func TestApplyStakeRewardsGuardsExpiredRewardAccount(t *testing.T) {
	t.Parallel()

	const initialReserves = uint64(100_000_000)

	// Gate off: the expired reward account is still credited (pre-CIP
	// behavior). Both leader and member receive their rewards, and the ADA is
	// fully conserved (fees=0, tau=0).
	off := applyGuardExpiredLeaderScenario(t, false)
	require.Greater(
		t,
		off.leaderReward,
		uint64(0),
		"gate off must credit the leader",
	)
	require.Greater(
		t,
		off.memberReward,
		uint64(0),
		"gate off must credit the member",
	)
	require.Equal(t, off.leaderOutputAmount, off.leaderReward)
	require.Equal(t, off.memberOutputAmount, off.memberReward)
	require.Equal(t,
		initialReserves,
		off.reserves+off.treasury+off.leaderReward+off.memberReward,
		"gate off ADA must be conserved",
	)

	// Gate on: the expired reward (leader) account is NOT credited; the active
	// member is still credited its full, unchanged amount.
	on := applyGuardExpiredLeaderScenario(t, true)
	require.Equal(t, uint64(0), on.leaderReward,
		"gate on must not credit the expired reward account")
	require.Equal(t, off.memberReward, on.memberReward,
		"gate on must still credit the active member unchanged")
	// The guard skips crediting only; it does not rewrite the computed reward
	// outputs, so the persisted leader output amount is unchanged.
	require.Equal(t, off.leaderOutputAmount, on.leaderOutputAmount)

	// Reconciliation: reserves gains exactly the skipped leader reward, and the
	// treasury is unchanged (the skipped amount is routed to undistributed ->
	// reserves, not unspendable -> treasury).
	require.Equal(t, off.reserves+off.leaderReward, on.reserves,
		"reserves must gain exactly the skipped leader reward")
	require.Equal(t, off.treasury, on.treasury,
		"treasury must be unchanged by the guard")
	// ADA conservation with the guard on: the only credited reward is the
	// member; the skipped leader amount stayed in reserves.
	require.Equal(t,
		initialReserves,
		on.reserves+on.treasury+on.memberReward,
		"gate on ADA must be conserved",
	)
}

func TestGuardedExpiredRewardCredentialsUsesSnapshotWitnessHistory(
	t *testing.T,
) {
	t.Parallel()

	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)
	cred := renewTestCred(0x71)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: cred, Active: true, ExpirationEpoch: 2 + inactivity,
		}),
	)
	seedRollbackCertificate(
		t, db, 150, rollbackStakeRegistrationCertificate(cred),
	)
	seedRollbackCertificate(
		t, db, 250, rollbackStakeRegistrationCertificate(cred),
	)
	app := &stakeRewardApplication{
		epochs:               stakeRewardEpochs{snapshot: 92},
		snapshotCapturedSlot: 199,
		accountOutputs: []*models.RewardAccountOutput{{
			StakingKey: cred, CredentialTag: 0,
		}},
	}
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		guarded, err := ls.guardedExpiredRewardCredentials(context.Background(), txn, app)
		if err != nil {
			return err
		}
		require.Contains(
			t,
			guarded,
			models.NewStakeCredentialRef(0, cred).MapKey(),
		)
		return nil
	}))
}

func TestApplyStakeRewardsAggregatesSharedRewardAccountBalance(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolA := rewardCalcHash(0x14)
	poolB := rewardCalcHash(0x15)
	sharedRewardAccount := rewardCalcHash(0x16)
	memberA := rewardCalcHash(0x17)
	memberB := rewardCalcHash(0x18)
	var poolIDA lcommon.PoolKeyHash
	var poolIDB lcommon.PoolKeyHash
	copy(poolIDA[:], poolA)
	copy(poolIDB[:], poolB)

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

	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	for i := range uint64(5) {
		require.NoError(
			t,
			db.UpdatePoolOpCertSequence(
				context.Background(),
				poolIDA,
				i+1,
				140+i,
				nil,
			),
		)
		require.NoError(
			t,
			db.UpdatePoolOpCertSequence(
				context.Background(),
				poolIDB,
				i+1,
				150+i,
				nil,
			),
		)
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 2_000,
		TotalPoolCount:   2,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolA,
			RewardAccount:              sharedRewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Cost:                       1_000,
			DelegatedStake:             1_000,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolB,
			RewardAccount:              sharedRewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Cost:                       1_000,
			DelegatedStake:             1_000,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:        rewardSnapshotEpoch,
			PoolKeyHash:  poolA,
			StakingKey:   memberA,
			Stake:        1_000,
			Registered:   true,
			CapturedSlot: 100,
			BoundarySlot: 100,
		},
		{
			Epoch:        rewardSnapshotEpoch,
			PoolKeyHash:  poolB,
			StakingKey:   memberB,
			Stake:        1_000,
			Registered:   true,
			CapturedSlot: 100,
			BoundarySlot: 100,
		},
	}, nil))
	for _, stakingKey := range [][]byte{sharedRewardAccount, memberA, memberB} {
		require.NoError(
			t,
			db.CreateAccount(context.Background(), nil, &models.Account{
				StakingKey: stakingKey,
				Active:     true,
			}),
		)
	}

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	var sharedLeaderOutputs int
	var sharedLeaderTotal uint64
	for _, output := range accountOutputs {
		if string(output.StakingKey) != string(sharedRewardAccount) {
			continue
		}
		require.Equal(t, string(rewards.RewardTypeLeader), output.RewardType)
		sharedLeaderOutputs++
		sharedLeaderTotal += uint64(output.Amount)
	}
	require.Equal(t, 2, sharedLeaderOutputs)
	require.Greater(t, sharedLeaderTotal, uint64(0))

	account, err := db.GetAccountByCredential(
		context.Background(),
		0,
		sharedRewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, sharedLeaderTotal, uint64(account.Reward))
}

func TestCalculateStakeRewardsRejectsPersistedStakeInputMismatch(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)

	rows := rewardCalcExecRows(
		t,
		db,
		`UPDATE reward_stake_input SET stake = '499'
WHERE epoch = ? AND pool_key_hash = ? AND staking_key = ?`,
		uint64(1),
		poolKey,
		rewardAccount,
	)
	require.Equal(t, int64(1), rows)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward stake input total mismatch")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedOwnerStakeInputMismatch(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)

	rows := rewardCalcExecRows(
		t,
		db,
		`UPDATE reward_pool_input SET owner_stake = '499'
WHERE epoch = ? AND pool_key_hash = ?`,
		uint64(1),
		poolKey,
	)
	require.Equal(t, int64(1), rows)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward owner stake input total mismatch")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedDelegatorCountMismatch(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)

	rows := rewardCalcExecRows(
		t,
		db,
		`UPDATE reward_pool_input SET delegator_count = 1
WHERE epoch = ? AND pool_key_hash = ?`,
		uint64(1),
		poolKey,
	)
	require.Equal(t, int64(1), rows)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "total delegator count")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedPoolCountMismatch(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)

	rows := rewardCalcExecRows(
		t,
		db,
		`UPDATE reward_snapshot SET total_pool_count = 2
WHERE epoch = ? AND snapshot_type = ?`,
		uint64(1),
		"mark",
	)
	require.Equal(t, int64(1), rows)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match snapshot pool count")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedPoolInputSnapshotSlotMismatch(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)

	rows := rewardCalcExecRows(
		t,
		db,
		`UPDATE reward_pool_input SET captured_slot = 99
WHERE epoch = ? AND pool_key_hash = ?`,
		uint64(1),
		poolKey,
	)
	require.Equal(t, int64(1), rows)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward pool input captured slot")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedStakeInputSnapshotSlotMismatch(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)
	member := rewardCalcHash(0x6a)

	rows := rewardCalcExecRows(
		t,
		db,
		`UPDATE reward_stake_input SET boundary_slot = 99
WHERE epoch = ? AND pool_key_hash = ? AND staking_key = ?`,
		uint64(1),
		poolKey,
		member,
	)
	require.Equal(t, int64(1), rows)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward stake input boundary slot")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestValidateRewardCalculatorInputsRejectsMalformedPoolInput(t *testing.T) {
	t.Parallel()

	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)
	snapshot := &models.RewardSnapshot{
		TotalActiveStake: 100,
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     10,
		BoundarySlot:     20,
	}
	validPoolInput := func() *models.RewardPoolInput {
		return &models.RewardPoolInput{
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			DelegatedStake:             100,
			OwnerStake:                 10,
			DelegatorCount:             1,
			CapturedSlot:               10,
			BoundarySlot:               20,
		}
	}
	stakeInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash:  poolKey,
			StakingKey:   member,
			Stake:        100,
			CapturedSlot: 10,
			BoundarySlot: 20,
		},
	}

	for _, tc := range []struct {
		name      string
		mutate    func(*models.RewardPoolInput)
		wantError string
	}{
		{
			name: "missing margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = nil
			},
			wantError: "reward pool input margin is missing",
		},
		{
			name: "missing margin rat",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{}
			},
			wantError: "reward pool input margin is missing",
		},
		{
			name: "negative margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(-1, 10)}
			},
			wantError: "reward pool input margin outside [0,1]",
		},
		{
			name: "margin above one",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(11, 10)}
			},
			wantError: "reward pool input margin outside [0,1]",
		},
		{
			name: "invalid reward account length",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccount = input.RewardAccount[:27]
			},
			wantError: "invalid reward pool input reward account",
		},
		{
			name: "invalid reward account credential tag",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccountCredentialTag = 2
			},
			wantError: "invalid reward pool input reward account",
		},
		{
			name: "owner stake above delegated",
			mutate: func(input *models.RewardPoolInput) {
				input.OwnerStake = 101
			},
			wantError: "reward pool input owner stake",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poolInput := validPoolInput()
			tc.mutate(poolInput)
			err := validateRewardCalculatorInputs(
				snapshot,
				[]*models.RewardPoolInput{poolInput},
				stakeInputs,
			)
			require.ErrorContains(t, err, tc.wantError)
		})
	}
}

func TestCalculateStakeRewardsRejectsUnknownPersistedStakeInputPool(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	unknownPool := rewardCalcHash(0x7a)
	stakingKey := rewardCalcHash(0x8a)

	rewardCalcExecRows(t, db, `
INSERT INTO reward_stake_input (
    epoch, pool_key_hash, credential_tag, staking_key, stake, owner,
    registered, captured_slot, boundary_slot
) VALUES (1, ?, 0, ?, '1', FALSE, TRUE, 100, 100)`,
		unknownPool,
		stakingKey,
	)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward stake input for unknown pool")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestApplyStakeRewardsUsesPrecomputedOutputs(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		precomputeSlot      = uint64(300)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x17)
	rewardAccount := rewardCalcHash(0x27)
	member := rewardCalcHash(0x37)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

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

	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: precomputeSlot,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
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
			Epoch:                      rewardSnapshotEpoch,
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
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Active:     true,
		}),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: member,
			Active:     true,
		}),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		21,
		rewardAccount,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		22,
		member,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.precomputeStakeRewards(
			txn,
			newEpoch,
			precomputeSlot,
			boundarySlot,
		)
	}))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, precomputeSlot, poolOutputs[0].CapturedSlot)
	require.Equal(t, boundarySlot, poolOutputs[0].BoundarySlot)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)
	for _, output := range accountOutputs {
		require.Equal(t, precomputeSlot, output.CapturedSlot)
		require.Equal(t, boundarySlot, output.BoundarySlot)
	}

	rewardOwner, err := db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(0), uint64(rewardOwner.Reward))

	txn = db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	rewardOwner, err = db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))

	rewardMember, err := db.GetAccountByCredential(
		context.Background(),
		0,
		member,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardMember)
	require.Equal(t, uint64(37_049), uint64(rewardMember.Reward))
}

func TestApplyPrecomputedStakeRewardsChecksFinalAccountRegistration(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x19)
	rewardAccount := rewardCalcHash(0x29)
	member := rewardCalcHash(0x39)

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

	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	// Seed pool block production so the reuse pool-reward re-derivation observes
	// the apparent performance that produced the persisted 83_333/46_283 rewards.
	for i := range uint64(10) {
		require.NoError(
			t,
			db.UpdatePoolOpCertSequence(
				context.Background(),
				poolID,
				i+1,
				140+i,
				nil,
			),
		)
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
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
			Epoch:                      rewardSnapshotEpoch,
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
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:             rewardSnapshotEpoch,
			PoolKeyHash:       poolKey,
			OptimalReward:     83_333,
			TotalReward:       83_333,
			LeaderReward:      46_283,
			MemberRewardTotal: 37_049,
			OwnerStake:        500,
			Undistributed:     1,
			CapturedSlot:      300,
			BoundarySlot:      boundarySlot,
		},
	}, nil))
	require.NoError(
		t,
		meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				PoolKeyHash:   poolKey,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        46_283,
				Spendable:     true,
				CapturedSlot:  300,
				BoundarySlot:  boundarySlot,
			},
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 0,
				StakingKey:    member,
				PoolKeyHash:   poolKey,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        37_049,
				Spendable:     true,
				CapturedSlot:  300,
				BoundarySlot:  boundarySlot,
			},
		}, nil),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Active:     true,
		}),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: member,
			Active:     true,
		}),
	)
	rewardCalcSetAccountActive(t, db, member, false)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	rewardOwner, err := db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))

	rewardMember, err := db.GetAccountByCredential(
		context.Background(),
		0,
		member,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardMember)
	require.Equal(t, uint64(0), uint64(rewardMember.Reward))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(99_916_668), uint64(state.Reserves))
	require.Equal(t, uint64(37_049), uint64(state.Treasury))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, uint64(37_049), uint64(poolOutputs[0].Unspendable))

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)
	for _, output := range accountOutputs {
		if string(output.StakingKey) == string(member) {
			require.False(t, output.Spendable)
		}
	}
}

func TestApplyStakeRewardsDoesNotMergeCredentialTags(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x21)
	sharedStakeHash := rewardCalcHash(0x22)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

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

	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 500,
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              sharedStakeHash,
			RewardAccountCredentialTag: 1,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             500,
			OwnerStake:                 500,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    sharedStakeHash,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:    sharedStakeHash,
			CredentialTag: 0,
			Reward:        7,
			Active:        true,
		}),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:    sharedStakeHash,
			CredentialTag: 1,
			Active:        true,
		}),
	)
	rewardCalcSetAccountActiveByCredential(t, db, 1, sharedStakeHash, false)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	keyAccount, err := db.GetAccountByCredential(
		context.Background(),
		0,
		sharedStakeHash,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, keyAccount)
	require.Equal(t, uint64(7), uint64(keyAccount.Reward))

	scriptAccount, err := db.GetAccountByCredential(
		context.Background(),
		1,
		sharedStakeHash,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, scriptAccount)
	require.False(t, scriptAccount.Active)
	require.Equal(t, uint64(0), uint64(scriptAccount.Reward))

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, uint8(1), accountOutputs[0].CredentialTag)
	require.Equal(t, sharedStakeHash, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)
	require.Greater(t, uint64(accountOutputs[0].Amount), uint64(0))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, accountOutputs[0].Amount, poolOutputs[0].Unspendable)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, accountOutputs[0].Amount, state.Treasury)
}

func TestPrecomputedStakeRewardsFinalEligibilityDoesNotMergeCredentialTags(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)
	sharedStakeHash := rewardCalcHash(0x77)
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:    sharedStakeHash,
			CredentialTag: 0,
			Reward:        7,
			Active:        true,
		}),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:    sharedStakeHash,
			CredentialTag: 1,
			Active:        true,
		}),
	)
	rewardCalcSetAccountActiveByCredential(t, db, 1, sharedStakeHash, false)
	// Replace the seed's multi-delegator pool with a coherent single-member pool
	// whose only non-owner delegator is the shared script credential (tag 1). The
	// reserves (100_000_000) yield an incentives-derived reward pot of 1_000_000,
	// from which this pool's reward re-derives to 6666; the reuse pool-reward and
	// amount checks both require that value. With cost 0, margin 0, and the member
	// holding the entire 1000 delegated stake, MemberReward = TotalReward = 6666.
	// The account is inactive, so that reward is unspendable and flows to the
	// treasury without merging into the shared credential's active tag-0 account.
	rewardCalcExecRows(
		t,
		db,
		"DELETE FROM reward_stake_input WHERE epoch = ? AND pool_key_hash = ?",
		rewardSnapshotEpoch,
		poolKey,
	)
	rewardCalcExecRows(
		t,
		db,
		"DELETE FROM reward_pool_input WHERE epoch = ? AND pool_key_hash = ?",
		rewardSnapshotEpoch,
		poolKey,
	)
	rewardCalcExecRows(t, db, `
UPDATE reward_snapshot
SET total_active_stake = '1000', total_delegators = 1
WHERE epoch = ? AND snapshot_type = 'mark'`,
		rewardSnapshotEpoch,
	)
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardCalcHash(0x5a),
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(0, 1)},
			Cost:                       0,
			DelegatedStake:             1_000,
			OwnerStake:                 0,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 1,
			StakingKey:    sharedStakeHash,
			Stake:         1_000,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:             rewardSnapshotEpoch,
			PoolKeyHash:       poolKey,
			TotalReward:       6666,
			LeaderReward:      0,
			MemberRewardTotal: 6666,
			OwnerStake:        0,
			CapturedSlot:      300,
			BoundarySlot:      boundarySlot,
		},
	}, nil))
	require.NoError(
		t,
		meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 1,
				StakingKey:    sharedStakeHash,
				PoolKeyHash:   poolKey,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        6666,
				Spendable:     true,
				CapturedSlot:  300,
				BoundarySlot:  boundarySlot,
			},
		}, nil),
	)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	keyAccount, err := db.GetAccountByCredential(
		context.Background(),
		0,
		sharedStakeHash,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, keyAccount)
	require.Equal(t, uint64(7), uint64(keyAccount.Reward))

	scriptAccount, err := db.GetAccountByCredential(
		context.Background(),
		1,
		sharedStakeHash,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, scriptAccount)
	require.False(t, scriptAccount.Active)
	require.Equal(t, uint64(0), uint64(scriptAccount.Reward))

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, uint8(1), accountOutputs[0].CredentialTag)
	require.Equal(t, sharedStakeHash, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, uint64(6666), uint64(poolOutputs[0].Unspendable))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(6666), uint64(state.Treasury))
}

func TestPrecomputedStakeRewardsRequireCompletePoolOutputs(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectOutputsWithoutStakeInputs(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			Undistributed: 10,
			OwnerStake:    500,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))
	rewardCalcExecRows(
		t,
		db,
		"DELETE FROM reward_stake_input WHERE epoch = ?",
		rewardSnapshotEpoch,
	)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectOutputsWithoutPoolInputs(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))
	rewardCalcExecRows(
		t,
		db,
		"DELETE FROM reward_pool_input WHERE epoch = ?",
		rewardSnapshotEpoch,
	)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectExtraPoolOutputs(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)
	stalePoolKey := rewardCalcHash(0x4b)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   stalePoolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectPoolOutputOutsideSnapshotInputs(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	stalePoolKey := rewardCalcHash(0x4b)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   stalePoolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectPoolOutputOwnerStakeMismatch(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			OwnerStake:    499,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRequireCompleteAccountOutputs(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x18)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   83_333,
			Undistributed: 1,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectOutputsOutsideApplicationBoundary(
	t *testing.T,
) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
		capturedSlot        = uint64(300)
	)

	for _, tc := range []struct {
		name                string
		poolCapturedSlot    uint64
		poolBoundarySlot    uint64
		accountCapturedSlot uint64
		accountBoundarySlot uint64
	}{
		{
			name:                "pool boundary slot zero",
			poolCapturedSlot:    capturedSlot,
			poolBoundarySlot:    0,
			accountCapturedSlot: capturedSlot,
			accountBoundarySlot: boundarySlot,
		},
		{
			name:                "pool captured after boundary",
			poolCapturedSlot:    boundarySlot + 1,
			poolBoundarySlot:    boundarySlot,
			accountCapturedSlot: capturedSlot,
			accountBoundarySlot: boundarySlot,
		},
		{
			name:                "account boundary slot zero",
			poolCapturedSlot:    capturedSlot,
			poolBoundarySlot:    boundarySlot,
			accountCapturedSlot: capturedSlot,
			accountBoundarySlot: 0,
		},
		{
			name:                "account captured after boundary",
			poolCapturedSlot:    capturedSlot,
			poolBoundarySlot:    boundarySlot,
			accountCapturedSlot: boundarySlot + 1,
			accountBoundarySlot: boundarySlot,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			poolKey := rewardCalcHash(0x4a)
			member := rewardCalcHash(0x6a)

			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:        potsEpoch,
				Reserves:     100_000_000,
				Rewards:      1_000,
				CapturedSlot: capturedSlot,
			}, nil))
			require.NoError(
				t,
				meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
					{
						Epoch:             rewardSnapshotEpoch,
						PoolKeyHash:       poolKey,
						TotalReward:       100,
						MemberRewardTotal: 100,
						OwnerStake:        500,
						CapturedSlot:      tc.poolCapturedSlot,
						BoundarySlot:      tc.poolBoundarySlot,
					},
				}, nil),
			)
			require.NoError(
				t,
				meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
					{
						Epoch:         rewardSnapshotEpoch,
						CredentialTag: 0,
						StakingKey:    member,
						PoolKeyHash:   poolKey,
						RewardType:    string(rewards.RewardTypeMember),
						Amount:        100,
						Spendable:     true,
						CapturedSlot:  tc.accountCapturedSlot,
						BoundarySlot:  tc.accountBoundarySlot,
					},
				}, nil),
			)

			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.precomputedStakeRewardApplication(
				txn,
				newEpoch,
				boundarySlot,
			)
			require.NoError(t, err)
			require.False(t, ok)
			require.Nil(t, app)
		})
	}
}

func TestPrecomputedRewardOutputsRequirePerPoolAccountTotals(t *testing.T) {
	t.Parallel()

	poolA := rewardCalcHash(0x19)
	poolB := rewardCalcHash(0x1a)
	accountA := rewardCalcHash(0x1b)
	accountB := rewardCalcHash(0x1c)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				MemberRewardTotal: 90,
				Undistributed:     10,
			},
			{
				PoolKeyHash:       poolB,
				TotalReward:       50,
				MemberRewardTotal: 50,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      140,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:   poolA,
				TotalReward:   100,
				Undistributed: 10,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
			{
				PoolKeyHash: poolB,
				StakingKey:  accountB,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      0,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     100,
				LeaderReward:      100,
				MemberRewardTotal: 0,
			},
		},
		nil,
		true,
	)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				MemberRewardTotal: 90,
				Undistributed:     10,
			},
			{
				PoolKeyHash:       poolB,
				TotalReward:       50,
				MemberRewardTotal: 50,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
			{
				PoolKeyHash: poolB,
				StakingKey:  accountB,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      50,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPrecomputedRewardOutputsRequirePoolBreakdownTotals(t *testing.T) {
	t.Parallel()

	poolA := rewardCalcHash(0x19)
	leaderA := rewardCalcHash(0x1b)
	memberA := rewardCalcHash(0x1c)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     10,
				LeaderReward:      50,
				MemberRewardTotal: 40,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  leaderA,
				RewardType:  string(rewards.RewardTypeLeader),
				Amount:      40,
			},
			{
				PoolKeyHash: poolA,
				StakingKey:  memberA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      50,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     10,
				LeaderReward:      100,
				MemberRewardTotal: 0,
			},
		},
		nil,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     10,
				LeaderReward:      50,
				MemberRewardTotal: 40,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  leaderA,
				RewardType:  string(rewards.RewardTypeLeader),
				Amount:      50,
			},
			{
				PoolKeyHash: poolA,
				StakingKey:  memberA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      40,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPrecomputedRewardOutputsRejectMalformedRows(t *testing.T) {
	t.Parallel()

	poolA := rewardCalcHash(0x19)
	accountA := rewardCalcHash(0x1b)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA[:27],
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA,
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  "unknown",
				Amount:      90,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA,
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA[:27],
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:   poolA,
				TotalReward:   90,
				Undistributed: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      0,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPrecomputedRewardOutputsRejectDuplicateAccountIdentities(
	t *testing.T,
) {
	t.Parallel()

	poolA := rewardCalcHash(0x19)
	accountA := rewardCalcHash(0x1b)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA,
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      40,
			},
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      50,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPrecomputedRewardAccountOutputsMatchPoolInputs(t *testing.T) {
	t.Parallel()

	poolA := rewardCalcHash(0x19)
	poolB := rewardCalcHash(0x1a)
	rewardAccount := rewardCalcHash(0x1b)
	otherAccount := rewardCalcHash(0x1c)

	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolA,
			RewardAccountCredentialTag: 0,
			RewardAccount:              rewardAccount,
		},
	}

	// With no stake inputs there is nothing to prove pool membership, so a
	// member reward output cannot be validated and the precomputed outputs are
	// rejected (the leader output alone would match).
	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		nil,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        10,
			},
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    otherAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		nil,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    otherAccount,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        10,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		nil,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolB,
				CredentialTag: 0,
				StakingKey:    otherAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))
}

func TestPrecomputedRewardAccountOutputsMatchStakeInputs(t *testing.T) {
	t.Parallel()

	poolA := rewardCalcHash(0x19)
	rewardAccount := rewardCalcHash(0x1b)
	memberAccount := rewardCalcHash(0x1c)
	nonMemberAccount := rewardCalcHash(0x1d)
	ownerAccount := rewardCalcHash(0x1e)

	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolA,
			RewardAccountCredentialTag: 0,
			RewardAccount:              rewardAccount,
		},
	}
	stakeInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    memberAccount,
			Stake:         100,
		},
		{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    ownerAccount,
			Stake:         200,
			Owner:         true,
		},
	}

	require.True(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		stakeInputs,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        10,
			},
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    memberAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		stakeInputs,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    nonMemberAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		stakeInputs,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    ownerAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))
}

func TestPrecomputedRewardAccountAmountsMatchInputs(t *testing.T) {
	t.Parallel()

	poolA := rewardCalcHash(0x40)
	rewardAccount := rewardCalcHash(0x41)
	memberA := rewardCalcHash(0x42)
	memberB := rewardCalcHash(0x43)

	const (
		poolReward   = uint64(1000)
		cost         = uint64(0)
		delegated    = uint64(300)
		stakeA       = uint64(200)
		stakeB       = uint64(100)
		leaderReward = uint64(100)
	)
	zeroMargin := new(big.Rat)
	wantA, err := rewards.MemberReward(
		poolReward,
		cost,
		zeroMargin,
		stakeA,
		delegated,
	)
	require.NoError(t, err)
	wantB, err := rewards.MemberReward(
		poolReward,
		cost,
		zeroMargin,
		stakeB,
		delegated,
	)
	require.NoError(t, err)
	// A non-uniform split is what makes a within-pool redistribution detectable;
	// if the shares were equal, swapping them would be invisible.
	require.NotEqual(t, wantA, wantB)

	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolA,
			RewardAccountCredentialTag: 0,
			RewardAccount:              rewardAccount,
			Cost:                       types.Uint64(cost),
			DelegatedStake:             types.Uint64(delegated),
		},
	}
	poolOutputs := []*models.RewardPoolOutput{
		{
			PoolKeyHash:       poolA,
			TotalReward:       types.Uint64(poolReward),
			LeaderReward:      types.Uint64(leaderReward),
			MemberRewardTotal: types.Uint64(wantA + wantB),
		},
	}
	stakeInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    memberA,
			Stake:         types.Uint64(stakeA),
		},
		{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    memberB,
			Stake:         types.Uint64(stakeB),
		},
	}

	leaderOut := func(amt uint64) *models.RewardAccountOutput {
		return &models.RewardAccountOutput{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			RewardType:    string(rewards.RewardTypeLeader),
			Amount:        types.Uint64(amt),
		}
	}
	memberOut := func(key []byte, amt uint64) *models.RewardAccountOutput {
		return &models.RewardAccountOutput{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    key,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        types.Uint64(amt),
		}
	}

	check := func(outs []*models.RewardAccountOutput) bool {
		return precomputedRewardAccountAmountsMatchInputs(
			poolInputs, poolOutputs, stakeInputs, outs, rewards.Parameters{},
		)
	}

	// The correct per-recipient split is accepted.
	require.True(t, check([]*models.RewardAccountOutput{
		leaderOut(
			leaderReward,
		), memberOut(memberA, wantA), memberOut(memberB, wantB),
	}))

	// Redistribution within the pool: member A absorbs B's share and B's row is
	// dropped. Per-pool and per-type totals are unchanged, so this is exactly the
	// case the membership and completeness checks miss; the amount check rejects it.
	require.False(t, check([]*models.RewardAccountOutput{
		leaderOut(leaderReward), memberOut(memberA, wantA+wantB),
	}))

	// Both members present but with each other's amounts (aggregate identical).
	require.False(t, check([]*models.RewardAccountOutput{
		leaderOut(
			leaderReward,
		), memberOut(memberA, wantB), memberOut(memberB, wantA),
	}))

	// A tampered leader amount is rejected (pinned to the pool output).
	require.False(t, check([]*models.RewardAccountOutput{
		leaderOut(
			leaderReward + 1,
		), memberOut(memberA, wantA), memberOut(memberB, wantB),
	}))

	// A member output whose credential has no stake input is rejected.
	require.False(t, check([]*models.RewardAccountOutput{
		memberOut(memberA, wantA), memberOut(rewardCalcHash(0x44), 1),
	}))

	// Dijkstra precompute uses the effective CIP-23 margin. A pool registered
	// below the floor must therefore validate against the floor-derived member
	// amounts, not amounts derived from its raw registration margin.
	poolInputs[0].Margin = &types.Rat{Rat: big.NewRat(1, 100)}
	minPoolMargin := big.NewRat(1, 20)
	wantFloorA, err := rewards.MemberReward(
		poolReward, cost, minPoolMargin, stakeA, delegated,
	)
	require.NoError(t, err)
	wantFloorB, err := rewards.MemberReward(
		poolReward, cost, minPoolMargin, stakeB, delegated,
	)
	require.NoError(t, err)
	floorOutputs := []*models.RewardAccountOutput{
		leaderOut(leaderReward),
		memberOut(memberA, wantFloorA),
		memberOut(memberB, wantFloorB),
	}
	require.True(t, precomputedRewardAccountAmountsMatchInputs(
		poolInputs,
		poolOutputs,
		stakeInputs,
		floorOutputs,
		rewards.Parameters{MinPoolMargin: minPoolMargin},
	))
	require.False(t, precomputedRewardAccountAmountsMatchInputs(
		poolInputs,
		poolOutputs,
		stakeInputs,
		floorOutputs,
		rewards.Parameters{},
	))

	// Guard the invariant this fix relies on: the pre-existing membership check
	// accepts the redistributed set, so the amount check is the only gate that
	// catches it.
	require.True(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs, stakeInputs,
		[]*models.RewardAccountOutput{
			leaderOut(leaderReward), memberOut(memberA, wantA+wantB),
		},
	))
}

func TestPrecomputedRewardPoolRewardsMatchInputs(t *testing.T) {
	t.Parallel()

	poolKey := rewardCalcHash(0x51)
	poolID, err := rewards.NewPoolID(poolKey)
	require.NoError(t, err)

	params := rewards.Parameters{
		Decentralization: new(big.Rat),
		OptimalPoolCount: 10,
		PledgeInfluence:  big.NewRat(1, 2),
	}
	const (
		availableRewards = uint64(1_000_000)
		totalActiveStake = uint64(1_000)
		totalCirculation = uint64(10_000)
		totalBlocks      = uint64(10)
	)
	blockCounts := map[string]uint64{string(poolKey): totalBlocks}
	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:    poolKey,
			Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:         500,
			Cost:           1_000,
			DelegatedStake: 1_000,
			OwnerStake:     500,
		},
	}

	expected, err := rewards.CalculatePoolReward(
		rewards.Pool{
			ID:             poolID,
			Margin:         big.NewRat(1, 10),
			Pledge:         500,
			Cost:           1_000,
			DelegatedStake: 1_000,
			OwnerStake:     500,
			BlocksProduced: totalBlocks,
			TotalBlocks:    totalBlocks,
		},
		availableRewards,
		totalActiveStake,
		totalCirculation,
		totalBlocks,
		params,
	)
	require.NoError(t, err)
	require.NotZero(t, expected.PoolReward)
	require.NotZero(t, expected.LeaderReward)

	check := func(outs []*models.RewardPoolOutput) bool {
		ok, err := precomputedRewardPoolRewardsMatchInputs(
			poolInputs,
			outs,
			blockCounts,
			availableRewards,
			totalActiveStake,
			totalCirculation,
			totalBlocks,
			params,
		)
		require.NoError(t, err)
		return ok
	}

	// The re-derivable pool reward is accepted.
	require.True(t, check([]*models.RewardPoolOutput{
		{
			PoolKeyHash:  poolKey,
			TotalReward:  types.Uint64(expected.PoolReward),
			LeaderReward: types.Uint64(expected.LeaderReward),
		},
	}))

	// A total reward that does not match the re-derived value is rejected, even
	// though the per-account amount check treats the stored total as authoritative.
	require.False(t, check([]*models.RewardPoolOutput{
		{
			PoolKeyHash:  poolKey,
			TotalReward:  types.Uint64(expected.PoolReward + 1),
			LeaderReward: types.Uint64(expected.LeaderReward),
		},
	}))

	// A tampered leader reward is rejected.
	require.False(t, check([]*models.RewardPoolOutput{
		{
			PoolKeyHash:  poolKey,
			TotalReward:  types.Uint64(expected.PoolReward),
			LeaderReward: types.Uint64(expected.LeaderReward + 1),
		},
	}))

	// A missing pool output for an input pool is rejected.
	require.False(t, check(nil))
}

// TestPrecomputedStakeRewardsRejectPoolRewardMismatch proves the pool-reward
// re-derivation closes the gap the per-account checks leave open: a stale or
// corrupted pool output paired with account outputs consistent with it passes
// every pre-existing reuse check but is rejected because the stored reward does
// not match the value re-derived from the frozen inputs.
func TestPrecomputedStakeRewardsRejectPoolRewardMismatch(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		capturedSlot        = uint64(300)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: capturedSlot,
	}, nil))

	// The seeded pool re-derives to TotalReward 83_333 / LeaderReward 46_283.
	// Inflate the stored reward to double that and build account outputs that are
	// internally consistent with the inflated total (member re-derived from the
	// inflated total, leader assigned the remainder so nothing is undistributed).
	const tamperedTotal = uint64(166_666)
	memberAmt, err := rewards.MemberReward(
		tamperedTotal,
		1_000,
		big.NewRat(1, 10),
		500,
		1_000,
	)
	require.NoError(t, err)
	require.Less(t, memberAmt, tamperedTotal)
	tamperedLeader := tamperedTotal - memberAmt

	poolOutputs := []*models.RewardPoolOutput{
		{
			Epoch:             rewardSnapshotEpoch,
			PoolKeyHash:       poolKey,
			TotalReward:       types.Uint64(tamperedTotal),
			LeaderReward:      types.Uint64(tamperedLeader),
			MemberRewardTotal: types.Uint64(memberAmt),
			OwnerStake:        500,
			Undistributed:     0,
			CapturedSlot:      capturedSlot,
			BoundarySlot:      boundarySlot,
		},
	}
	accountOutputs := []*models.RewardAccountOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			PoolKeyHash:   poolKey,
			RewardType:    string(rewards.RewardTypeLeader),
			Amount:        types.Uint64(tamperedLeader),
			Spendable:     true,
			CapturedSlot:  capturedSlot,
			BoundarySlot:  boundarySlot,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 0,
			StakingKey:    member,
			PoolKeyHash:   poolKey,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        types.Uint64(memberAmt),
			Spendable:     true,
			CapturedSlot:  capturedSlot,
			BoundarySlot:  boundarySlot,
		},
	}
	require.NoError(t, meta.SaveRewardPoolOutputs(poolOutputs, nil))
	require.NoError(t, meta.SaveRewardAccountOutputs(accountOutputs, nil))

	// Guard the premise: the pre-existing per-account amount and completeness
	// checks accept this internally consistent but inflated precompute, so the
	// pool-reward re-derivation is the only gate that rejects it.
	poolInputs, err := meta.GetRewardPoolInputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	stakeInputs, err := meta.GetRewardStakeInputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.True(t, precomputedRewardAccountAmountsMatchInputs(
		poolInputs,
		poolOutputs,
		stakeInputs,
		accountOutputs,
		rewards.Parameters{},
	))
	complete, err := precomputedRewardOutputsComplete(
		poolOutputs,
		accountOutputs,
		false,
	)
	require.NoError(t, err)
	require.True(t, complete)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestSaveStakeRewardOutputsReplacesEpochRows(t *testing.T) {
	t.Parallel()

	_, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	const rewardSnapshotEpoch = uint64(9)
	poolA := rewardCalcHash(0x41)
	poolB := rewardCalcHash(0x42)
	accountA := rewardCalcHash(0x51)
	accountB := rewardCalcHash(0x52)

	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:       rewardSnapshotEpoch,
			PoolKeyHash: poolA,
			TotalReward: 10,
		},
		{
			Epoch:       rewardSnapshotEpoch,
			PoolKeyHash: poolB,
			TotalReward: 20,
		},
	}, nil))
	require.NoError(
		t,
		meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 0,
				StakingKey:    accountA,
				PoolKeyHash:   poolA,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        10,
				Spendable:     true,
			},
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 0,
				StakingKey:    accountB,
				PoolKeyHash:   poolB,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
				Spendable:     true,
			},
		}, nil),
	)

	txn := db.Transaction(context.Background(), true)
	defer func() { _ = txn.Rollback() }()
	require.NoError(t, saveStakeRewardOutputs(
		meta,
		txn.Metadata(),
		&stakeRewardApplication{
			epochs: stakeRewardEpochs{snapshot: rewardSnapshotEpoch},
			poolOutputs: []*models.RewardPoolOutput{
				{
					Epoch:       rewardSnapshotEpoch,
					PoolKeyHash: poolA,
					TotalReward: 10,
				},
			},
			accountOutputs: []*models.RewardAccountOutput{
				{
					Epoch:         rewardSnapshotEpoch,
					CredentialTag: 0,
					StakingKey:    accountA,
					PoolKeyHash:   poolA,
					RewardType:    string(rewards.RewardTypeMember),
					Amount:        10,
					Spendable:     true,
				},
			},
		},
	))
	require.NoError(t, txn.Commit())

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, poolA, poolOutputs[0].PoolKeyHash)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, accountA, accountOutputs[0].StakingKey)
}

func TestPrecomputedStakeRewardsRejectPoolOutputsAboveAvailableRewards(
	t *testing.T,
) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)

	poolKey := rewardCalcHash(0x4a)

	t.Run(
		"within available accepts and accounts undistributed",
		func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:        potsEpoch,
				Reserves:     100_000_000,
				Rewards:      1_000_000,
				CapturedSlot: 300,
			}, nil))
			// A re-derivable precompute (the seeded pool re-derives to an 83_333
			// reward: 46_283 to the leader and 37_049 to the member) is accepted,
			// and the remainder of the 1_000_000 available pot is accounted as
			// undistributed back to reserves. A single sub-saturated pool can never
			// re-derive to the full available pot, so the fit-check boundary itself
			// (sum == available) is asserted directly below rather than through the
			// full path.
			require.NoError(
				t,
				meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
					{
						Epoch:             rewardSnapshotEpoch,
						PoolKeyHash:       poolKey,
						OptimalReward:     83_333,
						TotalReward:       83_333,
						LeaderReward:      46_283,
						MemberRewardTotal: 37_049,
						OwnerStake:        500,
						Undistributed:     1,
						CapturedSlot:      300,
						BoundarySlot:      boundarySlot,
					},
				}, nil),
			)
			require.NoError(
				t,
				meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
					{
						Epoch:         rewardSnapshotEpoch,
						CredentialTag: 0,
						StakingKey:    rewardCalcHash(0x5a),
						PoolKeyHash:   poolKey,
						RewardType:    string(rewards.RewardTypeLeader),
						Amount:        46_283,
						Spendable:     true,
						CapturedSlot:  300,
						BoundarySlot:  boundarySlot,
					},
					{
						Epoch:         rewardSnapshotEpoch,
						CredentialTag: 0,
						StakingKey:    rewardCalcHash(0x6a),
						PoolKeyHash:   poolKey,
						RewardType:    string(rewards.RewardTypeMember),
						Amount:        37_049,
						Spendable:     true,
						CapturedSlot:  300,
						BoundarySlot:  boundarySlot,
					},
				}, nil),
			)

			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.precomputedStakeRewardApplication(
				txn,
				newEpoch,
				boundarySlot,
			)
			require.NoError(t, err)
			require.True(t, ok)
			require.NotNil(t, app)
			require.Equal(t, uint64(1_000_000), app.availableRewards)
			require.Equal(t, uint64(83_332), app.effectiveRewards)
			require.Equal(t, uint64(916_668), app.undistributed)

			// The fit check accepts pool rewards summing to exactly the available
			// pot; the "above available" subtest rejects anything past it.
			fits, err := precomputedRewardPoolOutputsFitAvailable(
				[]*models.RewardPoolOutput{{TotalReward: 1_000_000}},
				1_000_000,
			)
			require.NoError(t, err)
			require.True(t, fits)
		},
	)

	t.Run("above available", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
			Epoch:        potsEpoch,
			Reserves:     100_000_000,
			Rewards:      1_000_000,
			CapturedSlot: 300,
		}, nil))
		require.NoError(
			t,
			meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
				{
					Epoch:         rewardSnapshotEpoch,
					PoolKeyHash:   poolKey,
					TotalReward:   1_000_001,
					Undistributed: 1_000_001,
					CapturedSlot:  300,
					BoundarySlot:  boundarySlot,
				},
			}, nil),
		)

		txn := db.Transaction(context.Background(), false)
		defer func() { _ = txn.Rollback() }()
		app, ok, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
		)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, app)
	})
}

func TestPrecomputedStakeRewardsRejectPoolInputsMismatchingSnapshot(
	t *testing.T,
) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)

	poolKey := rewardCalcHash(0x4a)
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 999,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:        rewardSnapshotEpoch,
			PoolKeyHash:  poolKey,
			CapturedSlot: 300,
			BoundarySlot: boundarySlot,
		},
	}, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedRewardPoolInputsRejectMalformedRows(t *testing.T) {
	t.Parallel()

	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	snapshot := &models.RewardSnapshot{
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     101,
	}
	validInput := func() *models.RewardPoolInput {
		return &models.RewardPoolInput{
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               101,
		}
	}

	valid, err := precomputedRewardPoolInputsMatchSnapshot(
		snapshot,
		[]*models.RewardPoolInput{validInput()},
	)
	require.NoError(t, err)
	require.True(t, valid)

	for _, tc := range []struct {
		name   string
		mutate func(*models.RewardPoolInput)
	}{
		{
			name: "invalid reward account length",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccount = input.RewardAccount[:27]
			},
		},
		{
			name: "invalid reward account credential tag",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccountCredentialTag = 2
			},
		},
		{
			name: "nil margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = nil
			},
		},
		{
			name: "nil margin rat",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{}
			},
		},
		{
			name: "negative margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(-1, 10)}
			},
		},
		{
			name: "margin above one",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(11, 10)}
			},
		},
		{
			name: "owner stake above delegated stake",
			mutate: func(input *models.RewardPoolInput) {
				input.OwnerStake = 1_001
			},
		},
		{
			name: "delegator count mismatch",
			mutate: func(input *models.RewardPoolInput) {
				input.DelegatorCount = 1
			},
		},
		{
			name: "captured slot mismatch",
			mutate: func(input *models.RewardPoolInput) {
				input.CapturedSlot = 99
			},
		},
		{
			name: "boundary slot mismatch",
			mutate: func(input *models.RewardPoolInput) {
				input.BoundarySlot = 102
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := validInput()
			tc.mutate(input)
			ok, err := precomputedRewardPoolInputsMatchSnapshot(
				snapshot,
				[]*models.RewardPoolInput{input},
			)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}

	snapshot.TotalPoolCount = 2
	ok, err := precomputedRewardPoolInputsMatchSnapshot(
		snapshot,
		[]*models.RewardPoolInput{validInput()},
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPrecomputedStakeRewardsRejectImpossibleRewardPot(t *testing.T) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	for _, tc := range []struct {
		name     string
		reserves uint64
		fees     uint64
		rewards  uint64
	}{
		{
			name:     "below fees",
			reserves: 100_000_000,
			fees:     200,
			rewards:  100,
		},
		{
			name:     "incentives exceed reserves",
			reserves: 999,
			fees:     0,
			rewards:  1_000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:        potsEpoch,
				Reserves:     types.Uint64(tc.reserves),
				Fees:         types.Uint64(tc.fees),
				Rewards:      types.Uint64(tc.rewards),
				CapturedSlot: 300,
			}, nil))
			require.NoError(
				t,
				meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
					{
						Epoch:        rewardSnapshotEpoch,
						PoolKeyHash:  poolKey,
						CapturedSlot: 300,
						BoundarySlot: boundarySlot,
					},
				}, nil),
			)

			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.precomputedStakeRewardApplication(
				txn,
				newEpoch,
				boundarySlot,
			)
			require.NoError(t, err)
			require.False(t, ok)
			require.Nil(t, app)
		})
	}
}

func TestPrecomputeStakeRewardsWaitsForPreBabbagePrefilterSlot(t *testing.T) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		capturedSlot        = uint64(300)
		boundarySlot        = uint64(1_200)
	)

	t.Run("pre-babbage waits", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 6)
		meta := db.Metadata()
		prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)

		txn := db.Transaction(context.Background(), true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return ls.precomputeStakeRewards(
				txn,
				newEpoch,
				capturedSlot,
				boundarySlot,
			)
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Empty(t, poolOutputs)
		accountOutputs, err := meta.GetRewardAccountOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Empty(t, accountOutputs)
		pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, uint64(0), uint64(pots.Rewards))
		ls.rewardPrecomputeMu.Lock()
		retry := ls.rewardPrecomputeRetry
		ls.rewardPrecomputeMu.Unlock()
		require.NotNil(t, retry)
		require.Equal(t, prefilterSlot, retry.cutoffSlot)
		require.Equal(t, newEpoch-1, retry.epochEvent.NewEpoch)

		actualCapturedSlot := prefilterSlot + 7
		ls.maybeQueueStakeRewardPrecomputeRetry(actualCapturedSlot)
		ls.rewardPrecomputeWG.Wait()
		poolOutputs, err = meta.GetRewardPoolOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, poolOutputs, 1)
		require.Equal(t, actualCapturedSlot, poolOutputs[0].CapturedSlot)
	})

	t.Run(
		"pre-babbage precomputes at first just-right slot",
		func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 6)
			meta := db.Metadata()
			prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
			require.NoError(t, err)
			rewardCalcSeedStakeCert(
				t,
				db,
				21,
				rewardCalcHash(0x5a),
				0,
				prefilterSlot-1,
				uint(lcommon.CertificateTypeStakeRegistration),
			)
			rewardCalcSeedStakeCert(
				t,
				db,
				22,
				rewardCalcHash(0x6a),
				0,
				prefilterSlot-1,
				uint(lcommon.CertificateTypeStakeRegistration),
			)

			txn := db.Transaction(context.Background(), true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return ls.precomputeStakeRewards(
					txn,
					newEpoch,
					prefilterSlot,
					boundarySlot,
				)
			}))

			poolOutputs, err := meta.GetRewardPoolOutputs(
				rewardSnapshotEpoch,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, poolOutputs, 1)
			accountOutputs, err := meta.GetRewardAccountOutputs(
				rewardSnapshotEpoch,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, accountOutputs, 2)
			pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
			require.NoError(t, err)
			require.NotNil(t, pots)
			require.Equal(t, uint64(100_000), uint64(pots.Rewards))
		},
	)

	t.Run("babbage precomputes immediately", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)

		txn := db.Transaction(context.Background(), true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return ls.precomputeStakeRewards(
				txn,
				newEpoch,
				capturedSlot,
				boundarySlot,
			)
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, poolOutputs, 1)
		accountOutputs, err := meta.GetRewardAccountOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, accountOutputs, 2)
		pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, uint64(100_000), uint64(pots.Rewards))
	})
}

func TestRewardPrecomputeEpochTransitionStoresNextBoundaryOutputs(
	t *testing.T,
) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		eventBoundarySlot   = uint64(200)
		applicationBoundary = uint64(1_200)
	)

	ls.handleRewardPrecomputeEpochTransition(event.NewEvent(
		event.EpochTransitionEventType,
		event.EpochTransitionEvent{
			PreviousEpoch: 2,
			NewEpoch:      3,
			BoundarySlot:  eventBoundarySlot,
			EpochNonce:    []byte{0x01},
			SnapshotSlot:  eventBoundarySlot - 1,
		},
	))
	ls.rewardPrecomputeWG.Wait()

	poolOutputs, err := meta.GetRewardPoolOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, eventBoundarySlot, poolOutputs[0].CapturedSlot)
	require.Equal(t, applicationBoundary, poolOutputs[0].BoundarySlot)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)
	for _, output := range accountOutputs {
		require.Equal(t, eventBoundarySlot, output.CapturedSlot)
		require.Equal(t, applicationBoundary, output.BoundarySlot)
	}

	pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(100_000), uint64(pots.Rewards))
}

func TestPrecomputedStakeRewardsRejectEarlyPreBabbageOutputs(t *testing.T) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		capturedSlot        = uint64(300)
		boundarySlot        = uint64(1_200)
	)

	seedOutputs := func(t *testing.T, db *database.Database) {
		t.Helper()
		meta := db.Metadata()
		poolKey := rewardCalcHash(0x4a)
		rewardAccount := rewardCalcHash(0x5a)
		member := rewardCalcHash(0x6a)

		require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
			Epoch:        potsEpoch,
			Reserves:     100_000_000,
			Rewards:      1_000_000,
			CapturedSlot: 200,
		}, nil))
		require.NoError(
			t,
			meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
				{
					Epoch:             rewardSnapshotEpoch,
					PoolKeyHash:       poolKey,
					TotalReward:       83_333,
					LeaderReward:      46_283,
					MemberRewardTotal: 37_049,
					OwnerStake:        500,
					Undistributed:     1,
					CapturedSlot:      capturedSlot,
					BoundarySlot:      boundarySlot,
				},
			}, nil),
		)
		require.NoError(
			t,
			meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
				{
					Epoch:         rewardSnapshotEpoch,
					CredentialTag: 0,
					StakingKey:    rewardAccount,
					PoolKeyHash:   poolKey,
					RewardType:    string(rewards.RewardTypeLeader),
					Amount:        46_283,
					Spendable:     true,
					CapturedSlot:  capturedSlot,
					BoundarySlot:  boundarySlot,
				},
				{
					Epoch:         rewardSnapshotEpoch,
					CredentialTag: 0,
					StakingKey:    member,
					PoolKeyHash:   poolKey,
					RewardType:    string(rewards.RewardTypeMember),
					Amount:        37_049,
					Spendable:     true,
					CapturedSlot:  capturedSlot,
					BoundarySlot:  boundarySlot,
				},
			}, nil),
		)
	}

	t.Run("pre-babbage rejects", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 6)
		prefilterSlot, err := ls.rewardPrefilterSlot(
			db.Metadata(),
			nil,
			potsEpoch,
		)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)
		seedOutputs(t, db)

		txn := db.Transaction(context.Background(), false)
		defer func() { _ = txn.Rollback() }()
		app, ok, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
		)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, app)
	})

	t.Run("babbage accepts", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		prefilterSlot, err := ls.rewardPrefilterSlot(
			db.Metadata(),
			nil,
			potsEpoch,
		)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)
		seedOutputs(t, db)

		txn := db.Transaction(context.Background(), false)
		defer func() { _ = txn.Rollback() }()
		app, ok, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, app)
		require.True(t, app.precomputed)
		require.Equal(
			t,
			rewards.Efficiency(app.totalBlocks, app.params),
			app.rewardEfficiency,
			"reused reward outputs must retain efficiency attribution",
		)
		require.Equal(t, uint64(100), app.snapshotCapturedSlot)
		require.Equal(t, uint64(100), app.snapshotBoundarySlot)
		require.NoError(t, txn.Rollback())

		// Exercise the application-time expiry guard with a renewal after the
		// captured snapshot. The fixture's reward epoch is 1, but the earliest
		// concrete expiry is also 1, so judge this isolated boundary case at
		// epoch 2: the slot-50 witness gives expiry 1 (expired), while the
		// slot-150 renewal projected onto the mutable account row gives expiry
		// 2 (active at equality). The precomputed application must retain its
		// captured-slot cutoff and therefore ignore that later renewal.
		ls.config.DelegatorInactivityEnabled = true
		ls.config.DelegatorInactivity = 1
		ls.epochCache = []models.Epoch{
			{
				EpochId: 0, StartSlot: 0, SlotLength: 1000,
				LengthInSlots: 100, EraId: eras.ShelleyEraDesc.Id,
			},
			{
				EpochId: 1, StartSlot: 100, SlotLength: 1000,
				LengthInSlots: 100, EraId: eras.ShelleyEraDesc.Id,
			},
		}
		ls.publishSnapshotsLocked()
		rewardAccount := rewardCalcHash(0x5a)
		seedRollbackCertificate(
			t, db, 50, rollbackStakeRegistrationCertificate(rewardAccount),
		)
		seedRollbackCertificate(
			t, db, 150, rollbackStakeRegistrationCertificate(rewardAccount),
		)
		require.NoError(t, db.RenewAccountExpirations(
			context.Background(),
			[]models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, rewardAccount),
			},
			2,
			nil,
		))
		app.epochs.snapshot = 2

		guardTxn := db.Transaction(context.Background(), false)
		require.NoError(t, guardTxn.Do(func(txn *database.Txn) error {
			guarded, err := ls.guardedExpiredRewardCredentials(context.Background(), txn, app)
			if err != nil {
				return err
			}
			require.Contains(
				t,
				guarded,
				models.NewStakeCredentialRef(0, rewardAccount).MapKey(),
			)
			return nil
		}))
	})
}

func TestPrecomputedStakeRewardsRejectMissingBabbageLeaderOutput(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)
	member := rewardCalcHash(0x6a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      100_000,
		CapturedSlot: 200,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:             rewardSnapshotEpoch,
			PoolKeyHash:       poolKey,
			TotalReward:       83_333,
			LeaderReward:      46_283,
			MemberRewardTotal: 37_049,
			OwnerStake:        500,
			Undistributed:     46_284,
			CapturedSlot:      300,
			BoundarySlot:      boundarySlot,
		},
	}, nil))
	require.NoError(
		t,
		meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 0,
				StakingKey:    member,
				PoolKeyHash:   poolKey,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        37_049,
				Spendable:     true,
				CapturedSlot:  300,
				BoundarySlot:  boundarySlot,
			},
		}, nil),
	)

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsCheckPreBabbageMissingLeaderPrefilter(
	t *testing.T,
) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)

	for _, tc := range []struct {
		name                    string
		registerRewardAtRUPD    bool
		expectPrecomputedReward bool
	}{
		{
			name:                    "inactive at RUPD accepts missing leader",
			expectPrecomputedReward: true,
		},
		{
			name:                 "registered at RUPD rejects missing leader",
			registerRewardAtRUPD: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 6)
			meta := db.Metadata()
			poolKey := rewardCalcHash(0x4a)
			rewardAccount := rewardCalcHash(0x5a)
			member := rewardCalcHash(0x6a)

			prefilterSlot, err := ls.rewardPrefilterSlot(
				meta,
				nil,
				potsEpoch,
			)
			require.NoError(t, err)
			rewardCalcSetAccountActive(t, db, rewardAccount, false)
			if tc.registerRewardAtRUPD {
				rewardCalcSeedStakeCert(
					t,
					db,
					41,
					rewardAccount,
					0,
					prefilterSlot-1,
					uint(lcommon.CertificateTypeStakeRegistration),
				)
			}

			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch: potsEpoch,
				// Reserves imply an incentives-derived reward pot of
				// 1_000_000 (Rho 1/100 of 100_000_000), which the reuse
				// pool-reward re-derivation checks against; the seeded
				// 83_333 pool reward is only consistent with that pot.
				Reserves:     100_000_000,
				Rewards:      1_000_000,
				CapturedSlot: 200,
			}, nil))
			require.NoError(
				t,
				meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
					{
						Epoch:             rewardSnapshotEpoch,
						PoolKeyHash:       poolKey,
						TotalReward:       83_333,
						LeaderReward:      46_283,
						MemberRewardTotal: 37_049,
						OwnerStake:        500,
						Undistributed:     46_284,
						CapturedSlot:      prefilterSlot,
						BoundarySlot:      boundarySlot,
					},
				}, nil),
			)
			require.NoError(
				t,
				meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
					{
						Epoch:         rewardSnapshotEpoch,
						CredentialTag: 0,
						StakingKey:    member,
						PoolKeyHash:   poolKey,
						RewardType:    string(rewards.RewardTypeMember),
						Amount:        37_049,
						Spendable:     true,
						CapturedSlot:  prefilterSlot,
						BoundarySlot:  boundarySlot,
					},
				}, nil),
			)

			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.precomputedStakeRewardApplication(
				txn,
				newEpoch,
				boundarySlot,
			)
			require.NoError(t, err)
			require.Equal(t, tc.expectPrecomputedReward, ok)
			if tc.expectPrecomputedReward {
				require.NotNil(t, app)
				require.True(t, app.precomputed)
			} else {
				require.Nil(t, app)
			}
		})
	}
}

func TestApplyStakeRewardsUsesRewardUpdatePrefilterAccountHistory(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x44)
	rewardAccount := rewardCalcHash(0x55)
	member := rewardCalcHash(0x66)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    6,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  6,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
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
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Active:     true,
		}),
	)
	rewardCalcSetAccountActive(t, db, rewardAccount, false)

	rewardCalcSeedStakeCert(
		t,
		db,
		11,
		rewardAccount,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		12,
		rewardAccount,
		0,
		350,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		13,
		member,
		0,
		150,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		14,
		member,
		0,
		250,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, rewardAccount, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)
	require.Greater(t, uint64(accountOutputs[0].Amount), uint64(0))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(
		t,
		uint64(accountOutputs[0].Amount),
		uint64(poolOutputs[0].Unspendable),
	)
	require.Greater(t, uint64(poolOutputs[0].Undistributed), uint64(0))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(accountOutputs[0].Amount), uint64(state.Treasury))

	rewardOwner, err := db.GetAccountByCredential(
		context.Background(),
		0,
		rewardAccount,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(0), uint64(rewardOwner.Reward))
}

func TestApplyStakeRewardsPrefilterUsesBeginningOfRUPDSlot(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 6)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)
	prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
	require.NoError(t, err)

	rewardCalcSetAccountActive(t, db, rewardAccount, false)
	rewardCalcSetAccountActive(t, db, member, false)
	rewardCalcSeedStakeCert(
		t,
		db,
		31,
		rewardAccount,
		0,
		prefilterSlot-1,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		32,
		member,
		0,
		prefilterSlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, rewardAccount, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, accountOutputs[0].Amount, poolOutputs[0].LeaderReward)
	require.Equal(
		t,
		uint64(poolOutputs[0].TotalReward-poolOutputs[0].LeaderReward),
		uint64(poolOutputs[0].Undistributed),
	)
	require.Equal(t, accountOutputs[0].Amount, poolOutputs[0].Unspendable)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(accountOutputs[0].Amount), uint64(state.Treasury))
}

func TestApplyStakeRewardsAccountsEmptySnapshotPots(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(
		t,
		meta.SetEpoch(
			100,
			performanceEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(
		t,
		meta.SetEpoch(
			200,
			potsEpoch,
			nil,
			nil,
			nil,
			nil,
			eras.ShelleyEraDesc.Id,
			1,
			100,
			nil,
		),
	)
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Treasury:     10,
		Reserves:     100_000_000,
		Fees:         2_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:           rewardSnapshotEpoch,
		SnapshotType:    "mark",
		CapturedSlot:    100,
		BoundarySlot:    100,
		ProtocolVersion: 7,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, newEpoch, boundarySlot)
	}))
	settleRewardCredits(t, ls)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(100_001_600), uint64(state.Reserves))
	require.Equal(t, uint64(410), uint64(state.Treasury))

	pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(2_000), uint64(pots.Rewards))
}

func TestStakeRewardEpochsForNewEpochMatchDelayedUpdate(t *testing.T) {
	t.Parallel()

	for _, newEpoch := range []uint64{0, 1, 2} {
		_, ok := stakeRewardEpochsForNewEpoch(newEpoch)
		require.False(t, ok, "epoch %d has no delayed reward update", newEpoch)
	}

	epochs, ok := stakeRewardEpochsForNewEpoch(4)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    1,
		performance: 2,
		pots:        3,
	}, epochs)

	epochs, ok = stakeRewardEpochsForNewEpoch(211)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    208,
		performance: 209,
		pots:        210,
	}, epochs)
}

// TestRewardParametersSplitCalculationAndPerformanceEpochInputs pins where
// each reward parameter is read from when the two epochs disagree.
//
// The epoch length is the calculation epoch's: it stands in for the
// slotsPerEpoch the RUPD rule passes for the epoch it runs in. Every
// protocol-parameter value is the performance epoch's, because
// cardano-ledger's startStep binds `pr = es ^. prevPParamsEpochStateL` and
// derives d, rho, tau and the pool-level parameters it passes to
// mkPoolRewardInfo from that. Reading tau or d from the calculation epoch
// instead silently changes reward amounts on any network where the parameters
// move across the boundary.
func TestRewardParametersSplitCalculationAndPerformanceEpochInputs(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		potsEpoch        = uint64(3)
	)
	performancePParams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(1, 2),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	calculationPParams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	performancePParamsCbor, err := cbor.Encode(performancePParams)
	require.NoError(t, err)
	calculationPParamsCbor, err := cbor.Encode(calculationPParams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	require.NoError(t, meta.SetEpoch(
		200,
		potsEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		1_000,
		nil,
	))
	require.NoError(t, db.SetPParams(
		performancePParamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, db.SetPParams(
		calculationPParamsCbor,
		200,
		potsEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	_, params, performanceDecentralization, err := ls.rewardParameters(
		txn,
		performanceEpoch,
		potsEpoch,
		&models.RewardAdaPots{Reserves: 100_000_000},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(1_000),
		params.EpochLength,
		"epoch length comes from the calculation epoch",
	)
	require.Equal(
		t,
		big.NewRat(0, 1),
		params.TreasuryExpansion,
		"tau comes from the performance epoch",
	)
	require.Equal(
		t,
		big.NewRat(1, 2),
		params.Decentralization,
		"d comes from the performance epoch",
	)
	require.Equal(
		t,
		params.Decentralization,
		performanceDecentralization,
		"the block-count decentralization is the same value the "+
			"calculation uses",
	)
}

func TestRewardParametersUsesDijkstraLeverageAtFirstEraRound(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	ls.activeEras = append(
		append([]eras.EraDesc(nil), eras.Eras...),
		eras.DijkstraEraDesc,
	)
	ls.config.PledgeLeverageEnabled = true
	ls.config.PledgeLeverage = 100
	ls.config.MinPoolMargin = 250
	performancePParamsValue := mockledger.NewMockConwayProtocolParams()
	performancePParamsValue.NOpt = 10
	performancePParamsValue.A0 = rewardCalcRat(1, 2)
	performancePParamsValue.Rho = rewardCalcRat(1, 100)
	performancePParamsValue.Tau = rewardCalcRat(0, 1)
	performancePParamsValue.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 9}
	performancePParams := &performancePParamsValue
	calculationPParams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters:  mockledger.NewMockConwayProtocolParams(),
		RefScriptCostMultiplier:   rewardCalcRat(1, 1),
		MaxPledgeLeverage:         rewardCalcRat(1, 2),
		MinPoolMargin:             rewardCalcRat(1, 20),
		LeiosQuorumStakeThreshold: rewardCalcRat(1, 2),
		CommitteeStakeCoverage:    rewardCalcRat(1, 2),
		QuorumStakeThreshold:      rewardCalcRat(1, 2),
	}
	performanceCBOR, err := cbor.Encode(performancePParams)
	require.NoError(t, err)
	calculationCBOR, err := cbor.Encode(calculationPParams)
	require.NoError(t, err)
	meta := db.Metadata()
	require.NoError(t, meta.SetEpoch(100, 2, nil, nil, nil, nil, eras.ConwayEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, 3, nil, nil, nil, nil, eras.DijkstraEraDesc.Id, 1, 1_000, nil))
	require.NoError(t, db.SetPParams(performanceCBOR, 100, 2, eras.ConwayEraDesc.Id, nil))
	require.NoError(t, db.SetPParams(calculationCBOR, 200, 3, eras.DijkstraEraDesc.Id, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	_, params, _, err := ls.rewardParameters(
		txn, 2, 3, &models.RewardAdaPots{Reserves: 100_000_000},
	)
	require.NoError(t, err)
	require.True(t, params.PledgeLeverageEnabled)
	require.Equal(t, big.NewRat(1, 2), params.PledgeLeverage)
	require.Equal(t, big.NewRat(1, 40), params.MinPoolMargin)
}

func TestRewardParametersBabbageDefaultsDecentralizationAndForgoesPrefilter(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := newRewardCalculationTestLedger(t)
	pparams := &babbage.BabbageProtocolParameters{
		NOpt:          10,
		A0:            rewardCalcRat(1, 2),
		Rho:           rewardCalcRat(1, 100),
		Tau:           rewardCalcRat(0, 1),
		ProtocolMajor: 7,
		ProtocolMinor: 0,
	}

	params, err := rewardParametersFromPParams(
		pparams,
		ls.config.CardanoNodeConfig,
		100,
	)
	require.NoError(t, err)
	require.NotNil(t, params.Decentralization)
	require.Zero(t, params.Decentralization.Sign())
	require.Equal(t, uint64(7), params.ProtocolMajorVersion)
	require.False(t, params.RequiresRewardPrefilter())
}

func TestRewardParametersRejectIncompletePParams(t *testing.T) {
	t.Parallel()

	ls, _ := newRewardCalculationTestLedger(t)
	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}

	_, err := rewardParametersFromPParams(
		pparams,
		ls.config.CardanoNodeConfig,
		100,
	)
	require.ErrorIs(t, err, rewards.ErrInvalidParameters)
	require.ErrorContains(t, err, "missing treasury expansion")
}

func TestApplyPledgeLeveragePreDijkstraUsesExperimentalConfig(t *testing.T) {
	t.Parallel()

	params := rewards.Parameters{}
	applyPledgeLeveragePParams(&params, &shelley.ShelleyProtocolParameters{}, LedgerStateConfig{
		PledgeLeverageEnabled: true,
		PledgeLeverage:        100,
	})
	require.True(t, params.PledgeLeverageEnabled)
	require.Equal(t, big.NewRat(100, 1), params.PledgeLeverage)
}

func TestApplyPledgeLeverageDijkstraPParamOverridesConfig(t *testing.T) {
	t.Parallel()

	params := rewards.Parameters{}
	applyPledgeLeveragePParams(
		&params,
		&dijkstra.DijkstraProtocolParameters{MaxPledgeLeverage: rewardCalcRat(5, 1)},
		LedgerStateConfig{PledgeLeverageEnabled: true, PledgeLeverage: 100},
	)
	require.True(t, params.PledgeLeverageEnabled)
	require.Equal(t, big.NewRat(5, 1), params.PledgeLeverage)
}

func TestApplyPledgeLeverageDijkstraNilIgnoresConfig(t *testing.T) {
	t.Parallel()

	params := rewards.Parameters{}
	applyPledgeLeveragePParams(
		&params,
		&dijkstra.DijkstraProtocolParameters{},
		LedgerStateConfig{PledgeLeverageEnabled: true, PledgeLeverage: 100},
	)
	require.False(t, params.PledgeLeverageEnabled)
	require.Nil(t, params.PledgeLeverage)
}

func TestRewardBlockCountsTotalIncludesPoolsOutsideSnapshot(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const performanceEpoch = uint64(2)
	poolKey := rewardCalcHash(0x71)
	otherPoolKey := rewardCalcHash(0x72)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	var otherPoolID lcommon.PoolKeyHash
	copy(otherPoolID[:], otherPoolKey)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	for i := range uint64(3) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			i+1,
			120+i,
			nil,
		))
	}
	for i := range uint64(2) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			otherPoolID,
			i+1,
			130+i,
			nil,
		))
	}

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{
			{PoolKeyHash: poolKey},
		},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, uint64(3), counts[string(poolKey)])
	require.Equal(t, uint64(5), total)
}

func TestRewardBlockCountsSkipsOverlaySlots(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const performanceEpoch = uint64(2)
	poolKey := rewardCalcHash(0x73)
	otherPoolKey := rewardCalcHash(0x74)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	var otherPoolID lcommon.PoolKeyHash
	copy(otherPoolID[:], otherPoolKey)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	for _, slot := range []uint64{100, 101, 102, 103} {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			slot,
			slot,
			nil,
		))
	}
	require.NoError(t, db.UpdatePoolOpCertSequence(
		context.Background(),
		otherPoolID,
		105,
		105,
		nil,
	))

	decentralization := big.NewRat(1, 2)
	require.True(t, rewardIsOverlaySlot(100, decentralization, 100))
	require.False(t, rewardIsOverlaySlot(100, decentralization, 101))
	require.True(t, rewardIsOverlaySlot(100, decentralization, 102))
	require.False(t, rewardIsOverlaySlot(100, decentralization, 103))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{
			{PoolKeyHash: poolKey},
		},
		decentralization,
	)
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, uint64(2), counts[string(poolKey)])
	require.Equal(t, uint64(3), total)
}

func TestRewardPrefilterSlotUsesRUPDRandomnessWindow(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	require.NoError(t, meta.SetEpoch(
		1_000,
		3,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		1_000,
		nil,
	))

	slot, err := ls.rewardPrefilterSlot(meta, nil, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(1_401), slot)
	require.Equal(
		t,
		uint64(300),
		ls.nonceStabilityWindow(eras.ShelleyEraDesc.Id),
	)
	require.Equal(t, uint64(400), ls.rewardUpdateStabilityWindow())
}

func TestProcessEpochRolloverSnapshotEventUsesProtocolMajor(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
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
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		0,
		0,
		eras.ShelleyEraDesc.Id,
		nil,
	))

	var got event.EpochTransitionEvent
	seedEmptyRewardBasisForRollover(t, db, models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1,
		LengthInSlots: 100,
		EraId:         eras.ShelleyEraDesc.Id,
	}, pparams)
	ls.SetEpochBoundarySnapshotHook(func(
		_ *database.Txn,
		evt event.EpochTransitionEvent,
	) error {
		got = evt
		return nil
	})

	txn := db.Transaction(context.Background(), true)
	err = txn.Do(func(txn *database.Txn) error {
		_, err := ls.processEpochRollover(context.Background(),
			txn,
			models.Epoch{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1,
				LengthInSlots: 100,
				EraId:         eras.ShelleyEraDesc.Id,
			},
			eras.ShelleyEraDesc,
			pparams,
			false,
		)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), got.NewEpoch)
	require.Equal(t, uint64(100), got.BoundarySlot)
	require.Equal(t, uint64(99), got.SnapshotSlot)
	require.Equal(t, uint(7), got.ProtocolVersion)
}

// TestPrecomputeStakeRewardsAsyncPathMatchesSingleTransactionPath verifies
// that splitting the async EventBus precompute path
// (precomputeStakeRewardsAfterEpochTransition) into a read-only calculation
// transaction plus a short write transaction produces byte-identical reward
// output rows and ADA pots to the single read-write-transaction
// precomputeStakeRewards path used directly by other tests in this file.
// This guards the read/write split introduced to stop the async precompute
// from holding SQLite's single writer for the entire calculation.
func TestPrecomputeStakeRewardsAsyncPathMatchesSingleTransactionPath(
	t *testing.T,
) {
	t.Parallel()

	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		eventBoundarySlot   = uint64(200)
		applicationBoundary = uint64(1_200)
		newEpoch            = uint64(4)
	)

	// Reference: the original single read-write-transaction path.
	lsRef, dbRef := seedRewardPrecomputeTimingState(t, 7)
	metaRef := dbRef.Metadata()
	refTxn := dbRef.Transaction(context.Background(), true)
	require.NoError(t, refTxn.Do(func(txn *database.Txn) error {
		return lsRef.precomputeStakeRewards(
			txn,
			newEpoch,
			eventBoundarySlot,
			applicationBoundary,
		)
	}))
	refPoolOutputs, err := metaRef.GetRewardPoolOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	refAccountOutputs, err := metaRef.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	refPots, err := metaRef.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)

	// Under test: the split async EventBus path, driven the same way the
	// EventBus itself would (handleRewardPrecomputeEpochTransition).
	lsSplit, dbSplit := seedRewardPrecomputeTimingState(t, 7)
	metaSplit := dbSplit.Metadata()
	lsSplit.handleRewardPrecomputeEpochTransition(event.NewEvent(
		event.EpochTransitionEventType,
		event.EpochTransitionEvent{
			PreviousEpoch: 2,
			NewEpoch:      3,
			BoundarySlot:  eventBoundarySlot,
			EpochNonce:    []byte{0x01},
			SnapshotSlot:  eventBoundarySlot - 1,
		},
	))
	lsSplit.rewardPrecomputeWG.Wait()
	splitPoolOutputs, err := metaSplit.GetRewardPoolOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	splitAccountOutputs, err := metaSplit.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	splitPots, err := metaSplit.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)

	require.NotEmpty(t, refPoolOutputs)
	require.NotEmpty(t, refAccountOutputs)
	require.Len(t, splitPoolOutputs, len(refPoolOutputs))
	require.Len(t, splitAccountOutputs, len(refAccountOutputs))

	require.Equal(
		t,
		rewardCalcNormalizePoolOutputs(refPoolOutputs),
		rewardCalcNormalizePoolOutputs(splitPoolOutputs),
	)
	require.Equal(
		t,
		rewardCalcNormalizeAccountOutputs(refAccountOutputs),
		rewardCalcNormalizeAccountOutputs(splitAccountOutputs),
	)
	require.NotNil(t, refPots)
	require.NotNil(t, splitPots)
	require.Equal(t, refPots.Rewards, splitPots.Rewards)
}

// rewardCalcNormalizePoolOutputs strips the auto-increment ID (which is an
// artifact of insert order into a particular database, not part of the
// calculated result) so reward pool output rows from two independently
// seeded databases can be compared for equality.
func rewardCalcNormalizePoolOutputs(
	outputs []*models.RewardPoolOutput,
) []models.RewardPoolOutput {
	ret := make([]models.RewardPoolOutput, len(outputs))
	for i, output := range outputs {
		normalized := *output
		normalized.ID = 0
		ret[i] = normalized
	}
	return ret
}

// rewardCalcNormalizeAccountOutputs is the RewardAccountOutput counterpart
// of rewardCalcNormalizePoolOutputs.
func rewardCalcNormalizeAccountOutputs(
	outputs []*models.RewardAccountOutput,
) []models.RewardAccountOutput {
	ret := make([]models.RewardAccountOutput, len(outputs))
	for i, output := range outputs {
		normalized := *output
		normalized.ID = 0
		ret[i] = normalized
	}
	return ret
}

// TestStakeRewardPrecomputeSnapshotGuardOK exercises the guard that protects
// the write phase of the split async precompute
// (precomputeStakeRewardsAfterEpochTransition) from persisting a computed
// stakeRewardApplication whose reward_snapshot row has moved since the
// read-only calculation ran. It runs the real read phase
// (precomputeStakeRewardsCalculate) to obtain an app the same way the
// production code does, then simulates the world changing between the read
// and write phases (a rollback replacing or removing the mark snapshot) and
// checks that the guard rejects the stale app -- so a caller honoring it
// never persists a stale result -- while still accepting a matching,
// unchanged snapshot.
func TestStakeRewardPrecomputeSnapshotGuardOK(t *testing.T) {
	t.Parallel()

	const (
		rewardSnapshotEpoch = uint64(1)
		newEpoch            = uint64(4)
		capturedSlot        = uint64(200)
		boundarySlot        = uint64(1_200)
	)

	calculate := func(
		t *testing.T,
		ls *LedgerState,
		db *database.Database,
	) *stakeRewardApplication {
		t.Helper()
		var app *stakeRewardApplication
		readTxn := db.Transaction(context.Background(), false)
		require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
			computed, ok, err := ls.precomputeStakeRewardsCalculate(
				txn,
				newEpoch,
				capturedSlot,
				boundarySlot,
			)
			require.NoError(t, err)
			require.True(t, ok)
			app = computed
			return nil
		}))
		require.NotNil(t, app)
		return app
	}

	t.Run(
		"matching snapshot passes and is safe to persist",
		func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			app := calculate(t, ls, db)
			require.Equal(t, uint64(100), app.snapshotCapturedSlot)
			require.Equal(t, uint64(100), app.snapshotBoundarySlot)

			writeTxn := db.Transaction(context.Background(), true)
			require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
				ok, err := stakeRewardPrecomputeSnapshotGuardOK(
					meta,
					txn.Metadata(),
					app,
				)
				require.NoError(t, err)
				require.True(t, ok)
				return ls.saveStakeRewardPrecompute(
					meta,
					txn.Metadata(),
					app,
					newEpoch,
					capturedSlot,
					boundarySlot,
				)
			}))

			poolOutputs, err := meta.GetRewardPoolOutputs(
				rewardSnapshotEpoch,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, poolOutputs, 1)
			accountOutputs, err := meta.GetRewardAccountOutputs(
				rewardSnapshotEpoch,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, accountOutputs, 2)
		},
	)

	t.Run(
		"snapshot replaced between read and write is dropped",
		func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			app := calculate(t, ls, db)

			// Simulate the world moving on between the read and write phases: a
			// rollback replaces the mark snapshot for the same epoch with
			// different captured/boundary slots (e.g. it was recaptured at a
			// different point after a reorg).
			require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:            rewardSnapshotEpoch,
				SnapshotType:     "mark",
				TotalActiveStake: 1_000,
				TotalPoolCount:   1,
				TotalDelegators:  2,
				CapturedSlot:     555,
				BoundarySlot:     555,
				ProtocolVersion:  7,
			}, nil))

			writeTxn := db.Transaction(context.Background(), true)
			require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
				ok, err := stakeRewardPrecomputeSnapshotGuardOK(
					meta,
					txn.Metadata(),
					app,
				)
				require.NoError(t, err)
				require.False(t, ok)
				return nil
			}))

			poolOutputs, err := meta.GetRewardPoolOutputs(
				rewardSnapshotEpoch,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, poolOutputs)
			accountOutputs, err := meta.GetRewardAccountOutputs(
				rewardSnapshotEpoch,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, accountOutputs)
		},
	)

	t.Run(
		"snapshot removed between read and write is dropped",
		func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			app := calculate(t, ls, db)

			// Simulate a rollback deleting the snapshot outright.
			require.NoError(t, meta.DeleteRewardStateAfterSlot(0, nil))

			writeTxn := db.Transaction(context.Background(), true)
			require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
				ok, err := stakeRewardPrecomputeSnapshotGuardOK(
					meta,
					txn.Metadata(),
					app,
				)
				require.NoError(t, err)
				require.False(t, ok)
				return nil
			}))

			poolOutputs, err := meta.GetRewardPoolOutputs(
				rewardSnapshotEpoch,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, poolOutputs)
		},
	)

	t.Run(
		"rollback generation changed between read and write is dropped",
		func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			app := calculate(t, ls, db)

			// The Mark snapshot is deliberately unchanged. The generation represents
			// a rollback-sensitive input outside that row (blocks, pots, pparams, or
			// certificate history) changing after calculation.
			ls.rewardInputGeneration.Add(1)

			writeTxn := db.Transaction(context.Background(), true)
			require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
				ok, err := stakeRewardPrecomputeSnapshotGuardOK(
					meta,
					txn.Metadata(),
					app,
				)
				require.NoError(t, err)
				require.False(t, ok)
				return nil
			}))
		},
	)
}

func TestRewardPrefilterAccountsSkipsHistoryWhenNotRequired(t *testing.T) {
	t.Parallel()

	activeAccounts := map[string]struct{}{"active": {}}
	accounts, err := rewardPrefilterAccounts(
		rewardAccountHistoryMustNotRun{},
		nil,
		nil,
		nil,
		100,
		false,
		activeAccounts,
	)
	require.NoError(t, err)
	require.Equal(t, activeAccounts, accounts)
}

type rewardAccountHistoryMustNotRun struct{}

func (rewardAccountHistoryMustNotRun) GetAccountsActiveAtSlot(
	[]models.StakeCredentialRef,
	uint64,
	types.Txn,
) (map[string]struct{}, error) {
	panic("reward account history query must not run")
}

// TestStakeRewardPrecomputeSnapshotGuardRejectsSameSlotContentChange verifies
// the deferred-persist guard rejects a mark snapshot that keeps identical
// captured/boundary slots but whose content changed between the read and write
// phases (a rollback re-deriving the same-slot snapshot could produce this).
// The guard compares snapshot content (epoch nonce and totals), not just slot
// numbers, so a slot-only check would have wrongly persisted stale rewards.
func TestStakeRewardPrecomputeSnapshotGuardRejectsSameSlotContentChange(
	t *testing.T,
) {
	t.Parallel()

	const (
		rewardSnapshotEpoch = uint64(1)
		newEpoch            = uint64(4)
		capturedSlot        = uint64(200)
		boundarySlot        = uint64(1_200)
	)

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	var app *stakeRewardApplication
	readTxn := db.Transaction(context.Background(), false)
	require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
		computed, ok, err := ls.precomputeStakeRewardsCalculate(
			txn,
			newEpoch,
			capturedSlot,
			boundarySlot,
		)
		require.NoError(t, err)
		require.True(t, ok)
		app = computed
		return nil
	}))
	require.NotNil(t, app)
	require.Equal(t, uint64(100), app.snapshotCapturedSlot)
	require.Equal(t, uint64(100), app.snapshotBoundarySlot)

	// Re-save the mark snapshot with identical captured/boundary slots but a
	// different active-stake total and epoch nonce, mimicking a rollback that
	// re-derived the same-slot snapshot with different content. A slot-only
	// guard would accept this and persist the stale precomputed rewards.
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 2_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		EpochNonce:       []byte{0xAB, 0xCD},
		ProtocolVersion:  7,
	}, nil))
	changedSnapshot, err := meta.GetRewardSnapshot(
		rewardSnapshotEpoch,
		"mark",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, changedSnapshot)
	require.Equal(t, types.Uint64(2_000), changedSnapshot.TotalActiveStake)
	require.Equal(t, []byte{0xAB, 0xCD}, changedSnapshot.EpochNonce)

	writeTxn := db.Transaction(context.Background(), true)
	require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
		ok, err := stakeRewardPrecomputeSnapshotGuardOK(
			meta,
			txn.Metadata(),
			app,
		)
		require.NoError(t, err)
		require.False(t, ok)
		return nil
	}))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Empty(t, poolOutputs)
}

func newRewardCalculationTestLedger(
	t testing.TB,
) (*LedgerState, *database.Database) {
	t.Helper()
	cfg := newRewardCalculationTestNodeConfig(t)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	return &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}, db
}

func seedEmptyRewardBasisForRollover(
	t testing.TB,
	db *database.Database,
	currentEpoch models.Epoch,
	pparams lcommon.ProtocolParameters,
) {
	t.Helper()
	epochs, ok := stakeRewardEpochsForApplication(currentEpoch.EpochId + 1)
	require.True(t, ok)
	meta := db.Metadata()
	for _, epochID := range []uint64{
		epochs.snapshot,
		epochs.performance,
		epochs.pots,
	} {
		epoch, err := meta.GetEpoch(epochID, nil)
		require.NoError(t, err)
		if epoch != nil {
			continue
		}
		delta := currentEpoch.EpochId - epochID
		startSlot := currentEpoch.StartSlot -
			delta*uint64(currentEpoch.LengthInSlots)
		require.NoError(t, meta.SetEpoch(
			startSlot,
			epochID,
			nil, nil, nil, nil,
			currentEpoch.EraId,
			currentEpoch.SlotLength,
			currentEpoch.LengthInSlots,
			nil,
		))
	}
	encoded, err := cbor.Encode(pparams)
	require.NoError(t, err)
	performanceEpoch, err := meta.GetEpoch(epochs.performance, nil)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		encoded,
		performanceEpoch.StartSlot,
		epochs.performance,
		currentEpoch.EraId,
		nil,
	))
	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	var treasury, reserves types.Uint64
	if state != nil {
		treasury = state.Treasury
		reserves = state.Reserves
	}
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        epochs.pots,
		Treasury:     treasury,
		Reserves:     reserves,
		CapturedSlot: currentEpoch.StartSlot,
	}, nil))
	require.NoError(t, meta.DeleteRewardInputsForEpoch(epochs.snapshot, nil))
	require.NoError(t, meta.DeleteRewardOutputsForEpoch(epochs.snapshot, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:        epochs.snapshot,
		SnapshotType: "mark",
		CapturedSlot: currentEpoch.StartSlot,
		BoundarySlot: currentEpoch.StartSlot,
	}, nil))
}

func newRewardCalculationTestNodeConfig(
	t testing.TB,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 100010000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	return cfg
}

func seedRewardPrecomputeTimingState(
	t *testing.T,
	protocolMajor uint,
) (*LedgerState, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	seedRewardPrecomputeTimingInputs(t, db, protocolMajor)
	return ls, db
}

// seedRewardPrecomputeTimingInputs writes the one-pool reward round that
// seedRewardPrecomputeTimingState uses onto any metadata backend.
func seedRewardPrecomputeTimingInputs(
	t *testing.T,
	db *database.Database,
	protocolMajor uint,
) {
	t.Helper()
	meta := db.Metadata()

	const (
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
	)
	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    protocolMajor,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	require.NoError(t, meta.SetEpoch(
		200,
		potsEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		1_000,
		nil,
	))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(),
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 200,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  protocolMajor,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
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
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: rewardAccount,
			Active:     true,
		}),
	)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: member,
			Active:     true,
		}),
	)
}

func rewardCalcHash(fill byte) []byte {
	ret := make([]byte, 28)
	for i := range ret {
		ret[i] = fill
	}
	return ret
}

func rewardCalcRat(num int64, denom int64) *cbor.Rat {
	return &cbor.Rat{Rat: big.NewRat(num, denom)}
}

func rewardCalcSQLDB(t *testing.T, db *database.Database) *sql.DB {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return raw
}

func rewardCalcExecRows(
	t *testing.T,
	db *database.Database,
	query string,
	args ...any,
) int64 {
	t.Helper()
	result, err := rewardCalcSQLDB(t, db).Exec(query, args...)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	return rows
}

func rewardCalcSetAccountActive(
	t *testing.T,
	db *database.Database,
	stakingKey []byte,
	active bool,
) {
	t.Helper()
	rewardCalcSetAccountActiveByCredential(t, db, 0, stakingKey, active)
}

func rewardCalcSetAccountActiveByCredential(
	t *testing.T,
	db *database.Database,
	credentialTag uint8,
	stakingKey []byte,
	active bool,
) {
	t.Helper()
	rows := rewardCalcExecRows(
		t,
		db,
		"UPDATE account SET active = ? WHERE credential_tag = ? AND staking_key = ?",
		active,
		credentialTag,
		stakingKey,
	)
	require.Equal(t, int64(1), rows)
}

func rewardCalcSeedStakeCert(
	t *testing.T,
	db *database.Database,
	id uint,
	stakingKey []byte,
	credentialTag uint8,
	slot uint64,
	certType uint,
) {
	t.Helper()
	raw := rewardCalcSQLDB(t, db)
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	_, err := raw.Exec(`
INSERT INTO "transaction" (id, hash, slot, block_index)
VALUES (?, ?, ?, 0)`,
		id, hash, slot,
	)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO certs (
    id, transaction_id, cert_index, slot, cert_type
) VALUES (?, ?, 0, ?, ?)`,
		id, id, slot, certType,
	)
	require.NoError(t, err)
	switch certType {
	case uint(lcommon.CertificateTypeStakeRegistration):
		_, err = raw.Exec(`
INSERT INTO stake_registration (
    id, staking_key, credential_tag, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?)`,
			id, stakingKey, credentialTag, id, slot,
		)
		require.NoError(t, err)
	case uint(lcommon.CertificateTypeStakeDeregistration):
		_, err = raw.Exec(`
INSERT INTO stake_deregistration (
    id, staking_key, credential_tag, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?)`,
			id, stakingKey, credentialTag, id, slot,
		)
		require.NoError(t, err)
	default:
		t.Fatalf("unsupported cert type %d", certType)
	}
}

// --- CIP-23 minimum pool margin wiring ---

func TestMinPoolMarginRat(t *testing.T) {
	t.Parallel()

	require.Nil(t, minPoolMarginRat(0))
	require.Zero(t, big.NewRat(150, 10_000).Cmp(minPoolMarginRat(150)))
	require.Zero(t, big.NewRat(1, 1).Cmp(minPoolMarginRat(10_000)))
}

// applyMinPoolMarginConfig sets the floor only when the value is nonzero AND the
// calculation era is Dijkstra or later; otherwise it leaves the field nil.
func TestApplyMinPoolMarginConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bp      uint
		eraID   uint
		wantRat *big.Rat // nil => expect nil
	}{
		{name: "disabled zero at dijkstra", bp: 0, eraID: eras.DijkstraEraDesc.Id},
		{name: "pre-dijkstra ignored", bp: 150, eraID: eras.ConwayEraDesc.Id},
		{
			name:    "dijkstra sets rat",
			bp:      150,
			eraID:   eras.DijkstraEraDesc.Id,
			wantRat: big.NewRat(150, 10_000),
		},
		{
			name:    "post-dijkstra sets rat",
			bp:      500,
			eraID:   eras.DijkstraEraDesc.Id + 1,
			wantRat: big.NewRat(500, 10_000),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := rewards.Parameters{ProtocolMajorVersion: 9}
			applyMinPoolMarginConfig(
				&params,
				LedgerStateConfig{MinPoolMargin: tt.bp},
				tt.eraID,
			)
			if tt.wantRat == nil {
				require.Nil(t, params.MinPoolMargin)
				return
			}
			require.NotNil(t, params.MinPoolMargin)
			require.Zero(t, tt.wantRat.Cmp(params.MinPoolMargin))
		})
	}
}

func TestLedgerStateMinPoolMargin(t *testing.T) {
	t.Parallel()

	ls, _ := newRewardCalculationTestLedger(t)
	require.Nil(t, ls.MinPoolMargin())
	ls.config.MinPoolMargin = 150
	require.Zero(t, big.NewRat(150, 10_000).Cmp(ls.MinPoolMargin()))
}

// --- CIP-0163 full-pot reward distribution -------------------------------

func TestApplyFullPotConfigEnabled(t *testing.T) {
	t.Parallel()

	params := rewards.Parameters{}
	applyFullPotConfig(&params, LedgerStateConfig{FullPotRewardsEnabled: true})
	require.True(t, params.FullPotRewardsEnabled)
}

func TestApplyFullPotConfigDisabled(t *testing.T) {
	t.Parallel()

	params := rewards.Parameters{FullPotRewardsEnabled: true}
	applyFullPotConfig(&params, LedgerStateConfig{FullPotRewardsEnabled: false})
	require.False(t, params.FullPotRewardsEnabled)
}

// TestPrecomputedRewardPoolRewardsMatchInputsFullPot verifies that under
// CIP-0163 full pot the reuse verifier reproduces the pot-filling apportionment:
// it accepts persisted totals equal to the apportioned pool rewards (with the
// leader split re-derived from the scaled total), and rejects both the
// unscaled base totals and any perturbed total or leader reward. With the gate
// off the same apportioned totals are rejected because the disabled path
// expects each pool's base reward.
func TestPrecomputedRewardPoolRewardsMatchInputsFullPot(t *testing.T) {
	t.Parallel()

	keyA := rewardCalcHash(0x51)
	keyB := rewardCalcHash(0x52)

	params := rewards.Parameters{
		Decentralization:      new(big.Rat),
		OptimalPoolCount:      10,
		PledgeInfluence:       big.NewRat(1, 2),
		FullPotRewardsEnabled: true,
	}
	const (
		availableRewards = uint64(1_000_000)
		totalActiveStake = uint64(1_000)
		totalCirculation = uint64(10_000)
		totalBlocks      = uint64(10)
	)
	blockCounts := map[string]uint64{
		string(keyA): 6,
		string(keyB): 4,
	}
	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:    keyA,
			Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:         100,
			Cost:           1_000,
			DelegatedStake: 600,
			OwnerStake:     100,
		},
		{
			PoolKeyHash:    keyB,
			Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:         50,
			Cost:           1_000,
			DelegatedStake: 400,
			OwnerStake:     50,
		},
	}

	baseFor := func(in *models.RewardPoolInput) uint64 {
		pr, err := rewards.CalculatePoolReward(
			rewards.Pool{
				Margin:         big.NewRat(1, 10),
				Pledge:         uint64(in.Pledge),
				Cost:           uint64(in.Cost),
				DelegatedStake: uint64(in.DelegatedStake),
				OwnerStake:     uint64(in.OwnerStake),
				BlocksProduced: blockCounts[string(in.PoolKeyHash)],
				TotalBlocks:    totalBlocks,
			},
			availableRewards,
			totalActiveStake,
			totalCirculation,
			totalBlocks,
			params,
		)
		require.NoError(t, err)
		return pr.PoolReward
	}
	baseA := baseFor(poolInputs[0])
	baseB := baseFor(poolInputs[1])
	scaled := rewards.ApportionFullPot([]uint64{baseA, baseB}, availableRewards)
	require.Equal(t, availableRewards, scaled[0]+scaled[1])
	require.Greater(t, scaled[0], baseA)
	require.Greater(t, scaled[1], baseB)

	leaderA, err := rewards.LeaderReward(
		scaled[0], 1_000, big.NewRat(1, 10), 100, 600,
	)
	require.NoError(t, err)
	leaderB, err := rewards.LeaderReward(
		scaled[1], 1_000, big.NewRat(1, 10), 50, 400,
	)
	require.NoError(t, err)

	check := func(p rewards.Parameters, outs []*models.RewardPoolOutput) bool {
		ok, err := precomputedRewardPoolRewardsMatchInputs(
			poolInputs,
			outs,
			blockCounts,
			availableRewards,
			totalActiveStake,
			totalCirculation,
			totalBlocks,
			p,
		)
		require.NoError(t, err)
		return ok
	}

	apportioned := []*models.RewardPoolOutput{
		{
			PoolKeyHash:  keyA,
			TotalReward:  types.Uint64(scaled[0]),
			LeaderReward: types.Uint64(leaderA),
		},
		{
			PoolKeyHash:  keyB,
			TotalReward:  types.Uint64(scaled[1]),
			LeaderReward: types.Uint64(leaderB),
		},
	}
	base := []*models.RewardPoolOutput{
		{PoolKeyHash: keyA, TotalReward: types.Uint64(baseA)},
		{PoolKeyHash: keyB, TotalReward: types.Uint64(baseB)},
	}

	// Gate on: the apportioned totals with re-derived leader rewards are
	// accepted.
	require.True(t, check(params, apportioned))

	// Gate on: the unscaled base totals are rejected.
	require.False(t, check(params, base))

	// Gate on: a total that is not the apportioned value is rejected.
	require.False(t, check(params, []*models.RewardPoolOutput{
		{
			PoolKeyHash:  keyA,
			TotalReward:  types.Uint64(scaled[0] + 1),
			LeaderReward: types.Uint64(leaderA),
		},
		{
			PoolKeyHash:  keyB,
			TotalReward:  types.Uint64(scaled[1]),
			LeaderReward: types.Uint64(leaderB),
		},
	}))

	// Gate on: a leader reward not re-derivable from the scaled total is
	// rejected.
	require.False(t, check(params, []*models.RewardPoolOutput{
		{
			PoolKeyHash:  keyA,
			TotalReward:  types.Uint64(scaled[0]),
			LeaderReward: types.Uint64(leaderA + 1),
		},
		{
			PoolKeyHash:  keyB,
			TotalReward:  types.Uint64(scaled[1]),
			LeaderReward: types.Uint64(leaderB),
		},
	}))

	// Gate off: the apportioned totals are rejected because the disabled path
	// expects each pool's base reward.
	paramsOff := params
	paramsOff.FullPotRewardsEnabled = false
	require.False(t, check(paramsOff, apportioned))
}

// TestRewardCalculatorInputsAllowExcludedPoolStake covers the reward-input
// shape snapshot capture writes when a pool is excluded for degraded
// registration data: reward_pool_input holds only the surviving pools, while
// reward_snapshot.total_active_stake still carries the excluded pool's
// delegated stake because that stake belongs in the sigma_a denominator (see
// snapshot.buildRewardStateInputs). Requiring the rows to sum to exactly the
// snapshot total forced the denominator down to the surviving pool set, which
// under-credits every reward the node reconstructs.
func TestRewardCalculatorInputsAllowExcludedPoolStake(t *testing.T) {
	t.Parallel()

	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)
	snapshot := func(totalActiveStake uint64) *models.RewardSnapshot {
		return &models.RewardSnapshot{
			TotalActiveStake: types.Uint64(totalActiveStake),
			TotalPoolCount:   1,
			TotalDelegators:  1,
			CapturedSlot:     10,
			BoundarySlot:     20,
		}
	}
	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			DelegatedStake:             100,
			OwnerStake:                 0,
			DelegatorCount:             1,
			CapturedSlot:               10,
			BoundarySlot:               20,
		},
	}
	stakeInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash:  poolKey,
			StakingKey:   member,
			Stake:        100,
			CapturedSlot: 10,
			BoundarySlot: 20,
		},
	}

	// 40 lovelace of the boundary's active stake belongs to an excluded pool.
	require.NoError(t, validateRewardCalculatorInputs(
		snapshot(140),
		poolInputs,
		stakeInputs,
	))
	match, err := precomputedRewardPoolInputsMatchSnapshot(
		snapshot(140),
		poolInputs,
	)
	require.NoError(t, err)
	require.True(t, match)

	// Rows summing to more than the declared active stake still fail: the row
	// set and the snapshot then describe different boundaries.
	err = validateRewardCalculatorInputs(
		snapshot(99),
		poolInputs,
		stakeInputs,
	)
	require.ErrorContains(
		t,
		err,
		"reward pool input total delegated stake 100 exceeds snapshot active stake 99",
	)
	match, err = precomputedRewardPoolInputsMatchSnapshot(
		snapshot(99),
		poolInputs,
	)
	require.NoError(t, err)
	require.False(t, match)
}

// TestRewardCalculatorInputsExactWithTrackedExcludedStake pins the exact bound
// when the excluded stake is tracked:
// TestRewardCalculatorInputsAllowExcludedPoolStake's non-exceeding bound
// tolerates one legitimately excluded pool's stake going missing, but it
// tolerates just as well a row set proportionally shrunk by some other bug --
// pool count, delegator count, and the per-pool cross-sums all stay
// internally consistent, so nothing else catches it. A snapshot with a
// tracked ExcludedActiveStake (set by snapshot.buildRewardStateInputs at
// capture) closes that gap: the rows must sum to exactly TotalActiveStake
// minus the tracked exclusion, not merely no more than TotalActiveStake.
func TestRewardCalculatorInputsExactWithTrackedExcludedStake(t *testing.T) {
	t.Parallel()

	poolKey := rewardCalcHash(0x4c)
	rewardAccount := rewardCalcHash(0x5c)
	member := rewardCalcHash(0x6c)
	snapshot := func(totalActiveStake, excludedActiveStake uint64) *models.RewardSnapshot {
		excluded := types.Uint64(excludedActiveStake)
		return &models.RewardSnapshot{
			TotalActiveStake:    types.Uint64(totalActiveStake),
			ExcludedActiveStake: &excluded,
			TotalPoolCount:      1,
			TotalDelegators:     1,
			CapturedSlot:        10,
			BoundarySlot:        20,
		}
	}
	poolInputsWithStake := func(stake uint64) []*models.RewardPoolInput {
		return []*models.RewardPoolInput{
			{
				PoolKeyHash:                poolKey,
				RewardAccount:              rewardAccount,
				RewardAccountCredentialTag: 0,
				Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
				DelegatedStake:             types.Uint64(stake),
				OwnerStake:                 0,
				DelegatorCount:             1,
				CapturedSlot:               10,
				BoundarySlot:               20,
			},
		}
	}
	stakeInputsWithStake := func(stake uint64) []*models.RewardStakeInput {
		return []*models.RewardStakeInput{
			{
				PoolKeyHash:  poolKey,
				StakingKey:   member,
				Stake:        types.Uint64(stake),
				CapturedSlot: 10,
				BoundarySlot: 20,
			},
		}
	}

	// 100 (rows) + 40 (tracked excluded) == 140 (declared total): exact match
	// passes, matching the one-legitimately-excluded-pool case.
	require.NoError(t, validateRewardCalculatorInputs(
		snapshot(140, 40),
		poolInputsWithStake(100),
		stakeInputsWithStake(100),
	))
	match, err := precomputedRewardPoolInputsMatchSnapshot(
		snapshot(140, 40),
		poolInputsWithStake(100),
	)
	require.NoError(t, err)
	require.True(t, match)

	// The same 40 tracked as excluded, but the row set is proportionally
	// shrunk to 50 instead of 100 -- as if every pool's stake had been halved.
	// 50+40=90 != 140, so this must now be rejected even though 50 <= 140
	// would have passed the old non-exceeding bound silently.
	err = validateRewardCalculatorInputs(
		snapshot(140, 40),
		poolInputsWithStake(50),
		stakeInputsWithStake(50),
	)
	require.ErrorContains(
		t,
		err,
		"reward pool input total delegated stake 50 does not match snapshot active stake 140 minus excluded active stake 40",
	)
	match, err = precomputedRewardPoolInputsMatchSnapshot(
		snapshot(140, 40),
		poolInputsWithStake(50),
	)
	require.NoError(t, err)
	require.False(t, match)

	// A tracked exclusion of exactly zero still demands an exact match: no
	// slack remains once the snapshot affirmatively says nothing was
	// excluded.
	err = validateRewardCalculatorInputs(
		snapshot(140, 0),
		poolInputsWithStake(100),
		stakeInputsWithStake(100),
	)
	require.ErrorContains(
		t,
		err,
		"reward pool input total delegated stake 100 does not match snapshot active stake 140 minus excluded active stake 0",
	)
}

// TestRewardCalculatorInputsRejectsDelegatorCountMismatch is the
// TotalDelegators companion to
// TestRewardCalculatorInputsAllowExcludedPoolStake: that test varies
// TotalActiveStake against a fixed pool/stake-input set and confirms rows may
// sum to no more than the declared active stake. TotalDelegators has no
// equivalent slack — unlike TotalActiveStake, the reward_pool_input rows are
// the only source of delegator counts, so the total must match exactly
// (validateRewardCalculatorInputs, reward_calculation.go; and
// precomputedRewardPoolInputsMatchSnapshot). This pins that a mismatch in
// either direction is rejected rather than silently tolerated the way
// TotalActiveStake's exclusion slack might suggest.
func TestRewardCalculatorInputsRejectsDelegatorCountMismatch(t *testing.T) {
	t.Parallel()

	poolKey := rewardCalcHash(0x4b)
	rewardAccount := rewardCalcHash(0x5b)
	member := rewardCalcHash(0x6b)
	snapshot := func(totalDelegators uint64) *models.RewardSnapshot {
		return &models.RewardSnapshot{
			TotalActiveStake: types.Uint64(100),
			TotalPoolCount:   1,
			TotalDelegators:  totalDelegators,
			CapturedSlot:     10,
			BoundarySlot:     20,
		}
	}
	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			DelegatedStake:             100,
			OwnerStake:                 0,
			DelegatorCount:             1,
			CapturedSlot:               10,
			BoundarySlot:               20,
		},
	}
	stakeInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash:  poolKey,
			StakingKey:   member,
			Stake:        100,
			CapturedSlot: 10,
			BoundarySlot: 20,
		},
	}

	// Matching delegator count passes.
	require.NoError(t, validateRewardCalculatorInputs(
		snapshot(1),
		poolInputs,
		stakeInputs,
	))
	match, err := precomputedRewardPoolInputsMatchSnapshot(
		snapshot(1),
		poolInputs,
	)
	require.NoError(t, err)
	require.True(t, match)

	// Snapshot claims more delegators than the rows carry.
	err = validateRewardCalculatorInputs(
		snapshot(2),
		poolInputs,
		stakeInputs,
	)
	require.ErrorContains(
		t,
		err,
		"reward pool input total delegator count 1 does not match snapshot delegator count 2",
	)
	match, err = precomputedRewardPoolInputsMatchSnapshot(
		snapshot(2),
		poolInputs,
	)
	require.NoError(t, err)
	require.False(t, match)

	// Snapshot claims fewer delegators than the rows carry.
	err = validateRewardCalculatorInputs(
		snapshot(0),
		poolInputs,
		stakeInputs,
	)
	require.ErrorContains(
		t,
		err,
		"reward pool input total delegator count 1 does not match snapshot delegator count 0",
	)
	match, err = precomputedRewardPoolInputsMatchSnapshot(
		snapshot(0),
		poolInputs,
	)
	require.NoError(t, err)
	require.False(t, match)
}

// settleRewardCredits folds every credited round's unfolded credits into the
// account rows, so a test can read account.Reward as the balance.
func settleRewardCredits(t *testing.T, ls *LedgerState) {
	t.Helper()
	meta := ls.db.Metadata()
	rounds, err := meta.GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	credited := make(map[string]models.StakeCredentialRef)
	for _, round := range rounds {
		outputs, err := meta.GetRewardAccountOutputs(round.SnapshotEpoch, nil)
		require.NoError(t, err)
		for _, output := range outputs {
			if output.Spendable && !output.Guarded {
				ref := models.NewStakeCredentialRef(
					output.CredentialTag, output.StakingKey,
				)
				credited[ref.MapKey()] = ref
			}
		}
	}
	txn := ls.db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, ref := range credited {
			if err := ls.foldRewardCreditFor(context.Background(), txn, ref.Tag, ref.Key); err != nil {
				return err
			}
		}
		return nil
	}))
}

func epochBoundaryBenchPartialPrecomputeT(
	b testing.TB,
	f *epochBoundaryBenchFixture,
) {
	b.Helper()
	evt := epochBoundaryBenchPrecomputeEvent()
	round, ok, err := f.ls.resolveStakeRewardPrecomputeRound(
		evt.NewEpoch+1,
		evt.BoundarySlot,
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1),
	)
	require.NoError(b, err)
	require.True(b, ok)
	chunks := (len(round.poolInputs) + f.ls.rewardPrecomputeChunkSize() - 1) /
		f.ls.rewardPrecomputeChunkSize()
	for range chunks / 2 {
		done, err := f.ls.stakeRewardPrecomputeChunkStep(round)
		require.NoError(b, err)
		require.False(b, done)
	}
}

func (ls *LedgerState) waitEpochBoundaryBenchBackground() {
	_ = ls.WaitEpochBoundaryJob(context.Background())
	ls.ratificationWG.Wait()
	ls.deferredStakeInputsWG.Wait()
	ls.rewardCreditCompactionWG.Wait()
}

// wireDeferredBoundarySnapshot mirrors node.go's deferred mark snapshot
// wiring.
func wireDeferredBoundarySnapshot(ls *LedgerState, mgr *snapshot.Manager) {
	ls.SetDeferredEpochBoundarySnapshotHooks(
		mgr.DeferEpochBoundaryCapture,
		mgr.DiscardEpochBoundaryCapture,
		func(
			txn *database.Txn,
			evt event.EpochTransitionEvent,
		) (DeferredBoundarySnapshot, error) {
			return mgr.PrepareEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
}

// epochBoundaryBenchWireDeferred mirrors node.go's
// wireDeferredRewardStakeInputs.
func epochBoundaryBenchWireDeferred(ls *LedgerState, mgr *snapshot.Manager) {
	ls.SetEpochBoundaryDeferredStakeInputsHook(
		func(
			txn *database.Txn,
		) (uint64, uint64, []*models.RewardStakeInput, bool) {
			deferred, ok := mgr.TakeDeferredRewardStakeInputs(txn)
			if !ok {
				return 0, 0, nil, false
			}
			return deferred.Epoch, deferred.BoundarySlot, deferred.Inputs, true
		},
	)
	mgr.SetDeferRewardStakeInputs(true)
}

const (
	epochBoundaryBenchEpochLength = uint64(432_000)
	// The rollover under measurement ends this epoch, so it applies the
	// reward round whose snapshot, performance and pots epochs are 8, 9 and
	// 10.
	epochBoundaryBenchEndedEpoch = uint64(10)
	epochBoundaryBenchMaxSupply  = uint64(45_000_000_000_000_000)
	epochBoundaryBenchReserves   = uint64(7_600_000_000_000_000)
	epochBoundaryBenchTreasury   = uint64(1_600_000_000_000_000)
	epochBoundaryBenchFees       = uint64(31_000_000_000)
	epochBoundaryBenchBlocks     = 21_600
)

func epochBoundaryBenchNodeConfig(tb testing.TB) *cardano.CardanoNodeConfig {
	tb.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("5a", 32),
	}
	require.NoError(tb, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"epochLength": 432000,
		"maxLovelaceSupply": 45000000000000000,
		"securityParam": 2160,
		"slotLength": 1,
		"updateQuorum": 5,
		"systemStart": "2017-09-23T21:44:51Z"
	}`)))
	return cfg
}

func epochBoundaryBenchPParams() *conway.ConwayProtocolParameters {
	rat := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	p := donationTestConwayPParams(10)
	p.MinFeeA = 44
	p.MinFeeB = 155_381
	p.MaxBlockBodySize = 90_112
	p.MaxTxSize = 16_384
	p.MaxBlockHeaderSize = 1_100
	p.KeyDeposit = 2_000_000
	p.PoolDeposit = 500_000_000
	p.MaxEpoch = 18
	p.NOpt = 500
	p.A0 = rat(3, 10)
	p.Rho = rat(3, 1000)
	p.Tau = rat(1, 5)
	p.MinPoolCost = 170_000_000
	p.AdaPerUtxoByte = 4_310
	p.MinCommitteeSize = 7
	p.CommitteeTermLimit = 146
	p.GovActionValidityPeriod = 6
	p.GovActionDeposit = 100_000_000_000
	p.DRepDeposit = 500_000_000
	p.DRepInactivityPeriod = 20
	p.MinFeeRefScriptCostPerByte = rat(15, 1)
	return p
}

// epochBoundaryBenchHash returns a deterministic 28-byte hash in its own
// domain, so credentials, pools and DReps never collide.
func epochBoundaryBenchHash(domain byte, index uint64) []byte {
	h := make([]byte, 28)
	h[0] = domain
	binary.BigEndian.PutUint64(h[20:], index)
	return h
}

func epochBoundaryBenchVRFHash(index uint64) []byte {
	h := make([]byte, 32)
	h[0] = 0x11
	binary.BigEndian.PutUint64(h[24:], index)
	return h
}

// splitmix64 gives the fixture a heavy-tailed but reproducible stake
// distribution without seeding math/rand.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// epochBoundaryBenchStake is log-uniform between 1 ADA and 100,000 ADA,
// which puts 1.3M delegators at roughly 11B ADA of active stake.
func epochBoundaryBenchStake(index uint64) uint64 {
	u := float64(splitmix64(index)>>11) / float64(1<<53)
	return uint64(math.Pow(10, 6+5*u))
}

type epochBoundaryBenchFixture struct {
	ls      *LedgerState
	db      *database.Database
	shape   epochBoundaryBenchShape
	pparams *conway.ConwayProtocolParameters
	epochs  map[uint64]models.Epoch
	phases  *epochBoundaryPhaseRecorder
}

// epochBoundaryPhaseRecorder collects the "epoch rollover phase" Debug records
// timeRolloverPhase emits, so the benchmark reports the same per-phase
// durations an operator reads from the log.
type epochBoundaryPhaseRecorder struct {
	mu     sync.Mutex
	phases []epochBoundaryPhase
}

type epochBoundaryPhase struct {
	name     string
	duration time.Duration
}

func (r *epochBoundaryPhaseRecorder) reset() {
	r.mu.Lock()
	r.phases = nil
	r.mu.Unlock()
}

func (r *epochBoundaryPhaseRecorder) snapshot() []epochBoundaryPhase {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]epochBoundaryPhase(nil), r.phases...)
}

// newEpochBoundaryBenchFixture seeds a fresh database, or copies the seeded
// template in templateDir when one is named.
func newEpochBoundaryBenchFixture(
	tb testing.TB,
	shape epochBoundaryBenchShape,
	templateDir string,
) *epochBoundaryBenchFixture {
	tb.Helper()
	dataDir := tb.TempDir()
	if template := templateDir; template != "" {
		epochBoundaryBenchTemplate(tb, template, shape)
		require.NoError(tb, os.CopyFS(
			dataDir, os.DirFS(filepath.Join(template, "data")),
		))
	}
	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: dataDir})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	f := &epochBoundaryBenchFixture{
		db:      db,
		shape:   shape,
		pparams: epochBoundaryBenchPParams(),
		epochs:  epochBoundaryBenchEpochs(),
		phases:  &epochBoundaryPhaseRecorder{},
	}
	if templateDir == "" {
		f.seed(tb)
	}
	f.wire(tb)
	return f
}

// epochBoundaryBenchTemplate seeds the shared template once, so repeated
// runs -- and runs of two different trees -- measure the same database.
func epochBoundaryBenchTemplate(
	tb testing.TB,
	dir string,
	shape epochBoundaryBenchShape,
) {
	tb.Helper()
	ready := filepath.Join(dir, "ready")
	want := fmt.Sprintf("%+v", shape)
	if raw, err := os.ReadFile(ready); err == nil {
		require.Equal(tb, want, string(raw), "template shape mismatch")
		return
	}
	dataDir := filepath.Join(dir, "data")
	require.NoError(tb, os.RemoveAll(dataDir))
	require.NoError(tb, os.MkdirAll(dataDir, 0o755))
	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: dataDir})
	require.NoError(tb, err)
	f := &epochBoundaryBenchFixture{
		db:      db,
		shape:   shape,
		pparams: epochBoundaryBenchPParams(),
		epochs:  epochBoundaryBenchEpochs(),
	}
	f.seed(tb)
	require.NoError(tb, dbtest.CloseDatabase(db))
	require.NoError(tb, os.WriteFile(ready, []byte(want), 0o644))
}

func (f *epochBoundaryBenchFixture) seed(tb testing.TB) {
	tb.Helper()
	start := time.Now()
	f.seedEpochs(tb)
	f.seedBulk(tb)
	bulk := time.Now()
	f.seedGovernance(tb)
	tb.Logf(
		"seeded: bulk %.1fs, governance %.1fs",
		bulk.Sub(start).Seconds(), time.Since(bulk).Seconds(),
	)
}

func (f *epochBoundaryBenchFixture) wire(tb testing.TB) {
	tb.Helper()
	db := f.db
	f.ls = &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   f.epochs[epochBoundaryBenchEndedEpoch],
		currentPParams: f.pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: epochBoundaryBenchNodeConfig(tb),
			Logger:            slog.New(f.phases),
		},
	}
	mgr := snapshot.NewManager(db, nil, slog.New(slog.NewTextHandler(
		io.Discard, nil,
	)))
	f.ls.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return mgr.ComputeEpochBoundarySnapshot(
				context.Background(),
				txn,
				evt,
			)
		},
	)
	f.ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return mgr.CaptureEpochBoundarySnapshot(
				context.Background(),
				txn,
				evt,
			)
		},
	)
	epochBoundaryBenchWireDeferred(f.ls, mgr)
	wireDeferredBoundarySnapshot(f.ls, mgr)
	f.ls.SetCurrentBoundarySPOStakeHook(
		func(
			txn *database.Txn,
			evt event.EpochTransitionEvent,
		) ([]*models.PoolStakeSnapshot, error) {
			return mgr.CurrentBoundarySPOStakeRows(
				context.Background(),
				txn,
				evt,
			)
		},
	)
}

func epochBoundaryBenchNonce(epoch uint64) []byte {
	nonce := make([]byte, 32)
	binary.BigEndian.PutUint64(nonce[24:], epoch+1)
	return nonce
}

func epochBoundaryBenchEpochs() map[uint64]models.Epoch {
	epochs := make(map[uint64]models.Epoch)
	for epoch := uint64(0); epoch <= epochBoundaryBenchEndedEpoch; epoch++ {
		nonce := epochBoundaryBenchNonce(epoch)
		epochs[epoch] = models.Epoch{
			EpochId:             epoch,
			StartSlot:           epochBoundaryBenchStart(epoch),
			Nonce:               nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			EraId:               eras.ConwayEraDesc.Id,
			SlotLength:          1_000,
			LengthInSlots:       uint(epochBoundaryBenchEpochLength),
		}
	}
	return epochs
}

func (f *epochBoundaryBenchFixture) seedEpochs(tb testing.TB) {
	tb.Helper()
	pparamsCbor, err := cbor.Encode(f.pparams)
	require.NoError(tb, err)
	for epoch := uint64(0); epoch <= epochBoundaryBenchEndedEpoch; epoch++ {
		e := f.epochs[epoch]
		require.NoError(tb, f.db.SetEpoch(
			e.StartSlot, epoch, e.Nonce, e.EvolvingNonce, e.CandidateNonce,
			e.LastEpochBlockNonce, e.EraId, e.SlotLength, e.LengthInSlots,
			nil,
		))
		require.NoError(tb, f.db.SetPParams(
			pparamsCbor, e.StartSlot, epoch, eras.ConwayEraDesc.Id, nil,
		))
	}
	require.NoError(tb, f.db.Metadata().SetNetworkState(
		epochBoundaryBenchTreasury,
		epochBoundaryBenchReserves,
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
		nil,
	))
}

type epochBoundaryBenchPool struct {
	key           []byte
	rewardAccount []byte
	margin        string
	cost          uint64
	pledge        uint64
	stake         uint64
	delegators    int
}

// seedBulk writes the delegator-scaled tables directly through SQL: at 1.3M
// rows, the model writers' per-row bookkeeping would dominate setup.
func (f *epochBoundaryBenchFixture) seedBulk(tb testing.TB) {
	tb.Helper()
	raw, err := dbtest.RawSQLiteMetadata(tb, f.db)
	require.NoError(tb, err)
	defer raw.Close()
	tx, err := raw.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	prepare := func(query string) *sql.Stmt {
		stmt, err := tx.Prepare(query)
		require.NoError(tb, err)
		return stmt
	}
	poolStmt := prepare(`
INSERT INTO pool (id, pool_key_hash, vrf_key_hash, reward_account,
    reward_account_credential_tag, margin, pledge, cost)
VALUES (?, ?, ?, ?, 0, ?, ?, ?)`)
	poolRegStmt := prepare(`
INSERT INTO pool_registration (id, pool_id, pool_key_hash, vrf_key_hash,
    reward_account, reward_account_credential_tag, margin, pledge, cost,
    added_slot, deposit_amount, deposit_held)
VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, 1, '500000000', '500000000')`)
	ownerStmt := prepare(`
INSERT INTO pool_registration_owner (key_hash, pool_registration_id, pool_id)
VALUES (?, ?, ?)`)
	accountStmt := prepare(`
INSERT INTO account (staking_key, credential_tag, pool, drep, drep_type,
    added_slot, created_slot, reward, active, expiration_epoch)
VALUES (?, 0, ?, ?, ?, 1, 1, ?, 1, 0)`)
	liveStmt := prepare(`
INSERT INTO reward_live_stake (pool_key_hash, staking_key, credential_tag,
    utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot,
    updated_slot, calculation_version)
VALUES (?, ?, 0, ?, ?, ?, 1, 1, 1, ?)`)
	utxoStmt := prepare(`
INSERT INTO utxo (tx_id, output_idx, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, payment_script)
VALUES (?, ?, ?, ?, 0, 1, 0, ?, 0)`)
	stakeInputStmt := prepare(`
INSERT INTO reward_stake_input (pool_key_hash, staking_key, epoch,
    credential_tag, stake, owner, registered, captured_slot, boundary_slot)
VALUES (?, ?, ?, 0, ?, ?, 1, ?, ?)`)
	poolInputStmt := prepare(`
INSERT INTO reward_pool_input (margin, pool_key_hash, reward_account, epoch,
    pledge, delegated_stake, owner_stake, cost, delegator_count,
    reward_account_credential_tag, captured_slot, boundary_slot)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`)
	poolSnapStmt := prepare(`
INSERT INTO pool_stake_snapshot (epoch, snapshot_type, pool_key_hash,
    total_stake, delegator_count, captured_slot, calculation_version)
VALUES (?, 'mark', ?, ?, ?, ?, ?)`)
	blockStmt := prepare(`
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, 0)`)

	shape := f.shape
	pools := make([]epochBoundaryBenchPool, shape.pools)
	for p := range pools {
		pool := &pools[p]
		pool.key = epochBoundaryBenchHash(0x10, uint64(p)+1)
		pool.rewardAccount = epochBoundaryBenchHash(0x20, uint64(p)+1)
		// Margins span the reference's rounding extremes: 0, 1 and
		// ordinary fractions.
		switch p % 7 {
		case 0:
			pool.margin = "0/1"
		case 1:
			pool.margin = "1/1"
		default:
			pool.margin = fmt.Sprintf("%d/1000", 5+(p%40))
		}
		pool.cost = 170_000_000 + uint64(p%5)*85_000_000
		pool.pledge = 1_000_000_000 * (1 + uint64(p%50))
	}
	// Every pool's own reward account delegates its pledge to the pool, so
	// the pledge check passes and owners are exercised.
	calcVersion := models.RewardStakeCalculationVersion
	var nextUtxo uint64
	delegatorPool := func(d int) int {
		// A skewed assignment: low-numbered pools attract more delegators,
		// and the last pool attracts none, so a zero-stake pool is present.
		u := float64(splitmix64(uint64(d)^0xabcdef)>>11) / float64(1<<53)
		p := int(float64(shape.pools-1) * u * u)
		if p >= shape.pools-1 {
			p = shape.pools - 2
		}
		return p
	}
	type stakeRow struct {
		pool  int
		key   []byte
		stake uint64
		owner bool
	}
	rows := make([]stakeRow, 0, shape.delegators+shape.pools)
	writeAccount := func(
		key []byte, pool []byte, stake uint64, reward uint64, d uint64,
	) {
		var drep any
		drepType := models.DrepTypeAddrKeyHash
		switch r := splitmix64(d^0x77) % 20; {
		case r < 11 && shape.dreps > 0:
			drep = epochBoundaryBenchHash(
				0x40,
				splitmix64(d)%uint64(shape.dreps)+1,
			)
		case r < 14:
			drepType = models.DrepTypeAlwaysAbstain
		case r < 15:
			drepType = models.DrepTypeAlwaysNoConfidence
		default:
			drepType = 0
		}
		_, err := accountStmt.Exec(
			key, pool, drep, drepType, strconv.FormatUint(reward, 10),
		)
		require.NoError(tb, err)
		utxoStake := stake - reward
		_, err = liveStmt.Exec(
			pool, key,
			strconv.FormatUint(utxoStake, 10),
			strconv.FormatUint(reward, 10),
			strconv.FormatUint(stake, 10),
			calcVersion,
		)
		require.NoError(tb, err)
		n := max(shape.utxosPerDelegator, 1)
		remaining := utxoStake
		for i := range n {
			amount := remaining / uint64(n-i)
			remaining -= amount
			nextUtxo++
			txID := make([]byte, 32)
			binary.BigEndian.PutUint64(txID[24:], nextUtxo)
			_, err := utxoStmt.Exec(
				txID, 0, epochBoundaryBenchHash(0x50, nextUtxo), key,
				strconv.FormatUint(amount, 10),
			)
			require.NoError(tb, err)
		}
	}
	for p := range pools {
		pool := &pools[p]
		vrfKeyHash := epochBoundaryBenchVRFHash(uint64(p) + 1)
		_, err := poolStmt.Exec(
			p+1, pool.key, vrfKeyHash,
			pool.rewardAccount, pool.margin,
			strconv.FormatUint(pool.pledge, 10),
			strconv.FormatUint(pool.cost, 10),
		)
		require.NoError(tb, err)
		_, err = poolRegStmt.Exec(
			p+1, p+1, pool.key, vrfKeyHash,
			pool.rewardAccount, pool.margin,
			strconv.FormatUint(pool.pledge, 10),
			strconv.FormatUint(pool.cost, 10),
		)
		require.NoError(tb, err)
		_, err = ownerStmt.Exec(pool.rewardAccount, p+1, p+1)
		require.NoError(tb, err)
		if p == shape.pools-1 {
			// The zero-stake pool: registered, no delegators, not even its
			// owner.
			continue
		}
		ownerStake := pool.pledge
		writeAccount(
			pool.rewardAccount, pool.key, ownerStake, 0,
			uint64(p)+0x1_0000_0000,
		)
		rows = append(rows, stakeRow{
			pool: p, key: pool.rewardAccount, stake: ownerStake, owner: true,
		})
		pool.stake += ownerStake
		pool.delegators++
	}
	for d := range shape.delegators {
		p := delegatorPool(d)
		key := epochBoundaryBenchHash(0x30, uint64(d)+1)
		stake := epochBoundaryBenchStake(uint64(d))
		reward := stake / 200
		writeAccount(key, pools[p].key, stake, reward, uint64(d))
		rows = append(rows, stakeRow{pool: p, key: key, stake: stake})
		pools[p].stake += stake
		pools[p].delegators++
	}
	var totalStake uint64
	for _, pool := range pools {
		totalStake += pool.stake
	}
	// The go, set and mark reward bases (snapshot epochs 8, 9, 10) and
	// their leader-election rows. Stake inputs are identical across the
	// three epochs: only the row count matters to the boundary's cost.
	for _, epoch := range []uint64{8, 9, 10} {
		boundary := epochBoundaryBenchStart(epoch)
		captured := boundary - 1
		for _, row := range rows {
			_, err := stakeInputStmt.Exec(
				pools[row.pool].key, row.key, epoch,
				strconv.FormatUint(row.stake, 10), row.owner,
				captured, boundary,
			)
			require.NoError(tb, err)
		}
		var poolCount, delegatorCount int
		for p := range pools {
			pool := &pools[p]
			if pool.delegators == 0 {
				continue
			}
			poolCount++
			delegatorCount += pool.delegators
			_, err := poolInputStmt.Exec(
				pool.margin, pool.key, pool.rewardAccount, epoch,
				strconv.FormatUint(pool.pledge, 10),
				strconv.FormatUint(pool.stake, 10),
				strconv.FormatUint(pool.pledge, 10),
				strconv.FormatUint(pool.cost, 10),
				pool.delegators, captured, boundary,
			)
			require.NoError(tb, err)
			_, err = poolSnapStmt.Exec(
				epoch, pool.key, strconv.FormatUint(pool.stake, 10),
				pool.delegators, captured, calcVersion,
			)
			require.NoError(tb, err)
		}
		_, err := tx.Exec(`
INSERT INTO reward_snapshot (epoch, snapshot_type, total_active_stake,
    total_pool_count, total_delegators, captured_slot, boundary_slot,
    epoch_nonce, protocol_version, authoritative, calculation_version,
    excluded_active_stake)
VALUES (?, 'mark', ?, ?, ?, ?, ?, ?, 10, 1, ?, '0')`,
			epoch, strconv.FormatUint(totalStake, 10), poolCount,
			delegatorCount, captured, boundary,
			f.epochs[epoch].Nonce, calcVersion,
		)
		require.NoError(tb, err)
		_, err = tx.Exec(`
INSERT INTO epoch_summary (epoch, total_active_stake, total_pool_count,
    total_delegators, epoch_nonce, boundary_slot, snapshot_ready)
VALUES (?, ?, ?, ?, ?, ?, 1)`,
			epoch, strconv.FormatUint(totalStake, 10), poolCount,
			delegatorCount, f.epochs[epoch].Nonce, boundary,
		)
		require.NoError(tb, err)
	}
	// Performance epoch 9: blocks in proportion to stake.
	perfStart := epochBoundaryBenchStart(9)
	slot := perfStart
	for p := range pools {
		share := uint64(
			float64(epochBoundaryBenchBlocks) *
				float64(pools[p].stake) / float64(totalStake),
		)
		for range share {
			_, err := blockStmt.Exec(pools[p].key, slot)
			require.NoError(tb, err)
			slot += 20
		}
	}
	_, err = tx.Exec(`
INSERT INTO reward_ada_pots (epoch, treasury, reserves, fees, rewards,
    captured_slot)
VALUES (?, ?, ?, ?, '0', ?)`,
		epochBoundaryBenchEndedEpoch,
		strconv.FormatUint(epochBoundaryBenchTreasury, 10),
		strconv.FormatUint(epochBoundaryBenchReserves, 10),
		strconv.FormatUint(epochBoundaryBenchFees, 10),
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
	)
	require.NoError(tb, err)
	_, err = tx.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		make([]byte, 32),
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1)-1,
		10_000_000,
	)
	require.NoError(tb, err)
	require.NoError(tb, tx.Commit())
}

func (f *epochBoundaryBenchFixture) seedGovernance(tb testing.TB) {
	tb.Helper()
	shape := f.shape
	raw, err := dbtest.RawSQLiteMetadata(tb, f.db)
	require.NoError(tb, err)
	defer raw.Close()
	tx, err := raw.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	drepStmt, err := tx.Prepare(`
INSERT INTO drep (credential, credential_tag, added_slot, last_activity_epoch,
    expiry_epoch, active)
VALUES (?, 0, 1, ?, ?, 1)`)
	require.NoError(tb, err)
	drepRegStmt, err := tx.Prepare(`
INSERT INTO registration_drep (drep_credential, credential_tag, added_slot,
    deposit_amount)
VALUES (?, 0, 1, '500000000')`)
	require.NoError(tb, err)
	for r := range shape.dreps {
		cred := epochBoundaryBenchHash(0x40, uint64(r)+1)
		_, err := drepStmt.Exec(cred, epochBoundaryBenchEndedEpoch, 40)
		require.NoError(tb, err)
		_, err = drepRegStmt.Exec(cred)
		require.NoError(tb, err)
	}
	for c := range shape.ccMembers {
		_, err := tx.Exec(`
INSERT INTO auth_committee_hot (cold_credential, host_credential,
    certificate_id, added_slot)
VALUES (?, ?, ?, 1)`,
			epochBoundaryBenchHash(0x60, uint64(c)+1),
			epochBoundaryBenchHash(0x61, uint64(c)+1),
			c+1,
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, tx.Commit())

	members := make([]*models.CommitteeMember, 0, shape.ccMembers)
	for c := range shape.ccMembers {
		members = append(members, &models.CommitteeMember{
			ColdCredHash: epochBoundaryBenchHash(0x60, uint64(c)+1),
			ExpiresEpoch: 100,
			AddedSlot:    1,
		})
	}
	require.NoError(tb, f.db.SetCommitteeMembers(context.Background(), members, nil))
	require.NoError(tb, f.db.SetCommitteeQuorum(context.Background(), big.NewRat(2, 3), 1, nil))

	for i := range shape.proposals {
		returnKey := epochBoundaryBenchHash(0x30, uint64(i)*997+1)
		returnAddr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey, lcommon.AddressNetworkMainnet,
			nil, returnKey,
		)
		require.NoError(tb, err)
		returnAddrBytes, err := returnAddr.Bytes()
		require.NoError(tb, err)
		var actionType lcommon.GovActionType
		var actionCbor []byte
		if i%4 == 0 {
			actionType = lcommon.GovActionTypeTreasuryWithdrawal
			actionCbor, err = cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
				Type: uint(lcommon.GovActionTypeTreasuryWithdrawal),
				Withdrawals: map[*lcommon.Address]uint64{
					&returnAddr: 1_000_000_000_000,
				},
			})
		} else {
			actionType = lcommon.GovActionTypeInfo
			actionCbor, err = cbor.Encode(&lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			})
		}
		require.NoError(tb, err)
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint64(txHash[24:], uint64(i)+1)
		txHash[0] = 0x70
		proposal := &models.GovernanceProposal{
			TxHash:        txHash,
			ActionIndex:   0,
			ActionType:    uint8(actionType),
			ProposedEpoch: epochBoundaryBenchEndedEpoch - uint64(i%3),
			ExpiresEpoch:  epochBoundaryBenchEndedEpoch + 6,
			AnchorURL:     "https://example.invalid/proposal",
			AnchorHash:    txHash,
			Deposit:       f.pparams.GovActionDeposit,
			ReturnAddress: returnAddrBytes,
			GovActionCbor: actionCbor,
			AddedSlot: epochBoundaryBenchStart(
				epochBoundaryBenchEndedEpoch,
			) + 100,
		}
		require.NoError(tb, f.db.SetGovernanceProposal(context.Background(), proposal, nil))
		vote := func(voterType uint8, cred []byte, choice uint8) {
			require.NoError(tb, f.db.SetGovernanceVote(context.Background(), &models.GovernanceVote{
				ProposalID:      proposal.ID,
				VoterType:       voterType,
				VoterCredential: cred,
				Vote:            choice,
				AddedSlot:       proposal.AddedSlot + 1,
			}, nil))
		}
		for v := range min(shape.drepVotes, shape.dreps) {
			r := (uint64(i)*131 + uint64(v)) % uint64(shape.dreps)
			vote(
				models.VoterTypeDRep,
				epochBoundaryBenchHash(0x40, r+1),
				uint8(splitmix64(r^uint64(i))%3),
			)
		}
		for v := range min(shape.spoVotes, shape.pools) {
			p := (uint64(i)*17 + uint64(v)) % uint64(shape.pools)
			vote(
				models.VoterTypeSPO,
				epochBoundaryBenchHash(0x10, p+1),
				uint8(splitmix64(p^uint64(i))%3),
			)
		}
		for c := range shape.ccMembers {
			vote(
				models.VoterTypeCC,
				epochBoundaryBenchHash(0x61, uint64(c)+1),
				models.VoteYes,
			)
		}
	}
}

// rollover runs the real boundary in one write transaction, exactly as the
// block pipeline does, and returns the time spent inside the transaction body
// and in its commit.
func (f *epochBoundaryBenchFixture) rollover(
	tb testing.TB,
) (time.Duration, time.Duration, []epochBoundaryPhase) {
	tb.Helper()
	f.phases.reset()
	start := time.Now()
	f.ls.fenceRewardPrecompute()
	var bodyDone time.Time
	txn := f.db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		_, err := f.ls.processEpochRollover(context.Background(),
			txn,
			f.epochs[epochBoundaryBenchEndedEpoch],
			eras.ConwayEraDesc,
			f.pparams,
			false,
		)
		bodyDone = time.Now()
		return err
	})
	require.NoError(tb, err)
	end := time.Now()
	return bodyDone.Sub(start), end.Sub(bodyDone), f.phases.snapshot()
}

// precomputeEvent is the epoch transition into the ended epoch: the event
// the reward precompute for the measured boundary is queued from.
func epochBoundaryBenchPrecomputeEvent() event.EpochTransitionEvent {
	return event.EpochTransitionEvent{
		PreviousEpoch: epochBoundaryBenchEndedEpoch - 1,
		NewEpoch:      epochBoundaryBenchEndedEpoch,
		BoundarySlot:  epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
		SnapshotSlot: epochBoundaryBenchStart(
			epochBoundaryBenchEndedEpoch,
		) - 1,
	}
}

// epochBoundaryDumpShape is small enough to seed in seconds and still covers
// the rounding and eligibility edges: margins 0 and 1, a zero-stake pool,
// owners, DRep, always-abstain and no-confidence delegators.
func epochBoundaryDumpShape() epochBoundaryBenchShape {
	shape := epochBoundaryBenchShape{
		pools:             23,
		delegators:        1_500,
		dreps:             17,
		utxosPerDelegator: 2,
		proposals:         6,
		drepVotes:         12,
		spoVotes:          9,
		ccMembers:         3,
	}
	if raw := os.Getenv("DINGO_BOUNDARY_DUMP_DELEGATORS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			shape.delegators = n
			shape.pools = max(shape.pools, n/400)
		}
	}
	return shape
}

// dumpEpochBoundaryState renders every table an epoch boundary writes, in a
// stable order and without surrogate keys, so the same boundary on two code
// versions can be compared byte for byte.
func dumpEpochBoundaryState(t *testing.T, raw *sql.DB) string {
	t.Helper()
	queries := []struct{ name, query string }{
		{"account", `SELECT credential_tag, hex(staking_key), reward, active,
    hex(pool), hex(drep), drep_type FROM account
ORDER BY credential_tag, staking_key`},
		{"account_reward_delta", `SELECT credential_tag, hex(staking_key),
    hex(tx_hash), amount, previous_reward, added_slot, withdrawal,
    post_snapshot FROM account_reward_delta
ORDER BY credential_tag, staking_key, tx_hash, added_slot, withdrawal`},
		{"reward_live_stake", `SELECT credential_tag, hex(staking_key),
    hex(pool_key_hash), utxo_stake, reward_stake, total_stake, registered,
    pool_delegation_slot, updated_slot, calculation_version
FROM reward_live_stake ORDER BY credential_tag, staking_key`},
		{"network_state", `SELECT slot, treasury, reserves FROM network_state
ORDER BY slot`},
		{"reward_ada_pots", `SELECT epoch, treasury, reserves, fees, rewards,
    captured_slot FROM reward_ada_pots ORDER BY epoch`},
		{"reward_pool_output", `SELECT epoch, hex(pool_key_hash),
    apparent_performance, optimal_reward, total_reward, leader_reward,
    member_reward_total, owner_stake, undistributed, unspendable,
    boundary_slot FROM reward_pool_output ORDER BY epoch, pool_key_hash`},
		{"reward_account_output", `SELECT epoch, credential_tag,
    hex(staking_key), hex(pool_key_hash), reward_type, amount, spendable,
    guarded, boundary_slot FROM reward_account_output
ORDER BY epoch, credential_tag, staking_key, pool_key_hash, reward_type`},
		{
			"pool_stake_snapshot",
			`SELECT epoch, snapshot_type, hex(pool_key_hash),
    total_stake, delegator_count, captured_slot, calculation_version,
    reward_account_auto_vote, reward_account_auto_vote_resolved
FROM pool_stake_snapshot ORDER BY epoch, snapshot_type, pool_key_hash`,
		},
		{"reward_snapshot", `SELECT epoch, snapshot_type, total_active_stake,
    total_pool_count, total_delegators, captured_slot, boundary_slot,
    hex(epoch_nonce), protocol_version, authoritative, calculation_version,
    excluded_active_stake FROM reward_snapshot
ORDER BY epoch, snapshot_type`},
		{"reward_pool_input", `SELECT epoch, hex(pool_key_hash), margin,
    hex(reward_account), pledge, delegated_stake, owner_stake, cost,
    delegator_count, captured_slot, boundary_slot FROM reward_pool_input
ORDER BY epoch, pool_key_hash`},
		{
			"reward_stake_input",
			`SELECT epoch, hex(pool_key_hash), credential_tag,
    hex(staking_key), stake, owner, registered, captured_slot, boundary_slot
FROM reward_stake_input
ORDER BY epoch, pool_key_hash, credential_tag, staking_key`,
		},
		{"epoch_summary", `SELECT epoch, total_active_stake, total_pool_count,
    total_delegators, hex(epoch_nonce), boundary_slot, snapshot_ready
FROM epoch_summary ORDER BY epoch`},
		{"governance_proposal", `SELECT hex(tx_hash), action_index,
    enacted_epoch, enacted_slot, ratified_epoch, ratified_slot, expired_epoch,
    expired_slot FROM governance_proposal ORDER BY tx_hash, action_index`},
		{"drep", `SELECT credential_tag, hex(credential), active,
    last_activity_epoch, expiry_epoch FROM drep
ORDER BY credential_tag, credential`},
		{"epoch", `SELECT epoch_id, start_slot, hex(nonce), hex(evolving_nonce),
    hex(candidate_nonce), era_id FROM epoch ORDER BY epoch_id`},
	}
	var sb strings.Builder
	for _, q := range queries {
		rows, err := raw.Query(q.query)
		require.NoError(t, err, q.name)
		cols, err := rows.Columns()
		require.NoError(t, err)
		fmt.Fprintf(&sb, "== %s\n", q.name)
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					v = string(b)
				}
				if i > 0 {
					sb.WriteString("|")
				}
				fmt.Fprintf(&sb, "%v", v)
			}
			sb.WriteString("\n")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return sb.String()
}

// dumpDRepVotingPower renders every DRep's voting power as governance reads it
// at the end of the boundary.
func dumpDRepVotingPower(t *testing.T, f *epochBoundaryBenchFixture) string {
	t.Helper()
	dreps, err := f.db.GetActiveDreps(context.Background(), nil)
	require.NoError(t, err)
	refs := make([]models.StakeCredentialRef, 0, len(dreps))
	for _, drep := range dreps {
		refs = append(refs, models.NewStakeCredentialRef(
			drep.CredentialTag, drep.Credential,
		))
	}
	powers, err := f.db.GetDRepVotingPowerBatch(context.Background(), refs, 0, nil)
	require.NoError(t, err)
	byType, err := f.db.GetDRepVotingPowerByType(context.Background(),
		[]uint64{
			models.DrepTypeAlwaysAbstain, models.DrepTypeAlwaysNoConfidence,
		}, 0, nil,
	)
	require.NoError(t, err)
	lines := make([]string, 0, len(powers)+2)
	for key, power := range powers {
		lines = append(lines, fmt.Sprintf("%x=%d", key, power))
	}
	sort.Strings(lines)
	lines = append(lines, fmt.Sprintf(
		"abstain=%d no_confidence=%d",
		byType[models.DrepTypeAlwaysAbstain],
		byType[models.DrepTypeAlwaysNoConfidence],
	))
	return strings.Join(lines, "\n") + "\n"
}

// deregisterEpochBoundaryDumpDelegators deregisters every 97th delegator
// during the ended epoch, after any precompute ran, the way a deregistration
// certificate does: the account, its live stake row and the certificate row.
// Their rewards become unspendable at the boundary.
func deregisterEpochBoundaryDumpDelegators(t *testing.T, raw *sql.DB) {
	t.Helper()
	shape := epochBoundaryDumpShape()
	for d := 0; d < shape.delegators; d += 97 {
		key := epochBoundaryBenchHash(0x30, uint64(d)+1)
		slot := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch) + 1_000 +
			uint64(d)
		for _, stmt := range []string{
			`UPDATE account SET active = 0, pool = NULL, added_slot = ?
WHERE credential_tag = 0 AND staking_key = ?`,
			`UPDATE reward_live_stake SET registered = 0, pool_key_hash = NULL,
    updated_slot = ? WHERE credential_tag = 0 AND staking_key = ?`,
			`INSERT INTO deregistration (added_slot, staking_key, credential_tag,
    amount) VALUES (?, ?, 0, '2000000')`,
		} {
			_, err := raw.Exec(stmt, slot, key)
			require.NoError(t, err)
		}
	}
}

// TestEpochBoundaryDumpForDifferential runs one boundary on the dump fixture
// for each precompute state and writes the resulting state to
// $DINGO_BOUNDARY_DUMP_DIR, so the same test on two code versions produces
// files to diff. It is skipped unless that directory is set.
func TestEpochBoundaryDumpForDifferential(t *testing.T) {
	t.Parallel()
	dir := os.Getenv("DINGO_BOUNDARY_DUMP_DIR")
	if dir == "" {
		t.Skip("set DINGO_BOUNDARY_DUMP_DIR to write boundary dumps")
	}
	for _, state := range []string{"complete", "partial", "missing"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
			switch state {
			case "complete":
				require.NoError(
					t,
					f.ls.precomputeStakeRewardsAfterEpochTransition(
						epochBoundaryBenchPrecomputeEvent(),
					),
				)
			case "partial":
				epochBoundaryBenchPartialPrecomputeT(t, f)
			}
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			defer raw.Close()
			deregisterEpochBoundaryDumpDelegators(t, raw)
			f.rollover(t)
			f.ls.waitEpochBoundaryBenchBackground()
			// DRep power is read from the derived balances; the tables are
			// dumped with every credit folded into its account, the shape an
			// eager boundary writes.
			power := dumpDRepVotingPower(t, f)
			settleRewardCredits(t, f.ls)
			dump := dumpEpochBoundaryState(t, raw) + "== drep_power\n" +
				power
			require.NoError(t, os.WriteFile(
				filepath.Join(dir, "boundary-"+state+".txt"),
				[]byte(dump), 0o644,
			))
		})
	}
}

func smallEpochBoundaryBenchShape() epochBoundaryBenchShape {
	return epochBoundaryBenchShape{
		pools:             6,
		delegators:        60,
		dreps:             4,
		utxosPerDelegator: 1,
		proposals:         2,
		drepVotes:         4,
		spoVotes:          3,
		ccMembers:         3,
	}
}

// TestEpochRolloverMarkSnapshotIncludesSameBoundaryRewards pins the SNAP
// ordering contract: the mark snapshot captured at a boundary includes the
// reward round that boundary applies, so every credited delegator's frozen
// stake is its pre-boundary stake plus its reward.
func TestEpochRolloverMarkSnapshotIncludesSameBoundaryRewards(t *testing.T) {
	t.Parallel()
	f := newEpochBoundaryBenchFixture(t, smallEpochBoundaryBenchShape(), "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	before, err := f.db.Metadata().GetRewardStakeInputs(
		epochBoundaryBenchEndedEpoch, nil,
	)
	require.NoError(t, err)
	outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
	require.NoError(t, err)
	require.NotEmpty(t, outputs)

	f.rollover(t)
	f.ls.waitEpochBoundaryBenchBackground()

	after, err := f.db.Metadata().GetRewardStakeInputs(
		epochBoundaryBenchEndedEpoch+1, nil,
	)
	require.NoError(t, err)
	credit := make(map[string]uint64)
	for _, output := range outputs {
		if output.Spendable && !output.Guarded {
			credit[string(output.StakingKey)] += uint64(output.Amount)
		}
	}
	require.NotEmpty(t, credit)
	stakeAfter := make(map[string]uint64, len(after))
	for _, input := range after {
		stakeAfter[string(input.StakingKey)] = uint64(input.Stake)
	}
	for _, input := range before {
		key := string(input.StakingKey)
		require.Equal(
			t, uint64(input.Stake)+credit[key], stakeAfter[key],
			"mark stake of %x must include the reward credited at the"+
				" same boundary", input.StakingKey,
		)
	}
}

// TestBoundaryRechecksRegistrationChangedAfterPrecompute pins the boundary's
// eligibility recheck: a delegator deregistered after the precompute ran is
// not credited, and exactly its reward moves to the treasury, although the
// precompute recorded its output as spendable.
func TestBoundaryRechecksRegistrationChangedAfterPrecompute(t *testing.T) {
	t.Parallel()
	run := func(deregister bool) (*epochBoundaryBenchFixture, uint64) {
		f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
		require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
			epochBoundaryBenchPrecomputeEvent(),
		))
		if deregister {
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			deregisterEpochBoundaryDumpDelegators(t, raw)
			require.NoError(t, raw.Close())
		}
		f.rollover(t)
		f.ls.waitEpochBoundaryBenchBackground()
		state, err := f.db.Metadata().GetNetworkState(nil)
		require.NoError(t, err)
		return f, uint64(state.Treasury)
	}
	_, treasuryKept := run(false)
	f, treasuryDeregistered := run(true)

	deregistered := make(map[string]bool)
	for d := 0; d < epochBoundaryDumpShape().delegators; d += 97 {
		deregistered[string(epochBoundaryBenchHash(0x30, uint64(d)+1))] = true
	}
	outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
	require.NoError(t, err)
	var moved uint64
	for _, output := range outputs {
		if !deregistered[string(output.StakingKey)] {
			continue
		}
		require.False(
			t, output.Spendable,
			"a delegator deregistered before the boundary is unspendable",
		)
		moved += uint64(output.Amount)
	}
	require.NotZero(t, moved, "fixture must deregister a rewarded delegator")
	for key := range deregistered {
		account, err := f.db.GetAccountByCredential(context.Background(), 0, []byte(key), true, nil)
		require.NoError(t, err)
		require.Equal(
			t, stakeRewardSeedReward(key), uint64(account.Reward),
			"a deregistered delegator is not credited",
		)
	}
	require.Equal(
		t, treasuryKept+moved, treasuryDeregistered,
		"exactly the deregistered delegators' rewards move to the treasury",
	)
}

// stakeRewardSeedReward is the reward balance the fixture seeds for a
// delegator key.
func stakeRewardSeedReward(key string) uint64 {
	index := binary.BigEndian.Uint64([]byte(key)[20:]) - 1
	return epochBoundaryBenchStake(index) / 200
}

func TestMithrilImportProvidesPreview1398RewardPParams(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	seedEligiblePreviewGoRewardBasis(t, db)

	currentParams := mithrilRewardConwayPParams()
	previousParams := *currentParams
	previousParams.MinFeeA++
	currentData, err := cbor.Encode(currentParams)
	require.NoError(t, err)
	previousData, err := cbor.Encode(&previousParams)
	require.NoError(t, err)

	eraBounds := make([]ledgerstate.EraBound, ledgerstate.EraConway+1)
	nonce := make([]byte, 32)
	require.NoError(t, ledgerstate.ImportLedgerState(
		context.Background(),
		ledgerstate.ImportConfig{
			Database: db,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			State: &ledgerstate.RawLedgerState{
				PParamsData:         currentData,
				PrevPParamsData:     previousData,
				Epoch:               1397,
				EraIndex:            ledgerstate.EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Reserves:            100_000_000,
				Tip: &ledgerstate.SnapshotTip{
					Slot:      1_397_799,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		},
	))
	require.NoError(t, db.Metadata().SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        1397,
			Reserves:     100_000_000,
			CapturedSlot: 1_397_799,
		},
		nil,
	))
	prefilterSlot, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 1397)
	require.NoError(t, err)
	require.LessOrEqual(t, prefilterSlot, uint64(1_397_799))

	epochs, ok := stakeRewardEpochsForNewEpoch(1398)
	require.True(t, ok)
	require.Equal(t, uint64(1395), epochs.snapshot)
	require.Equal(t, uint64(1396), epochs.performance)
	require.Equal(t, uint64(1397), epochs.pots)

	currentEpoch, err := db.Metadata().GetEpoch(1397, nil)
	require.NoError(t, err)
	require.NotNil(t, currentEpoch)
	ls.currentEpoch = *currentEpoch
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = currentParams

	var rollover *EpochRolloverResult
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		rollover, rolloverErr = ls.processEpochRollover(context.Background(),
			txn,
			*currentEpoch,
			eras.ConwayEraDesc,
			currentParams,
			false,
		)
		return rolloverErr
	}))
	require.NotNil(t, rollover)
	require.Equal(t, uint64(1398), rollover.NewCurrentEpoch.EpochId)
	poolOutputs, err := db.Metadata().GetRewardPoolOutputs(1395, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Positive(t, uint64(poolOutputs[0].TotalReward))
	accountOutputs, err := db.Metadata().GetRewardAccountOutputs(1395, nil)
	require.NoError(t, err)
	require.NotEmpty(t, accountOutputs)
	var credited uint64
	for _, output := range accountOutputs {
		credited += uint64(output.Amount)
	}
	require.Positive(t, credited)
}

func seedEligiblePreviewGoRewardBasis(
	t *testing.T,
	db *database.Database,
) {
	t.Helper()
	const (
		rewardSnapshotEpoch = uint64(1395)
		capturedSlot        = uint64(1_397_799)
		boundarySlot        = uint64(1_395_000)
	)
	poolKey := rewardCalcHash(0x71)
	rewardAccount := rewardCalcHash(0x72)
	member := rewardCalcHash(0x73)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(context.Background(),
			poolID,
			i+1,
			1_396_640+i,
			nil,
		))
	}
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     capturedSlot,
		BoundarySlot:     boundarySlot,
		ProtocolVersion:  10,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs(
		[]*models.RewardPoolInput{{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               capturedSlot,
			BoundarySlot:               boundarySlot,
		}},
		nil,
	))
	require.NoError(t, meta.SaveRewardStakeInputs(
		[]*models.RewardStakeInput{
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				Stake:         500,
				Owner:         true,
				Registered:    true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				CredentialTag: 0,
				StakingKey:    member,
				Stake:         500,
				Registered:    true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
		},
		nil,
	))

	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(t, db.ImportPool(context.Background(), nil, &pool, &models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolKey,
		AddedSlot:   boundarySlot,
	}))
	for _, account := range [][]byte{rewardAccount, member} {
		require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: account,
			Pool:       poolKey,
			Active:     true,
		}))
	}
	rewardCalcSeedStakeCert(
		t,
		db,
		1,
		rewardAccount,
		0,
		boundarySlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		2,
		member,
		0,
		boundarySlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
}

func mithrilRewardConwayPParams() *conway.ConwayProtocolParameters {
	params := donationTestConwayPParams(10)
	params.MinFeeA = 44
	params.NOpt = 500
	params.A0 = &cbor.Rat{Rat: big.NewRat(3, 10)}
	params.Rho = &cbor.Rat{Rat: big.NewRat(3, 1000)}
	params.Tau = &cbor.Rat{Rat: big.NewRat(1, 5)}
	return params
}

// A bootstrapped node applies no block at or below its trust anchor, so every
// slot of an epoch that ended below the anchor is uncountable. The blocks were
// nonetheless minted, and the reference credits their rewards, so the counts
// have to come from the snapshot's own BlocksMade rather than from a floor of
// zero.
func TestRewardBlockCountsMergesImportedCountsAcrossTheAnchor(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
		anchorSlot       = uint64(150)
	)
	poolKey := rewardCalcHash(0x81)
	otherPoolKey := rewardCalcHash(0x82)
	retiredPoolKey := rewardCalcHash(0x83)
	var poolID, otherPoolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	copy(otherPoolID[:], otherPoolKey)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(anchorSlot, 10),
		nil,
	))
	// Blocks this node applied itself, all strictly above the anchor.
	for _, slot := range []uint64{160, 170} {
		require.NoError(t, db.UpdatePoolOpCertSequence(context.Background(), poolID, slot, slot, nil))
	}
	require.NoError(t, db.UpdatePoolOpCertSequence(context.Background(), otherPoolID, 180, 180, nil))
	// Blocks the snapshot reports for the same epoch, minted at or below the
	// anchor. retiredPoolKey is not one of the pools asked about, but its
	// blocks still belong to the epoch total that every pool's beta divides by.
	require.NoError(t, meta.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    poolKey,
				BlocksProduced: 5,
				CapturedSlot:   anchorSlot,
			},
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    otherPoolKey,
				BlocksProduced: 3,
				CapturedSlot:   anchorSlot,
			},
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    retiredPoolKey,
				BlocksProduced: 2,
				CapturedSlot:   anchorSlot,
			},
		},
		nil,
	))
	require.NoError(t, meta.SaveImportedEpochBlockTotal(
		performanceEpoch,
		5+3+2,
		anchorSlot,
		nil,
	))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{
			{PoolKeyHash: poolKey},
			{PoolKeyHash: otherPoolKey},
		},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	assert.Equal(t, uint64(2+5), counts[string(poolKey)])
	assert.Equal(t, uint64(1+3), counts[string(otherPoolKey)])
	assert.Equal(t, uint64(3+10), total)
}

// Zero blocks and no block history are different answers. The first is a real
// epoch outcome; the second is an epoch this node cannot count, and reading it
// as zero gives every pool zero performance and credits every delegator
// nothing while reporting a completed round.
func TestRewardBlockCountsUnknownWhenAnchorHidesTheEpoch(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
	)
	poolKey := rewardCalcHash(0x84)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	// The anchor sits past the end of the epoch, so none of it is observable.
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(epochStartSlot+epochLength, 10),
		nil,
	))

	_, _, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{{PoolKeyHash: poolKey}},
		nil,
	)
	require.NoError(t, err)
	require.False(
		t,
		known,
		"an epoch that ended below the anchor with no imported counts has "+
			"unknown block counts, not zero",
	)
}

func TestRewardBlockCountsAcceptsObservableZero(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
	)
	poolKey := rewardCalcHash(0x85)
	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{{PoolKeyHash: poolKey}},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	assert.Zero(t, counts[string(poolKey)])
	assert.Zero(t, total)
}

// The imported counts are consulted only for an epoch the anchor actually
// covers. A node that never bootstrapped counts its own blocks exactly as it
// did before.
func TestRewardBlockCountsIgnoresImportedCountsAboveTheAnchor(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
	)
	poolKey := rewardCalcHash(0x85)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(epochStartSlot-1, 10),
		nil,
	))
	require.NoError(t, db.UpdatePoolOpCertSequence(context.Background(), poolID, 1, 120, nil))
	require.NoError(t, meta.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    poolKey,
				BlocksProduced: 7,
				CapturedSlot:   epochStartSlot - 1,
			},
		},
		nil,
	))
	require.NoError(t, meta.SaveImportedEpochBlockTotal(
		performanceEpoch,
		7,
		epochStartSlot-1,
		nil,
	))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{{PoolKeyHash: poolKey}},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	assert.Equal(t, uint64(1), counts[string(poolKey)])
	assert.Equal(t, uint64(1), total)
}

// The round-level consequence. seedRewardPrecomputeTimingState places ten
// blocks for the single pool inside performance epoch 2; putting the anchor
// past that epoch removes every one of them from the node's reach. The
// authoritative boundary must reject that incomplete basis.
func TestStakeRewardRoundRejectsHiddenBlockCounts(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	var logs bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	require.NoError(t, db.Metadata().SetSyncState(
		mithrilLedgerSlotSyncKey,
		"199",
		nil,
	))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)
	require.False(
		t,
		ok,
		"a round whose performance epoch cannot be counted must be rejected",
	)
	require.Nil(t, app)
	assert.Contains(
		t,
		logs.String(),
		"no block counts for the performance epoch",
	)
}

// A recorded anchor sits at or above slot 0 and so covers epoch 0, the
// performance epoch of both bootstrap rounds. Those rounds must still run:
// they distribute no pool or account rewards but do move the ADA pots, and
// declining one would leave treasury and reserves at their genesis values for
// the life of the chain. They are safe because epoch 0's mark snapshot holds
// no pools, and an empty pool set is answered before the anchor is consulted;
// the reference agrees that zero rather than unknown is the answer there,
// since NEWEPOCH's initialRules construct the genesis state with BlocksMade
// Map.empty. This pins that, rather than proving a fix.
func TestBootstrapStakeRewardRoundSurvivesAMithrilAnchor(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.ShelleyEraDesc.Id, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Reserves:     100_000_000,
		CapturedSlot: 0,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:           0,
		SnapshotType:    "mark",
		CapturedSlot:    0,
		BoundarySlot:    0,
		ProtocolVersion: 7,
	}, nil))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		"50",
		nil,
	))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(txn, 1, 100, 100, true)
	require.NoError(t, err)
	require.True(
		t,
		ok,
		"an anchor covers epoch 0 by construction; the bootstrap round still "+
			"has to move the pots",
	)
	require.NotNil(t, app)
	assert.True(t, app.epochs.bootstrap)
	assert.Empty(t, app.poolOutputs)
	assert.Empty(t, app.accountOutputs)
}

// TestStakeRewardEpochsForInitialApplication pins the two bootstrap rounds.
// The round into epoch 1 reads genesis pots and empty previous block counts;
// the round into epoch 2 reads epoch 1's pots and epoch 0's blocks. Both have
// genesis Go distributions. Byron-prefix networks are suppressed by
// applyStakeRewards' Byron performance-epoch guard, not by this helper.
func TestStakeRewardEpochsForInitialApplication(t *testing.T) {
	t.Parallel()

	_, ok := stakeRewardEpochsForApplication(0)
	require.False(t, ok, "epoch 0 is not a boundary and applies no rewards")

	epochs, ok := stakeRewardEpochsForApplication(1)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 0,
		pots:        0,
		bootstrap:   true,
	}, epochs)

	epochs, ok = stakeRewardEpochsForApplication(2)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 0,
		pots:        1,
		bootstrap:   true,
	}, epochs)

	epochs, ok = stakeRewardEpochsForApplication(3)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 1,
		pots:        2,
	}, epochs)
}

func TestBootstrapStakeRewardsRejectStalePrecompute(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	require.NoError(t, db.Metadata().SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:   1,
		Rewards: types.Uint64(1_000),
	}, nil))

	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(txn, 2, 200)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)

	app, ok, err = ls.precomputeStakeRewardsCalculate(txn, 2, 100, 200)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

// The first RUPD reads an empty nesBprev, not epoch 0's nesBcur.
// With d=0 this gives eta=0; the same 180 blocks enter the next update,
// giving eta=180/(500*0.4)=0.9. Fees collected in epoch 0 enter that update
// too. These are the reference devnet inputs and pots.
func TestApplyStakeRewardsPrototypeGenesisPerformance(t *testing.T) {
	t.Parallel()
	ls, db := newRewardCalculationTestLedger(t)
	ls.config.EnableDijkstra = true
	zero := uint64(0)
	ls.config.CardanoNodeConfig.TestShelleyHardForkAtEpoch = &zero
	ls.currentEra = eras.ConwayEraDesc
	require.NoError(t, ls.config.CardanoNodeConfig.
		LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.4,
		"epochLength": 500,
		"maxLovelaceSupply": 6000000000000,
		"securityParam": 40,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	pp := mockledger.NewMockConwayProtocolParams()
	pp.NOpt = 150
	pp.A0 = rewardCalcRat(3, 10)
	pp.Rho = rewardCalcRat(3, 1_000)
	pp.Tau = rewardCalcRat(1, 5)
	pp.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 10}
	encoded, err := cbor.Encode(&pp)
	require.NoError(t, err)
	meta := db.Metadata()
	for epoch := range uint64(3) {
		require.NoError(t, meta.SetEpoch(
			epoch*500, epoch, nil, nil, nil, nil,
			eras.ConwayEraDesc.Id, 1, 500, nil,
		))
		require.NoError(t, db.SetPParams(
			encoded, epoch*500, epoch, eras.ConwayEraDesc.Id, nil,
		))
	}
	require.NoError(t, meta.SetNetworkState(0, 2_000_000_000_000, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 0, Reserves: 2_000_000_000_000,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: 0, SnapshotType: "mark", ProtocolVersion: 10,
		TotalActiveStake: 2_000_000_000_000,
		TotalPoolCount:   2, TotalDelegators: 2,
	}, nil))
	for _, key := range []byte{0x11, 0x22} {
		poolKey := rewardCalcHash(key)
		poolID := seedLiveStakeFixture(
			t, db, poolKey, bytes.Repeat([]byte{key}, 32),
			1_000_000_000_000, 0,
		)
		require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
			Epoch: 0, PoolKeyHash: poolKey, RewardAccount: poolKey,
			Margin:         &types.Rat{Rat: big.NewRat(0, 1)},
			DelegatedStake: 1_000_000_000_000, DelegatorCount: 1,
		}}, nil))
		require.NoError(
			t,
			meta.SaveRewardStakeInputs([]*models.RewardStakeInput{{
				Epoch: 0, PoolKeyHash: poolKey, StakingKey: poolKey,
				Stake: 1_000_000_000_000, Registered: true,
			}}, nil),
		)
		for i := range uint64(90) {
			require.NoError(t, db.UpdatePoolOpCertSequence(context.Background(),
				poolID, i+1, 1+2*i+uint64(key), nil,
			))
		}
	}
	_, err = rewardCalcSQLDB(t, db).Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, 60, 7, '400000', '0', '0', 0, TRUE)`,
		[]byte("genesis-performance-tx"), []byte("genesis-performance-block"))
	require.NoError(t, err)

	for _, tc := range []struct {
		epoch    uint64
		treasury uint64
		reserves uint64
		fraction *big.Rat
	}{
		{1, 0, 2_000_000_000_000, big.NewRat(1, 4)},
		{2, 1_080_080_000, 1_998_876_009_026, big.NewRat(1_000_022_155_487, 4_001_123_990_974)},
	} {
		if tc.epoch == 2 {
			readTxn := db.Transaction(t.Context(), false)
			app, ok, err := ls.calculateStakeRewardApplication(
				readTxn,
				2,
				1000,
				1000,
				true,
			)
			require.NoError(t, readTxn.Rollback())
			require.NoError(t, err)
			require.True(t, ok)
			require.NotEmpty(
				t,
				app.accountOutputs,
				"genesis staking must receive rewards from epoch 0's blocks",
			)
		}

		boundary := tc.epoch * 500
		ended, err := meta.GetEpoch(tc.epoch-1, nil)
		require.NoError(t, err)
		require.NotNil(t, ended)
		txn := db.Transaction(context.Background(), true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			if err := ls.applyStakeRewards(context.Background(), txn, tc.epoch, boundary); err != nil {
				return err
			}
			return ls.saveRewardAdaPotsForEpoch(txn, tc.epoch, *ended, boundary)
		}))
		state, err := meta.GetNetworkState(nil)
		require.NoError(t, err)
		require.NotNil(t, state)
		require.Equal(t, tc.treasury, uint64(state.Treasury),
			"treasury at boundary into epoch %d", tc.epoch)
		require.Equal(t, tc.reserves, uint64(state.Reserves),
			"reserves at boundary into epoch %d", tc.epoch)
		pots, err := meta.GetRewardAdaPots(tc.epoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, state.Treasury, pots.Treasury)
		require.Equal(t, state.Reserves, pots.Reserves)
		if tc.epoch == 1 {
			require.Equal(t, uint64(400_000), uint64(pots.Fees))
		}

		hash := bytes.Repeat([]byte{byte(tc.epoch)}, 32)
		seedBlockAtSlot(t, ls, boundary, hash)
		require.NoError(t, db.SetTip(ochainsync.Tip{
			Point: ocommon.NewPoint(boundary, hash),
		}, nil))
		result, err := ls.Query(t.Context(), stakeDistributionQuery(), QueryPoint{})
		require.NoError(t, err)
		dist := decodeStakeDistributionResult(t, result)
		require.Len(t, dist.Results, 2)
		for _, entry := range dist.Results {
			require.Equal(t, tc.fraction, entry.StakeFraction.Rat,
				"stake fraction at boundary into epoch %d", tc.epoch)
		}
	}
}

// A Mithril bootstrap anchored mid-epoch seeds the imported epoch's own
// RewardAdaPots row with ImportedEpochFees (the fees collected up to and
// including the anchor block) and a CapturedSlot at the anchor. The node's
// locally stored transactions for that epoch only cover slots after the
// anchor -- plus, once the historical backfill has run, slots at or
// before it too. saveRewardAdaPotsForEpoch must sum the local fees strictly
// after the anchor and add the imported amount, not sum the whole epoch:
// summing the whole epoch either silently drops the pre-anchor fees or
// double-counts them once backfill has stored
// pre-anchor transactions locally.
func TestSaveRewardAdaPotsForEpochUsesImportedPreAnchorFees(t *testing.T) {
	t.Parallel()
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		endedEpoch           = uint64(5)
		epochStartSlot       = uint64(1000)
		epochLengthInSlots   = uint(100) // slots [1000, 1099]
		anchorSlot           = uint64(1050)
		importedPreAnchor    = uint64(1_000_000)
		postAnchorFee        = uint64(500_000)
		preAnchorBackfillFee = uint64(300_000)
		newEpochBoundarySlot = uint64(1100)
	)

	// Simulates seedImportedRewardBasis's write for the anchor epoch: the
	// pots row this epoch's own boundary would have produced, had the node
	// been running, carrying the pre-anchor fee pot the import derived from
	// State.Fees - snapshots.Fee.
	importedFees := types.Uint64(importedPreAnchor)
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:             endedEpoch,
		CapturedSlot:      anchorSlot,
		ImportedEpochFees: &importedFees,
	}, nil))

	// A transaction at the anchor slot itself: excluded, because the
	// imported amount already accounts for fees up to and including the
	// anchor block. Sum range is (CapturedSlot, epochEnd].
	rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
		[]byte("pre-anchor-backfill-tx"), []byte("pre-anchor-block"),
		anchorSlot, strconv.FormatUint(preAnchorBackfillFee, 10),
	)
	// A transaction after the anchor: included.
	rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (2, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
		[]byte("post-anchor-tx"), []byte("post-anchor-block"),
		anchorSlot+30, strconv.FormatUint(postAnchorFee, 10),
	)

	ended := models.Epoch{
		EpochId:       endedEpoch,
		StartSlot:     epochStartSlot,
		LengthInSlots: epochLengthInSlots,
	}
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.saveRewardAdaPotsForEpoch(
			txn, endedEpoch+1, ended, newEpochBoundarySlot,
		)
	}))

	pots, err := meta.GetRewardAdaPots(endedEpoch+1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(
		t,
		importedPreAnchor+postAnchorFee,
		uint64(pots.Fees),
		"fees for the epoch after an imported anchor epoch must be the "+
			"imported pre-anchor amount plus only the post-anchor local sum",
	)
}

// The imported pots row's CapturedSlot is the anchor block's slot, which can
// be any slot of its epoch, including the first and the last. The anchor
// block's own fees are part of ImportedEpochFees at both ends, so the local
// sum must exclude the anchor slot and still add the imported amount.
func TestSaveRewardAdaPotsForEpochImportedAnchorAtEpochEdges(t *testing.T) {
	t.Parallel()
	const (
		endedEpoch         = uint64(5)
		epochStartSlot     = uint64(1000)
		epochLengthInSlots = uint(100) // slots [1000, 1099]
		epochEndSlot       = uint64(1099)
		importedPreAnchor  = uint64(1_000_000)
		anchorBlockFee     = uint64(300_000)
		laterFee           = uint64(500_000)
	)
	tests := []struct {
		name       string
		anchorSlot uint64
		laterSlot  uint64
		want       uint64
	}{
		{
			name:       "anchor at first slot",
			anchorSlot: epochStartSlot,
			laterSlot:  epochStartSlot + 1,
			want:       importedPreAnchor + laterFee,
		},
		{
			name:       "anchor at last slot",
			anchorSlot: epochEndSlot,
			want:       importedPreAnchor,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, db := newRewardCalculationTestLedger(t)
			meta := db.Metadata()
			importedFees := types.Uint64(importedPreAnchor)
			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:             endedEpoch,
				CapturedSlot:      tc.anchorSlot,
				ImportedEpochFees: &importedFees,
			}, nil))
			// A backfilled copy of the anchor block's transaction, already
			// counted in ImportedEpochFees.
			rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
				[]byte("anchor-tx"), []byte("anchor-block"),
				tc.anchorSlot, strconv.FormatUint(anchorBlockFee, 10),
			)
			if tc.laterSlot != 0 {
				rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (2, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
					[]byte("later-tx"), []byte("later-block"),
					tc.laterSlot, strconv.FormatUint(laterFee, 10),
				)
			}
			ended := models.Epoch{
				EpochId:       endedEpoch,
				StartSlot:     epochStartSlot,
				LengthInSlots: epochLengthInSlots,
			}
			txn := db.Transaction(context.Background(), true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return ls.saveRewardAdaPotsForEpoch(
					txn, endedEpoch+1, ended, epochEndSlot+1,
				)
			}))
			pots, err := meta.GetRewardAdaPots(endedEpoch+1, nil)
			require.NoError(t, err)
			require.NotNil(t, pots)
			require.Equal(t, tc.want, uint64(pots.Fees))
		})
	}
}

// TestRewardParametersDecentralizationIsZeroWhenCalculatedInBabbage pins the
// d that startStep reads across the Alonzo to Babbage boundary. The reward
// update runs in the epoch after the performance epoch, in that epoch's era.
// Babbage's PParams has no d field (ppDG = to (const minBound)), and the
// translated prevPParams read back as 0, so the round for the last Alonzo
// epoch uses d = 0 even though the Alonzo parameters held d = 7/10. Reading
// d from the performance epoch's Alonzo parameters overstates eta's
// expectedBlocks denominator reduction and inflates every reward of the
// round (Prime Mainnet performance epoch 39).
//
// Block counts are the exception: BBODY accumulated the performance epoch's
// BlocksMade under that epoch's curPParams, so incrBlocks skipped overlay
// slots with the Alonzo d. The d returned for block counting must stay the
// performance epoch's.
func TestRewardParametersDecentralizationIsZeroWhenCalculatedInBabbage(
	t *testing.T,
) {
	t.Parallel()

	const (
		performanceEpoch = uint64(2)
		potsEpoch        = uint64(3)
	)
	tests := []struct {
		name         string
		calcEra      uint
		expectedDRat *big.Rat
	}{
		{
			name:         "alonzo calculation keeps the performance epoch d",
			calcEra:      eras.AlonzoEraDesc.Id,
			expectedDRat: big.NewRat(7, 10),
		},
		{
			name:         "babbage calculation reads d as zero",
			calcEra:      eras.BabbageEraDesc.Id,
			expectedDRat: big.NewRat(0, 1),
		},
		{
			name:         "conway calculation reads d as zero",
			calcEra:      eras.ConwayEraDesc.Id,
			expectedDRat: big.NewRat(0, 1),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls, db := newRewardCalculationTestLedger(t)
			meta := db.Metadata()
			pparams := &alonzo.AlonzoProtocolParameters{
				NOpt:             10,
				A0:               rewardCalcRat(0, 1),
				Rho:              rewardCalcRat(1, 100),
				Tau:              rewardCalcRat(1, 5),
				Decentralization: rewardCalcRat(7, 10),
				ProtocolMajor:    6,
			}
			pparamsCbor, err := cbor.Encode(pparams)
			require.NoError(t, err)
			require.NoError(t, meta.SetEpoch(
				100, performanceEpoch, nil, nil, nil, nil,
				eras.AlonzoEraDesc.Id, 1, 100, nil,
			))
			require.NoError(t, meta.SetEpoch(
				200, potsEpoch, nil, nil, nil, nil,
				tc.calcEra, 1, 1_000, nil,
			))
			require.NoError(t, db.SetPParams(
				pparamsCbor, 100, performanceEpoch,
				eras.AlonzoEraDesc.Id, nil,
			))

			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Rollback() }()
			_, params, performanceD, err := ls.rewardParameters(
				txn,
				performanceEpoch,
				potsEpoch,
				&models.RewardAdaPots{Reserves: 100_000_000},
			)
			require.NoError(t, err)
			require.Zero(t, tc.expectedDRat.Cmp(params.Decentralization),
				"d = %s, want %s", params.Decentralization, tc.expectedDRat)
			require.Zero(t, big.NewRat(7, 10).Cmp(performanceD),
				"block-count d = %s, want 7/10", performanceD)
			require.Equal(t, big.NewRat(1, 5), params.TreasuryExpansion,
				"tau still comes from the performance epoch")
		})
	}
}

func TestRewardPrecomputeCoalescesEpochTransitionBurst(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	enqueued := make(chan uint64)
	var (
		processedMu sync.Mutex
		processed   []uint64
	)
	precompute := func(evt event.EpochTransitionEvent) error {
		if evt.NewEpoch == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		processedMu.Lock()
		processed = append(processed, evt.NewEpoch)
		processedMu.Unlock()
		return nil
	}
	eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		func(evt event.Event) {
			ls.handleRewardPrecomputeEpochTransitionWith(evt, precompute)
			epochEvt := evt.Data.(event.EpochTransitionEvent)
			enqueued <- epochEvt.NewEpoch
		},
	)

	eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{NewEpoch: 1, EpochNonce: []byte{1}},
		),
	)
	require.Equal(
		t,
		uint64(1),
		testutil.RequireReceive(
			t,
			enqueued,
			testutil.AsyncWait,
			"reward precompute callback did not enqueue first epoch",
		),
	)
	testutil.RequireReceive(
		t,
		firstStarted,
		testutil.AsyncWait,
		"first reward precompute did not start",
	)

	// Deliver a sequence longer than the EventBus default buffer's total capacity
	// while the first simulated calculation remains blocked. Waiting for each
	// callback isolates the behavior under test: callback delivery stays
	// independent of reward calculation, and the ledger retains only the newest
	// pending epoch.
	latestEpoch := uint64(event.DefaultSubscriberBuffer + 100)
	for epoch := uint64(2); epoch <= latestEpoch; epoch++ {
		eventBus.Publish(
			event.EpochTransitionEventType,
			event.NewEvent(
				event.EpochTransitionEventType,
				event.EpochTransitionEvent{
					NewEpoch:   epoch,
					EpochNonce: []byte{byte(epoch)},
				},
			),
		)
		require.Equal(
			t,
			epoch,
			testutil.RequireReceive(
				t,
				enqueued,
				testutil.AsyncWait,
				"reward precompute callback did not enqueue epoch",
			),
		)
	}
	close(releaseFirst)

	done := make(chan struct{})
	go func() {
		ls.rewardPrecomputeWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"coalesced reward precompute did not finish",
	)

	processedMu.Lock()
	defer processedMu.Unlock()
	require.Equal(t, []uint64{1, latestEpoch}, processed)
}

func TestRewardPrecomputeContinuesAfterPanic(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var processed []uint64
	precompute := func(evt event.EpochTransitionEvent) error {
		if evt.NewEpoch == 1 {
			close(firstStarted)
			<-releaseFirst
			panic("broken reward input")
		}
		processed = append(processed, evt.NewEpoch)
		return nil
	}

	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 1, EpochNonce: []byte{1}},
		precompute,
	)
	testutil.RequireReceive(
		t,
		firstStarted,
		testutil.AsyncWait,
		"panicking reward precompute did not start",
	)
	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 2, EpochNonce: []byte{2}},
		precompute,
	)
	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 3, EpochNonce: []byte{3}},
		precompute,
	)
	close(releaseFirst)

	done := make(chan struct{})
	go func() {
		ls.rewardPrecomputeWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"reward precompute worker stopped after panic",
	)
	require.Equal(t, []uint64{3}, processed)
}

func TestRewardPrecomputeRetryRejectsAbandonedGeneration(t *testing.T) {
	t.Parallel()

	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback active %t", active), func(t *testing.T) {
			t.Parallel()
			ls := &LedgerState{rewardPrecomputeRunning: true}
			if active {
				ls.rewardInputRollbackActive.Add(1)
			} else {
				ls.rewardInputGeneration.Add(2)
			}
			ls.deferStakeRewardPrecompute(4, 100, 0)
			require.Nil(t, ls.rewardPrecomputeRetry,
				"an old calculation must not reinstall an abandoned retry")

			ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
				epochEvent: event.EpochTransitionEvent{NewEpoch: 3},
				cutoffSlot: 100,
			}
			ls.maybeQueueStakeRewardPrecomputeRetry(100)
			require.Nil(t, ls.rewardPrecomputePending,
				"an abandoned retry must not replace the current pending epoch")
			require.Nil(t, ls.rewardPrecomputeRetry)
		})
	}
}

// The pre-Babbage prefilter reads account registration at the RUPD slot, so a
// rollback across that slot can change which delegators are paid. The
// replacement must be derived from the surviving certificate history and match
// the authoritative boundary calculation exactly.
func TestRollbackRewardPrecomputeDropsAbandonedPrefilterHistory(
	t *testing.T,
) {
	t.Parallel()

	seed, db := seedRewardPrecomputeTimingState(t, 6)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, db.SetEpoch(
		200, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 1_000, nil,
	))
	cfg := seed.config
	cfg.Database = db
	cfg.ChainManager = cm
	ls, err := NewLedgerState(cfg)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	epoch, err := db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
	require.NoError(t, err)
	member := rewardCalcHash(0x6a)
	// member is registered before the epoch; the abandoned chain deregisters
	// it after the rollback point and before the RUPD slot.
	rewardCalcSeedStakeCert(
		t, db, 21, member, 0, 150,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t, db, 22, member, 0, cutoff-5,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	ancestor := chain.RawBlock{
		Slot: cutoff - 10, Hash: testHashBytes("prefilter-ancestor"),
		BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
	}
	abandoned := chain.RawBlock{
		Slot: cutoff + 1, Hash: testHashBytes("prefilter-abandoned"),
		PrevHash:    ancestor.Hash,
		BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(),
		[]chain.RawBlock{ancestor, abandoned},
	))
	for _, block := range []chain.RawBlock{ancestor, abandoned} {
		require.NoError(t, db.SetBlockNonce(
			block.Hash, block.Slot, nonce, true, nil,
		))
	}
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(abandoned.Slot, abandoned.Hash),
		BlockNumber: abandoned.BlockNumber,
	}
	require.NoError(t, db.SetTip(ls.currentTip, nil))

	require.NoError(t, ls.precomputeStakeRewardsAfterEpochTransition(
		event.EpochTransitionEvent{
			NewEpoch:     3,
			BoundarySlot: abandoned.Slot,
			EpochNonce:   nonce,
		},
	))
	require.False(t, rewardOutputsPayKey(t, db, member),
		"control: the abandoned chain's prefilter excludes member")

	require.NoError(t, ls.rollbackWithBlocks(context.Background(),
		ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
	))
	ls.rewardPrecomputeWG.Wait()

	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.Empty(t, outputs,
		"no output computed from the abandoned history may survive")
	ls.rewardPrecomputeMu.Lock()
	retry := ls.rewardPrecomputeRetry
	ls.rewardPrecomputeMu.Unlock()
	require.NotNil(t, retry,
		"the replacement must wait for the RUPD slot on the surviving chain")
	require.Equal(t, uint64(3), retry.epochEvent.NewEpoch)
	require.Equal(t, cutoff, retry.cutoffSlot)

	replacement := ocommon.NewPoint(
		cutoff+1, testHashBytes("prefilter-replacement"),
	)
	ls.Lock()
	ls.currentTip = ochainsync.Tip{Point: replacement, BlockNumber: 2}
	ls.Unlock()
	ls.maybeQueueStakeRewardPrecomputeRetry(replacement.Slot)
	ls.rewardPrecomputeWG.Wait()

	require.True(t, rewardOutputsPayKey(t, db, member),
		"the replacement must use the surviving registration history")
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		want, ok, err := ls.calculateStakeRewardApplication(
			txn, 4, replacement.Slot, 1_200, false,
		)
		require.NoError(t, err)
		require.True(t, ok)
		poolOutputs, err := db.Metadata().GetRewardPoolOutputs(
			1, txn.Metadata(),
		)
		require.NoError(t, err)
		accountOutputs, err := db.Metadata().GetRewardAccountOutputs(
			1, txn.Metadata(),
		)
		require.NoError(t, err)
		require.Equal(t,
			rewardPoolOutputAmounts(want.poolOutputs),
			rewardPoolOutputAmounts(poolOutputs),
		)
		require.Equal(t,
			rewardAccountOutputAmounts(want.accountOutputs),
			rewardAccountOutputAmounts(accountOutputs),
		)
		pots, err := db.Metadata().GetRewardAdaPots(3, txn.Metadata())
		require.NoError(t, err)
		require.Equal(t, want.totalRewardPot, uint64(pots.Rewards))
		_, reusable, err := ls.precomputedStakeRewardApplication(
			txn, 4, 1_200,
		)
		require.NoError(t, err)
		require.True(
			t,
			reusable,
			"the next boundary must reuse the replacement",
		)
		return nil
	}))
	account, err := db.GetAccountByCredential(context.Background(), 0, member, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Zero(t, uint64(account.Reward),
		"precomputation must not credit rewards before the boundary")
}

func rewardOutputsPayKey(
	t *testing.T,
	db *database.Database,
	stakingKey []byte,
) bool {
	t.Helper()
	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, outputs)
	for _, output := range outputs {
		if bytes.Equal(output.StakingKey, stakingKey) && output.Amount > 0 {
			return true
		}
	}
	return false
}

func rewardPoolOutputAmounts(outputs []*models.RewardPoolOutput) []string {
	ret := make([]string, 0, len(outputs))
	for _, output := range outputs {
		ret = append(ret, fmt.Sprintf(
			"%x total=%d leader=%d members=%d undistributed=%d unspendable=%d",
			output.PoolKeyHash,
			output.TotalReward,
			output.LeaderReward,
			output.MemberRewardTotal,
			output.Undistributed,
			output.Unspendable,
		))
	}
	slices.Sort(ret)
	return ret
}

func rewardAccountOutputAmounts(
	outputs []*models.RewardAccountOutput,
) []string {
	ret := make([]string, 0, len(outputs))
	for _, output := range outputs {
		ret = append(ret, fmt.Sprintf(
			"%d:%x %s pool=%x amount=%d spendable=%t",
			output.CredentialTag,
			output.StakingKey,
			output.RewardType,
			output.PoolKeyHash,
			output.Amount,
			output.Spendable,
		))
	}
	slices.Sort(ret)
	return ret
}

// The EventBus subscription that drives the reward precompute only fires at an
// epoch boundary, so an epoch already in progress when the process starts has
// no event to carry it. Startup must queue that round itself, or the next
// boundary calculates it inline inside the rollover write transaction.
func TestQueueStartupRewardPrecomputeQueuesInProgressEpoch(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentEpoch = models.Epoch{
		EpochId:       655,
		StartSlot:     197596800,
		LengthInSlots: 432000,
		Nonce:         []byte{0x30, 0x93, 0x65, 0x6a},
	}

	queued := make(chan event.EpochTransitionEvent, 1)
	ls.queueStartupRewardPrecomputeWith(
		func(evt event.EpochTransitionEvent) error {
			queued <- evt
			return nil
		},
	)

	evt := testutil.RequireReceive(
		t, queued, 2*time.Second, "startup precompute queued",
	)
	// precomputeStakeRewardsAfterEpochTransition derives the application epoch
	// as NewEpoch+1 and uses BoundarySlot as the capture slot, so these two
	// fields are what decide which round gets precomputed.
	require.Equal(t, uint64(655), evt.NewEpoch)
	require.Equal(t, uint64(197596800), evt.BoundarySlot)
	require.Equal(t, uint64(654), evt.PreviousEpoch)
	require.Equal(t, uint64(197596799), evt.SnapshotSlot)
	require.Equal(t, ls.currentEpoch.Nonce, evt.EpochNonce)
}

// A nonce-less or zero-length epoch is one that was never established, so there
// is no round to catch up and nothing should be queued. queueRewardPrecompute
// also drops an event without a nonce, so queueing one would spawn a worker
// that immediately does nothing.
func TestQueueStartupRewardPrecomputeSkipsUnestablishedEpoch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		epoch models.Epoch
	}{
		{
			name: "no length",
			epoch: models.Epoch{
				EpochId: 655,
				Nonce:   []byte{0x01},
			},
		},
		{
			name: "no nonce",
			epoch: models.Epoch{
				EpochId:       655,
				LengthInSlots: 432000,
			},
		},
		{
			name:  "zero value",
			epoch: models.Epoch{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{}
			ls.currentEpoch = tc.epoch

			queued := make(chan event.EpochTransitionEvent, 1)
			ls.queueStartupRewardPrecomputeWith(
				func(evt event.EpochTransitionEvent) error {
					queued <- evt
					return nil
				},
			)

			testutil.RequireNoReceive(
				t, queued, 100*time.Millisecond,
				"unestablished epoch must not queue a precompute",
			)
		})
	}
}

// Epoch 0 has no predecessor and starts at slot 0; neither derived field may
// underflow into a bogus epoch or slot.
func TestQueueStartupRewardPrecomputeHandlesEpochZero(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentEpoch = models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 432000,
		Nonce:         []byte{0x01},
	}

	queued := make(chan event.EpochTransitionEvent, 1)
	ls.queueStartupRewardPrecomputeWith(
		func(evt event.EpochTransitionEvent) error {
			queued <- evt
			return nil
		},
	)

	evt := testutil.RequireReceive(
		t, queued, 2*time.Second, "startup precompute queued",
	)
	require.Equal(t, uint64(0), evt.NewEpoch)
	require.Equal(t, uint64(0), evt.PreviousEpoch)
	require.Equal(t, uint64(0), evt.SnapshotSlot)
	require.Equal(t, uint64(0), evt.BoundarySlot)
}

// Preview's on-chain ADA pots, as reported by Koios
// (https://preview.koios.rest/api/v1/totals). Preview declares
// TestShelleyHardForkAtEpoch: 0, so epoch 0 is already Alonzo and every epoch
// boundary from 0->1 onward runs cardano-ledger's NEWEPOCH monetary expansion.
// Preview's genesis decentralisationParam is 1, so eta is 1 by definition and
// no stake rewards are distributed in these epochs: the whole reward pot is
// split between the treasury tax and the reserves refund.
const (
	previewGenesisReserves = uint64(15_000_000_000_000_000)
	previewMaxSupply       = uint64(45_000_000_000_000_000)

	// Epoch 0's fee pot is empty: nothing was collected before epoch 0.
	previewEpoch1Treasury = uint64(9_000_000_000_000)
	previewEpoch1Reserves = uint64(14_991_000_000_000_000)

	// Epoch 0 collected 437793 lovelace in fees, which the 1->2 boundary
	// folds into the reward pot.
	previewEpoch1Fees     = uint64(437_793)
	previewEpoch2Treasury = uint64(17_994_600_087_558)
	previewEpoch2Reserves = uint64(14_982_005_400_350_235)

	// Epoch 1 collected 206597 lovelace in fees, which the 2->3 boundary
	// folds into the reward pot. Preview's decentralisation is 1 at epochs 0
	// and 1 and 0 from epoch 2 onward, so the 2->3 round -- whose parameters
	// come from performance epoch 1 -- still takes the d >= 0.8 short circuit
	// and expands by the full rho * reserves.
	previewEpoch2Fees     = uint64(206_597)
	previewEpoch3Treasury = uint64(26_983_803_369_087)
	previewEpoch3Reserves = uint64(14_973_016_197_275_303)

	previewEpochLength = uint64(86_400)
)

// newPreviewRewardPotsTestLedger builds a LedgerState configured with
// Preview's Shelley genesis and seeds the epoch rows, protocol parameters and
// empty mark snapshots that the delayed reward calculation reads for the first
// two boundaries.
func newPreviewRewardPotsTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"epochLength": 86400,
		"maxLovelaceSupply": 45000000000000000,
		"securityParam": 432,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	ls := &LedgerState{
		db:         db,
		currentEra: eras.AlonzoEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	// Preview's genesis protocol parameters: rho 0.003, tau 0.2, d 1.
	pparams := &alonzo.AlonzoProtocolParameters{
		NOpt:             150,
		A0:               rewardCalcRat(3, 10),
		Rho:              rewardCalcRat(3, 1_000),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(1, 1),
		ProtocolMajor:    6,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	// Preview's decentralisation drops from 1 to 0 at epoch 2, so each epoch
	// is seeded with its own protocol parameters.
	decentralizedPParams := *pparams
	decentralizedPParams.Decentralization = rewardCalcRat(0, 1)
	decentralizedCbor, err := cbor.Encode(&decentralizedPParams)
	require.NoError(t, err)

	meta := db.Metadata()
	for _, epoch := range []uint64{0, 1, 2} {
		epochPParamsCbor := pparamsCbor
		if epoch >= 2 {
			epochPParamsCbor = decentralizedCbor
		}
		startSlot := epoch * previewEpochLength
		require.NoError(t, meta.SetEpoch(
			startSlot,
			epoch,
			nil,
			nil,
			nil,
			nil,
			eras.AlonzoEraDesc.Id,
			1,
			uint(previewEpochLength),
			nil,
		))
		require.NoError(t, db.SetPParams(
			epochPParamsCbor,
			startSlot,
			epoch,
			eras.AlonzoEraDesc.Id,
			nil,
		))
		// Preview has no stake delegated to non-overlay pools in these
		// epochs, so the mark snapshot is empty. Epoch 0's is seeded at
		// startup by snapshot.Manager.CaptureGenesisSnapshot.
		require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch:           epoch,
			SnapshotType:    "mark",
			CapturedSlot:    startSlot,
			BoundarySlot:    startSlot,
			ProtocolVersion: 6,
		}, nil))
	}
	return ls, db
}

// TestApplyStakeRewardsPreviewEpoch1Pots pins the 0->1 boundary. cardano-ledger
// applies monetary expansion and the treasury tax at the first boundary of a
// network whose epoch 0 is already Shelley-era, with an empty fee pot and no
// distribution. Skipping that round leaves the treasury at 0 and the reserves
// at their genesis value, which is what observed on Preview.
func TestApplyStakeRewardsPreviewEpoch1Pots(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(0, previewGenesisReserves, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Treasury:     0,
		Reserves:     types.Uint64(previewGenesisReserves),
		Fees:         0,
		CapturedSlot: 0,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 1, previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch1Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch1Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewEpoch2Pots pins the 1->2 boundary against the
// same Koios reference. It is seeded with the epoch-1 pots the previous
// boundary must produce, so it isolates the epoch-2 arithmetic from the
// epoch-1 seeding defect.
func TestApplyStakeRewardsPreviewEpoch2Pots(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(
		previewEpoch1Treasury, previewEpoch1Reserves, previewEpochLength, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        1,
		Treasury:     types.Uint64(previewEpoch1Treasury),
		Reserves:     types.Uint64(previewEpoch1Reserves),
		Fees:         types.Uint64(previewEpoch1Fees),
		CapturedSlot: previewEpochLength,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 2, 2*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch2Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch2Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewGenesisToEpoch2 chains both boundaries the way a
// genesis replay does: the 0->1 round, the epoch-1 ADA pots capture that
// records its result, then the 1->2 round that reads it back. Preview's epoch 0
// carries exactly two transactions, at slots 60 and 320, whose fees (200000 and
// 237793) are the 437793 the 1->2 boundary folds into the reward pot.
//
// This is the unit-level counterpart of reproduction: the
// epoch-2 treasury and reserves must equal the Koios Preview reference values.
func TestApplyStakeRewardsPreviewGenesisToEpoch2(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(0, previewGenesisReserves, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Treasury:     0,
		Reserves:     types.Uint64(previewGenesisReserves),
		Fees:         0,
		CapturedSlot: 0,
	}, nil))

	// Preview's two epoch-0 transactions.
	_, err := rewardCalcSQLDB(t, db).Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES
    (1, ?, ?, 60, 5, '200000', '0', '0', 0, TRUE),
    (2, ?, ?, 320, 5, '237793', '0', '0', 0, TRUE)`,
		[]byte("preview-tx-0"), []byte("preview-block-0"),
		[]byte("preview-tx-1"), []byte("preview-block-1"),
	)
	require.NoError(t, err)

	epoch0, err := meta.GetEpoch(0, nil)
	require.NoError(t, err)
	require.NotNil(t, epoch0)

	// Boundary into epoch 1: apply the reward round, then capture the epoch-1
	// ADA pots the way processEpochRollover does.
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := ls.applyStakeRewards(context.Background(),
			txn, 1, previewEpochLength,
		); err != nil {
			return err
		}
		return ls.saveRewardAdaPotsForEpoch(
			txn, 1, *epoch0, previewEpochLength,
		)
	}))

	pots1, err := meta.GetRewardAdaPots(1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots1)
	require.Equal(t, previewEpoch1Treasury, uint64(pots1.Treasury))
	require.Equal(t, previewEpoch1Reserves, uint64(pots1.Reserves))
	require.Equal(t, previewEpoch1Fees, uint64(pots1.Fees))

	// Boundary into epoch 2, reading the row the previous boundary wrote.
	txn = db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 2, 2*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch2Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch2Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewEpoch3Pots pins the 2->3 boundary, where
// preview's decentralisation differs between the round's performance epoch (1,
// d = 1) and its calculation epoch (2, d = 0).
//
// cardano-ledger's startStep builds the whole reward update from
// prevPParams -- the parameters in force during the epoch whose blocks are
// counted, which is dingo's performance epoch -- so d is 1 here and eta takes
// the d >= 0.8 short circuit. Reading d from the calculation epoch instead
// gives d = 0, no short circuit, and an eta of zero against an empty epoch-0
// mark snapshot, which drops the monetary expansion entirely and moves only
// the fee pot.
func TestApplyStakeRewardsPreviewEpoch3Pots(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(
		previewEpoch2Treasury,
		previewEpoch2Reserves,
		2*previewEpochLength,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        2,
		Treasury:     types.Uint64(previewEpoch2Treasury),
		Reserves:     types.Uint64(previewEpoch2Reserves),
		Fees:         types.Uint64(previewEpoch2Fees),
		CapturedSlot: 2 * previewEpochLength,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 3, 3*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch3Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch3Reserves, uint64(state.Reserves))
}

// An unavailable reward basis is not a benign no-op. The reference node
// credits the round, so continuing leaves this node's reward balances and the
// leadership stake distribution derived from them permanently short.
//
// That shortfall is what rejects canonical blocks: leader eligibility
// compares a VRF value against a stake-derived threshold, so a sigma
// shortfall of eps flips a decision with probability about eps per block.
// On Preview, the shortfall was ~3 epochs of reward
// accrual, sigma was 0.042% short, and the rejected block's leader value sat
// between this node's threshold and the reference's.
func TestUnavailableStakeRewardBasisIsReportedAndRejected(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				Level: slog.LevelError,
			})),
		},
	}

	err := ls.requiredStakeRewardBasisUnavailable(
		true, 1386, "missing ADA pots", "pots_epoch", 1385,
	)
	require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)

	logs := buf.String()
	require.NotEmpty(t, logs)
	assert.Contains(t, logs, "level=ERROR")
	assert.Contains(t, logs, "missing ADA pots")
	assert.Contains(t, logs, "new_epoch=1386")
	assert.Contains(t, logs, "pots_epoch=1385")
	// The consequence, not just the event: whoever reads this needs to know
	// the balances stay short rather than catching up on their own.
	assert.Contains(t, logs, "permanently")
	assert.Contains(t, logs, "ledgerstate import warnings")
	assert.NotContains(t, logs, "expected after a Mithril bootstrap",
		"a failed imported-basis seed must not be misreported as an "+
			"inherent bootstrap limitation")
}

// The reporting path must tolerate a LedgerState with no logger and no
// metrics, since it runs on the epoch-boundary hot path where a nil
// dereference would take down block application.
func TestUnavailableStakeRewardBasisSurvivesNilDependencies(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	require.NotPanics(t, func() {
		err := ls.requiredStakeRewardBasisUnavailable(
			true,
			1386,
			"missing reward snapshot",
			"reward_snapshot_epoch",
			1383,
		)
		require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)
	})
}

func TestMissingRewardSnapshotReportsImportedSeedFailure(t *testing.T) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		failureReason       = "historical protocol parameters are unavailable"
	)

	for _, tc := range []struct {
		name        string
		seedFailure bool
		wantReason  string
		notReason   string
	}{
		{
			name:        "durable import failure",
			seedFailure: true,
			wantReason: "imported reward basis seeding failed: " +
				failureReason,
			notReason: "cannot apply stake rewards: missing reward snapshot;",
		},
		{
			name:       "genuinely missing import",
			wantReason: "cannot apply stake rewards: missing reward snapshot;",
			notReason:  "imported reward basis seeding failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := newRewardCalculationTestLedger(t)
			var logs bytes.Buffer
			ls.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))

			meta := db.Metadata()
			require.NoError(t, meta.SaveRewardAdaPots(
				&models.RewardAdaPots{
					Epoch:        potsEpoch,
					CapturedSlot: 300,
				},
				nil,
			))
			if tc.seedFailure {
				require.NoError(t, meta.SaveRewardSeedFailure(
					rewardSnapshotEpoch,
					"mark",
					failureReason,
					100,
					nil,
				))
			}

			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.calculateStakeRewardApplication(
				txn,
				newEpoch,
				400,
				400,
				true,
			)
			require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)
			require.False(t, ok)
			require.Nil(t, app)
			assert.Contains(t, logs.String(), tc.wantReason)
			assert.NotContains(t, logs.String(), tc.notReason)
		})
	}
}

func TestApplyStakeRewardsConwayGenesisPerformance(t *testing.T) {
	t.Parallel()
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.ConwayEraDesc
	require.NoError(t, ls.config.CardanoNodeConfig.
		LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.4,
		"epochLength": 500,
		"maxLovelaceSupply": 6000000000000,
		"securityParam": 40,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	pp := mockledger.NewMockConwayProtocolParams()
	pp.NOpt = 150
	pp.A0 = rewardCalcRat(3, 10)
	pp.Rho = rewardCalcRat(3, 1_000)
	pp.Tau = rewardCalcRat(1, 5)
	pp.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 10}
	encoded, err := cbor.Encode(&pp)
	require.NoError(t, err)
	meta := db.Metadata()
	for epoch := range uint64(3) {
		require.NoError(t, meta.SetEpoch(
			epoch*500, epoch, nil, nil, nil, nil,
			eras.ConwayEraDesc.Id, 1, 500, nil,
		))
		require.NoError(t, db.SetPParams(
			encoded, epoch*500, epoch, eras.ConwayEraDesc.Id, nil,
		))
	}
	require.NoError(t, meta.SetNetworkState(0, 2_000_000_000_000, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 0, Reserves: 2_000_000_000_000,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: 0, SnapshotType: "mark", ProtocolVersion: 10,
		TotalActiveStake: 2_000_000_000_000,
		TotalPoolCount:   2, TotalDelegators: 2,
	}, nil))
	for _, key := range []byte{0x11, 0x22} {
		poolKey := rewardCalcHash(key)
		poolID := seedLiveStakeFixture(
			t, db, poolKey, bytes.Repeat([]byte{key}, 32),
			1_000_000_000_000, 0,
		)
		require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
			Epoch: 0, PoolKeyHash: poolKey, RewardAccount: poolKey,
			Margin:         &types.Rat{Rat: big.NewRat(0, 1)},
			DelegatedStake: 1_000_000_000_000, DelegatorCount: 1,
		}}, nil))
		require.NoError(
			t,
			meta.SaveRewardStakeInputs([]*models.RewardStakeInput{{
				Epoch: 0, PoolKeyHash: poolKey, StakingKey: poolKey,
				Stake: 1_000_000_000_000, Registered: true,
			}}, nil),
		)
		for i := range uint64(90) {
			require.NoError(t, db.UpdatePoolOpCertSequence(
				t.Context(),
				poolID, i+1, 1+2*i+uint64(key), nil,
			))
		}
	}
	_, err = rewardCalcSQLDB(t, db).Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, 60, 7, '400000', '0', '0', 0, TRUE)`,
		[]byte("genesis-performance-tx"), []byte("genesis-performance-block"))
	require.NoError(t, err)

	for _, tc := range []struct {
		epoch    uint64
		treasury uint64
		reserves uint64
		fraction *big.Rat
	}{
		{1, 0, 2_000_000_000_000, big.NewRat(1, 4)},
		{2, 1_080_080_000, 1_998_920_320_000, big.NewRat(1_562_500, 6_251_687)},
	} {
		boundary := tc.epoch * 500
		ended, err := meta.GetEpoch(tc.epoch-1, nil)
		require.NoError(t, err)
		require.NotNil(t, ended)
		txn := db.Transaction(t.Context(), true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			if err := ls.applyStakeRewards(t.Context(), txn, tc.epoch, boundary); err != nil {
				return err
			}
			return ls.saveRewardAdaPotsForEpoch(txn, tc.epoch, *ended, boundary)
		}))
		state, err := meta.GetNetworkState(nil)
		require.NoError(t, err)
		require.NotNil(t, state)
		require.Equal(t, tc.treasury, uint64(state.Treasury),
			"treasury at boundary into epoch %d", tc.epoch)
		require.Equal(t, tc.reserves, uint64(state.Reserves),
			"reserves at boundary into epoch %d", tc.epoch)
		pots, err := meta.GetRewardAdaPots(tc.epoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, state.Treasury, pots.Treasury)
		require.Equal(t, state.Reserves, pots.Reserves)
		if tc.epoch == 1 {
			require.Equal(t, uint64(400_000), uint64(pots.Fees))
		}

		hash := bytes.Repeat([]byte{byte(tc.epoch)}, 32)
		seedBlockAtSlot(t, ls, boundary, hash)
		require.NoError(t, db.SetTip(ochainsync.Tip{
			Point: ocommon.NewPoint(boundary, hash),
		}, nil))
		result, err := ls.Query(t.Context(), stakeDistributionQuery(), QueryPoint{})
		require.NoError(t, err)
		dist := decodeStakeDistributionResult(t, result)
		require.Len(t, dist.Results, 2)
		for _, entry := range dist.Results {
			require.Equal(t, tc.fraction, entry.StakeFraction.Rat,
				"stake fraction at boundary into epoch %d", tc.epoch)
		}
	}
}

func TestSuppressBootstrapStakeRewardsReturnsAvailableRewardsToReserves(
	t *testing.T,
) {
	t.Parallel()

	result := &rewards.Result{
		PoolRewards:      []rewards.PoolReward{{PoolReward: 600}},
		AccountRewards:   []rewards.AccountReward{{Amount: 600}},
		TotalRewardPot:   1_000,
		AvailableRewards: 800,
		EffectiveRewards: 600,
		Unspendable:      50,
		Undistributed:    150,
	}
	suppressBootstrapStakeRewards(result)

	require.Empty(t, result.PoolRewards)
	require.Empty(t, result.AccountRewards)
	require.Zero(t, result.EffectiveRewards)
	require.Zero(t, result.Unspendable)
	require.Equal(t, uint64(800), result.Undistributed)

	app := &stakeRewardApplication{
		params: rewards.Parameters{
			TreasuryExpansion: big.NewRat(1, 5),
		},
		pots: &models.RewardAdaPots{
			Reserves: types.Uint64(10_000),
			Treasury: types.Uint64(10),
		},
		totalRewardPot:   result.TotalRewardPot,
		availableRewards: result.AvailableRewards,
		undistributed:    result.Undistributed,
	}
	reserves, treasury, err := stakeRewardUpdatedPots(app)
	require.NoError(t, err)
	require.Equal(t, uint64(9_800), reserves)
	require.Equal(t, uint64(210), treasury)
}

// Each required input the authoritative boundary reads fails with an error
// that names the epoch, the missing input and the operator recovery, while the
// opportunistic precompute reading the same state stays silent and error-free.
func TestRequiredRewardBasisErrorNamesEpochInputAndRecovery(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		setup     func(t *testing.T) (*LedgerState, *database.Database)
		wantInput string
		wantEpoch string
	}{
		{
			name: "missing ADA pots",
			setup: func(t *testing.T) (*LedgerState, *database.Database) {
				return newRewardCalculationTestLedger(t)
			},
			wantInput: "missing ADA pots",
			wantEpoch: "pots_epoch=3",
		},
		{
			name: "missing reward snapshot",
			setup: func(t *testing.T) (*LedgerState, *database.Database) {
				ls, db := newRewardCalculationTestLedger(t)
				seedRetentionRewardEpochs(t, db)
				return ls, db
			},
			wantInput: "missing reward snapshot",
			wantEpoch: "reward_snapshot_epoch=1",
		},
		{
			name: "pruned reward stake inputs",
			setup: func(t *testing.T) (*LedgerState, *database.Database) {
				ls, db := newRewardCalculationTestLedger(t)
				seedRetentionRewardEpochs(t, db)
				seedPrunedStakeInputSnapshot(
					t, db, rewardCalcHash(0x55), rewardCalcHash(0x66),
				)
				return ls, db
			},
			wantInput: "reward stake inputs",
			wantEpoch: "reward_snapshot_epoch=1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, db := tc.setup(t)

			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Rollback() }()

			app, ok, err := ls.calculateStakeRewardApplication(
				txn, retentionNewEpoch, retentionBoundarySlot,
				retentionBoundarySlot, true,
			)
			require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)
			require.False(t, ok)
			require.Nil(t, app)
			assert.Contains(t, err.Error(), tc.wantInput)
			assert.Contains(t, err.Error(), tc.wantEpoch)
			assert.Contains(t, err.Error(), "new epoch 4")
			assert.Contains(t, err.Error(), "re-running Mithril sync")
			assert.Contains(t, err.Error(), "ledger-state import")
			assert.Contains(t, err.Error(), "ledgerstate import warnings")

			app, ok, err = ls.calculateStakeRewardApplication(
				txn, retentionNewEpoch, retentionBoundarySlot,
				retentionBoundarySlot, false,
			)
			require.NoError(t, err)
			require.False(t, ok)
			require.Nil(t, app)
		})
	}
}

func TestRequiredRewardBasisErrorNamesHiddenBlockCounts(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	require.NoError(t, db.Metadata().SetSyncState(
		mithrilLedgerSlotSyncKey, "199", nil,
	))
	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Rollback() }()

	_, ok, err := ls.calculateStakeRewardApplication(txn, 4, 1_200, 1_200, true)
	require.ErrorIs(t, err, errRequiredStakeRewardBasisUnavailable)
	require.False(t, ok)
	assert.Contains(t, err.Error(), "new epoch 4")
	assert.Contains(t, err.Error(), "performance_epoch=2")
	assert.Contains(t, err.Error(), "re-running Mithril sync")

	_, ok, err = ls.calculateStakeRewardApplication(txn, 4, 1_200, 1_200, false)
	require.NoError(t, err)
	require.False(t, ok)
}

// The only absences the authoritative boundary accepts: epoch 0 has no round,
// epochs 1 and 2 are bootstrap rounds, and a round whose ended epoch is Byron
// is suppressed. Every other epoch from 3 up requires a reward round.
func TestRewardRoundEnumeratedAbsences(t *testing.T) {
	t.Parallel()

	_, ok := stakeRewardEpochsForApplication(0)
	require.False(t, ok, "epoch 0 has no reward round")
	for _, e := range []uint64{1, 2} {
		epochs, ok := stakeRewardEpochsForApplication(e)
		require.True(t, ok, "epoch %d", e)
		require.True(t, epochs.bootstrap, "epoch %d", e)
	}
	for e := uint64(3); e < 10; e++ {
		epochs, ok := stakeRewardEpochsForApplication(e)
		require.True(t, ok, "epoch %d", e)
		require.False(t, epochs.bootstrap, "epoch %d", e)
	}

	ls, db := newRewardCalculationTestLedger(t)
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 0, 0)
	}))
}
