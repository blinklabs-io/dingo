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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/stretchr/testify/require"
)

// markRewardStakeInputsPending marks the snapshot of epoch as holding only
// a part of its reward_stake_input rows, as a store migrated mid-write does.
func markRewardStakeInputsPending(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	epoch uint64,
	boundarySlot uint64,
) error {
	pending, err := epochengine.LoadRewardStakeInputsPending(meta, metaTxn)
	if err != nil {
		return err
	}
	entry := RewardStakeInputsPendingEntry{
		Epoch: epoch, BoundarySlot: boundarySlot,
	}
	if i := pending.Find(epoch); i >= 0 {
		pending.Entries[i] = entry
	} else {
		pending.Entries = append(pending.Entries, entry)
	}
	return epochengine.SaveRewardStakeInputsPending(meta, metaTxn, pending)
}

// A v0.94.2 database can stop with a snapshot's reward_stake_input rows
// written only in part, which its sync_state pending record says. The
// migration moves the rows into the set, so the set must carry the mark:
// without it the part reads as the whole snapshot and rewards are computed
// from it.
func TestMigratedPartialRewardStakeInputsStayIncomplete(t *testing.T) {
	t.Parallel()
	const (
		epoch        = uint64(7)
		capturedSlot = uint64(690)
		boundarySlot = uint64(700)
	)
	db := fxReleasedLedgerDB(t, t.TempDir(), func(released *database.Database) {
		meta := released.Metadata()
		pool := bytes.Repeat([]byte{0x31}, 28)
		require.NoError(t, dbadapter.New(released).SetEpoch(
			boundarySlot, epoch, nil, nil, nil, nil, 6, 1000, 100, nil,
		))
		require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch: epoch, SnapshotType: "mark", CapturedSlot: capturedSlot,
			BoundarySlot: boundarySlot, TotalDelegators: 2, Authoritative: true,
		}, nil))
		require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
			Epoch: epoch, PoolKeyHash: pool, DelegatorCount: 2,
			CapturedSlot: capturedSlot, BoundarySlot: boundarySlot,
		}}, nil))
		// One of the snapshot's two rows was written before the store
		// stopped.
		require.NoError(t, meta.SaveRewardStakeInputs(
			[]*models.RewardStakeInput{{
				Epoch: epoch, PoolKeyHash: pool,
				StakingKey: bytes.Repeat([]byte{0x41}, 28), Stake: 5,
				CapturedSlot: capturedSlot, BoundarySlot: boundarySlot,
			}}, nil,
		))
		require.NoError(t, meta.SetSyncState(
			epochengine.RewardStakeInputsPendingKey,
			`{"entries":[{"epoch":7,"boundary_slot":700}]}`, nil,
		))
	})

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: newRewardCalculationTestNodeConfig(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	fxAttachLedgerDB(t, ls)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })

	rows, err := ls.ledgerDB().GetRewardStakeInputs(epoch, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the set holds the rows the store had")

	incomplete, err := ls.RewardStakeInputsIncompleteCommitted(epoch, epoch+1)
	require.NoError(t, err)
	require.True(t, incomplete,
		"a part of a snapshot's stake rows must not read as the whole")
	other, err := ls.RewardStakeInputsIncompleteCommitted(epoch+1, epoch+2)
	require.NoError(t, err)
	require.False(t, other)

	// A reader that cannot rebuild the rows is refused them.
	read := ls.ledgerDB().Begin(t.Context(), false)
	defer read.Release()
	require.ErrorIs(
		t,
		ls.engine().EnsureRewardStakeInputsReady(read, epoch),
		epochengine.ErrRewardStakeInputsNotReady,
	)
}

// seedPartialSQLRewardStakeInputs leaves the v0.94.2 database db as that
// release leaves it after stopping mid-write: the epoch 1 mark snapshot
// with one of its two stake rows, the pending record naming it, and the
// UTxO and delegation history that rebuilds the other. It returns the rows
// the whole capture wrote.
func seedPartialSQLRewardStakeInputs(
	t *testing.T,
	db *database.Database,
) []*models.RewardStakeInput {
	t.Helper()
	const (
		epoch        = uint64(1)
		boundarySlot = uint64(100)
	)
	ctx := context.Background()
	sql := dbadapter.New(db)
	poolHash := bytes.Repeat([]byte{0xd1}, 28)
	ownerKey := bytes.Repeat([]byte{0x71}, 28)
	delegatorKey := bytes.Repeat([]byte{0x72}, 28)
	require.NoError(t, db.ImportPool(ctx, nil,
		&models.Pool{
			PoolKeyHash: poolHash, VrfKeyHash: make([]byte, 32),
			Pledge: 5_000_000, Cost: 340_000_000,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			RewardAccount: ownerKey,
		},
		&models.PoolRegistration{
			PoolKeyHash: poolHash, Pledge: 5_000_000, Cost: 340_000_000,
			Margin:     &types.Rat{Rat: big.NewRat(1, 100)},
			VrfKeyHash: make([]byte, 32), RewardAccount: ownerKey,
			Owners: []models.PoolRegistrationOwner{
				{KeyHash: bytes.Clone(ownerKey)},
			},
		},
	))
	for i, key := range [][]byte{ownerKey, delegatorKey} {
		require.NoError(t, db.CreateAccount(ctx, nil, &models.Account{
			StakingKey: key, Pool: poolHash, Active: true,
		}))
		require.NoError(t, db.CreateUtxo(ctx, nil, &models.Utxo{
			TxId:       bytes.Repeat([]byte{byte(0x10 + i)}, 32),
			StakingKey: key, Amount: types.Uint64(20_000_000),
		}))
	}
	require.NoError(t, sql.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	mgr := snapshot.NewManager(sql, newTestEventBus(t, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch: 0, NewEpoch: epoch, BoundarySlot: boundarySlot,
		EpochNonce: []byte{0x0a, 0x0b}, ProtocolVersion: 7, SnapshotSlot: 99,
	}
	capture := db.Transaction(ctx, true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(ctx, capture, evt))
	require.NoError(t, sql.SetEpoch(
		boundarySlot, epoch, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 100, capture,
	))
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(ctx, capture, evt))
	require.NoError(t, capture.Commit())
	whole, err := sql.GetRewardStakeInputs(epoch, nil)
	require.NoError(t, err)
	require.Len(t, whole, 2, "the capture wrote a row per delegator")

	// The store stopped with one of the two rows written.
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		"DELETE FROM reward_stake_input WHERE staking_key = ?", delegatorKey,
	)
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	require.NoError(t, db.Metadata().SetSyncState(
		epochengine.RewardStakeInputsPendingKey,
		`{"entries":[{"epoch":1,"boundary_slot":100}]}`, nil,
	))
	fxStoreUtxoRowBodies(t, db)
	return whole
}

// The stake rows a migrated store holds only in part are rebuilt, from the
// history the set took over, by the first reader that can write: the whole
// snapshot's rows replace the part and the mark is cleared with them.
func TestMigratedPartialRewardStakeInputsAreRebuilt(t *testing.T) {
	t.Parallel()
	const epoch = uint64(1)
	ctx := context.Background()
	var whole []*models.RewardStakeInput
	db := fxReleasedLedgerDB(t, t.TempDir(), func(released *database.Database) {
		whole = seedPartialSQLRewardStakeInputs(t, released)
	})

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: newRewardCalculationTestNodeConfig(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	fxAttachLedgerDB(t, ls)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	part, err := ls.ledgerDB().GetRewardStakeInputs(epoch, nil)
	require.NoError(t, err)
	require.Len(t, part, 1)
	incomplete, err := ls.RewardStakeInputsIncompleteCommitted(epoch, epoch+1)
	require.NoError(t, err)
	require.True(t, incomplete)

	tx := db.Transaction(ctx, true)
	require.NoError(t, ls.engine().EnsureRewardStakeInputsReady(tx, epoch))
	require.NoError(t, tx.Commit())

	rebuilt, err := ls.ledgerDB().GetRewardStakeInputs(epoch, nil)
	require.NoError(t, err)
	require.Len(t, rebuilt, len(whole))
	stakes := func(rows []*models.RewardStakeInput) map[string]uint64 {
		out := make(map[string]uint64, len(rows))
		for _, r := range rows {
			out[string(r.StakingKey)] = uint64(r.Stake)
		}
		return out
	}
	require.Equal(t, stakes(whole), stakes(rebuilt))
	incomplete, err = ls.RewardStakeInputsIncompleteCommitted(epoch, epoch+1)
	require.NoError(t, err)
	require.False(t, incomplete, "the rebuild must clear the mark")
}

// The fake set of the output tests holds no delegator inactivity activation
// and no pending ratification record.

func (committedUtxoSet) InactivityLoaded() (bool, error) { return true, nil }

func (committedUtxoSet) InactivityActivation(*uint64) (uint64, bool, error) {
	return 0, false, nil
}

func (committedUtxoSet) RatifyPendingLoaded() (bool, error) { return true, nil }

func (committedUtxoSet) RatifyPending(*uint64) ([]byte, bool, error) {
	return nil, false, nil
}

// Startup rebuilds a pending snapshot's stake rows from the SQL history before
// that history moves into the set, and clears the SQL record, so the set loads
// the whole snapshot and no mark.
func TestResolveReleasedRewardStakeInputsRebuildsFromMetadataHistory(
	t *testing.T,
) {
	t.Parallel()
	const epoch = uint64(1)
	var whole []*models.RewardStakeInput
	db := fxReleasedLedgerDB(t, t.TempDir(), func(released *database.Database) {
		whole = seedPartialSQLRewardStakeInputs(t, released)
		sql := dbadapter.New(released)
		require.NoError(t, ResolveReleasedRewardStakeInputs(
			t.Context(), sql, nil, false, 0,
		))

		rows, err := sql.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, rows, len(whole))
		pending, err := epochengine.LoadRewardStakeInputsPending(sql, nil)
		require.NoError(t, err)
		require.Empty(t, pending.Entries)
	})

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: newRewardCalculationTestNodeConfig(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	fxAttachLedgerDB(t, ls)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	loaded, err := ls.ledgerDB().GetRewardStakeInputs(epoch, nil)
	require.NoError(t, err)
	require.Len(t, loaded, len(whole))
	incomplete, err := ls.RewardStakeInputsIncompleteCommitted(epoch, epoch+1)
	require.NoError(t, err)
	require.False(t, incomplete)
}
