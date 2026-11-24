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
	"context"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type overlayTxn struct{ ledgerdb.Txn }

func (overlayTxn) IsCommitted() bool { return true }
func (overlayTxn) OnFinish(func())   {}

// overlayLedger returns a ledger whose committed-but-unapplied batches are
// the given ones, in order.
func overlayLedger(batches ...*utxoSetBatch) *LedgerState {
	ls := &LedgerState{utxoSetBatches: map[ledgerdb.Txn]*utxoSetBatch{}}
	for i, b := range batches {
		t := &overlayTxn{}
		b.txn = t
		b.seq = uint64(i + 1)
		ls.utxoSetBatches[t] = b
	}
	return ls
}

func rollbackBatch(slot uint64) *utxoSetBatch {
	return &utxoSetBatch{rollbackTo: &ocommon.Point{Slot: slot}}
}

// A staged change older than a later rollback's target survives it, even when
// a newer change to the same key does not.

type overlayDRepReader struct{ DRepReader }

func (overlayDRepReader) Drep(uint8, []byte) (*models.Drep, error)           { return nil, nil }
func (overlayDRepReader) DrepAt(uint8, []byte, uint64) (*models.Drep, error) { return nil, nil }
func (overlayDRepReader) DormantEpochs() (uint64, error)                     { return 0, nil }
func (overlayDRepReader) DormantEpochsAt(uint64) (uint64, error)             { return 0, nil }
func (overlayDRepReader) DRepLinkLive(_, _ models.StakeCredentialRef) (bool, error) {
	return false, nil
}

func (overlayDRepReader) DRepLinkLiveAt(_, _ models.StakeCredentialRef, _ uint64) (bool, error) {
	return false, nil
}

func TestDRepViewsKeepSurvivingStagedChangeAfterRollback(t *testing.T) {
	t.Parallel()
	cred := []byte{1, 2, 3}
	drep := models.StakeCredentialRef{Tag: 0, Key: cred}
	stake := models.StakeCredentialRef{Tag: 0, Key: []byte{9}}
	ls := overlayLedger(
		&utxoSetBatch{
			dreps:     []DRepChange{{Slot: 50, Drep: models.Drep{Credential: cred, Active: true}}},
			dormancy:  []DormancyChange{{Slot: 50, Epochs: 3}},
			drepLinks: []DRepLinkChange{{Slot: 50, Drep: drep, Stake: stake, Live: true}},
		},
		&utxoSetBatch{
			dreps:     []DRepChange{{Slot: 150, Drep: models.Drep{Credential: cred, Active: false}}},
			dormancy:  []DormancyChange{{Slot: 150, Epochs: 4}},
			drepLinks: []DRepLinkChange{{Slot: 150, Drep: drep, Stake: stake, Live: false}},
		},
		rollbackBatch(100),
	)
	r := overlayDRepReader{}

	d, err := ls.drepView(r, nil, 0, cred)
	require.NoError(t, err)
	assert.True(t, d != nil && d.Active, "the registration at slot 50 survives a rollback to 100")

	n, err := ls.dormancyView(r, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), n, "the counter staged at slot 50 survives")

	live, err := ls.drepLinkView(r, nil, drep, stake)
	require.NoError(t, err)
	assert.True(t, live, "the link staged live at slot 50 survives")
}

type overlayDepositReader struct{ RegistrationHistoryReader }

func (overlayDepositReader) DRepDeposit(uint8, []byte) (*uint64, error) { return nil, nil }
func (overlayDepositReader) DRepDepositAt(uint8, []byte, uint64) (*uint64, error) {
	return nil, nil
}

func TestDRepDepositViewKeepsSurvivingStagedChangeAfterRollback(t *testing.T) {
	t.Parallel()
	cred := []byte{1, 2, 3}
	first, second := uint64(500), uint64(600)
	ls := overlayLedger(
		&utxoSetBatch{drepDeposits: []DRepDepositChange{{Slot: 50, Credential: cred, Deposit: &first}}},
		&utxoSetBatch{drepDeposits: []DRepDepositChange{{Slot: 150, Credential: cred, Deposit: &second}}},
		rollbackBatch(100),
	)
	got, err := ls.drepDepositView(overlayDepositReader{}, nil, 0, cred)
	require.NoError(t, err)
	require.NotNil(t, got, "the deposit staged at slot 50 survives")
	require.Equal(t, first, *got)
}

type overlayPoolReader struct{ PoolReader }

func (overlayPoolReader) PoolState([]byte) (*PoolState, error)           { return nil, nil }
func (overlayPoolReader) PoolStateAt([]byte, uint64) (*PoolState, error) { return nil, nil }

func TestPoolViewKeepsSurvivingStagedChangeAfterRollback(t *testing.T) {
	t.Parallel()
	pool := []byte{7, 7}
	first, second := uint64(1), uint64(2)
	ls := overlayLedger(
		&utxoSetBatch{pools: []PoolChange{{Slot: 50, State: PoolState{Pool: pool, DepositHeld: &first}}}},
		&utxoSetBatch{pools: []PoolChange{{Slot: 150, State: PoolState{Pool: pool, DepositHeld: &second}}}},
		rollbackBatch(100),
	)
	st, err := ls.poolView(overlayPoolReader{}, nil, pool)
	require.NoError(t, err)
	require.NotNil(t, st, "the pool staged at slot 50 survives")
	require.Equal(t, first, *st.DepositHeld)
}

func TestPoolsChangedAfterIgnoresOnlyTheReadersBatch(t *testing.T) {
	t.Parallel()
	const slot = uint64(50)
	ownTxn := &overlayTxn{}
	own := &utxoSetBatch{
		txn:   ownTxn,
		seq:   2,
		pools: []PoolChange{{Slot: slot + 1}},
	}
	ls := overlayLedger(&utxoSetBatch{
		pools: []PoolChange{{Slot: slot - 1}},
	})
	ls.utxoSetBatches[ownTxn] = own

	require.False(t, ls.poolsChangedAfter(ownTxn, slot))

	foreignTxn := &overlayTxn{}
	ls.utxoSetBatches[foreignTxn] = &utxoSetBatch{
		txn:   foreignTxn,
		seq:   3,
		pools: []PoolChange{{Slot: slot + 1}},
	}
	require.True(t, ls.poolsChangedAfter(ownTxn, slot))
}

// A rollback drops staged changes after its own target; only the committed
// read keeps the lowest target seen.

type overlayRewardBlocksReader struct{ RewardBlocksReader }

func (overlayRewardBlocksReader) RewardBlocksEpoch(uint64, *uint64) (RewardEpochBlocks, error) {
	return RewardEpochBlocks{
		CountsKnown: true, AllKnown: true, FeesKnown: true,
		Pools: map[string]uint64{}, AllPools: map[string]uint64{},
	}, nil
}

func TestRewardBlocksViewDropsByCurrentRollbackTarget(t *testing.T) {
	t.Parallel()
	ls := overlayLedger(
		rollbackBatch(50),
		&utxoSetBatch{rewardBlocks: []RewardBlockChange{
			{Slot: 60, Epoch: 1, Pool: []byte{9}},
			{Slot: 60, Epoch: 1, Fee: 7},
		}},
		rollbackBatch(100),
	)
	v, err := ls.rewardBlocksView(overlayRewardBlocksReader{}, nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), v.Total, "the block at slot 60 survives a rollback to 100")
	require.Equal(t, uint64(7), v.Fees)
}

type overlayOpCertReader struct{ OpCertReader }

func (overlayOpCertReader) OpCertState([]byte) (*OpCertState, error)           { return nil, nil }
func (overlayOpCertReader) OpCertStateAt([]byte, uint64) (*OpCertState, error) { return nil, nil }

func TestOpCertViewDropsByCurrentRollbackTarget(t *testing.T) {
	t.Parallel()
	pool := []byte{4, 4}
	ls := overlayLedger(
		rollbackBatch(50),
		&utxoSetBatch{opCerts: []OpCertChange{{Slot: 60, State: OpCertState{Pool: pool, Slot: 60}}}},
		rollbackBatch(100),
	)
	st, err := ls.opCertView(overlayOpCertReader{}, nil, pool, nil)
	require.NoError(t, err)
	require.NotNil(t, st, "the counter staged at slot 60 survives a rollback to 100")
}

// A drained family's SQL write is skipped, so a write the set cannot stage
// must fail rather than return success.

func drainedStaging(f nodesettings.Family) stagingLedgerDB {
	ls := &LedgerState{}
	ls.config.Logger = slog.New(slog.DiscardHandler)
	return stagingLedgerDB{ls: ls}
}

func TestDrainedDRepWritesFailWhenSetCannotStage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cred := []byte{1}
	writes := map[string]func(s stagingLedgerDB) error{
		"UpdateDRepActivity": func(s stagingLedgerDB) error {
			return s.UpdateDRepActivity(ctx, 0, cred, 1, 2, 3, nil)
		},
		"InsertDrepIfAbsent": func(s stagingLedgerDB) error {
			return s.InsertDrepIfAbsent(ctx, 0, cred, 3, "", nil, true, nil)
		},
		"ResetDormantDRepEpochs": func(s stagingLedgerDB) error {
			return s.ResetDormantDRepEpochs(ctx, 3, nil)
		},
		"ClearDanglingDRepDelegations": func(s stagingLedgerDB) error {
			_, err := s.ClearDanglingDRepDelegations(ctx, 3, nil)
			return err
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := write(drainedStaging(nodesettings.FamilyDReps))
			require.ErrorIs(t, err, ErrLedgerStateUnserved)
		})
	}
}

func TestDrainedAccountWriteStopsSetWhenUnstageable(t *testing.T) {
	t.Parallel()
	s := drainedStaging(nodesettings.FamilyAccounts)
	err := s.AddAccountRewardsByCredential(
		context.Background(),
		[]models.AccountRewardCredit{{StakingKey: []byte{1}, Amount: 5, Slot: 3}},
		nil,
	)
	require.NoError(t, err)
	require.True(
		t, s.ls.utxoSetFailed.Load(),
		"an account credit the set cannot stage must stop the set",
	)
}

// drainedBumpSet is a set that serves the DRep and account readers of a
// drained ledger; its committed dormancy counter steps to 1 at bumpSlot.
type drainedBumpSet struct {
	LedgerChangesWriter
	AccountReader
	DRepReader
	bumpSlot uint64
}

func (drainedBumpSet) Dreps(func(*models.Drep) error) error { return nil }
func (drainedBumpSet) DormantEpochs() (uint64, error)       { return 1, nil }
func (d drainedBumpSet) DormantEpochsAt(slot uint64) (uint64, error) {
	if slot >= d.bumpSlot {
		return 1, nil
	}
	return 0, nil
}

// drainedBumpLedger returns the ledger and the transaction whose batch is the
// last one, registered up front so staging into it needs no database
// transaction.
func drainedBumpLedger(
	set drainedBumpSet,
	batches ...*utxoSetBatch,
) (stagingLedgerDB, ledgerdb.Txn) {
	own := &utxoSetBatch{}
	ls := overlayLedger(append(batches, own)...)
	ls.config.Logger = slog.New(slog.DiscardHandler)
	ls.config.UtxoSet = set
	return stagingLedgerDB{ls: ls}, own.txn
}

// A boundary bump a staged rollback undid is applied again when the
// boundary replays, in the same window, exactly once.
func TestDrainedDormantBumpReappliesAfterStagedRollbackOfStagedBump(t *testing.T) {
	t.Parallel()
	s, txn := drainedBumpLedger(
		drainedBumpSet{bumpSlot: 1 << 62},
		&utxoSetBatch{dormancy: []DormancyChange{{Slot: 200, Epochs: 1}}},
		rollbackBatch(150),
	)
	for range 2 {
		_, err := s.bumpDrainedDormantDRepExpiries(200, txn)
		require.NoError(t, err)
	}
	n, err := s.ls.dormancyView(s.ls.drepReader(txn), txn)
	require.NoError(t, err)
	require.Equal(t, uint64(1), n, "the replayed boundary bumps once")
}

func TestDrainedDormantBumpReappliesAfterStagedRollbackOfCommittedBump(t *testing.T) {
	t.Parallel()
	s, txn := drainedBumpLedger(drainedBumpSet{bumpSlot: 200}, rollbackBatch(150))
	for range 2 {
		_, err := s.bumpDrainedDormantDRepExpiries(200, txn)
		require.NoError(t, err)
	}
	n, err := s.ls.dormancyView(s.ls.drepReader(txn), txn)
	require.NoError(t, err)
	require.Equal(t, uint64(1), n, "the committed bump was rolled back, so the replay applies it")
}

func TestDrainedDormantBumpSkipsWhenBumpSurvivesRollback(t *testing.T) {
	t.Parallel()
	s, txn := drainedBumpLedger(
		drainedBumpSet{bumpSlot: 1 << 62},
		&utxoSetBatch{dormancy: []DormancyChange{{Slot: 200, Epochs: 1}}},
		rollbackBatch(250),
	)
	_, err := s.bumpDrainedDormantDRepExpiries(200, txn)
	require.NoError(t, err)
	n, err := s.ls.dormancyView(s.ls.drepReader(txn), txn)
	require.NoError(t, err)
	require.Equal(t, uint64(1), n, "a bump the rollback keeps is not applied twice")
}
