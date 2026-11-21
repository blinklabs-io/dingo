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
	"cmp"
	"context"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// fxMintBlock records the block pool minted at slot in db's LedgerDB set: its
// operational certificate counter and its count in the reward block totals of
// the epoch holding slot, which must already be seeded. A ledger applying the
// block resolves that epoch from its published epoch cache, which a seeding
// ledger does not have. Before the set opens the block goes to the metadata
// store, for the set to take over. It takes the arguments of
// UpdatePoolOpCertSequence so it stands where a fixture called that.
func fxMintBlock(
	t testing.TB,
	db *database.Database,
	ctx context.Context,
	pool lcommon.PoolKeyHash,
	sequence uint64,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	t.Helper()
	require.Nil(t, txn, "a seeded block is written outside any transaction")
	ldb := seedDB(t, db)
	if err := ldb.UpdatePoolOpCertSequence(
		ctx, pool, sequence, slot, nil,
	); err != nil {
		return err
	}
	if seedLedgerOf(db) == nil {
		// The set counts the block from the store's rows when it opens.
		return nil
	}
	epoch, err := ldb.GetEpochBySlot(slot, nil)
	if err != nil {
		return err
	}
	require.NotNil(t, epoch, "no seeded epoch holds slot %d", slot)
	fxSeed(t, db, LedgerChanges{RewardBlocks: []RewardBlockChange{{
		Slot: slot, Epoch: epoch.EpochId, Pool: bytes.Clone(pool.Bytes()),
	}}})
	return nil
}

// fxEditRewardPoolInput rewrites the reward pool input the set holds for pool
// in epoch as edit leaves it.
func fxEditRewardPoolInput(
	t testing.TB,
	db *database.Database,
	epoch uint64,
	pool []byte,
	edit func(*models.RewardPoolInput),
) {
	t.Helper()
	ldb := seedDB(t, db)
	inputs, err := ldb.GetRewardPoolInputs(epoch, nil)
	require.NoError(t, err)
	for _, in := range inputs {
		if !bytes.Equal(in.PoolKeyHash, pool) {
			continue
		}
		edit(in)
		require.NoError(t, ldb.SaveRewardPoolInputs(
			[]*models.RewardPoolInput{in}, nil,
		))
		return
	}
	require.FailNow(t, "the set holds no reward pool input to edit")
}

// fxEditRewardStakeInput rewrites the reward stake input the set holds for a
// key-hash credential delegated to pool in epoch as edit leaves it.
func fxEditRewardStakeInput(
	t testing.TB,
	db *database.Database,
	epoch uint64,
	pool []byte,
	stakingKey []byte,
	edit func(*models.RewardStakeInput),
) {
	t.Helper()
	ldb := seedDB(t, db)
	inputs, err := ldb.GetRewardStakeInputs(epoch, nil)
	require.NoError(t, err)
	for _, in := range inputs {
		if !bytes.Equal(in.PoolKeyHash, pool) ||
			!bytes.Equal(in.StakingKey, stakingKey) {
			continue
		}
		edit(in)
		require.NoError(t, ldb.SaveRewardStakeInputs(
			[]*models.RewardStakeInput{in}, nil,
		))
		return
	}
	require.FailNow(t, "the set holds no reward stake input to edit")
}

// fxEditRewardSnapshot rewrites the mark reward snapshot header the set holds
// for epoch as edit leaves it.
func fxEditRewardSnapshot(
	t testing.TB,
	db *database.Database,
	epoch uint64,
	edit func(*models.RewardSnapshot),
) {
	t.Helper()
	ldb := seedDB(t, db)
	snapshot, err := ldb.GetRewardSnapshot(epoch, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snapshot, "the set holds no reward snapshot to edit")
	edit(snapshot)
	require.NoError(t, ldb.SaveRewardSnapshot(snapshot, nil))
}

// fxSetAccountActive leaves the account of a credential registered or
// deregistered in db's LedgerDB set without recording a certificate for it.
func fxSetAccountActive(
	t testing.TB,
	db *database.Database,
	credentialTag uint8,
	stakingKey []byte,
	active bool,
) {
	t.Helper()
	account, err := seedDB(t, db).GetAccountByCredential(
		context.Background(), credentialTag, stakingKey, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	account.Active = active
	fxSeed(t, db, LedgerChanges{Accounts: []AccountChange{{
		Slot: account.AddedSlot, Account: *account,
	}}})
}

// fxStakeCert records a stake registration or deregistration certificate of
// a credential at slot in the registration history of db's LedgerDB set,
// leaving the account itself as it is. id tells the certificates of one
// fixture apart.
func fxStakeCert(
	t testing.TB,
	db *database.Database,
	id uint,
	credentialTag uint8,
	stakingKey []byte,
	slot uint64,
	registered bool,
) {
	t.Helper()
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	class := StakeClassOther
	if registered {
		class = StakeClassLegacy
	}
	fxSeed(t, db, LedgerChanges{StakeEvents: []StakeEventChange{{
		Ref: models.NewStakeCredentialRef(
			credentialTag, bytes.Clone(stakingKey),
		),
		Event: StakeEvent{
			Slot:       slot,
			Position:   uint64(id),
			Registered: registered,
			TxHash:     hash,
			TxSlot:     slot,
			Class:      class,
		},
	}}})
}

// fxDeregisterStake applies a stake deregistration certificate of a key-hash
// credential at slot to db's LedgerDB set, as the certificate's transaction
// does.
func fxDeregisterStake(
	t testing.TB,
	db *database.Database,
	stakingKey []byte,
	slot uint64,
) {
	t.Helper()
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the database has no open LedgerDB set")
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], slot)
	copy(txID, stakingKey[:8])
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithCertificates(&lcommon.StakeDeregistrationCertificate{
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(stakingKey),
		},
	})
	require.NoError(t, ls.stageAccountTransaction(
		nil, tx, slot, 0, txID, database.BatchedTxIngestOpts{}, nil,
	))
	require.False(t, ls.utxoSetFailed.Load(), "the set did not take the certificate")
}

// fxMinted is a block for fxMintBlocks to record.
type fxMinted struct {
	pool     lcommon.PoolKeyHash
	sequence uint64
	slot     uint64
}

// fxMintBlocks records blocks with fxMintBlock in slot order, the order a
// chain applies them in: the set counts a block only above the last slot it
// counted in the epoch.
func fxMintBlocks(t testing.TB, db *database.Database, blocks []fxMinted) {
	t.Helper()
	slices.SortStableFunc(blocks, func(a, b fxMinted) int {
		return cmp.Compare(a.slot, b.slot)
	})
	for _, b := range blocks {
		require.NoError(t, fxMintBlock(
			t, db, context.Background(), b.pool, b.sequence, b.slot, nil,
		))
	}
}

// fxSeedFee adds fee to the fee pot of the epoch holding slot in db's
// LedgerDB set, as a transaction applied at slot does. The epoch must already
// be seeded.
func fxSeedFee(t testing.TB, db *database.Database, slot, fee uint64) {
	t.Helper()
	epoch, err := seedDB(t, db).GetEpochBySlot(slot, nil)
	require.NoError(t, err)
	require.NotNil(t, epoch, "no seeded epoch holds slot %d", slot)
	fxSeed(t, db, LedgerChanges{RewardBlocks: []RewardBlockChange{{
		Slot: slot, Epoch: epoch.EpochId, Fee: fee,
	}}})
}

// fxCompleteSnapshot gives state the empty UTxO map and certificate state a
// fixture left out. A real snapshot carries both, empty or not: loading the
// map is what empties the set for the import, and the certificate state is
// where the import takes the dormant DRep epoch count the set then loads.
func fxCompleteSnapshot(t testing.TB, state *ledgerstate.RawLedgerState) {
	t.Helper()
	emptyMap := []byte{0xa0}
	if state.UTxOTablePath == "" && state.UTxOData == nil {
		state.UTxOData = emptyMap
	}
	if state.CertStateData != nil {
		return
	}
	// [VState, PState, DState], each with no entries.
	accounts, err := cbor.Encode([]any{
		cbor.RawMessage(emptyMap), cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)
	certState, err := cbor.Encode([]any{
		[]any{cbor.RawMessage(emptyMap)},
		[]any{cbor.RawMessage(emptyMap)},
		[]any{cbor.RawMessage(accounts)},
	})
	require.NoError(t, err)
	state.CertStateData = certState
}

// fxImportLedgerState imports cfg into a ledger whose LedgerDB set is
// already open, completing the snapshot as fxImportThenOpen does.
func fxImportLedgerState(
	t *testing.T,
	ls *LedgerState,
	cfg ledgerstate.ImportConfig,
) error {
	t.Helper()
	fxCompleteSnapshot(t, cfg.State)
	return importLedgerStateForTest(t, ls, cfg)
}

// fxRetiringPool puts a pool in db's LedgerDB set as its registration at
// regSlot and its retirement certificate at retireSlot leave it: registered
// with deposit held, and retiring at retireEpoch.
func fxRetiringPool(
	t testing.TB,
	db *database.Database,
	keyHash, rewardAccount []byte,
	deposit, regSlot, retireEpoch, retireSlot uint64,
) {
	t.Helper()
	reg := &models.PoolRegistration{
		PoolKeyHash:   bytes.Clone(keyHash),
		RewardAccount: bytes.Clone(rewardAccount),
		AddedSlot:     regSlot,
	}
	fxSeed(t, db, LedgerChanges{
		Pools: []PoolChange{
			{
				Slot: regSlot,
				State: PoolState{
					Pool:        bytes.Clone(keyHash),
					Params:      CleanPoolRegistration(reg),
					DepositHeld: &deposit,
				},
			},
			{
				Slot: retireSlot,
				State: PoolState{
					Pool:          bytes.Clone(keyHash),
					Params:        CleanPoolRegistration(reg),
					DepositHeld:   &deposit,
					RetiringEpoch: &retireEpoch,
				},
			},
		},
		PoolEvents: []PoolEventChange{
			{
				Pool:  bytes.Clone(keyHash),
				Event: PoolEvent{Slot: regSlot, Registration: reg},
			},
			{
				Pool: bytes.Clone(keyHash),
				Event: PoolEvent{
					Slot: retireSlot, RetiringEpoch: retireEpoch,
				},
			},
		},
	})
}
