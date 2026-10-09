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

package ledgerstate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// accountCredTagKey is the stake-credential tag used by every account built
// in this file: all test accounts are key-hash credentials.
const accountCredTagKey uint8 = 0

// accountCertStateData builds CertStateData ([VState, PState, DState]) whose
// DState holds exactly one Conway-shaped account entry (reward, deposit) for
// stakingKey, with empty VState/PState/DRep maps.
func accountCertStateData(
	t *testing.T,
	stakingKey []byte,
	reward, deposit uint64,
) cbor.RawMessage {
	t.Helper()
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)
	vState, err := cbor.Encode([]any{cbor.RawMessage(emptyMap)})
	require.NoError(t, err)
	pState, err := cbor.Encode([]any{cbor.RawMessage(emptyMap)})
	require.NoError(t, err)

	credMap := encodeCredentialMapEntry(
		t,
		testCredentialKey{Type: 0, Hash: toFixed28(stakingKey)},
		[]any{reward, deposit},
	)
	dStateAccounts, err := cbor.Encode([]any{
		cbor.RawMessage(credMap), cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)
	dState, err := cbor.Encode([]any{cbor.RawMessage(dStateAccounts)})
	require.NoError(t, err)
	certState, err := cbor.Encode([]any{
		cbor.RawMessage(vState),
		cbor.RawMessage(pState),
		cbor.RawMessage(dState),
	})
	require.NoError(t, err)
	return certState
}

// rewardAddressFromStakingKey builds the reward (stake) address for a
// key-hash credential, matching the 0xe1 header used in
// ledger/reward_withdrawal_validation_test.go.
func rewardAddressFromStakingKey(
	t *testing.T,
	stakingKey []byte,
) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe1}, stakingKey...),
	)
	require.NoError(t, err)
	return addr
}

// applyWithdrawalTransaction builds a minimal, valid transaction that spends
// (inputTxID, inputIdx) and withdraws amount from rewardAddr's reward
// balance, applying it through database.Database.SetTransaction -- the
// ordinary block-apply path -- at the given slot.
func applyWithdrawalTransaction(
	t *testing.T,
	db *database.Database,
	txSeed byte,
	inputTxID []byte,
	inputIdx uint32,
	slot uint64,
	rewardAddr lcommon.Address,
	amount uint64,
) error {
	t.Helper()

	input, err := mockledger.NewSimpleTransactionInput(inputTxID, inputIdx)
	require.NoError(t, err)

	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)

	// WithWithdrawals is declared on the concrete *MockTransaction and is not
	// part of the TransactionBuilder interface the other With* methods
	// return, so the chain cannot pass through it; call it on the concrete
	// builder directly instead.
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(bytes.Repeat([]byte{txSeed}, 32))
	builder.WithInputs(input)
	builder.WithOutputs(output)
	builder.WithFee(200_000)
	builder.WithWithdrawals(map[*lcommon.Address]uint64{&rewardAddr: amount})
	tx, err := builder.Build()
	require.NoError(t, err)

	var txHashArray [32]byte
	copy(txHashArray[:], tx.Hash().Bytes())
	blockHash := bytes.Repeat([]byte{txSeed}, 32)
	var blockHashArray [32]byte
	copy(blockHashArray[:], blockHash)
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHashArray: {
				BlockSlot:  slot,
				BlockHash:  blockHashArray,
				ByteLength: 1,
			},
		},
		UtxoOffsets: map[database.UtxoRef]database.CborOffset{
			{TxId: txHashArray, OutputIdx: 0}: {
				BlockSlot:  slot,
				BlockHash:  blockHashArray,
				ByteLength: 1,
			},
		},
	}

	point := ocommon.Point{Slot: slot, Hash: blockHash}
	return db.SetTransaction(context.Background(), tx, point, 0, 0, nil, nil, offsets, nil)
}

// TestImportLedgerStateCatchUpRollsBackPostAnchorAccountRewardCredit covers
// a reward credit and a reward withdrawal applied after a Mithril snapshot's
// anchor. ImportLedgerState must reverse both the credit and the withdrawal
// journal (account_reward_delta, account_withdrawal_witness) before
// cert-state import overwrites account.reward from the snapshot --
// otherwise the credit and withdrawal amounts are silently dropped when
// ordinary replay re-applies them, because AddAccountRewardByCredential and
// the withdrawal path both no-op on an INSERT ... ON CONFLICT DO NOTHING
// hit against the stale journal row left behind by the first sync.
func TestImportLedgerStateCatchUpRollsBackPostAnchorAccountRewardCredit(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x81}, 28),
		bytes.Repeat([]byte{0x82}, 28),
	)
	// inlineUTxOMap keys its single entry's tx hash as 0x40 repeated 32
	// times (see inlineUTxOMap in import_test.go).
	utxoTxID := bytes.Repeat([]byte{0x40}, 32)

	stakingKey := bytes.Repeat([]byte{0xaa}, 28)
	const anchorReward = uint64(1_000_000)
	const anchorDeposit = uint64(2_000_000)
	certData := accountCertStateData(t, stakingKey, anchorReward, anchorDeposit)

	newImportConfig := func(tipSlot uint64) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				CertStateData:       certData,
				Epoch:               tipSlot / 1_000,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Bootstrap: the account is live at anchor slot 100000 with the
	// snapshot's reward balance.
	const anchorSlot = 100_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))
	acctAfterBootstrap, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, anchorReward, uint64(acctAfterBootstrap.Reward),
		"precondition: account carries the snapshot's reward at bootstrap",
	)

	// 2. A real post-anchor epoch boundary credits the account (ordinary
	// delegator reward).
	const creditAmount = uint64(300_000)
	const creditSlot = uint64(100_200)
	creditSourceHash := []byte{0xc1}
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))

	// 3. A real post-anchor transaction withdraws part of the reward
	// balance.
	const withdrawAmount = uint64(200_000)
	const withdrawSlot = uint64(100_500)
	rewardAddr := rewardAddressFromStakingKey(t, stakingKey)
	const txSeed = 0x51
	require.NoError(t, applyWithdrawalTransaction(
		t, db, txSeed, utxoTxID, 0, withdrawSlot, rewardAddr, withdrawAmount,
	))

	acctBeforeCatchUp, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	wantLocalReward := anchorReward + creditAmount - withdrawAmount
	require.Equal(
		t, wantLocalReward, uint64(acctBeforeCatchUp.Reward),
		"precondition: local credit and withdrawal applied",
	)

	// 4. Re-import the same anchor; the snapshot still reports the
	// anchor-time reward.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// 5. Replay the credit and the withdrawal, as ordinary chain replay
	// would after a catch-up import.
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))
	require.NoError(t, applyWithdrawalTransaction(
		t, db, txSeed, utxoTxID, 0, withdrawSlot, rewardAddr, withdrawAmount,
	))

	acctAfterReplay, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, wantLocalReward, uint64(acctAfterReplay.Reward),
		"the re-import must roll back the stale reward journal so replay "+
			"re-applies the credit and withdrawal instead of leaving the "+
			"account frozen at the snapshot's anchor-time reward",
	)
}

// TestImportLedgerStateReconcileCatchUpRollsBackPostAnchorAccountRewardCredit
// is the same case with the second import run as Reconcile: true, so a
// genuine catch-up sync (not only the ordinary resumed-sync shape) is also
// covered by the single DeleteAccountRewardsAfterSlot call site in
// ImportLedgerState.
func TestImportLedgerStateReconcileCatchUpRollsBackPostAnchorAccountRewardCredit(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x83}, 28),
		bytes.Repeat([]byte{0x84}, 28),
	)
	utxoTxID := bytes.Repeat([]byte{0x40}, 32)
	govStateTxHash := bytes.Repeat([]byte{0x95}, 32)

	stakingKey := bytes.Repeat([]byte{0xbb}, 28)
	const anchorReward = uint64(4_000_000)
	const anchorDeposit = uint64(2_000_000)
	certData := accountCertStateData(t, stakingKey, anchorReward, anchorDeposit)

	newImportConfig := func(tipSlot uint64, reconcile bool) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database:  db,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			Reconcile: reconcile,
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				CertStateData:       certData,
				GovStateData:        testGovStateData(t, govStateTxHash, tipSlot/1_000),
				Epoch:               tipSlot / 1_000,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Bootstrap (Reconcile: false).
	const anchorSlot = 100_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, false),
	))

	// 2. A real post-anchor credit and withdrawal, as above.
	const creditAmount = uint64(500_000)
	const creditSlot = uint64(100_200)
	creditSourceHash := []byte{0xc2}
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))

	const withdrawAmount = uint64(100_000)
	const withdrawSlot = uint64(100_500)
	rewardAddr := rewardAddressFromStakingKey(t, stakingKey)
	const txSeed = 0x53
	require.NoError(t, applyWithdrawalTransaction(
		t, db, txSeed, utxoTxID, 0, withdrawSlot, rewardAddr, withdrawAmount,
	))

	wantLocalReward := anchorReward + creditAmount - withdrawAmount
	acctBeforeCatchUp, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, wantLocalReward, uint64(acctBeforeCatchUp.Reward),
		"precondition: local credit and withdrawal applied",
	)

	// 3. A literal catch-up import: Reconcile: true, same anchor.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, true),
	))

	// 4. Replay the credit and withdrawal.
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))
	require.NoError(t, applyWithdrawalTransaction(
		t, db, txSeed, utxoTxID, 0, withdrawSlot, rewardAddr, withdrawAmount,
	))

	acctAfterReplay, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, wantLocalReward, uint64(acctAfterReplay.Reward),
		"the reconcile catch-up must roll back the stale reward journal so "+
			"replay re-applies the credit and withdrawal instead of leaving "+
			"the account frozen at the snapshot's anchor-time reward",
	)
}

// TestImportLedgerStateCatchUpRollsBackPostAnchorPostSnapshotRewardCredit
// covers a non-ordinary credit path: a POOLREAP deposit refund or an enacted
// treasury withdrawal/MIR-style credit, both of which funnel through
// AddPostSnapshotAccountRewardByCredential rather than
// AddAccountRewardByCredential. The underlying journal row and ON CONFLICT
// DO NOTHING short-circuit are identical, so this exercises the same defect
// through a different, non-delegator-reward call path.
func TestImportLedgerStateCatchUpRollsBackPostAnchorPostSnapshotRewardCredit(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x85}, 28),
		bytes.Repeat([]byte{0x86}, 28),
	)

	stakingKey := bytes.Repeat([]byte{0xcc}, 28)
	const anchorReward = uint64(7_000_000)
	const anchorDeposit = uint64(500_000_000)
	certData := accountCertStateData(t, stakingKey, anchorReward, anchorDeposit)

	newImportConfig := func(tipSlot uint64) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				CertStateData:       certData,
				Epoch:               tipSlot / 1_000,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Bootstrap: the account is live at anchor slot 100000 with the
	// snapshot's reward balance.
	const anchorSlot = 100_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// 2. A real post-anchor epoch boundary credits a POOLREAP pool-deposit
	// refund (or an enacted treasury withdrawal/MIR credit): both apply
	// through AddPostSnapshotAccountRewardByCredential. Kept below
	// anchorReward: rolling back a credit at the wrong point relative to
	// cert-state import computes current-minus-amount against whichever
	// value cert-state import last wrote, so a refund this size turns a
	// wrong-order mutation into an observably wrong balance instead of an
	// underflow error masking the same defect.
	const refundAmount = uint64(500_000)
	const refundSlot = uint64(100_200)
	poolKeyHash := bytes.Repeat([]byte{0xdd}, 28)
	require.NoError(t, db.AddPostSnapshotAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, refundAmount, refundSlot,
		poolKeyHash, nil,
	))

	acctBeforeCatchUp, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	wantLocalReward := anchorReward + refundAmount
	require.Equal(
		t, wantLocalReward, uint64(acctBeforeCatchUp.Reward),
		"precondition: local post-snapshot credit applied",
	)

	// 3. Re-import the same anchor; the snapshot still reports the
	// anchor-time reward.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// 4. Replay the refund, as ordinary POOLREAP/treasury-withdrawal
	// processing would after a catch-up import.
	require.NoError(t, db.AddPostSnapshotAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, refundAmount, refundSlot,
		poolKeyHash, nil,
	))

	acctAfterReplay, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, wantLocalReward, uint64(acctAfterReplay.Reward),
		"the re-import must roll back the stale post-snapshot reward "+
			"journal so replay re-applies the refund instead of leaving the "+
			"account frozen at the snapshot's anchor-time reward",
	)
}

// TestImportLedgerStateRepairAfterPreFixImportDoesNotUnderflow covers a
// database that already went through one import with the account-reward
// journal bug still present: a catch-up or reconcile import that
// overwrites account.reward from the snapshot without clearing the stale
// post-anchor journal first (the pre-fix behavior importCertState alone
// reproduces, since the journal cleanup lives in ImportLedgerState's
// wrapper around it, not in importCertState itself). account.reward is
// left at the snapshot's anchor value with no trace that it never
// included the dropped credit.
//
// A second, fixed import must not try to reverse that credit by
// subtracting it from the current balance: the current balance never
// held it, and a credit larger than the anchor-time balance -- a POOLREAP
// deposit refund routinely is -- underflows that subtraction and fails
// the whole import outright, a hard failure that is worse than the
// silent data loss it replaces. The fixed import must instead let the
// snapshot's value stand and only clear the stale journal, so a
// subsequent replay of the dropped credit lands on the correct total.
func TestImportLedgerStateRepairAfterPreFixImportDoesNotUnderflow(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x87}, 28),
		bytes.Repeat([]byte{0x88}, 28),
	)
	stakingKey := bytes.Repeat([]byte{0xee}, 28)
	// Deliberately larger than anchorReward: a POOLREAP refund is a whole
	// pool deposit and routinely exceeds whatever reward balance an
	// account happened to hold at the snapshot's anchor.
	const anchorReward = uint64(100_000)
	const creditAmount = uint64(500_000)
	const creditSlot = uint64(100_200)
	certData := accountCertStateData(t, stakingKey, anchorReward, 2_000_000)
	creditSourceHash := []byte{0xc3}

	newCfg := func() ImportConfig {
		nonce := make([]byte, 32)
		return ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				CertStateData:       certData,
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           make([]EraBound, EraConway+1),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      100_000,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) { return 1, 1_000, nil },
		}
	}

	// 1. Bootstrap: the account is live at the anchor with the snapshot's
	// reward balance.
	require.NoError(t, ImportLedgerState(context.Background(), newCfg()))

	// 2. A real post-anchor credit (e.g. a POOLREAP refund) is applied
	// locally.
	require.NoError(t, db.AddPostSnapshotAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))

	// 3. Simulate the pre-fix import: cert-state overwrites account.reward
	// from the snapshot with no journal cleanup at all. This is exactly
	// what importCertState alone does -- the cleanup lives in
	// ImportLedgerState's wrapper around it.
	preFixCfg := newCfg()
	_, _, err = importCertState(
		context.Background(), preFixCfg, 100_000, func(ImportProgress) {},
	)
	require.NoError(t, err)
	acctAfterPreFixImport, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, anchorReward, uint64(acctAfterPreFixImport.Reward),
		"precondition: the pre-fix import reset reward to the anchor value "+
			"with the credit's journal row still present",
	)

	// 4. The repair: a fixed ImportLedgerState run at the same anchor must
	// not fail. The stale journal row's credit (500,000) exceeds the
	// current balance (100,000, the pre-fix import's anchor-reset value),
	// so reversing it by subtraction would underflow.
	require.NoError(t, ImportLedgerState(context.Background(), newCfg()))
	acctAfterRepair, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, anchorReward, uint64(acctAfterRepair.Reward),
		"the repair must leave the snapshot's own anchor-time value in "+
			"place rather than attempting to reverse a credit the current "+
			"balance never held",
	)

	// 5. Replaying the dropped credit must land on the correct total: the
	// repair's journal cleanup must have cleared the stale row, or this
	// insert no-ops against it exactly as it did before the repair.
	require.NoError(t, db.AddPostSnapshotAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))
	acctAfterReplay, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, anchorReward+creditAmount, uint64(acctAfterReplay.Reward),
		"replaying the dropped credit after the repair must correctly "+
			"land on anchor reward plus the credit",
	)
}

// TestImportLedgerStateCatchUpLeavesUncoveredAccountUntouched covers a
// credential registered strictly after the anchor, so absent from the
// snapshot's own cert-state account list: cert-state import never writes
// this account's row, so nothing overwrites its balance, and the journal
// rollback must leave it -- and its journal rows -- completely alone.
// Deleting its journal unconditionally (rather than scoping the delete to
// the accounts cert-state import actually covers) would let a later
// replay of the same credit double-apply it, since the account's current
// balance already reflects it correctly.
func TestImportLedgerStateCatchUpLeavesUncoveredAccountUntouched(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x89}, 28),
		bytes.Repeat([]byte{0x8a}, 28),
	)
	coveredKey := bytes.Repeat([]byte{0xf1}, 28)
	uncoveredKey := bytes.Repeat([]byte{0xf2}, 28)
	const anchorReward = uint64(1_000_000)
	certData := accountCertStateData(t, coveredKey, anchorReward, 2_000_000)

	newCfg := func() ImportConfig {
		nonce := make([]byte, 32)
		return ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				CertStateData:       certData,
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           make([]EraBound, EraConway+1),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      100_000,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) { return 1, 1_000, nil },
		}
	}

	// 1. Bootstrap: only coveredKey is in the snapshot.
	require.NoError(t, ImportLedgerState(context.Background(), newCfg()))

	// 2. uncoveredKey registers and earns a reward credit strictly after
	// the anchor -- the real-chain equivalent of a stake registration
	// certificate followed by a reward credit, neither of which the
	// anchor's snapshot can know about.
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey:    uncoveredKey,
		CredentialTag: accountCredTagKey,
		Active:        true,
		Reward:        types.Uint64(0),
		AddedSlot:     100_100,
	}))
	const creditAmount = uint64(300_000)
	const creditSlot = uint64(100_200)
	creditSourceHash := []byte{0xc4}
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, uncoveredKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))
	uncoveredBeforeCatchUp, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, uncoveredKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(t, creditAmount, uint64(uncoveredBeforeCatchUp.Reward))

	// 3. Catch-up at the same anchor; the snapshot still only names
	// coveredKey.
	require.NoError(t, ImportLedgerState(context.Background(), newCfg()))

	// 4. uncoveredKey's balance must be exactly as it was: nothing
	// overwrote it, so nothing needed reconciling.
	uncoveredAfterCatchUp, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, uncoveredKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, creditAmount, uint64(uncoveredAfterCatchUp.Reward),
		"an account absent from the snapshot's own account list must keep "+
			"its current balance untouched by the journal rollback",
	)

	// 5. Replaying the same credit must no-op against the surviving
	// journal row, not double-apply it. If the rollback had deleted
	// uncoveredKey's journal row despite it being absent from the
	// snapshot, this would incorrectly double the balance.
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, uncoveredKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))
	uncoveredAfterReplay, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, uncoveredKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, creditAmount, uint64(uncoveredAfterReplay.Reward),
		"replaying the same credit against a surviving journal row must "+
			"no-op, not double-apply it",
	)

	// coveredKey must still be correctly reconciled regardless.
	coveredAfterCatchUp, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, coveredKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(t, anchorReward, uint64(coveredAfterCatchUp.Reward))
}

// resumeImportConfig builds an ImportConfig for a one-account snapshot
// anchored at slot 100000, with resume tracking under importKey.
func resumeImportConfig(
	t *testing.T,
	db *database.Database,
	addrSeed byte,
	stakingKey []byte,
	anchorReward uint64,
	importKey string,
) ImportConfig {
	t.Helper()
	nonce := make([]byte, 32)
	return ImportConfig{
		Database:  db,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		ImportKey: importKey,
		State: &RawLedgerState{
			UTxOData: inlineUTxOMap(
				t,
				buildShelleyAddr(
					0, 1,
					bytes.Repeat([]byte{addrSeed}, 28),
					bytes.Repeat([]byte{addrSeed + 1}, 28),
				),
				[]uint64{5_000_000},
			),
			CertStateData: accountCertStateData(
				t, stakingKey, anchorReward, 2_000_000,
			),
			Epoch:               100,
			EraIndex:            EraConway,
			EraBounds:           make([]EraBound, EraConway+1),
			EpochNonce:          nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			Tip: &SnapshotTip{
				Slot:      100_000,
				BlockHash: make([]byte, 32),
			},
		},
		EpochLength: func(uint) (uint, uint, error) { return 1, 1_000, nil },
	}
}

// TestImportLedgerStateResumePastCertStateRollsBackJournal covers an import
// resumed from a cert-state checkpoint whose cert-state phase overwrote
// account.reward without the journal cleanup. The resumed run skips the
// cert-state phase, so it must still clear the stale post-anchor journal
// rows for the snapshot's accounts; otherwise replay of the dropped credit
// no-ops against the surviving row.
func TestImportLedgerStateResumePastCertStateRollsBackJournal(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	stakingKey := bytes.Repeat([]byte{0xd1}, 28)
	const anchorReward = uint64(1_000_000)
	const creditAmount = uint64(300_000)
	const creditSlot = uint64(100_200)
	creditSourceHash := []byte{0xc5}
	const importKey = "resume:100000"

	// 1. Bootstrap without resume tracking.
	require.NoError(t, ImportLedgerState(
		context.Background(),
		resumeImportConfig(t, db, 0x8b, stakingKey, anchorReward, ""),
	))

	// 2. A post-anchor credit is applied locally.
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))

	// 3. An interrupted import without the cleanup: cert-state overwrote
	// account.reward from the snapshot and the run checkpointed cert-state
	// before failing in a later phase.
	cfg := resumeImportConfig(t, db, 0x8b, stakingKey, anchorReward, importKey)
	_, _, err = importCertState(
		context.Background(), cfg, 100_000, func(ImportProgress) {},
	)
	require.NoError(t, err)
	require.NoError(t, setCheckpoint(context.Background(), cfg, models.ImportPhaseCertState))
	acct, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, anchorReward, uint64(acct.Reward),
		"precondition: reward reset to the anchor value, journal row kept",
	)

	// 4. Resume the same import.
	require.NoError(t, ImportLedgerState(context.Background(), cfg))

	// 5. Replay of the credit must land on top of the anchor value.
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))
	acct, err = db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, anchorReward+creditAmount, uint64(acct.Reward),
		"a resumed import past cert-state must clear the stale journal so "+
			"replay re-applies the credit",
	)
}

// TestImportLedgerStateCompletedCheckpointKeepsJournal covers a re-run of an
// import whose checkpoint is already at tip. Every phase is skipped and no
// account balance is overwritten, so post-anchor journal rows recorded
// since that import completed are legitimate and must survive; deleting
// them would let replay double-apply a credit the balance already holds.
func TestImportLedgerStateCompletedCheckpointKeepsJournal(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	stakingKey := bytes.Repeat([]byte{0xd2}, 28)
	const anchorReward = uint64(1_000_000)
	const creditAmount = uint64(300_000)
	const creditSlot = uint64(100_200)
	creditSourceHash := []byte{0xc6}
	cfg := resumeImportConfig(
		t, db, 0x8d, stakingKey, anchorReward, "completed:100000",
	)

	// 1. A completed import leaves its checkpoint at tip.
	require.NoError(t, ImportLedgerState(context.Background(), cfg))
	cp, err := db.Metadata().GetImportCheckpoint(cfg.ImportKey, nil)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, models.ImportPhaseTip, cp.Phase)

	// 2. The node then applies a post-anchor credit.
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))

	// 3. Re-running the same import skips every phase.
	require.NoError(t, ImportLedgerState(context.Background(), cfg))

	// 4. Replay of the credit must no-op against the surviving journal row.
	require.NoError(t, db.AddAccountRewardByCredential(
		context.Background(), accountCredTagKey, stakingKey, creditAmount, creditSlot,
		creditSourceHash, nil,
	))
	acct, err := db.GetAccountByCredential(
		context.Background(), accountCredTagKey, stakingKey, false, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t, anchorReward+creditAmount, uint64(acct.Reward),
		"a completed import's re-run must leave the journal intact",
	)
}
