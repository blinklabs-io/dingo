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

package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDB creates an in-memory database for tests.
func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "", // in-memory
		Logger:  logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		dbtest.CloseDatabase(db) //nolint:errcheck
	})
	return db
}

func newFileTestDB(t *testing.T) *database.Database {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		dbtest.CloseDatabase(db) //nolint:errcheck
	})
	return db
}

func addValidBackfillBlocks(t *testing.T, db *database.Database, count int) {
	t.Helper()
	addValidBackfillBlocksFrom(t, db, 0, count)
}

func addValidBackfillBlocksFrom(
	t *testing.T,
	db *database.Database,
	startSlot uint64,
	count int,
) {
	t.Helper()
	blocks, err := testfixtures.GenerateConwayChainAt(1, startSlot, count)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:   block.SlotNumber(),
			Hash:   block.Hash().Bytes(),
			Number: block.BlockNumber(),
			Cbor:   block.Cbor(),
			Type:   uint(block.Type()),
		}, nil))
	}
}

func TestBackfillProcessBlockGovernanceRecordsDRepActivityFromCertificateOnly(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	backfill := NewBackfill(db, nil, slog.Default())

	credentialBytes := bytes.Repeat([]byte{0xAB}, 28)
	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], credentialBytes)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		CredentialTag:     0,
		Credential:        credentialBytes,
		AddedSlot:         10,
		LastActivityEpoch: 5,
		ExpiryEpoch:       25,
		Active:            true,
	}))

	tx := mockledger.NewTransactionBuilder()
	tx.WithCertificates(&lcommon.UpdateDrepCertificate{
		CertType: uint(lcommon.CertificateTypeUpdateDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: credentialHash,
		},
	})
	tx.WithValid(true)
	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.DRepInactivityPeriod = 20

	txn := db.Transaction(true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return backfill.processBlockGovernance(
			tx,
			ocommon.NewPoint(1000, bytes.Repeat([]byte{0xCD}, 32)),
			100,
			&pparams,
			txn,
		)
	}))

	drep, err := db.GetDrepByCredential(0, credentialBytes, true, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), drep.LastActivityEpoch)
	// Backfill replays blocks below a Mithril anchor, whose DRepState expiry
	// already counts the dormant epochs replay never sees, so the activity
	// epoch is recorded and the stored expiry is kept rather than renewed to
	// 100 + 20 as the live ledger would.
	assert.Equal(t, uint64(25), drep.ExpiryEpoch)
}

func TestBackfillReplaysRegistrationBeforeHistoricalWithdrawal(
	t *testing.T,
) {
	t.Parallel()
	testBackfillReplaysRegistrationBeforeHistoricalWithdrawal(t, newTestDB)
}

func testBackfillReplaysRegistrationBeforeHistoricalWithdrawal(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	db := newDB(t)
	backfill := NewBackfill(db, nil, slog.Default())
	backfill.SetDelegatorInactivityEnabled(false)
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()

	stakeKeyBytes := bytes.Repeat([]byte{0x51}, lcommon.AddressHashSize)
	stakeKey := lcommon.NewBlake2b224(stakeKeyBytes)
	credential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: stakeKey,
	}
	registration := &lcommon.StakeRegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: credential,
	}
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x52}, lcommon.AddressHashSize),
		stakeKeyBytes,
	)
	require.NoError(t, err)
	_, err = db.GetAccountByCredential(0, stakeKeyBytes, true, nil)
	require.ErrorIs(t, err, models.ErrAccountNotFound)

	makeTransaction := func(
		id byte,
		certificates []lcommon.Certificate,
		withdrawals map[*lcommon.Address]uint64,
	) lcommon.Transaction {
		idBytes := bytes.Repeat([]byte{id}, 32)
		input, inputErr := mockledger.NewTransactionInputBuilder().
			WithTxId(bytes.Repeat([]byte{id + 1}, 32)).
			WithIndex(0).
			Build()
		require.NoError(t, inputErr)
		output, outputErr := mockledger.NewTransactionOutputBuilder().
			WithAddress(address.String()).
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, outputErr)

		tx := mockledger.NewTransactionBuilder().WithCertificates(certificates...)
		tx.WithId(idBytes)
		tx.WithInputs(input)
		tx.WithOutputs(output)
		tx.WithWithdrawals(withdrawals)
		tx.WithValid(true)
		built, buildErr := tx.Build()
		require.NoError(t, buildErr)
		return built
	}
	makeOffsets := func(
		tx lcommon.Transaction,
		slot uint64,
	) *database.BlockIngestionResult {
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		utxoOffsets := make(map[database.UtxoRef]database.CborOffset)
		for _, produced := range tx.Produced() {
			var producedTxID [32]byte
			copy(producedTxID[:], produced.Id.Id().Bytes())
			utxoOffsets[database.UtxoRef{
				TxId:      producedTxID,
				OutputIdx: produced.Id.Index(),
			}] = database.CborOffset{BlockSlot: slot, ByteLength: 1}
		}
		return &database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{
				txHash: {BlockSlot: slot, ByteLength: 1},
			},
			UtxoOffsets: utxoOffsets,
		}
	}
	process := func(
		tx lcommon.Transaction,
		slot uint64,
	) error {
		return backfill.processBlockTxsBatched(
			[]lcommon.Transaction{tx},
			ocommon.Point{
				Slot: slot,
				Hash: bytes.Repeat([]byte{byte(slot)}, 32),
			},
			0,
			eras.ConwayEraDesc.Id,
			nil,
			makeOffsets(tx, slot),
			acc,
			txn,
			nil,
			true,
		)
	}

	registrationTx := makeTransaction(
		0x61,
		[]lcommon.Certificate{registration},
		nil,
	)
	require.NoError(t, process(registrationTx, 100))

	withdrawalTx := makeTransaction(
		0x63,
		nil,
		map[*lcommon.Address]uint64{&address: 1},
	)
	require.NoError(t, process(withdrawalTx, 200))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	account, err := db.GetAccountByCredential(0, stakeKeyBytes, true, nil)
	require.NoError(t, err)
	assert.True(t, account.Active)
	count, err := db.CountAccountWithdrawalHistoryByCredential(
		0, stakeKeyBytes, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestBackfillProcessBlockGovernanceRecordsDRepActivityInDijkstra(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	backfill := NewBackfill(db, nil, slog.Default())

	credentialBytes := bytes.Repeat([]byte{0xBC}, 28)
	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], credentialBytes)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		CredentialTag:     0,
		Credential:        credentialBytes,
		AddedSlot:         10,
		LastActivityEpoch: 5,
		ExpiryEpoch:       25,
		Active:            true,
	}))

	tx := mockledger.NewTransactionBuilder()
	tx.WithCertificates(&lcommon.UpdateDrepCertificate{
		CertType: uint(lcommon.CertificateTypeUpdateDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: credentialHash,
		},
	})
	tx.WithValid(true)
	conwayPParams := mockledger.NewMockConwayProtocolParams()
	conwayPParams.DRepInactivityPeriod = 20
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conwayPParams,
	}

	txn := db.Transaction(true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return backfill.processBlockGovernance(
			tx,
			ocommon.NewPoint(1000, bytes.Repeat([]byte{0xCD}, 32)),
			100,
			pparams,
			txn,
		)
	}))

	drep, err := db.GetDrepByCredential(0, credentialBytes, true, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), drep.LastActivityEpoch)
	// Backfill replays blocks below a Mithril anchor, whose DRepState expiry
	// already counts the dormant epochs replay never sees, so the activity
	// epoch is recorded and the stored expiry is kept rather than renewed to
	// 100 + 20 as the live ledger would.
	assert.Equal(t, uint64(25), drep.ExpiryEpoch)
}

func closeTestDB(db *database.Database) error {
	return dbtest.CloseDatabase(db)
}

func TestBackfillBatchSizeDefaultAndOverride(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	require.Equal(t, DefaultBackfillBatchSize, bf.batchSize)
	require.NoError(t, bf.SetBatchSize(200))
	require.Equal(t, 200, bf.batchSize)
}

func TestBackfillSetBatchSizeRejectsInvalid(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	require.Error(t, bf.SetBatchSize(0))
	require.Error(t, bf.SetBatchSize(-1))
	require.Equal(t, DefaultBackfillBatchSize, bf.batchSize)
}

func TestNeedsBackfill_NoCheckpoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// No checkpoint and no blocks => nothing to backfill
	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.False(t, needed)
}

func TestNeedsBackfill_NoCheckpointWithBlocks(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Blocks present but no checkpoint => backfill is NOT needed.
	// Backfill is driven by the checkpoint alone. Normal-sync API
	// nodes have blocks but never run backfill, so the absence of
	// a checkpoint must mean "don't backfill".
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(i)
	}
	err := db.BlockCreate(models.Block{
		Slot: 100,
		Hash: hash,
		Cbor: []byte{0x82, 0x01},
		Type: 1,
	}, nil)
	require.NoError(t, err)

	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.False(t, needed)
}

func TestNeedsBackfill_IncompleteCheckpoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Create an incomplete checkpoint
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   5000,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	// Incomplete checkpoint => NeedsBackfill should return true
	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.True(t, needed)
}

func TestNeedsBackfill_CompletedCheckpoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Create a completed checkpoint
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   100000,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  true,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	// Completed checkpoint => NeedsBackfill should return false
	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.False(t, needed)
}

func TestRun_EmptyBlobStore(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// No blocks in blob store => Run should return nil immediately
	err := bf.Run(context.Background())
	require.NoError(t, err)

	// No checkpoint should have been created
	needed, needsErr := bf.NeedsBackfill()
	require.NoError(t, needsErr)
	assert.False(t, needed)
}

func TestRun_AlreadyCompleted(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Pre-create a completed checkpoint
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   100000,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  true,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	// Run should return immediately without error
	err = bf.Run(context.Background())
	require.NoError(t, err)
}

func TestRun_CancelledContext_EmptyBlobStore(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Create an incomplete checkpoint so Run will attempt work
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   0,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With an empty blob store (tipSlot=0) Run returns nil early
	// before reaching the iteration loop, even with a cancelled
	// context.
	err = bf.Run(ctx)
	require.NoError(t, err)
}

func TestRun_IncompleteCheckpointAtZeroStartsAtSlotZero(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   0,
		TotalSlots: 1,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(cp, nil))

	addValidBackfillBlocks(t, db, 2)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bf := NewBackfill(db, nil, logger)

	require.NoError(t, bf.Run(context.Background()))
	got, err := db.Metadata().GetBackfillCheckpoint(BackfillPhase, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(1), got.LastSlot)
}

func TestRun_EndSlotLeavesLaterBlocksForLedgerReplay(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      BackfillPhase,
			LastSlot:   0,
			TotalSlots: 2,
			StartedAt:  time.Now(),
			UpdatedAt:  time.Now(),
			Completed:  false,
		},
		nil,
	))
	addValidBackfillBlocks(t, db, 2)
	malformedHash := make([]byte, 32)
	malformedHash[0] = 3
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: 2,
		Hash: malformedHash,
		Cbor: []byte{0xff},
		Type: 1,
	}, nil))

	bf := NewBackfill(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bf.SetEndSlot(1)

	// If iteration crosses the configured end slot, parsing the malformed
	// block at slot 2 fails the run instead of leaving it for ledger replay.
	require.NoError(t, bf.Run(context.Background()))
	checkpoint, err := db.Metadata().GetBackfillCheckpoint(
		BackfillPhase,
		nil,
	)
	require.NoError(t, err)
	require.True(t, checkpoint.Completed)
	require.Equal(t, uint64(1), checkpoint.LastSlot)
	require.Equal(t, uint64(1), checkpoint.TotalSlots)
}

// TestRun_ResumeStopsAtRecordedMithrilAnchor covers a Mithril API backfill
// resumed by `dingo serve`, which calls Run without SetEndSlot. Blocks after
// the recorded ledger anchor belong to ledger replay: a historical pass over
// them journals each withdrawal without debiting the reward balance, and the
// replay then finds the journal row and skips the debit as well.
func TestRun_ResumeStopsAtRecordedMithrilAnchor(t *testing.T) {
	t.Parallel()
	testRun_ResumeStopsAtRecordedMithrilAnchor(t, newTestDB)
}

func testRun_ResumeStopsAtRecordedMithrilAnchor(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	db := newDB(t)
	blocks, err := testfixtures.GenerateConwayChainWithTransactions(2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	for _, block := range blocks {
		require.NotEmpty(t, block.Transactions())
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:   block.SlotNumber(),
			Hash:   block.Hash().Bytes(),
			Number: block.BlockNumber(),
			Cbor:   block.Cbor(),
			Type:   uint(block.Type()),
		}, nil))
	}
	anchor := blocks[0].SlotNumber()
	require.NoError(t, db.SetSyncState(
		"mithril_ledger_slot",
		strconv.FormatUint(anchor, 10),
		nil,
	))
	now := time.Now()
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:     BackfillPhase,
			StartedAt: now,
			UpdatedAt: now,
		},
		nil,
	))

	bf := NewBackfill(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, bf.Run(context.Background()))

	checkpoint, err := db.Metadata().GetBackfillCheckpoint(
		BackfillPhase,
		nil,
	)
	require.NoError(t, err)
	require.True(t, checkpoint.Completed)
	assert.Equal(t, anchor, checkpoint.LastSlot)
	assert.Equal(t, anchor, checkpoint.TotalSlots)
	for _, tx := range blocks[0].Transactions() {
		stored, err := db.Metadata().GetTransactionByHash(
			tx.Hash().Bytes(),
			nil,
		)
		require.NoError(t, err)
		assert.NotNil(t, stored, "anchor block transaction was not backfilled")
	}
	for _, tx := range blocks[1].Transactions() {
		stored, err := db.Metadata().GetTransactionByHash(
			tx.Hash().Bytes(),
			nil,
		)
		require.NoError(t, err)
		assert.Nil(t, stored, "post-anchor transaction was backfilled")
	}
}

// TestRun_RestoresSnapshotAccountDelegationAtAnchor covers delegation state
// the snapshot holds but certificate replay cannot reproduce. POOLREAP clears
// delegations to a retired pool and the PV10 HARDFORK rule clears delegations
// to an unregistered DRep; backfill runs neither, so replaying the historical
// delegation certificate re-points the imported account. At the anchor the
// snapshot is authoritative: a stale pool rejoins that pool's stake if it
// re-registers (dingo #3794), and a stale DRep lets a PV10/PV11 withdrawal
// validate that cardano-node rejects.
func TestRun_RestoresSnapshotAccountDelegationAtAnchor(t *testing.T) {
	t.Parallel()
	testRun_RestoresSnapshotAccountDelegationAtAnchor(t, newTestDB)
}

func testRun_RestoresSnapshotAccountDelegationAtAnchor(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	db := newDB(t)
	addValidBackfillBlocks(t, db, 3)
	const anchor = uint64(2)
	require.NoError(t, db.SetSyncState(
		"mithril_ledger_slot",
		strconv.FormatUint(anchor, 10),
		nil,
	))

	stakeKey := bytes.Repeat([]byte{0x71}, lcommon.AddressHashSize)
	importTxn := db.MetadataTxn(true)
	require.NoError(t, importTxn.Do(func(txn *database.Txn) error {
		return db.Metadata().ImportAccount(&models.Account{
			StakingKey: stakeKey,
			AddedSlot:  anchor,
			Reward:     5_000,
			Active:     true,
		}, txn.Metadata())
	}))

	retiredPool := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0x72}, lcommon.AddressHashSize)),
	)
	unregisteredDRep := bytes.Repeat([]byte{0x73}, lcommon.AddressHashSize)
	delegation := &lcommon.StakeVoteDelegationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeVoteDelegation),
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(stakeKey),
		},
		PoolKeyHash: retiredPool,
		Drep: lcommon.Drep{
			Type:       lcommon.DrepTypeAddrKeyHash,
			Credential: unregisteredDRep,
		},
	}
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId(bytes.Repeat([]byte{0x74}, 32)).
		WithIndex(0).
		Build()
	require.NoError(t, err)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x77}, lcommon.AddressHashSize),
		stakeKey,
	)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(address.String()).
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	builder := mockledger.NewTransactionBuilder().WithCertificates(delegation)
	builder.WithId(bytes.Repeat([]byte{0x75}, 32))
	builder.WithInputs(input)
	builder.WithOutputs(output)
	builder.WithValid(true)
	tx, err := builder.Build()
	require.NoError(t, err)
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	utxoOffsets := make(map[database.UtxoRef]database.CborOffset)
	for _, produced := range tx.Produced() {
		var producedTxID [32]byte
		copy(producedTxID[:], produced.Id.Id().Bytes())
		utxoOffsets[database.UtxoRef{
			TxId:      producedTxID,
			OutputIdx: produced.Id.Index(),
		}] = database.CborOffset{BlockSlot: 1, ByteLength: 1}
	}

	// Replay the delegation the way an earlier, interrupted run of the same
	// backfill would have, then resume from the checkpoint after it.
	bf := NewBackfill(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bf.DisableNonceComputation()
	acc := db.NewBatchAccumulator()
	replayTxn := db.Transaction(true)
	require.NoError(t, bf.processBlockTxsBatched(
		[]lcommon.Transaction{tx},
		ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x76}, 32)},
		0,
		eras.ConwayEraDesc.Id,
		nil,
		&database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{
				txHash: {BlockSlot: 1, ByteLength: 1},
			},
			UtxoOffsets: utxoOffsets,
		},
		acc,
		replayTxn,
		nil,
		false,
	))
	require.NoError(t, db.FlushBatch(acc, replayTxn))
	require.NoError(t, replayTxn.Commit())
	replayTxn.Release()
	replayed, err := db.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, retiredPool.Bytes(), replayed.Pool)
	require.Equal(t, unregisteredDRep, replayed.Drep)

	now := time.Now()
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:     BackfillPhase,
			LastSlot:  1,
			StartedAt: now,
			UpdatedAt: now,
		},
		nil,
	))
	require.NoError(t, bf.Run(context.Background()))

	account, err := db.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	assert.True(t, account.Active)
	assert.Empty(t, account.Pool, "retired-pool delegation survived backfill")
	assert.Empty(t, account.Drep, "unregistered-DRep delegation survived backfill")
	assert.Equal(t, models.DrepTypeAddrKeyHash, account.DrepType)
	assert.Equal(t, uint64(5_000), uint64(account.Reward))
}

// TestRun_KeepsSnapshotDRepExpiryAtAnchor covers DRep expiry, which the
// snapshot records after rules backfill does not replay. In Conway a
// registration (PV10+), an update, or a vote sets expiry to
// epoch + drepActivity - numDormantEpochs; a transaction with proposals then
// adds the accumulated dormant epochs to every DRep's expiry, and EPOCH counts
// an epoch dormant when no proposal is active (Conway/Rules/GovCert.hs,
// Certs.hs updateDormantDRepExpiry, Epoch.hs updateNumDormantEpochs).
//
// DRep A registers at epoch 500 with drepActivity 20 and no dormant epochs
// (520); epochs 501-503 are dormant and a proposal at 504 adds 3, so the
// snapshot records 523. DRep B votes at epoch 510 (530); epochs 517-519 are
// dormant and a proposal at 520 adds 3, so the snapshot records 533. Replay
// sees only the registration and the vote, so rewriting expiry from them
// leaves 520 and 530.
func TestRun_KeepsSnapshotDRepExpiryAtAnchor(t *testing.T) {
	t.Parallel()
	testRun_KeepsSnapshotDRepExpiryAtAnchor(t, newTestDB)
}

func testRun_KeepsSnapshotDRepExpiryAtAnchor(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	db := newDB(t)
	addValidBackfillBlocks(t, db, 3)
	const anchor = uint64(2)
	require.NoError(t, db.SetSyncState(
		"mithril_ledger_slot",
		strconv.FormatUint(anchor, 10),
		nil,
	))

	drepA := bytes.Repeat([]byte{0x81}, lcommon.AddressHashSize)
	drepB := bytes.Repeat([]byte{0x82}, lcommon.AddressHashSize)
	importTxn := db.MetadataTxn(true)
	require.NoError(t, importTxn.Do(func(txn *database.Txn) error {
		for _, imported := range []struct {
			credential []byte
			expiry     uint64
		}{{drepA, 523}, {drepB, 533}} {
			if err := db.Metadata().ImportDrep(
				&models.Drep{
					Credential:  imported.credential,
					AddedSlot:   anchor,
					ExpiryEpoch: imported.expiry,
					Active:      true,
				},
				&models.RegistrationDrep{
					DrepCredential: imported.credential,
					AddedSlot:      anchor,
					DepositAmount:  500_000_000,
				},
				txn.Metadata(),
			); err != nil {
				return err
			}
		}
		return nil
	}))
	proposalTxHash := bytes.Repeat([]byte{0x83}, 32)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        proposalTxHash,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 509,
		ExpiresEpoch:  515,
		AnchorHash:    bytes.Repeat([]byte{0x84}, 32),
		Deposit:       1,
		ReturnAddress: append([]byte{0xE0}, bytes.Repeat([]byte{0x85}, 28)...),
		AddedSlot:     1,
	}, nil))

	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x86}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion:      lcommon.ProtocolParametersProtocolVersion{Major: 10},
		DRepDeposit:          500_000_000,
		DRepInactivityPeriod: 20,
	}
	bf := NewBackfill(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bf.DisableNonceComputation()
	replay := func(
		id byte,
		epoch uint64,
		configure func(*mockledger.MockTransaction),
	) {
		t.Helper()
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(bytes.Repeat([]byte{id + 1}, 32)).
			WithIndex(0).
			Build()
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(address.String()).
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		builder := mockledger.NewTransactionBuilder()
		configure(builder)
		builder.WithId(bytes.Repeat([]byte{id}, 32))
		builder.WithInputs(input)
		builder.WithOutputs(output)
		builder.WithValid(true)
		tx, err := builder.Build()
		require.NoError(t, err)
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		offsets := &database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{
				txHash: {BlockSlot: 1, ByteLength: 1},
			},
			UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
		}
		for _, produced := range tx.Produced() {
			var producedTxID [32]byte
			copy(producedTxID[:], produced.Id.Id().Bytes())
			offsets.UtxoOffsets[database.UtxoRef{
				TxId:      producedTxID,
				OutputIdx: produced.Id.Index(),
			}] = database.CborOffset{BlockSlot: 1, ByteLength: 1}
		}
		acc := db.NewBatchAccumulator()
		txn := db.Transaction(true)
		defer txn.Release()
		require.NoError(t, bf.processBlockTxsBatched(
			[]lcommon.Transaction{tx},
			ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{id + 2}, 32)},
			epoch,
			eras.ConwayEraDesc.Id,
			pp,
			offsets,
			acc,
			txn,
			nil,
			false,
		))
		require.NoError(t, db.FlushBatch(acc, txn))
		require.NoError(t, txn.Commit())
	}
	replay(0x90, 500, func(builder *mockledger.MockTransaction) {
		builder.WithCertificates(&lcommon.RegistrationDrepCertificate{
			CertType: uint(lcommon.CertificateTypeRegistrationDrep),
			DrepCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(drepA),
			},
			Amount: 500_000_000,
		})
	})
	var voterHash [28]byte
	copy(voterHash[:], drepB)
	var actionTxHash [32]byte
	copy(actionTxHash[:], proposalTxHash)
	replay(0xa0, 510, func(builder *mockledger.MockTransaction) {
		builder.WithVotingProcedures(lcommon.VotingProcedures{
			&lcommon.Voter{
				Type: lcommon.VoterTypeDRepKeyHash,
				Hash: voterHash,
			}: {
				&lcommon.GovActionId{TransactionId: actionTxHash}: {
					Vote: models.VoteYes,
				},
			},
		})
	})

	now := time.Now()
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:     BackfillPhase,
			LastSlot:  1,
			StartedAt: now,
			UpdatedAt: now,
		},
		nil,
	))
	require.NoError(t, bf.Run(context.Background()))

	for _, want := range []struct {
		credential   []byte
		expiry       uint64
		lastActivity uint64
	}{{drepA, 523, 500}, {drepB, 533, 510}} {
		drep, err := db.GetDrep(want.credential, true, nil)
		require.NoError(t, err)
		require.NotNil(t, drep)
		assert.True(t, drep.Active)
		assert.Equal(t, want.expiry, drep.ExpiryEpoch,
			"DRep %x expiry was rewritten by historical replay",
			want.credential[:1],
		)
		assert.Equal(t, want.lastActivity, drep.LastActivityEpoch)
	}
}

// TestRun_SettlesReplayedProposalsTheSnapshotDoesNotHold covers governance
// proposals replayed below the anchor. The snapshot holds every proposal live
// at the anchor; any other replayed proposal was already enacted, expired or
// dropped on chain, with its deposit refunded or its action paid, and those
// effects are in the snapshot. Left unmarked, such a row reads as live: the
// next boundaries expire it and refund its deposit again, and one still
// inside its lifetime stays eligible for ratification.
func TestRun_SettlesReplayedProposalsTheSnapshotDoesNotHold(t *testing.T) {
	t.Parallel()
	testRun_SettlesReplayedProposalsTheSnapshotDoesNotHold(t, newTestDB)
}

func testRun_SettlesReplayedProposalsTheSnapshotDoesNotHold(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	db := newDB(t)
	addValidBackfillBlocks(t, db, 3)
	const anchor = uint64(2)
	const anchorEpoch = uint64(510)
	require.NoError(t, db.SetSyncState(
		"mithril_ledger_slot",
		strconv.FormatUint(anchor, 10),
		nil,
	))

	rewardAccount, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE0}, bytes.Repeat([]byte{0x91}, 28)...),
	)
	require.NoError(t, err)
	rewardAccountBytes, err := rewardAccount.Bytes()
	require.NoError(t, err)
	liveTxHash := bytes.Repeat([]byte{0xb0}, 32)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        liveTxHash,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 508,
		ExpiresEpoch:  514,
		AnchorHash:    bytes.Repeat([]byte{0x92}, 32),
		Deposit:       100_000,
		ReturnAddress: rewardAccountBytes,
		AddedSlot:     anchor,
	}, nil))

	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x93}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion:         lcommon.ProtocolParametersProtocolVersion{Major: 10},
		GovActionValidityPeriod: 6,
		DRepInactivityPeriod:    20,
	}
	bf := NewBackfill(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bf.DisableNonceComputation()
	replayProposal := func(txHash []byte, epoch uint64) {
		t.Helper()
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(append([]byte{0x01}, txHash[1:]...)).
			WithIndex(0).
			Build()
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(address.String()).
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		builder := mockledger.NewTransactionBuilder().WithProposalProcedures(
			&conway.ConwayProposalProcedure{
				PPDeposit:       100_000,
				PPRewardAccount: rewardAccount,
				PPGovAction: conway.ConwayGovAction{
					Type: uint(lcommon.GovActionTypeInfo),
					Action: &lcommon.InfoGovAction{
						Type: uint(lcommon.GovActionTypeInfo),
					},
				},
				PPAnchor: lcommon.GovAnchor{
					DataHash: [32]byte(bytes.Repeat([]byte{0x92}, 32)),
				},
			},
		)
		builder.WithId(txHash)
		builder.WithInputs(input)
		builder.WithOutputs(output)
		builder.WithValid(true)
		tx, err := builder.Build()
		require.NoError(t, err)
		var hash [32]byte
		copy(hash[:], tx.Hash().Bytes())
		offsets := &database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{
				hash: {BlockSlot: 1, ByteLength: 1},
			},
			UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
		}
		for _, produced := range tx.Produced() {
			var producedTxID [32]byte
			copy(producedTxID[:], produced.Id.Id().Bytes())
			offsets.UtxoOffsets[database.UtxoRef{
				TxId:      producedTxID,
				OutputIdx: produced.Id.Index(),
			}] = database.CborOffset{BlockSlot: 1, ByteLength: 1}
		}
		acc := db.NewBatchAccumulator()
		txn := db.Transaction(true)
		defer txn.Release()
		require.NoError(t, bf.processBlockTxsBatched(
			[]lcommon.Transaction{tx},
			ocommon.Point{Slot: 1, Hash: append([]byte{0x02}, txHash[1:]...)},
			epoch,
			eras.ConwayEraDesc.Id,
			pp,
			offsets,
			acc,
			txn,
			nil,
			false,
		))
		require.NoError(t, db.FlushBatch(acc, txn))
		require.NoError(t, txn.Commit())
	}
	expiredTxHash := bytes.Repeat([]byte{0xb1}, 32)
	settledTxHash := bytes.Repeat([]byte{0xb2}, 32)
	replayProposal(expiredTxHash, 500)
	replayProposal(settledTxHash, 508)
	replayProposal(liveTxHash, 508)

	now := time.Now()
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:     BackfillPhase,
			LastSlot:  1,
			StartedAt: now,
			UpdatedAt: now,
		},
		nil,
	))
	require.NoError(t, bf.Run(context.Background()))

	txHashes := func(proposals []*models.GovernanceProposal) [][]byte {
		ret := make([][]byte, 0, len(proposals))
		for _, proposal := range proposals {
			ret = append(ret, proposal.TxHash)
		}
		return ret
	}
	active, err := db.GetActiveGovernanceProposals(anchorEpoch, nil)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{liveTxHash}, txHashes(active))
	expiring, err := db.GetExpiringGovernanceProposals(anchorEpoch+1, nil)
	require.NoError(t, err)
	assert.Empty(t, txHashes(expiring), "replayed proposal would expire again")
	awaitingDrop, err := db.GetExpiredAwaitingDropGovernanceProposals(
		anchorEpoch+2,
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, txHashes(awaitingDrop), "replayed proposal would be refunded again")
	for _, txHash := range [][]byte{expiredTxHash, settledTxHash} {
		stored, err := db.GetGovernanceProposal(txHash, 0, nil)
		require.NoError(t, err)
		assert.NotNil(t, stored, "replayed proposal history was dropped")
	}
}

// TestRun_KeepsImportedProtocolParameters covers resolvePParams, which derives
// parameters from genesis and hard forks and writes them per epoch. The
// snapshot's parameters for its epoch include governance-enacted changes that
// derivation cannot reproduce, and GetPParams prefers the newest row for an
// epoch, so a derived row written for an epoch the import already recorded
// replaces the snapshot's. Two shapes reach that write: an import whose era
// bounds could not be extracted records only the anchor epoch (the first-epoch
// write), and an anchor in an era's first epoch (the hard-fork write).
func TestRun_KeepsImportedProtocolParameters(t *testing.T) {
	t.Parallel()
	testRun_KeepsImportedProtocolParameters(t, newTestDB)
}

func testRun_KeepsImportedProtocolParameters(
	t *testing.T,
	newDB func(*testing.T) *database.Database,
) {
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	decodeConway := eras.ConwayEraDesc.DecodePParamsFunc
	for _, tc := range []struct {
		name        string
		priorEpoch  bool
		priorEraID  uint
		anchorEpoch uint64
	}{
		{name: "single imported epoch", anchorEpoch: 901},
		{
			name:        "anchor in era first epoch",
			priorEpoch:  true,
			priorEraID:  eras.BabbageEraDesc.Id,
			anchorEpoch: 901,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			if tc.priorEpoch {
				require.NoError(t, db.SetEpoch(
					0, tc.anchorEpoch-1, nil, nil, nil, nil,
					tc.priorEraID, 1000, 10, nil,
				))
			}
			require.NoError(t, db.SetEpoch(
				10, tc.anchorEpoch, nil, nil, nil, nil,
				eras.ConwayEraDesc.Id, 1000, 10, nil,
			))
			imported := mockledger.NewMockConwayProtocolParams()
			imported.MinFeeA = 12345
			importedCbor, err := cbor.Encode(&imported)
			require.NoError(t, err)
			require.NoError(t, db.SetPParams(
				importedCbor, 10, tc.anchorEpoch, eras.ConwayEraDesc.Id, nil,
			))
			addValidBackfillBlocksFrom(t, db, 10, 3)
			require.NoError(t, db.SetSyncState("mithril_ledger_slot", "12", nil))

			bf := NewBackfill(
				db,
				nodeCfg,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
			)
			bf.DisableNonceComputation()
			require.NoError(t, bf.Run(context.Background()))

			pp, err := db.GetPParams(
				tc.anchorEpoch,
				eras.ConwayEraDesc.Id,
				decodeConway,
				nil,
			)
			require.NoError(t, err)
			conwayPP, ok := pp.(*conway.ConwayProtocolParameters)
			require.True(t, ok, "anchor epoch parameters are %T", pp)
			assert.Equal(t, uint(12345), conwayPP.MinFeeA,
				"imported anchor-epoch parameters were replaced")
			if tc.priorEpoch {
				rows, err := db.Metadata().GetPParams(
					tc.anchorEpoch-1,
					tc.priorEraID,
					nil,
				)
				require.NoError(t, err)
				require.Len(t, rows, 1, "derived parameters were not written")
				assert.Equal(t, tc.anchorEpoch-1, rows[0].Epoch)
			}
		})
	}
}

// TestRun_EmitsFinalProgressForShortRun ensures final interval metrics are
// published even when the run finishes before the normal 10s progress tick.
func TestRun_EmitsFinalProgressForShortRun(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   0,
		TotalSlots: 1,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(cp, nil))

	addValidBackfillBlocks(t, db, 2)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bf := NewBackfill(db, nil, logger)
	var progress []BackfillProgress
	bf.SetProgressFunc(func(p BackfillProgress) {
		progress = append(progress, p)
	})

	require.NoError(t, bf.Run(context.Background()))
	require.Len(t, progress, 1)
	assert.Equal(t, uint64(1), progress[0].Slot)
	assert.Equal(t, uint64(2), progress[0].Stats.Blocks)
}

// TestRun_IncompleteCheckpointAtZeroVisitsSlotZero proves the iterator
// starts at slot 0 rather than LastSlot+1 when a checkpoint records
// LastSlot 0, which is ambiguous between "slot 0 completed" and "an initial
// checkpoint was written before any block did".
//
// Asserting the final cp.LastSlot is too weak: it lands on 1 whether or not
// slot 0 was visited. The malformed block at slot 0 is what makes the
// difference observable, because fail-closed backfill names the slot it
// stopped on: a run that skips slot 0 processes the valid block at slot 1
// and returns no error at all.
func TestRun_IncompleteCheckpointAtZeroVisitsSlotZero(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	now := time.Now()
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      BackfillPhase,
			LastSlot:   0,
			TotalSlots: 1,
			StartedAt:  now,
			UpdatedAt:  now,
			Completed:  false,
		},
		nil,
	))

	hash := make([]byte, 32)
	hash[0] = 1
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: 0,
		Hash: hash,
		Cbor: []byte{0x82, 0x01},
		Type: 1,
	}, nil))
	addValidBackfillBlocksFrom(t, db, 1, 1)

	bf := NewBackfill(db, nil, slog.New(
		slog.NewTextHandler(io.Discard, nil),
	))
	err := bf.Run(context.Background())
	require.ErrorContains(t, err, "parsing block at slot 0")
}

func TestRun_MalformedBlockDoesNotAdvanceCheckpoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   0,
		TotalSlots: 1,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(cp, nil))

	addValidBackfillBlocks(t, db, 4)
	hash := make([]byte, 32)
	hash[0] = 5
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: 4,
		Hash: hash,
		Cbor: []byte{0xff},
		Type: 1,
	}, nil))

	bf := NewBackfill(db, nil, slog.Default())
	require.NoError(t, bf.SetBatchSize(2))
	err := bf.Run(context.Background())
	require.ErrorContains(t, err, "parsing block at slot 4")

	checkpoint, err := db.Metadata().GetBackfillCheckpoint(
		BackfillPhase,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, checkpoint)
	assert.Equal(t, uint64(3), checkpoint.LastSlot)
	assert.False(t, checkpoint.Completed)
}

func TestRun_OffsetFailureKeepsLastCommittedCheckpoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	blocks, err := testfixtures.GenerateConwayChainWithTransactions(2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	for _, block := range blocks {
		blockCbor := block.Cbor()
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:   block.SlotNumber(),
			Hash:   block.Hash().Bytes(),
			Number: block.BlockNumber(),
			Cbor:   blockCbor,
			Type:   uint(block.Type()),
		}, nil))
	}

	bf := NewBackfill(db, nil, slog.Default())
	require.NoError(t, bf.SetBatchSize(1))
	offsetFailure := errors.New("injected offset computation failure")
	bf.computeOffsets = func(
		slot uint64,
		hash, blockCbor []byte,
		block gledger.Block,
	) (*database.BlockIngestionResult, error) {
		if slot == blocks[1].SlotNumber() {
			return nil, offsetFailure
		}
		return database.NewBlockIndexer(slot, hash).ComputeOffsets(
			blockCbor,
			block,
		)
	}
	err = bf.Run(context.Background())
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf(
			"computing offsets for block at slot %d",
			blocks[1].SlotNumber(),
		),
	)
	assert.ErrorIs(t, err, offsetFailure)

	checkpoint, err := db.Metadata().GetBackfillCheckpoint(
		BackfillPhase,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, checkpoint)
	assert.Equal(t, blocks[0].SlotNumber(), checkpoint.LastSlot)
	assert.False(t, checkpoint.Completed)
}

// TestBackfill_AutoDetectsImmutableUtxoOffsetsTip ensures that without an
// explicit setter call, Run() picks up the sync-state marker and applies it
// as the skip threshold. This is the path Mithril sync depends on.
func TestBackfill_AutoDetectsImmutableUtxoOffsetsTip(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	require.NoError(t, db.SetSyncState(
		immutableUtxoOffsetsSyncStateKey, "4242", nil,
	))
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      BackfillPhase,
			LastSlot:   0,
			TotalSlots: 1,
			StartedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}, nil,
	))

	// Empty blob store: Run completes the checkpoint after auto-detect.
	require.NoError(t, bf.Run(context.Background()))
	assert.Equal(t, uint64(4242), bf.immutableUtxoOffsetsTipSlot)
	assert.True(t, bf.immutableUtxoOffsetsTipSet)
}

// TestBackfill_ExplicitZeroOverridesAutoDetect addresses the reviewer
// concern that SetImmutableUtxoOffsetsTipSlot(0) is documented as disabling
// the optimisation. The override bit must beat auto-detection so callers
// that intentionally need offset repair below the immutable-copy tip cannot
// have the optimisation silently re-enabled behind their back.
func TestBackfill_ExplicitZeroOverridesAutoDetect(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	require.NoError(t, db.SetSyncState(
		immutableUtxoOffsetsSyncStateKey, "4242", nil,
	))
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      BackfillPhase,
			LastSlot:   0,
			TotalSlots: 1,
			StartedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}, nil,
	))

	// Explicit "disable the optimisation" before Run.
	bf.SetImmutableUtxoOffsetsTipSlot(0)
	require.NoError(t, bf.Run(context.Background()))

	assert.Equal(
		t,
		uint64(0),
		bf.immutableUtxoOffsetsTipSlot,
		"explicit SetImmutableUtxoOffsetsTipSlot(0) must beat sync-state auto-detect",
	)
}

// TestBackfill_ExplicitNonZeroOverridesAutoDetect rounds out the override
// semantics: a non-zero explicit value also wins over a different sync-state
// value, so callers can target a specific threshold (e.g. a unit-test fake
// or a stricter repair threshold) without depending on what the
// immutable-copy phase happened to leave behind.
func TestBackfill_ExplicitNonZeroOverridesAutoDetect(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	require.NoError(t, db.SetSyncState(
		immutableUtxoOffsetsSyncStateKey, "4242", nil,
	))
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      BackfillPhase,
			LastSlot:   0,
			TotalSlots: 1,
			StartedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}, nil,
	))

	bf.SetImmutableUtxoOffsetsTipSlot(100)
	require.NoError(t, bf.Run(context.Background()))

	assert.Equal(t, uint64(100), bf.immutableUtxoOffsetsTipSlot)
}

func TestRun_CancelledContext_WithBlocks(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Insert a block so tipSlot > 0 and Run reaches the
	// iteration loop.
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(i)
	}
	err := db.BlockCreate(models.Block{
		Slot: 100,
		Hash: hash,
		Cbor: []byte{0x82, 0x01},
		Type: 1,
	}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With blocks present, Run enters the iteration loop and
	// detects the cancelled context, returning an error.
	err = bf.Run(ctx)
	require.Error(
		t,
		err,
		"Run should return error on cancelled context with blocks",
	)
	assert.ErrorIs(t, err, context.Canceled)
}
