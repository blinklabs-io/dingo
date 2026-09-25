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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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

func TestBackfillProcessBlockGovernanceRenewsDRepFromCertificateOnly(
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
	assert.Equal(t, uint64(120), drep.ExpiryEpoch)
}

func TestBackfillProcessBlockGovernanceRenewsDRepInDijkstra(t *testing.T) {
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
	assert.Equal(t, uint64(120), drep.ExpiryEpoch)
}

func TestBackfillProcessBlockGovernanceCleansDeregistrationVotes(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	backfill := NewBackfill(db, nil, slog.Default())
	drepCredential := bytes.Repeat([]byte{0xA7}, 28)
	stakeCredential := bytes.Repeat([]byte{0xC9}, 28)
	require.NoError(t, db.Metadata().ImportDrep(
		&models.Drep{
			CredentialTag: uint8(lcommon.CredentialTypeAddrKeyHash),
			Credential:    drepCredential,
			AddedSlot:     900,
			Active:        true,
			Delegators: []models.StakeCredentialRef{{
				Tag: uint8(lcommon.CredentialTypeAddrKeyHash),
				Key: stakeCredential,
			}},
		},
		&models.RegistrationDrep{
			CredentialTag:  uint8(lcommon.CredentialTypeAddrKeyHash),
			DrepCredential: drepCredential,
			AddedSlot:      900,
			DepositAmount:  types.Uint64(500),
		},
		nil,
	))
	proposalHash := bytes.Repeat([]byte{0xB8}, 32)
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    stakeCredential,
		CredentialTag: uint8(lcommon.CredentialTypeAddrKeyHash),
		Drep:          drepCredential,
		DrepType:      models.DrepTypeAddrKeyHash,
		AddedSlot:     950,
		CreatedSlot:   950,
		Active:        true,
	}, nil))
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        proposalHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 100,
		ExpiresEpoch:  120,
		AddedSlot:     900,
	}, nil))
	proposal, err := db.GetGovernanceProposal(proposalHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:         proposal.ID,
		VoterType:          uint8(models.VoterTypeDRep),
		VoterCredentialTag: uint8(lcommon.CredentialTypeAddrKeyHash),
		VoterCredential:    drepCredential,
		Vote:               uint8(models.VoteYes),
		AddedSlot:          900,
	}, nil))

	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], drepCredential)
	tx := mockledger.NewTransactionBuilder()
	tx.WithCertificates(&lcommon.DeregistrationDrepCertificate{
		CertType: uint(lcommon.CertificateTypeDeregistrationDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: credentialHash,
		},
	})
	tx.WithValid(true)
	pparams := mockledger.NewMockConwayProtocolParams()

	point := ocommon.NewPoint(1000, bytes.Repeat([]byte{0xCD}, 32))
	var blockHash [32]byte
	copy(blockHash[:], point.Hash)
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  point.Slot,
				BlockHash:  blockHash,
				ByteLength: 1,
			},
		},
	}
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := backfill.processBlockTxsBatched(
			[]lcommon.Transaction{tx},
			point,
			100,
			eras.ConwayEraDesc.Id,
			&pparams,
			offsets,
			acc,
			txn,
			nil,
			false,
		); err != nil {
			return err
		}
		return db.FlushBatch(acc, txn)
	}))

	votes, err := db.GetGovernanceVotes(proposal.ID, nil)
	require.NoError(t, err)
	require.Empty(t, votes, "backfill must apply DRep deregistration cleanup")
	account, err := db.GetAccountByCredential(
		uint8(lcommon.CredentialTypeAddrKeyHash),
		stakeCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Nil(t, account.Drep, "backfill must clear deregistered DRep delegations")
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
