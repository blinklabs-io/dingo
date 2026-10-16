package indexer

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

type transactionWithAppliedEffects struct {
	lcommon.Transaction
	consumed []lcommon.TransactionInput
	produced []lcommon.Utxo
}

func (tx transactionWithAppliedEffects) Consumed() []lcommon.TransactionInput {
	return tx.consumed
}

func (tx transactionWithAppliedEffects) Produced() []lcommon.Utxo {
	return tx.produced
}

func registrationRebuildConfig(store *testStore) Config {
	return Config{
		EventBus:                event.NewEventBus(nil, nil),
		Metadata:                store,
		SlotTimer:               timestampFunc(func(slot uint64) (time.Time, error) { return time.UnixMilli(int64(slot)), nil }),
		Logger:                  slog.New(slog.NewTextHandler(os.Stderr, nil)),
		MappingValidatorAddress: testMappingAddr,
		AuthTokenPolicyID:       testAuthPolicyID,
		AuthTokenAssetName:      testAuthAssetNameHex,
		CanonicalBlockIterator: func(uint64, uint64, func(models.Block) error) error {
			return nil
		},
		LedgerTipBlock: func() (*models.Block, error) { return nil, nil },
	}
}

func validRegistrationHistoryBlock(t *testing.T) models.Block {
	t.Helper()
	raw := testutil.BuildBlockBytesFromComponents(
		t,
		ledger.BlockTypeShelley,
		100,
		0,
		[]byte{0x80},
		[]byte{0x80},
		[]byte{0xa0},
	)
	decoded, err := ledger.NewBlockFromCbor(ledger.BlockTypeShelley, raw)
	require.NoError(t, err)
	return models.Block{
		ID: 1, Type: ledger.BlockTypeShelley, Number: decoded.BlockNumber(),
		Slot: 100, Hash: decoded.Hash().Bytes(), PrevHash: decoded.PrevHash().Bytes(), Cbor: raw,
	}
}

func seedRegistration(t *testing.T, store *testStore, byteValue byte) {
	t.Helper()
	require.NoError(t, store.CreateMidnightRegistration(nil, &models.MidnightRegistration{
		FullDatum:   []byte{0x80},
		TxHash:      []byte{byteValue},
		OutputIndex: uint32(byteValue),
		BlockNumber: 1,
		BlockHash:   []byte{byteValue},
		TxIndex:     1,
	}))
}

func seedDeregistration(t *testing.T, store *testStore, byteValue byte) {
	t.Helper()
	require.NoError(t, store.CreateMidnightDeregistration(nil, &models.MidnightDeregistration{
		FullDatum:   []byte{0x80},
		TxHash:      []byte{byteValue},
		UtxoTxHash:  []byte{byteValue},
		UtxoIndex:   uint32(byteValue),
		BlockNumber: 1,
		BlockHash:   []byte{byteValue},
		TxIndex:     1,
	}))
}

func TestRegistrationRebuildClearsAndCompletes(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	seedRegistration(t, store, 0x11)
	seedDeregistration(t, store, 0x11)
	block := validRegistrationHistoryBlock(t)
	cfg := registrationRebuildConfig(store)
	cfg.LedgerTipBlock = func() (*models.Block, error) { return &block, nil }
	cfg.CanonicalBlockIterator = func(_, _ uint64, fn func(models.Block) error) error { return fn(block) }
	cfg.blockDecoder = func(models.Block) ([]lcommon.Transaction, error) { return nil, nil }
	idx, err := New(cfg)
	require.NoError(t, err)

	require.NoError(t, idx.rebuildRegistrationStateIfNeeded())
	require.Empty(t, allRegistrations(t, store))
	require.Empty(t, allDeregistrations(t, store))
	fingerprint, err := store.GetSyncState(midnightRegistrationFingerprintKey, nil)
	require.NoError(t, err)
	require.Equal(t, idx.registrationFingerprint(), fingerprint)
	pending, err := store.GetSyncState(midnightRegistrationPendingKey, nil)
	require.NoError(t, err)
	require.Empty(t, pending)
	require.Empty(t, idx.regUTxOs)
}

func TestRegistrationRebuildRequiresHistoryWhenRowsExist(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	seedRegistration(t, store, 0x13)
	cfg := registrationRebuildConfig(store)
	cfg.CanonicalBlockIterator = nil
	cfg.LedgerTipBlock = nil
	idx, err := New(cfg)
	require.NoError(t, err)
	require.ErrorContains(t, idx.rebuildRegistrationStateIfNeeded(), "without retained-block iterator")
	require.Len(t, allRegistrations(t, store), 1)
}

func TestRegistrationRebuildPreservesRowsWhenAppliedTipIsOrigin(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	seedRegistration(t, store, 0x14)
	cfg := registrationRebuildConfig(store)
	idx, err := New(cfg)
	require.NoError(t, err)
	require.ErrorContains(t, idx.rebuildRegistrationStateIfNeeded(), "applied tip is at origin")
	require.Len(t, allRegistrations(t, store), 1)
}

func TestRegistrationRebuildValidatesAndReplaysCanonicalHistory(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	seedRegistration(t, store, 0x12)
	block := validRegistrationHistoryBlock(t)
	cfg := registrationRebuildConfig(store)
	cfg.LedgerTipBlock = func() (*models.Block, error) { return &block, nil }
	iterated := 0
	cfg.CanonicalBlockIterator = func(startIndex, endIndex uint64, fn func(models.Block) error) error {
		require.Equal(t, uint64(1), startIndex)
		require.Equal(t, uint64(2), endIndex)
		iterated++
		return fn(block)
	}
	datum := simpleDatumCbor(t)
	correctHash := pad32("cc000001")
	wrongHash := pad32("dd000001")
	correctTx := buildTx(t, correctHash,
		[]lcommon.TransactionInput{buildInput(t, pad32("ee000001"), 0)},
		[]lcommon.TransactionOutput{buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datum)},
	)
	wrongTx := buildTx(t, wrongHash,
		[]lcommon.TransactionInput{buildInput(t, pad32("ee000002"), 0)},
		[]lcommon.TransactionOutput{buildAuthOutput(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", testAuthAssetNameHex, datum)},
	)
	decoderCalled := false
	cfg.blockDecoder = func(block models.Block) ([]lcommon.Transaction, error) {
		decoderCalled = true
		return []lcommon.Transaction{correctTx, wrongTx}, nil
	}
	idx, err := New(cfg)
	require.NoError(t, err)
	require.True(t, idx.hasAuthToken(correctTx.Outputs()[0]))
	require.Equal(t, testMappingAddr, correctTx.Outputs()[0].Address().String())
	require.NotNil(t, correctTx.Outputs()[0].Datum())
	require.NotEmpty(t, correctTx.Outputs()[0].Datum().Cbor())
	require.NoError(t, idx.rebuildRegistrationStateIfNeeded())
	require.Equal(t, 2, iterated, "history is preflighted before it is replayed")
	require.True(t, decoderCalled)
	idx.mu.RLock()
	_, correctTracked := idx.regUTxOs[utxoKey{TxHash: correctHash, Index: 0}]
	_, wrongTracked := idx.regUTxOs[utxoKey{TxHash: wrongHash, Index: 0}]
	idx.mu.RUnlock()
	require.True(t, correctTracked)
	require.False(t, wrongTracked)
	var correctCount, wrongCount int
	require.NoError(t, store.raw.QueryRow("SELECT COUNT(*) FROM midnight_registrations WHERE tx_hash = ?", validHashBytes(t, correctHash)).Scan(&correctCount))
	require.NoError(t, store.raw.QueryRow("SELECT COUNT(*) FROM midnight_registrations WHERE tx_hash = ?", validHashBytes(t, wrongHash)).Scan(&wrongCount))
	require.Equal(t, 1, correctCount)
	require.Zero(t, wrongCount)
	var restoredDatum []byte
	var restoredBlock uint64
	var restoredOutput uint32
	require.NoError(t, store.raw.QueryRow(
		"SELECT full_datum, block_number, output_index FROM midnight_registrations WHERE tx_hash = ?",
		validHashBytes(t, correctHash),
	).Scan(&restoredDatum, &restoredBlock, &restoredOutput))
	require.Equal(t, datum, restoredDatum)
	require.Zero(t, restoredBlock)
	require.Zero(t, restoredOutput)
	registrationCheckpoint, err := store.GetBackfillCheckpoint(midnightRegistrationCheckpointPhase, nil)
	require.NoError(t, err)
	require.NotNil(t, registrationCheckpoint)
	require.Equal(t, uint64(100), registrationCheckpoint.LastSlot)
	require.Zero(t, idx.checkpointSlot, "registration rebuild must not rewind the regular Midnight checkpoint")
}

func TestRegistrationRebuildUsesAppliedTransactionEffects(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	block := validRegistrationHistoryBlock(t)
	cfg := registrationRebuildConfig(store)
	cfg.LedgerTipBlock = func() (*models.Block, error) { return &block, nil }
	cfg.CanonicalBlockIterator = func(_, _ uint64, fn func(models.Block) error) error {
		return fn(block)
	}

	datum := simpleDatumCbor(t)
	validHash := pad32("ab000001")
	invalidHash := pad32("ab000002")
	validRegistration := buildTx(t, validHash,
		[]lcommon.TransactionInput{buildInput(t, pad32("ab000003"), 0)},
		[]lcommon.TransactionOutput{
			buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datum),
		},
	)
	require.Len(t, validRegistration.Produced(), 1)
	invalidBodyInput := buildInput(t, validHash, 0)
	collateralInput := buildInput(t, pad32("ab000004"), 0)
	invalidOutput := buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datum)
	collateralReturn := buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datum)
	invalidBuilder := mockledger.NewTransactionBuilder()
	invalidBuilder.WithId(validHashBytes(t, invalidHash)).
		WithInputs(invalidBodyInput).
		WithOutputs(invalidOutput).
		WithValid(false)
	invalidBuilder.WithCollateral(collateralInput).
		WithCollateralReturn(collateralReturn)
	invalidBody, err := invalidBuilder.Build()
	require.NoError(t, err)
	invalidTx := transactionWithAppliedEffects{
		Transaction: invalidBody,
		consumed:    []lcommon.TransactionInput{collateralInput},
		produced: []lcommon.Utxo{{
			Id:     buildInput(t, invalidHash, 1),
			Output: collateralReturn,
		}},
	}
	cfg.blockDecoder = func(models.Block) ([]lcommon.Transaction, error) {
		return []lcommon.Transaction{validRegistration, invalidTx}, nil
	}
	idx, err := New(cfg)
	require.NoError(t, err)
	require.True(t, idx.hasAuthToken(validRegistration.Produced()[0].Output))
	require.NoError(t, idx.rebuildRegistrationStateIfNeeded())

	var registrationCount, deregistrationCount, invalidBodyOutputCount int
	require.NoError(t, store.raw.QueryRow(
		"SELECT COUNT(*) FROM midnight_registrations",
	).Scan(&registrationCount))
	require.NoError(t, store.raw.QueryRow(
		"SELECT COUNT(*) FROM midnight_deregistrations",
	).Scan(&deregistrationCount))
	require.NoError(t, store.raw.QueryRow(
		"SELECT COUNT(*) FROM midnight_registrations WHERE tx_hash = ? AND output_index = 0",
		validHashBytes(t, invalidHash),
	).Scan(&invalidBodyOutputCount))
	require.Equal(t, 2, registrationCount)
	require.Zero(t, deregistrationCount)
	require.Zero(t, invalidBodyOutputCount)
	idx.mu.RLock()
	_, validTracked := idx.regUTxOs[utxoKey{TxHash: validHash, Index: 0}]
	_, invalidOutputTracked := idx.regUTxOs[utxoKey{TxHash: invalidHash, Index: 0}]
	_, collateralReturnTracked := idx.regUTxOs[utxoKey{TxHash: invalidHash, Index: 1}]
	idx.mu.RUnlock()
	require.True(t, validTracked)
	require.False(t, invalidOutputTracked)
	require.True(t, collateralReturnTracked)

	require.NoError(t, idx.processBlock(block, []lcommon.Transaction{validRegistration, invalidTx}, 100))
	require.NoError(t, store.raw.QueryRow(
		"SELECT COUNT(*) FROM midnight_registrations",
	).Scan(&registrationCount))
	require.NoError(t, store.raw.QueryRow(
		"SELECT COUNT(*) FROM midnight_deregistrations",
	).Scan(&deregistrationCount))
	require.Equal(t, 2, registrationCount)
	require.Zero(t, deregistrationCount)
}

func TestRegistrationRebuildRetriesAfterInterruptedReplay(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	cfg := registrationRebuildConfig(store)
	block := validRegistrationHistoryBlock(t)
	cfg.LedgerTipBlock = func() (*models.Block, error) { return &block, nil }
	iteratorCalls := 0
	cfg.CanonicalBlockIterator = func(_, _ uint64, fn func(models.Block) error) error {
		iteratorCalls++
		if iteratorCalls == 2 {
			return context.Canceled
		}
		return fn(block)
	}
	cfg.blockDecoder = func(models.Block) ([]lcommon.Transaction, error) { return nil, nil }
	idx, err := New(cfg)
	require.NoError(t, err)
	require.ErrorIs(t, idx.rebuildRegistrationStateIfNeeded(), context.Canceled)
	pending, err := store.GetSyncState(midnightRegistrationPendingKey, nil)
	require.NoError(t, err)
	require.Equal(t, idx.registrationFingerprint(), pending)

	idx, err = New(cfg)
	require.NoError(t, err)
	require.NoError(t, idx.rebuildRegistrationStateIfNeeded())
	pending, err = store.GetSyncState(midnightRegistrationPendingKey, nil)
	require.NoError(t, err)
	require.Empty(t, pending)
	fingerprint, err := store.GetSyncState(midnightRegistrationFingerprintKey, nil)
	require.NoError(t, err)
	require.Equal(t, idx.registrationFingerprint(), fingerprint)
}

func TestRegistrationRebuildPreflightFailurePreservesRows(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	seedRegistration(t, store, 0x22)
	cfg := registrationRebuildConfig(store)
	cfg.LedgerTipBlock = func() (*models.Block, error) {
		return &models.Block{ID: 1, Number: 1, Slot: 10, Hash: []byte{0x33}}, nil
	}
	cfg.CanonicalBlockIterator = func(_, _ uint64, fn func(models.Block) error) error {
		return fn(models.Block{ID: 1, Number: 1, Slot: 10, Hash: []byte{0x33}})
	}
	idx, err := New(cfg)
	require.NoError(t, err)
	require.ErrorContains(t, idx.rebuildRegistrationStateIfNeeded(), "decode slot=10")
	require.Len(t, allRegistrations(t, store), 1)
	pending, err := store.GetSyncState(midnightRegistrationPendingKey, nil)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestRegistrationPolicyChangeRebuildsState(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	seedRegistration(t, store, 0x44)
	oldConfig := registrationRebuildConfig(store)
	oldConfig.AuthTokenPolicyID = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	oldIdx, err := New(oldConfig)
	require.NoError(t, err)
	require.NoError(t, store.SetSyncState(midnightRegistrationFingerprintKey, oldIdx.registrationFingerprint(), nil))

	block := validRegistrationHistoryBlock(t)
	newConfig := registrationRebuildConfig(store)
	newConfig.LedgerTipBlock = func() (*models.Block, error) { return &block, nil }
	newConfig.CanonicalBlockIterator = func(_, _ uint64, fn func(models.Block) error) error { return fn(block) }
	newConfig.blockDecoder = func(models.Block) ([]lcommon.Transaction, error) { return nil, nil }
	newIdx, err := New(newConfig)
	require.NoError(t, err)
	require.NoError(t, newIdx.rebuildRegistrationStateIfNeeded())
	require.Empty(t, allRegistrations(t, store))
	fingerprint, err := store.GetSyncState(midnightRegistrationFingerprintKey, nil)
	require.NoError(t, err)
	require.Equal(t, newIdx.registrationFingerprint(), fingerprint)
}
