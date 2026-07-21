package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	fxcbor "github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractHeaderCbor(t *testing.T) {
	t.Parallel()

	header := gcbor.RawMessage{0x01}
	body := gcbor.RawMessage{0x02}
	blockCbor, err := fxcbor.Marshal([]gcbor.RawMessage{header, body})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	got, err := extractHeaderCbor(blockCbor)
	if err != nil {
		t.Fatalf("extractHeaderCbor returned error: %v", err)
	}
	if !bytes.Equal(got, header) {
		t.Fatalf("unexpected header bytes: got %x want %x", got, header)
	}
}

func TestCborArrayHeaderLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		want    int
		wantErr bool
	}{
		{
			name: "small definite",
			data: []byte{gcbor.CborTypeArray + 2, 0x01, 0x02},
			want: 1,
		},
		{
			name: "uint8 length",
			data: []byte{gcbor.CborTypeArray + 24, 0x20},
			want: 2,
		},
		{
			name: "uint16 length",
			data: []byte{gcbor.CborTypeArray + 25, 0x01, 0x00},
			want: 3,
		},
		{
			name: "uint32 length",
			data: []byte{gcbor.CborTypeArray + 26, 0x00, 0x01, 0x00, 0x00},
			want: 5,
		},
		{
			name: "uint64 length",
			data: []byte{gcbor.CborTypeArray + 27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
			want: 9,
		},
		{
			name: "indefinite",
			data: []byte{gcbor.CborTypeArray + 31, 0x01, 0xff},
			want: 1,
		},
		{
			name:    "empty input",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "non-array major type",
			data:    []byte{gcbor.CborTypeMap + 2},
			wantErr: true,
		},
		{
			name:    "truncated uint8 header",
			data:    []byte{gcbor.CborTypeArray + 24},
			wantErr: true,
		},
		{
			name:    "truncated uint16 header",
			data:    []byte{gcbor.CborTypeArray + 25, 0x01},
			wantErr: true,
		},
		{
			name:    "truncated uint32 header",
			data:    []byte{gcbor.CborTypeArray + 26, 0x00, 0x01},
			wantErr: true,
		},
		{
			name:    "truncated uint64 header",
			data:    []byte{gcbor.CborTypeArray + 27, 0x00, 0x00, 0x00},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := cborArrayHeaderLen(test.data)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("cborArrayHeaderLen returned error: %v", err)
			}
			if got != test.want {
				t.Fatalf("unexpected header len: got %d want %d", got, test.want)
			}
		})
	}
}

func TestCopyBlocksRaw_PreservesByronEbbLinkageAtOrigin(t *testing.T) {
	t.Parallel()

	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err)
	defer iter.Close()

	ebbBlock, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, ebbBlock)
	require.True(t, ebbBlock.IsEbb)

	nextBlock, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, nextBlock)

	ebbHeader, err := decodeImmutableBlockHeader(ebbBlock)
	require.NoError(t, err)
	nextHeader, err := decodeImmutableBlockHeader(nextBlock)
	require.NoError(t, err)
	require.Equal(
		t,
		ebbHeader.Hash().Bytes(),
		nextHeader.PrevHash().Bytes(),
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	blocksCopied, _, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Greater(t, blocksCopied, 1)

	ebbPoint := ocommon.NewPoint(
		ebbHeader.SlotNumber(),
		ebbHeader.Hash().Bytes(),
	)
	importedEbb, err := cm.BlockByPoint(ebbPoint, nil)
	require.NoError(t, err)
	require.NotNil(t, importedEbb)
	assert.Equal(t, ebbPoint.Hash, importedEbb.Hash)

	nextPoint := ocommon.NewPoint(
		nextHeader.SlotNumber(),
		nextHeader.Hash().Bytes(),
	)
	importedNext, err := cm.BlockByPoint(nextPoint, nil)
	require.NoError(t, err)
	require.NotNil(t, importedNext)
	assert.Equal(t, ebbPoint.Hash, importedNext.PrevHash)
}

func TestCopyBlocksRawWithCallback_StoresUtxoOffsets(t *testing.T) {
	// No t.Parallel(): newTestDB shares process-wide plugin state
	// (see database.go:164), so concurrent test runs race on
	// SetPluginOption and the in-memory schema migration.
	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	var (
		expectedPoint      ocommon.Point
		expectedTxHash     []byte
		expectedOutputIdx  uint32
		expectedOutputCbor []byte
	)
	const maxIterations = 10_000
	for i := range maxIterations {
		next, err := iter.Next()
		require.NoError(t, err)
		if next == nil {
			require.Failf(
				t,
				"no non-EBB block with produced outputs found",
				"iterator ended after %d iterations",
				i+1,
			)
		}
		require.NotNil(t, next)
		if next.IsEbb {
			continue
		}
		block, err := gledger.NewBlockFromCbor(
			next.Type,
			next.Cbor,
			lcommon.VerifyConfig{SkipBodyHashValidation: true},
		)
		require.NoError(t, err)
		for _, tx := range block.Transactions() {
			produced := tx.Produced()
			if len(produced) == 0 {
				continue
			}
			expectedPoint = ocommon.NewPoint(next.Slot, next.Hash)
			expectedTxHash = bytes.Clone(tx.Hash().Bytes())
			expectedOutputIdx = produced[0].Id.Index()
			expectedOutputCbor = bytes.Clone(produced[0].Output.Cbor())
			break
		}
		if len(expectedTxHash) > 0 {
			break
		}
	}
	require.NotEmptyf(
		t,
		expectedTxHash,
		"no non-EBB block with produced outputs found after %d iterations",
		maxIterations,
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	blocksCopied, _, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		func(rb chain.RawBlock, txn *database.Txn) error {
			_, err := storeRawBlockUtxoOffsets(txn, rb)
			return err
		},
		nil,
	)
	require.NoError(t, err)
	require.Greater(t, blocksCopied, 1)

	blobTxn := db.BlobTxn(false)
	defer blobTxn.Rollback() //nolint:errcheck
	offsetData, err := db.Blob().GetUtxo(
		blobTxn.Blob(),
		expectedTxHash,
		expectedOutputIdx,
	)
	require.NoError(t, err)
	require.True(t, database.IsUtxoOffsetStorage(offsetData))

	offset, err := database.DecodeUtxoOffset(offsetData)
	require.NoError(t, err)
	assert.Equal(t, expectedPoint.Slot, offset.BlockSlot)
	assert.Equal(t, expectedPoint.Hash, offset.BlockHash[:])

	storedBlock, err := database.BlockByPoint(db, expectedPoint)
	require.NoError(t, err)
	end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
	require.LessOrEqual(t, end, uint64(len(storedBlock.Cbor)))
	assert.Equal(
		t,
		expectedOutputCbor,
		storedBlock.Cbor[offset.ByteOffset:end],
	)
}

func TestStoreRawBlockUtxoOffsetsPropagatesExtractError(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck

	_, err := storeRawBlockUtxoOffsets(txn, chain.RawBlock{
		Slot: 42,
		Hash: bytes.Repeat([]byte{0x42}, 32),
		Cbor: []byte{0x82, 0x01},
		Type: 1,
	})
	require.ErrorContains(
		t,
		err,
		"block at slot 42: extract transaction offsets",
	)
}

func TestStoreRawBlockUtxoOffsetsSkipsByronEbb(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck

	bodyItems := make([]gcbor.RawMessage, 21_600)
	for i := range bodyItems {
		bodyItems[i] = gcbor.RawMessage{0x00}
	}
	bodyCbor, err := fxcbor.Marshal(bodyItems)
	require.NoError(t, err)
	extraCbor, err := fxcbor.Marshal([]gcbor.RawMessage{{0x00}})
	require.NoError(t, err)
	blockCbor, err := fxcbor.Marshal([]gcbor.RawMessage{
		{0x80},
		gcbor.RawMessage(bodyCbor),
		gcbor.RawMessage(extraCbor),
	})
	require.NoError(t, err)

	stored, err := storeRawBlockUtxoOffsets(txn, chain.RawBlock{
		Slot: 0,
		Hash: bytes.Repeat([]byte{0x42}, 32),
		Cbor: blockCbor,
		Type: gledger.BlockTypeByronEbb,
	})
	require.NoError(t, err)
	require.Zero(t, stored)
}

func TestCopyBlocksRawWithCallback_BackfillsWhenChainTipPastImmutableTip(
	t *testing.T,
) {
	// No t.Parallel(): newTestDB shares process-wide plugin state
	// (see database.go:164), so concurrent test runs race on
	// SetPluginOption and the in-memory schema migration.
	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, immutableTipSlot, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		nil,
		nil,
	)
	require.NoError(t, err)

	currentTip := cm.PrimaryChain().Tip()
	// AddRawBlocks treats Cbor as opaque — a stub byte string is
	// sufficient to advance the chain tip past immutableTipSlot
	// for the resume check exercised below. If a future change
	// makes AddRawBlocks decode the body, swap this for a real
	// raw block from the immutable testdata directory.
	err = cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
		{
			Slot:        immutableTipSlot + 5,
			Hash:        bytes.Repeat([]byte{0x42}, 32),
			BlockNumber: currentTip.BlockNumber + 1,
			Type:        0,
			PrevHash:    currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	})
	require.NoError(t, err)

	var offsetsStored int
	blocksCopied, resumedImmutableTipSlot, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		func(rb chain.RawBlock, txn *database.Txn) error {
			stored, err := storeRawBlockUtxoOffsets(txn, rb)
			offsetsStored += stored
			return err
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 0, blocksCopied)
	assert.Equal(t, immutableTipSlot, resumedImmutableTipSlot)
	assert.Greater(t, offsetsStored, 0)
}

func TestLoadWithDBWiresEpochBoundarySnapshotHook(t *testing.T) {
	db := newTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	stopAfterHook := errors.New("stop after snapshot hook capture")
	var captured ledger.LedgerStateConfig
	var capturedHook func(*database.Txn, event.EpochTransitionEvent) error
	oldNewLedgerStateForLoad := newLedgerStateForLoad
	oldInstallHook := installEpochBoundarySnapshotHookForLoad
	newLedgerStateForLoad = func(
		cfg ledger.LedgerStateConfig,
	) (*ledger.LedgerState, error) {
		captured = cfg
		return &ledger.LedgerState{}, nil
	}
	installEpochBoundarySnapshotHookForLoad = func(
		_ *ledger.LedgerState,
		fn func(*database.Txn, event.EpochTransitionEvent) error,
	) error {
		capturedHook = fn
		return stopAfterHook
	}
	t.Cleanup(func() {
		newLedgerStateForLoad = oldNewLedgerStateForLoad
		installEpochBoundarySnapshotHookForLoad = oldInstallHook
	})

	err := LoadWithDB(
		context.Background(),
		&config.Config{Network: "preview"},
		logger,
		"unused",
		db,
	)
	require.ErrorIs(t, err, stopAfterHook)
	require.Same(t, db, captured.Database)
	require.NotNil(t, captured.ChainManager)
	require.NotNil(t, capturedHook)
	require.True(t, captured.TrustedReplay)
	require.True(t, captured.ManualBlockProcessing)
}

// TestLoadWithDBPropagatesFullPotRewards verifies that the CIP-0163 full-pot
// feature gate flows from the loaded config into the load-mode ledger config,
// so `dingo load` computes the same reward state as an enabled serve node
// instead of the legacy residual-to-reserves behavior.
func TestLoadWithDBPropagatesFullPotRewards(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stopAfterCapture := errors.New("stop after ledger config capture")

	run := func(enabled bool) ledger.LedgerStateConfig {
		db := newTestDB(t)
		var captured ledger.LedgerStateConfig
		oldNewLedgerStateForLoad := newLedgerStateForLoad
		newLedgerStateForLoad = func(
			cfg ledger.LedgerStateConfig,
		) (*ledger.LedgerState, error) {
			captured = cfg
			return nil, stopAfterCapture
		}
		t.Cleanup(func() {
			newLedgerStateForLoad = oldNewLedgerStateForLoad
		})
		err := LoadWithDB(
			context.Background(),
			&config.Config{
				Network:                                "preview",
				FullPotRewardsEnabled:                  enabled,
				UnsafeFullPotRewardsOnStandardNetworks: enabled,
			},
			logger,
			"unused",
			db,
		)
		require.ErrorIs(t, err, stopAfterCapture)
		return captured
	}

	require.True(t, run(true).FullPotRewardsEnabled)
	require.False(t, run(false).FullPotRewardsEnabled)
}

func TestLoadWithDBRejectsFullPotRewardsOnStandardNetwork(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := LoadWithDB(
		context.Background(),
		&config.Config{
			Network:               "preview",
			FullPotRewardsEnabled: true,
		},
		logger,
		"unused",
		nil,
	)
	require.ErrorContains(
		t,
		err,
		"fullPotRewardsEnabled is not permitted on standard network \"preview\"",
	)
}

func TestLoadWithDBRejectsFullPotRewardsFromCardanoConfigNetwork(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, test := range []struct {
		name         string
		network      string
		networkMagic uint32
	}{
		{name: "configured identity unset"},
		{
			name:         "configured identity claims custom network",
			network:      "devnet",
			networkMagic: 42,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := LoadWithDB(
				context.Background(),
				&config.Config{
					Network:               test.network,
					NetworkMagic:          test.networkMagic,
					CardanoConfig:         "preview/config.json",
					FullPotRewardsEnabled: true,
				},
				logger,
				"unused",
				nil,
			)
			require.ErrorContains(
				t,
				err,
				"fullPotRewardsEnabled is not permitted on standard network \"preview\"",
			)
		})
	}
}

// TestLoadWithDBPropagatesDelegatorInactivity verifies that load mode
// (`dingo load`) sets LedgerStateConfig.DelegatorInactivityEnabled /
// DelegatorInactivity from the operator config, matching serve mode, since a
// mismatch between load and serve would diverge consensus on replay.
func TestLoadWithDBPropagatesDelegatorInactivity(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stop := errors.New("stop after ledger config capture")
	run := func(enabled bool, epochs uint64) ledger.LedgerStateConfig {
		db := newTestDB(t)
		var captured ledger.LedgerStateConfig
		old := newLedgerStateForLoad
		newLedgerStateForLoad = func(cfg ledger.LedgerStateConfig) (*ledger.LedgerState, error) {
			captured = cfg
			return nil, stop
		}
		t.Cleanup(func() { newLedgerStateForLoad = old })
		err := LoadWithDB(context.Background(),
			&config.Config{Network: "preview", DelegatorInactivityEnabled: enabled, DelegatorInactivity: epochs},
			logger, "unused", db)
		require.ErrorIs(t, err, stop)
		return captured
	}
	on := run(true, 90)
	require.True(t, on.DelegatorInactivityEnabled)
	require.Equal(t, uint64(90), on.DelegatorInactivity)
	require.False(t, run(false, 0).DelegatorInactivityEnabled)
}

// TestLoadCaptureFailureTrackerCleanReturnsNil verifies that a tracker with no
// recorded failures reports success, so a clean load is never turned into an
// error.
func TestLoadCaptureFailureTrackerCleanReturnsNil(t *testing.T) {
	t.Parallel()
	tracker := &loadCaptureFailureTracker{}
	require.NoError(t, tracker.err())
}

// TestLoadCaptureFailureTrackerSurfacesFailures verifies that a recorded capture
// failure surfaces as an error that preserves the first cause and names every
// failed epoch. This is the load-mode safety net for #1959: the ledger
// suppresses the authoritative capture error and load has no event-driven
// fallback, so without this the missing mark/reward snapshot would be silent.
func TestLoadCaptureFailureTrackerSurfacesFailures(t *testing.T) {
	t.Parallel()
	tracker := &loadCaptureFailureTracker{}

	first := errors.New("capture epoch 3 failed")
	tracker.record(3, first)
	tracker.record(7, errors.New("capture epoch 7 failed"))

	err := tracker.err()
	require.Error(t, err)
	// First error is kept as the representative cause.
	require.ErrorIs(t, err, first)
	// Every failed epoch is named for the operator.
	require.Contains(t, err.Error(), "3")
	require.Contains(t, err.Error(), "7")
	require.Contains(t, err.Error(), "re-imported")
}

// TestLoadCaptureFailureTrackerConcurrentRecord exercises the tracker under the
// race detector to confirm record/err are safe to call from the replay
// goroutine while the load goroutine reads the result.
func TestLoadCaptureFailureTrackerConcurrentRecord(t *testing.T) {
	t.Parallel()
	tracker := &loadCaptureFailureTracker{}

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range uint64(n) {
		go func(epoch uint64) {
			defer wg.Done()
			tracker.record(epoch, fmt.Errorf("capture epoch %d failed", epoch))
		}(i)
	}
	wg.Wait()

	err := tracker.err()
	require.Error(t, err)
}

// TestCaptureLoadGenesisSnapshot_BlockProducerFatal verifies that
// captureLoadGenesisSnapshot returns a fatal, wrapped error for a
// block-producer load when the underlying capture fails, mirroring
// node.go's handleGenesisSnapshotError guard for the normal Run path.
func TestCaptureLoadGenesisSnapshot_BlockProducerFatal(t *testing.T) {
	db := newTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := snapshot.NewManager(db, nil, logger)

	// Close the database so the capture call fails deterministically.
	require.NoError(t, db.Close())

	err := captureLoadGenesisSnapshot(
		context.Background(),
		mgr,
		&config.Config{BlockProducer: true},
		logger,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to capture genesis snapshot")
}

// TestCaptureLoadGenesisSnapshot_RelayWarnsAndContinues verifies that a
// non-block-producer load only warns and continues when the capture fails,
// matching the relay behavior of node.go's handleGenesisSnapshotError.
func TestCaptureLoadGenesisSnapshot_RelayWarnsAndContinues(t *testing.T) {
	db := newTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := snapshot.NewManager(db, nil, logger)

	require.NoError(t, db.Close())

	err := captureLoadGenesisSnapshot(
		context.Background(),
		mgr,
		&config.Config{BlockProducer: false},
		logger,
	)
	require.NoError(t, err)
}

// TestLoadWithDBCapturesGenesisMarkSnapshotForShelleyGenesisStaking verifies
// finding B (#1959): replaying a genesis with Shelley-genesis staking (as
// devnets configure) through `dingo load` must seed the epoch-0 "mark"
// RewardSnapshot the same way the normal node.Run startup path does via
// CaptureGenesisSnapshot, or the first reward round applied at the epoch-3
// boundary is silently skipped.
//
// The chain tip is advanced past the immutable testdata tip before calling
// LoadWithDB so copyBlocksDirect takes its "chain tip already beyond
// immutable DB tip" short-circuit (see load.go) and skips decoding the real
// mainnet blocks in testdata, which are unrelated to the devnet genesis
// configured here. This isolates the test to the genesis-application and
// genesis-snapshot-capture behavior that finding B is about.
func TestLoadWithDBCapturesGenesisMarkSnapshotForShelleyGenesisStaking(
	t *testing.T,
) {
	// No t.Parallel(): newTestDB shares process-wide plugin state (see
	// database.go:164), so concurrent test runs race on SetPluginOption and
	// the in-memory schema migration.
	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	immutableTip, err := imm.GetTip()
	require.NoError(t, err)
	require.NotNil(t, immutableTip)

	db := newTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	currentTip := cm.PrimaryChain().Tip()
	stubSlot := immutableTip.Slot + 5
	stubHash := bytes.Repeat([]byte{0xAB}, 32)
	require.NoError(t, cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
		{
			Slot:        stubSlot,
			Hash:        stubHash,
			BlockNumber: currentTip.BlockNumber + 1,
			Type:        0,
			PrevHash:    currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.NewPoint(stubSlot, stubHash),
		BlockNumber: currentTip.BlockNumber + 1,
	}, nil))

	cfg := &config.Config{
		Network:       "devnet",
		CardanoConfig: "devnet/config.json",
	}

	err = LoadWithDB(context.Background(), cfg, logger, immutableDir, db)
	require.NoError(t, err)

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cfg.CardanoConfig,
		cfg.Network,
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	shelleyGenesis := nodeCfg.ShelleyGenesis()
	require.NotNil(t, shelleyGenesis)
	require.NotEmpty(
		t,
		shelleyGenesis.Staking.Pools,
		"devnet genesis fixture must declare staking for this test to be meaningful",
	)

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(0, "mark", nil)
	require.NoError(t, err)
	require.NotNil(
		t,
		rewardSnapshot,
		"expected epoch-0 mark RewardSnapshot after loading a genesis "+
			"with Shelley staking via `dingo load`",
	)
}

func decodeImmutableBlockHeader(
	block *immutable.Block,
) (gledger.BlockHeader, error) {
	headerCbor, err := extractHeaderCbor(block.Cbor)
	if err != nil {
		return nil, fmt.Errorf(
			"decodeImmutableBlockHeader: extractHeaderCbor failed: %w",
			err,
		)
	}
	header, err := gledger.NewBlockHeaderFromCbor(block.Type, headerCbor)
	if err != nil {
		return nil, fmt.Errorf(
			"decodeImmutableBlockHeader: NewBlockHeaderFromCbor failed for type %v: %w",
			block.Type,
			err,
		)
	}
	return header, nil
}

// TestRunPlannerStats_WithSQLiteStore verifies that RunPlannerStats succeeds
// against an in-memory SQLite database and populates sqlite_stat1.
func TestRunPlannerStats_WithSQLiteStore(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Metadata().ImportUtxos([]models.Utxo{
		{
			TxId:      []byte("run_planner_stats_tx_id_00000001"),
			OutputIdx: 0,
			AddedSlot: 1,
			Amount:    types.Uint64(1),
		},
	}, nil))

	require.NoError(t, RunPlannerStats(db, slog.Default()))

	sqliteStore, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "test database should use SQLite metadata")

	var count int64
	err := sqliteStore.DB().Raw(
		"SELECT COUNT(*) FROM sqlite_stat1",
	).Scan(&count).Error
	require.NoError(t, err)
	assert.Positive(t, count, "sqlite_stat1 should be populated")
}

// TestRunPlannerStats_Idempotent verifies that repeated planner-stat
// maintenance stays safe for resume/restart paths.
func TestRunPlannerStats_Idempotent(t *testing.T) {
	db := newTestDB(t)

	require.NoError(t, RunPlannerStats(db, slog.Default()))
	require.NoError(t, RunPlannerStats(db, slog.Default()))

	sqliteStore, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "test database should use SQLite metadata")

	var count int64
	err := sqliteStore.DB().Raw(
		"SELECT COUNT(*) FROM sqlite_stat1",
	).Scan(&count).Error
	require.NoError(t, err)
	assert.Positive(t, count, "sqlite_stat1 should remain populated")
}

func TestRunPlannerStats_ReturnsErrorWhenUpdaterFails(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Close())

	err := RunPlannerStats(db, slog.Default())
	require.Error(t, err)
	assert.ErrorContains(t, err, "planner statistics maintenance")
}
