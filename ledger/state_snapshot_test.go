package ledger

import (
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerStateSnapshotPublicationIsImmutable verifies that publishing a
// replacement snapshot does not mutate snapshots retained by existing readers.
func TestLedgerStateSnapshotPublicationIsImmutable(t *testing.T) {
	ls := &LedgerState{
		currentEpoch: models.Epoch{
			EpochId: 7,
			Nonce:   []byte{7},
		},
		epochCache: []models.Epoch{{EpochId: 7, Nonce: []byte{7}}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{Slot: 70, Hash: []byte{7}},
		},
		currentTipBlockNonce: []byte{17},
		transitionInfo:       hardfork.NewTransitionUnknown(),
	}
	// Publish the initial writer-owned state and retain the exact pointers a
	// reader could still be using when a later update is published.
	ls.publishSnapshotsLocked()

	oldConsensus := ls.consensus.Load()
	oldTip := ls.tip.Load()
	// Replace slice-backed epoch-cache state before mutation, matching the
	// production copy-on-write invariant, then publish a new generation. The
	// retained snapshots must continue to expose generation 7.
	ls.currentEpoch.EpochId = 8
	ls.currentEpoch.Nonce[0] = 8
	ls.epochCache = cloneEpochs(ls.epochCache)
	ls.epochCache[0].Nonce[0] = 8
	ls.currentTip.Point.Hash[0] = 8
	ls.currentTipBlockNonce[0] = 18
	ls.publishSnapshotsLocked()

	require.Equal(t, uint64(7), oldConsensus.currentEpoch.EpochId)
	require.Equal(t, byte(7), oldConsensus.currentEpoch.Nonce[0])
	require.Equal(t, byte(7), oldConsensus.epochCache[0].Nonce[0])
	require.Equal(t, byte(7), oldTip.currentTip.Point.Hash[0])
	require.Equal(t, byte(17), oldTip.currentTipBlockNonce[0])
}

// TestLedgerStatePublishedEpochCacheRejectsInPlaceAppend verifies that
// publication removes spare capacity from the writer's cache view. An
// accidental append must allocate instead of extending storage shared with a
// snapshot retained by a concurrent reader.
func TestLedgerStatePublishedEpochCacheRejectsInPlaceAppend(t *testing.T) {
	cache := make([]models.Epoch, 1, 2)
	cache[0] = models.Epoch{EpochId: 7}
	ls := &LedgerState{epochCache: cache}
	ls.publishSnapshotsLocked()

	oldConsensus := ls.consensus.Load()
	require.Equal(t, len(ls.epochCache), cap(ls.epochCache))
	ls.epochCache = append(ls.epochCache, models.Epoch{EpochId: 8})
	ls.epochCache[0].EpochId = 9

	require.Len(t, oldConsensus.epochCache, 1)
	require.Equal(t, uint64(7), oldConsensus.epochCache[0].EpochId)
}

// TestSetEpochCachePublishesPartialStateOnError verifies that startup cache
// mutations are published even when validation returns an error afterward.
func TestSetEpochCachePublishesPartialStateOnError(t *testing.T) {
	ls := &LedgerState{}
	ls.publishSnapshotsLocked()
	previousGeneration := ls.consensus.Load().generation
	invalidEpoch := models.Epoch{
		EpochId:   7,
		StartSlot: 0,
		EraId:     ^uint(0),
	}

	err := ls.setEpochCache(nil, []models.Epoch{invalidEpoch})
	require.ErrorContains(t, err, "unknown era ID")

	// The deferred publication must expose the mutation through the atomic
	// reader path despite setEpochCache returning before normal completion.
	snapshot := ls.consensus.Load()
	require.Equal(t, previousGeneration+1, snapshot.generation)
	require.Equal(t, invalidEpoch.EpochId, snapshot.currentEpoch.EpochId)
	require.Equal(t, invalidEpoch.EraId, snapshot.currentEpoch.EraId)
	require.Len(t, snapshot.epochCache, 1)
}

// TestAdvanceEpochCachePreservesPublishedSnapshot exercises the production
// writer and verifies that extending the cache cannot alter a retained view.
func TestAdvanceEpochCachePreservesPublishedSnapshot(t *testing.T) {
	// An empty nonce selects the deterministic initial-epoch path, allowing the
	// production writer to run without seeding block-nonce database records.
	initialEpoch := models.Epoch{
		EpochId:       7,
		StartSlot:     70,
		LengthInSlots: 10,
		SlotLength:    1_000,
		EraId:         eras.ConwayEraDesc.Id,
	}
	ls := &LedgerState{
		currentEpoch: initialEpoch,
		currentEra:   eras.ConwayEraDesc,
		epochCache:   []models.Epoch{initialEpoch},
		config: LedgerStateConfig{
			CardanoNodeConfig: &cardano.CardanoNodeConfig{
				ShelleyGenesisHash: strings.Repeat("01", 32),
			},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	// Keep the exact snapshot pointer that a concurrent reader may retain while
	// advanceEpochCache publishes the next generation.
	ls.publishSnapshotsLocked()
	oldConsensus := ls.consensus.Load()

	// Exercise the real header-verification writer rather than reproducing its
	// copy-on-write logic inside this test.
	require.NoError(t, ls.advanceEpochCache())

	// The current view must advance, while the retained view must remain on the
	// original cache and epoch values.
	newConsensus := ls.consensus.Load()
	require.Len(t, newConsensus.epochCache, 2)
	require.Equal(t, uint64(8), newConsensus.epochCache[1].EpochId)
	require.Len(t, oldConsensus.epochCache, 1)
	require.Equal(t, uint64(7), oldConsensus.epochCache[0].EpochId)
}

// TestEpochRolloverPParamsClonePreservesPublishedSnapshot verifies that epoch
// updates mutate a transaction-owned parameter value, not a retained snapshot.
func TestEpochRolloverPParamsClonePreservesPublishedSnapshot(t *testing.T) {
	rat := func() *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(1, 2)} }
	original := &shelley.ShelleyProtocolParameters{
		MinFeeA:          44,
		A0:               rat(),
		Rho:              rat(),
		Tau:              rat(),
		Decentralization: rat(),
	}
	ls := &LedgerState{
		currentEra:     eras.ShelleyEraDesc,
		currentPParams: original,
	}
	ls.publishSnapshotsLocked()
	oldConsensus := ls.consensus.Load()

	// Use the same ownership boundary as processEpochRollover, then exercise
	// the real era update function, which mutates its concrete pointer in place.
	owned, err := cloneProtocolParametersForEra(
		eras.ShelleyEraDesc,
		oldConsensus.currentPParams,
	)
	require.NoError(t, err)
	newMinFeeA := uint(99)
	updated, err := eras.PParamsUpdateShelley(
		owned,
		shelley.ShelleyProtocolParameterUpdate{MinFeeA: &newMinFeeA},
	)
	require.NoError(t, err)
	updatedShelley := updated.(*shelley.ShelleyProtocolParameters)

	// The in-place scalar update must stay isolated from the protocol parameters
	// held by the previously published snapshot.
	oldShelley := oldConsensus.currentPParams.(*shelley.ShelleyProtocolParameters)
	require.Equal(t, uint(44), oldShelley.MinFeeA)
	require.Equal(t, uint(99), updatedShelley.MinFeeA)
}

// TestProcessEpochRolloverAppliesUpdateToOwnedCopy drives the real
// processEpochRollover writer end-to-end with a pending on-chain pparam
// update, so the era's update function runs and mutates its concrete
// parameter pointer in place. A previously published snapshot's pparams must
// stay untouched; only cloneProtocolParametersForEra's copy may change.
func TestProcessEpochRolloverAppliesUpdateToOwnedCopy(t *testing.T) {
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 432000,
		"slotLength": 1,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	currentEpoch := models.Epoch{
		EpochId:       5,
		StartSlot:     500,
		SlotLength:    1000,
		LengthInSlots: 100,
		EraId:         eras.ShelleyEraDesc.Id,
	}
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot, currentEpoch.EpochId,
		nil, nil, nil, nil,
		currentEpoch.EraId, currentEpoch.SlotLength, currentEpoch.LengthInSlots,
		nil,
	))

	// A pending update targeting the rollover's next epoch. Quorum defaults
	// to 0 (no UpdateQuorum in the genesis JSON above), so a single proposal
	// is enough for ComputeAndApplyPParamUpdates to apply it.
	newMinFeeA := uint(99)
	updateCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameterUpdate{
		MinFeeA: &newMinFeeA,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x01, 0x02, 0x03},
		updateCbor,
		currentEpoch.StartSlot+1,
		currentEpoch.EpochId+1,
		nil,
	))

	rat := func() *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(1, 2)} }
	original := &shelley.ShelleyProtocolParameters{
		MinFeeA:          44,
		A0:               rat(),
		Rho:              rat(),
		Tau:              rat(),
		Decentralization: rat(),
	}
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ShelleyEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: original,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	oldConsensus := ls.consensus.Load()

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			ls.currentEra,
			ls.currentPParams,
		)
		return rolloverErr
	}))

	updatedShelley, ok := result.NewCurrentPParams.(*shelley.ShelleyProtocolParameters)
	require.True(t, ok)
	require.Equal(t, uint(99), updatedShelley.MinFeeA)

	oldShelley := oldConsensus.currentPParams.(*shelley.ShelleyProtocolParameters)
	require.Equal(t, uint(44), oldShelley.MinFeeA)
}

// TestLedgerStateTipGetterReturnsDefensiveHashCopy verifies that callers cannot
// mutate the published tip hash through the value returned by Tip.
func TestLedgerStateTipGetterReturnsDefensiveHashCopy(t *testing.T) {
	ls := &LedgerState{currentTip: ochainsync.Tip{
		Point: ocommon.Point{Slot: 1, Hash: []byte{1, 2, 3}},
	}}
	ls.publishSnapshotsLocked()

	// Mutate only the caller-owned return value. A subsequent read must still
	// return the hash stored in the immutable tip snapshot.
	tip := ls.Tip()
	tip.Point.Hash[0] = 9
	require.Equal(t, byte(1), ls.Tip().Point.Hash[0])
}

// TestLedgerStateSnapshotsStayConsistentWithConcurrentReaders verifies that
// readers never observe fields from different generations within either the
// consensus snapshot or the tip snapshot while a writer repeatedly publishes.
func TestLedgerStateSnapshotsStayConsistentWithConcurrentReaders(
	t *testing.T,
) {
	ls := &LedgerState{}
	ls.publishSnapshotsLocked()

	const generations = 500
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	done := make(chan struct{})

	// Readers continuously validate generation markers encoded in the paired
	// fields. Any mismatched marker would indicate a torn snapshot read.
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				consensusState := ls.loadConsensusSnapshot()
				if consensusState.currentEpoch.EpochId !=
					uint64(consensusState.currentEra.Id) {
					select {
					case errCh <- fmt.Errorf("torn consensus snapshot"):
					default:
					}
					return
				}
				tipState := ls.loadTipSnapshot()
				if len(tipState.currentTipBlockNonce) > 0 &&
					tipState.currentTip.Point.Slot !=
						uint64(tipState.currentTipBlockNonce[0]) {
					select {
					case errCh <- fmt.Errorf("torn tip snapshot"):
					default:
					}
					return
				}
			}
		}()
	}

	// Publish many generations while all readers are active. The existing
	// LedgerState lock continues to serialize writers; readers use only atomics.
	for generation := 1; generation <= generations; generation++ {
		marker := byte(generation % 256)
		ls.Lock()
		ls.currentEpoch.EpochId = uint64(marker)
		ls.currentEra.Id = uint(marker)
		ls.currentTip.Point.Slot = uint64(marker)
		ls.currentTipBlockNonce = []byte{marker}
		ls.publishSnapshotsLocked()
		ls.Unlock()
	}
	close(done)
	wg.Wait()
	// Report the first consistency failure, if any. The channel is buffered so
	// a reader can record an error without blocking other goroutines.
	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}

// TestLedgerStatePairedSnapshotsUseOneGeneration verifies that callers which
// combine consensus and tip fields never observe adjacent publications while a
// writer is between the two atomic stores.
func TestLedgerStatePairedSnapshotsUseOneGeneration(t *testing.T) {
	ls := &LedgerState{}
	ls.publishSnapshotsLocked()

	const generations = 1_000
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	done := make(chan struct{})

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				consensusState, tipState := ls.loadStateSnapshots()
				if consensusState.generation != tipState.generation ||
					consensusState.currentEpoch.EpochId !=
						tipState.currentTip.Point.Slot {
					select {
					case errCh <- fmt.Errorf(
						"cross-snapshot generation was torn",
					):
					default:
					}
					return
				}
			}
		}()
	}

	for generation := 1; generation <= generations; generation++ {
		ls.Lock()
		ls.currentEpoch.EpochId = uint64(generation)
		ls.currentTip.Point.Slot = uint64(generation)
		ls.publishSnapshotsLocked()
		ls.Unlock()
	}
	close(done)
	wg.Wait()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}
