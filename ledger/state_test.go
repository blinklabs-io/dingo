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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/big"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/dingo/utxoref"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloseStopsForgingScheduler verifies that Close stops ls.Scheduler,
// not just ls.slotClock/ls.dbWorkerPool. initForge registers the
// dev-mode block-forging task (ls.forgeBlock) on this scheduler as a
// fixed-interval task that writes directly to ls.chain/the database in
// its own transaction, entirely bypassing ls.dbWorkerPool -- so shutting
// down dbWorkerPool alone does not stop it. Left running past Close, a
// live restore/truncate's quiesce (which only stops the production
// BlockForger, node_lifecycle.go) would leave this scheduler free to
// keep firing forgeBlock against a LedgerState being closed and replaced
// out from under it, racing the live operation's own storage mutations
// and the subsequently-constructed LedgerState's own new Scheduler. A
// stray block landing in that window can leave the persistent block-ID
// index with a gap whose far side doesn't chain from the post-operation
// tip -- surfacing later as a "persistent chain index gap" error from
// the chain iterator (chain/chain.go) and a permanently stalled tip.
//
// This registers a plain counting task directly on ls.Scheduler rather
// than exercising the real forgeBlock (which needs a full genesis/
// mempool/VRF setup to run without erroring) -- the bug is specifically
// about Close's own resource-shutdown discipline forgetting this
// scheduler, not about forgeBlock's own logic, so a minimal stand-in
// task exercises the exact same missing-Stop-call gap.
func TestCloseStopsForgingScheduler(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.Scheduler = NewScheduler(time.Millisecond)
	ls.Scheduler.Start()

	var ticks atomic.Int64
	ls.Scheduler.Register(1, func() { ticks.Add(1) }, nil)

	// Confirm the scheduler is actually running before Close -- otherwise
	// the require.Never check below would pass vacuously against a
	// scheduler that was never ticking in the first place.
	require.Eventually(
		t, func() bool { return ticks.Load() > 0 },
		testutil.AsyncWait, time.Millisecond,
		"scheduler must be ticking before Close",
	)

	require.NoError(t, ls.Close())

	afterClose := ticks.Load()
	require.Never(
		t, func() bool { return ticks.Load() != afterClose },
		100*time.Millisecond, 5*time.Millisecond,
		"Close must stop the scheduler: no further ticks may fire afterward",
	)
}

// TestLedgerStateSnapshotPublicationIsImmutable verifies that publishing a
// replacement snapshot does not mutate snapshots retained by existing readers.
func TestLedgerStateSnapshotPublicationIsImmutable(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	ls := &LedgerState{}
	ls.publishSnapshotsLocked()
	previousGeneration := ls.consensus.Load().generation
	invalidEpoch := models.Epoch{
		EpochId:   7,
		StartSlot: 0,
		EraId:     ^uint(0),
	}

	err := ls.setEpochCache(&database.Txn{}, []models.Epoch{invalidEpoch})
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

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

	// A pending update submitted in the current epoch, which the rollover
	// enacts as the next epoch's parameters (submission epoch e -> enacted
	// for e+1). Quorum defaults to 0 (no UpdateQuorum in the genesis JSON
	// above), so a single proposal is enough to apply it.
	newMinFeeA := uint(99)
	updateCbor, err := cbor.Encode(map[uint64]any{0: newMinFeeA})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x01, 0x02, 0x03},
		updateCbor,
		currentEpoch.StartSlot+1,
		currentEpoch.EpochId, // submission epoch (enacted for EpochId+1)
		nil,
	))

	rat := func() *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(1, 2)} }
	original := &shelley.ShelleyProtocolParameters{
		MinFeeA:          44,
		A0:               rat(),
		Rho:              rat(),
		Tau:              rat(),
		Decentralization: rat(),
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
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
			false,
		)
		return rolloverErr
	}))
	if result == nil {
		t.Fatal("epoch rollover returned no result")
	}

	updatedShelley, ok := result.NewCurrentPParams.(*shelley.ShelleyProtocolParameters)
	require.True(t, ok)
	require.Equal(t, uint(99), updatedShelley.MinFeeA)

	oldShelley := oldConsensus.currentPParams.(*shelley.ShelleyProtocolParameters)
	require.Equal(t, uint(44), oldShelley.MinFeeA)
}

func TestProcessEpochRolloverRetainsDijkstraProtocolParameters(t *testing.T) {
	t.Parallel()

	cfg := dijkstraRetentionNodeConfig(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	currentEpoch := models.Epoch{
		EpochId:       5,
		StartSlot:     500,
		SlotLength:    1_000,
		LengthInSlots: 100,
		EraId:         eras.DijkstraEraDesc.Id,
	}
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		nil,
		nil,
		nil,
		nil,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))
	original := dijkstraRetentionPParams()
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.DijkstraEraDesc,
		activeEras:     eras.ErasWithDijkstra,
		currentEpoch:   currentEpoch,
		currentPParams: original,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = ls.processEpochRollover(
			txn,
			currentEpoch,
			eras.DijkstraEraDesc,
			original,
			false,
		)
		return rolloverErr
	}))
	require.NotNil(t, result)
	retained, ok := result.NewCurrentPParams.(*gdijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	assertDijkstraRetentionPParams(t, retained)
}

func TestLoadPParamsRehydratesPersistedDijkstraProtocolParameters(
	t *testing.T,
) {
	t.Parallel()

	cfg := dijkstraRetentionNodeConfig(t)
	ls := dijkstraRetentionRestartLedgerState(t, cfg)
	require.NoError(t, ls.loadPParams())
	retained, ok := ls.currentPParams.(*gdijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	assertDijkstraRetentionPParams(t, retained)
}

func TestLoadPParamsRejectsInvalidRehydratedDijkstraCommitteeParameters(
	t *testing.T,
) {
	t.Parallel()

	cfg := dijkstraRetentionNodeConfigWithCommitteeParameters(t, 0.5, 0.6)
	ls := dijkstraRetentionRestartLedgerState(t, cfg)

	err := ls.loadPParams()
	require.ErrorContains(t, err, "validate persisted Dijkstra pparams")
	require.ErrorContains(
		t,
		err,
		"quorum stake threshold must be less than committee stake coverage",
	)
	require.Nil(t, ls.currentPParams)
}

func dijkstraRetentionRestartLedgerState(
	t *testing.T,
	cfg *cardano.CardanoNodeConfig,
) *LedgerState {
	t.Helper()
	dataDir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dataDir,
		Logger:  logger,
	})
	require.NoError(t, err)

	epoch := models.Epoch{
		EpochId:       5,
		StartSlot:     500,
		SlotLength:    1_000,
		LengthInSlots: 100,
		EraId:         eras.DijkstraEraDesc.Id,
	}
	require.NoError(t, db.SetEpoch(
		epoch.StartSlot,
		epoch.EpochId,
		nil,
		nil,
		nil,
		nil,
		epoch.EraId,
		epoch.SlotLength,
		epoch.LengthInSlots,
		nil,
	))
	encoded, err := cbor.Encode(dijkstraRetentionPParams())
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		encoded,
		epoch.StartSlot,
		epoch.EpochId,
		epoch.EraId,
		nil,
	))
	require.NoError(t, dbtest.CloseDatabase(db))

	reopened, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dataDir,
		Logger:  logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(reopened))
	})
	return &LedgerState{
		db:           reopened,
		currentEra:   eras.DijkstraEraDesc,
		activeEras:   eras.ErasWithDijkstra,
		currentEpoch: epoch,
		epochCache:   []models.Epoch{epoch},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            logger,
		},
	}
}

func dijkstraRetentionNodeConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	return dijkstraRetentionNodeConfigWithCommitteeParameters(t, 0.8, 0.6)
}

func dijkstraRetentionNodeConfigWithCommitteeParameters(
	t *testing.T,
	committeeStakeCoverage float64,
	quorumStakeThreshold float64,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
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
	}`)))
	dijkstraGenesis := fmt.Sprintf(`{
		"maxRefScriptSizePerBlock": 100,
		"maxRefScriptSizePerTx": 50,
		"refScriptCostStride": 16,
		"refScriptCostMultiplier": 1.25,
		"committeeStakeCoverage": %v,
		"quorumStakeThreshold": %v
	}`, committeeStakeCoverage, quorumStakeThreshold)
	require.NoError(
		t,
		cfg.LoadDijkstraGenesisFromReader(strings.NewReader(dijkstraGenesis)),
	)
	return cfg
}

func dijkstraRetentionPParams() *gdijkstra.DijkstraProtocolParameters {
	conwayPParams := mockledger.NewMockConwayProtocolParams()
	conwayPParams.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{
		Major: gdijkstra.MinProtocolVersionDijkstra,
	}
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conwayPParams,
		MaxRefScriptSizePerBlock: 2_000,
		MaxRefScriptSizePerTx:    1_000,
		RefScriptCostStride:      128,
		RefScriptCostMultiplier: &cbor.Rat{
			Rat: big.NewRat(7, 4),
		},
		CommitteeStakeCoverage: &cbor.Rat{
			Rat: big.NewRat(4, 5),
		},
		QuorumStakeThreshold: &cbor.Rat{
			Rat: big.NewRat(3, 5),
		},
	}
}

func assertDijkstraRetentionPParams(
	t *testing.T,
	pparams *gdijkstra.DijkstraProtocolParameters,
) {
	t.Helper()
	require.Equal(t, uint32(2_000), pparams.MaxRefScriptSizePerBlock)
	require.Equal(t, uint32(1_000), pparams.MaxRefScriptSizePerTx)
	require.Equal(t, uint32(128), pparams.RefScriptCostStride)
	require.NotNil(t, pparams.RefScriptCostMultiplier)
	require.NotNil(t, pparams.CommitteeStakeCoverage)
	require.NotNil(t, pparams.QuorumStakeThreshold)
	require.Zero(t, pparams.RefScriptCostMultiplier.Cmp(big.NewRat(7, 4)))
	require.Zero(t, pparams.CommitteeStakeCoverage.Cmp(big.NewRat(4, 5)))
	require.Zero(t, pparams.QuorumStakeThreshold.Cmp(big.NewRat(3, 5)))
}

// TestLedgerStateTipGetterReturnsDefensiveHashCopy verifies that callers cannot
// mutate the published tip hash through the value returned by Tip.
func TestLedgerStateTipGetterReturnsDefensiveHashCopy(t *testing.T) {
	t.Parallel()

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

// TestLedgerStateSnapshotLoadersDoNotRaceWithWriters guards the loader path
// against regressing from atomic snapshot loads to reads of live writer-owned
// fields. Intra-snapshot consistency itself is guaranteed by atomic.Pointer;
// this test's primary value is its execution under the race detector.
func TestLedgerStateSnapshotLoadersDoNotRaceWithWriters(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{}
	ls.publishSnapshotsLocked()

	const generations = 500
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	done := make(chan struct{})

	// Readers continuously exercise both atomic loaders. Matching markers make
	// an accidental return to independently-read live fields fail logically as
	// well as through the race detector.
	for range 8 {
		wg.Go(func() {
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
					case errCh <- fmt.Errorf("inconsistent consensus read"):
					default:
					}
					return
				}
				tipState := ls.loadTipSnapshot()
				if len(tipState.currentTipBlockNonce) > 0 &&
					tipState.currentTip.Point.Slot !=
						uint64(tipState.currentTipBlockNonce[0]) {
					select {
					case errCh <- fmt.Errorf("inconsistent tip read"):
					default:
					}
					return
				}
			}
		})
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
	t.Parallel()

	ls := &LedgerState{}
	ls.publishSnapshotsLocked()

	const generations = 1_000
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	done := make(chan struct{})

	for range 8 {
		wg.Go(func() {
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
		})
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

// newTipGapTestLedgerState builds the smallest LedgerState that can run
// handleSlotTicks: initialised metrics, a published tip snapshot, and a slot
// tick channel. reachedTip stays false, so the loop takes its catch-up
// `continue` immediately after the tip-gap update this test is about.
func newTipGapTestLedgerState(
	t *testing.T,
	tipSlot uint64,
	report ReportTipGapFunc,
) (*LedgerState, chan SlotTick, *stateMetrics) {
	t.Helper()

	ticks := make(chan SlotTick, 1)
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
			ReportTipGapFunc: report,
		},
		slotTickChan: ticks,
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, []byte("tip")),
	}
	ls.publishSnapshotsLocked()
	return ls, ticks, &ls.metrics
}

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, gauge.Write(&metric))
	require.NotNil(t, metric.Gauge)
	return metric.Gauge.GetValue()
}

// TestHandleSlotTicksReportsTipGap pins the producer end of the readiness
// signal: every slot tick hands the health reporter the same wall-clock-to-tip
// distance it publishes as dingo_tip_gap_slots. Reading it from the ledger
// rather than scraping Prometheus is what lets /readyz work with the metrics
// listener disabled.
func TestHandleSlotTicksReportsTipGap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tipSlot  uint64
		tickSlot uint64
		want     uint64
	}{
		{
			name:     "tip behind wall clock",
			tipSlot:  500,
			tickSlot: 1750,
			want:     1250,
		},
		{name: "tip at wall clock", tipSlot: 900, tickSlot: 900, want: 0},
		// A tip ahead of the slot clock is not a negative gap.
		{name: "tip ahead of wall clock", tipSlot: 900, tickSlot: 880, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reported := make(chan uint64, 4)
			ls, ticks, metrics := newTipGapTestLedgerState(
				t,
				test.tipSlot,
				func(gap uint64) { reported <- gap },
			)

			done := make(chan struct{})
			go func() {
				ls.handleSlotTicks()
				close(done)
			}()

			ticks <- SlotTick{Slot: test.tickSlot}

			select {
			case got := <-reported:
				assert.Equal(t, test.want, got)
			case <-time.After(10 * time.Second):
				t.Fatal("slot tick did not report a tip gap")
			}

			close(ticks)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("handleSlotTicks did not return")
			}

			// The reported value and the exported gauge must not drift.
			assert.Equal(
				t,
				float64(test.want),
				gaugeValue(t, metrics.tipGapSlots),
			)
		})
	}
}

// A nil reporter is the configuration every ledger test and every
// non-node caller uses; it must not panic.
func TestHandleSlotTicksToleratesNilTipGapReporter(t *testing.T) {
	t.Parallel()

	ls, ticks, _ := newTipGapTestLedgerState(t, 100, nil)
	done := make(chan struct{})
	go func() {
		ls.handleSlotTicks()
		close(done)
	}()
	ticks <- SlotTick{Slot: 200}
	close(ticks)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handleSlotTicks did not return")
	}
}

// While the applied ledger is behind the wall clock the slot clock emits no
// ticks, so handleBehindHorizon is the only thing keeping the gauges live.
// Before it existed a from-genesis sync read as a fully synced node.
func TestHandleBehindHorizonPublishesGaugesButNotReadiness(t *testing.T) {
	t.Parallel()

	reported := make(chan uint64, 1)
	ls, _, metrics := newTipGapTestLedgerState(
		t,
		6_500_000,
		func(gap uint64) { reported <- gap },
	)
	ls.currentEpoch.LengthInSlots = 432_000
	ls.publishSnapshotsLocked()

	ls.handleBehindHorizon(74_600_000)

	assert.Equal(t, float64(68_100_000), gaugeValue(t, metrics.tipGapSlots))
	assert.Equal(t, float64(432_000), gaugeValue(t, metrics.epochLengthSlots))
	// The readiness probe must not learn a gap from a paused-tick report.
	select {
	case gap := <-reported:
		t.Fatalf("ReportTipGapFunc called during catch-up with gap %d", gap)
	default:
	}
}

// An epoch length that is not yet known is left unset rather than
// published as a fabricated value.
func TestHandleBehindHorizonLeavesUnknownEpochLengthUnset(t *testing.T) {
	t.Parallel()

	ls, _, metrics := newTipGapTestLedgerState(t, 100, nil)

	ls.handleBehindHorizon(1_000)

	assert.Equal(t, float64(900), gaugeValue(t, metrics.tipGapSlots))
	assert.Zero(t, gaugeValue(t, metrics.epochLengthSlots))
}

// initScheduler is where the slot clock is handed handleBehindHorizon. The
// handleBehindHorizon tests call it directly and the slot clock tests build
// their own config, so without this test deleting that wiring leaves every
// other test green and a from-genesis sync reads as fully synced again.
func TestInitSchedulerWiresBehindHorizonCallback(t *testing.T) {
	t.Parallel()

	ls, _, metrics := newTipGapTestLedgerState(t, 100, nil)
	ls.currentEpoch.SlotLength = 1000
	require.NoError(t, ls.initScheduler())
	t.Cleanup(ls.Scheduler.Stop)

	callback := ls.slotClock.config.OnBehindHorizon
	require.NotNil(t, callback)
	callback(1_000)

	assert.Equal(t, float64(900), gaugeValue(t, metrics.tipGapSlots))
}

func TestLedgerProcessBlocksFromSourceReturnsNilWhenReaderCloses(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{
		validationEnabled: true,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	readChainResultCh := make(chan readChainResult, 1)
	close(readChainResultCh)

	err := ls.ledgerProcessBlocksFromSource(
		context.Background(),
		readChainResultCh,
	)
	require.NoError(t, err)
}

func TestLedgerProcessBlocksFromSourceReturnsReadChainError(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		validationEnabled: true,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	resultDone := make(chan struct{})
	readChainResultCh := make(chan readChainResult, 1)
	readChainResultCh <- readChainResult{
		err:  errors.New("decode block at slot 20"),
		done: resultDone,
	}
	close(readChainResultCh)

	err := ls.ledgerProcessBlocksFromSource(t.Context(), readChainResultCh)
	require.ErrorContains(t, err, "read-chain decode or validation")
	select {
	case <-resultDone:
	default:
		t.Fatal("reader result was not released after decode failure")
	}
}

func TestHandleLedgerProcessBlocksErrorLogsPersistentValidationFailure(
	t *testing.T,
) {
	t.Parallel()

	haltErr := fmt.Errorf("process block batch: %w", errHaltLedgerPipeline)
	fatalCalled := false
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			FatalErrorFunc: func(error) {
				fatalCalled = true
			},
		},
	}

	ls.handleLedgerProcessBlocksError(haltErr)
	require.False(t, fatalCalled)
}

func TestHandleLedgerProcessBlocksErrorDoesNotReportFatalErrors(
	t *testing.T,
) {
	t.Parallel()

	fatalCalled := false
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			FatalErrorFunc: func(error) {
				fatalCalled = true
			},
		},
	}

	ls.handleLedgerProcessBlocksError(errRestartLedgerPipeline)
	require.False(t, fatalCalled)

	ls.handleLedgerProcessBlocksError(errors.New("transient"))
	require.False(t, fatalCalled)
}

// It verifies that calculating the stability window is synchronized with
// concurrent currentEra updates from block processing.
func TestCalculateStabilityWindowConcurrentCurrentEraAccess(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		<-start
		for i := range 100 {
			ls.Lock()
			if i%2 == 0 {
				ls.currentEra = eras.BabbageEraDesc
			} else {
				ls.currentEra = eras.ConwayEraDesc
			}
			ls.Unlock()
		}
		close(done)
	})

	for range 8 {
		wg.Go(func() {
			<-start
			for {
				select {
				case <-done:
					return
				default:
					_ = ls.calculateStabilityWindow()
				}
			}
		})
	}

	close(start)
	wg.Wait()
}

func TestSecurityParamConcurrentCurrentEraAccess(t *testing.T) {
	t.Parallel()

	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 3
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		<-start
		for i := range 100 {
			ls.Lock()
			if i%2 == 0 {
				ls.currentEra = eras.BabbageEraDesc
			} else {
				ls.currentEra = eras.ConwayEraDesc
			}
			ls.Unlock()
		}
		close(done)
	})

	for range 8 {
		wg.Go(func() {
			<-start
			for {
				select {
				case <-done:
					return
				default:
					_ = ls.SecurityParam()
				}
			}
		})
	}

	close(start)
	wg.Wait()
}

// TestCalculateStabilityWindow_ByronEra tests the stability window calculation for Byron era
func TestCalculateStabilityWindow_ByronEra(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		k              int
		expectedWindow uint64
	}{
		{
			name:           "Byron era with k=432",
			k:              432,
			expectedWindow: 864,
		},
		{
			name:           "Byron era with k=2160",
			k:              2160,
			expectedWindow: 4320,
		},
		{
			name:           "Byron era with k=1",
			k:              1,
			expectedWindow: 2,
		},
		{
			name:           "Byron era with k=100",
			k:              100,
			expectedWindow: 200,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			byronGenesisJSON := fmt.Sprintf(`{
				"protocolConsts": {
					"k": %d,
					"protocolMagic": 2
				}
			}`, tc.k)

			shelleyGenesisJSON := `{
				"activeSlotsCoeff": 0.05,
				"securityParam": 432,
				"systemStart": "2022-10-25T00:00:00Z"
			}`

			cfg := &cardano.CardanoNodeConfig{}
			if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
				t.Fatalf("failed to load Byron genesis: %v", err)
			}
			if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
				t.Fatalf("failed to load Shelley genesis: %v", err)
			}

			ls := &LedgerState{
				currentEra: eras.ByronEraDesc, // Byron era has Id = 0
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			result := ls.calculateStabilityWindow()
			if result != tc.expectedWindow {
				t.Errorf(
					"expected stability window %d, got %d",
					tc.expectedWindow,
					result,
				)
			}
		})
	}
}

// TestCalculateStabilityWindow_ShelleyEra tests the stability window calculation for Shelley+ eras
func TestCalculateStabilityWindow_ShelleyEra(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		k                int
		activeSlotsCoeff float64
		expectedWindow   uint64
		description      string
	}{
		{
			name:             "Shelley era with k=432, f=0.05",
			k:                432,
			activeSlotsCoeff: 0.05,
			// 3k/f = 3*432/0.05 = 1296/0.05 = 25920
			expectedWindow: 25920,
			description:    "Standard Shelley parameters",
		},
		{
			name:             "Shelley era with k=2160, f=0.05",
			k:                2160,
			activeSlotsCoeff: 0.05,
			// 3k/f = 3*2160/0.05 = 6480/0.05 = 129600
			expectedWindow: 129600,
			description:    "Mainnet parameters",
		},
		{
			name:             "Shelley era with k=100, f=0.1",
			k:                100,
			activeSlotsCoeff: 0.1,
			// 3k/f = 3*100/0.1 = 300/0.1 = 3000
			expectedWindow: 3000,
			description:    "Higher active slots coefficient",
		},
		{
			name:             "Shelley era with k=432, f=0.2",
			k:                432,
			activeSlotsCoeff: 0.2,
			// 3k/f = 3*432/0.2 = 1296/0.2 = 6480
			expectedWindow: 6480,
			description:    "Even higher active slots coefficient",
		},
		{
			name:             "Shelley era with k=50, f=0.5",
			k:                50,
			activeSlotsCoeff: 0.5,
			// 3k/f = 3*50/0.5 = 150/0.5 = 300
			expectedWindow: 300,
			description:    "Very high active slots coefficient",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			byronGenesisJSON := `{
				"protocolConsts": {
					"k": 432,
					"protocolMagic": 2
				}
			}`

			shelleyGenesisJSON := fmt.Sprintf(`{
				"activeSlotsCoeff": %f,
				"securityParam": %d,
				"systemStart": "2022-10-25T00:00:00Z"
			}`, tc.activeSlotsCoeff, tc.k)

			cfg := &cardano.CardanoNodeConfig{}
			if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
				t.Fatalf("failed to load Byron genesis: %v", err)
			}
			if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
				t.Fatalf("failed to load Shelley genesis: %v", err)
			}

			ls := &LedgerState{
				currentEra: eras.ShelleyEraDesc, // Shelley era has Id = 1
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			result := ls.calculateStabilityWindow()
			if result != tc.expectedWindow {
				t.Errorf(
					"%s: expected stability window %d, got %d",
					tc.description,
					tc.expectedWindow,
					result,
				)
			}
		})
	}
}

// TestCalculateStabilityWindow_EdgeCases tests edge cases and error conditions
func TestCalculateStabilityWindow_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("Missing Byron genesis returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 432,
			"systemStart": "2022-10-25T00:00:00Z"
		}`
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != blockfetchBatchSlotThresholdDefault {
			t.Errorf(
				"expected default threshold %d, got %d",
				blockfetchBatchSlotThresholdDefault,
				result,
			)
		}
	})

	t.Run("Missing Shelley genesis returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != 864 {
			t.Errorf("expected default threshold %d, got %d", 864, result)
		}
	})

	t.Run("Zero k in Byron era returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 0,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 432,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		_ = loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON))
		_ = cfg.LoadShelleyGenesisFromReader(
			strings.NewReader(shelleyGenesisJSON),
		)

		ls := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != blockfetchBatchSlotThresholdDefault {
			t.Errorf(
				"expected default threshold %d for zero k, got %d",
				blockfetchBatchSlotThresholdDefault,
				result,
			)
		}
	})

	t.Run("Zero k in Shelley era returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 0,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		_ = loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON))
		_ = cfg.LoadShelleyGenesisFromReader(
			strings.NewReader(shelleyGenesisJSON),
		)

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != blockfetchBatchSlotThresholdDefault {
			t.Errorf(
				"expected default threshold %d for zero k, got %d",
				blockfetchBatchSlotThresholdDefault,
				result,
			)
		}
	})
}

// TestCalculateStabilityWindow_ActiveSlotsCoefficientEdgeCases tests various active slots coefficient scenarios
func TestCalculateStabilityWindow_ActiveSlotsCoefficientEdgeCases(
	t *testing.T,
) {
	t.Parallel()

	t.Run("Very small active slots coefficient", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.01,
			"securityParam": 432,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		// 3*432/0.01 = 129600
		expectedWindow := uint64(129600)
		if result != expectedWindow {
			t.Errorf(
				"expected stability window %d, got %d",
				expectedWindow,
				result,
			)
		}
	})

	t.Run("Rounding up with remainder", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.07,
			"securityParam": 100,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		// 3*100/0.07 = 300/0.07 = 4285.714... should round up to 4286
		if result < 4285 || result > 4287 {
			t.Errorf("expected stability window around 4286, got %d", result)
		}
	})

	t.Run("Precision with fractional coefficient", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.333333,
			"securityParam": 1000,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		// 3*1000/0.333333 ≈ 9000
		if result == 0 {
			t.Error("expected non-zero stability window")
		}
		if result < 8999 || result > 9002 {
			t.Errorf("expected stability window around 9000, got %d", result)
		}
	})
}

// TestCalculateStabilityWindow_AllEras tests calculation across different eras
func TestCalculateStabilityWindow_AllEras(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	testCases := []struct {
		name           string
		era            eras.EraDesc
		expectedWindow uint64
	}{
		{
			name:           "Byron era",
			era:            eras.ByronEraDesc,
			expectedWindow: 864, // 2k
		},
		{
			name:           "Shelley era",
			era:            eras.ShelleyEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Allegra era",
			era:            eras.AllegraEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Mary era",
			era:            eras.MaryEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Alonzo era",
			era:            eras.AlonzoEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Babbage era",
			era:            eras.BabbageEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Conway era",
			era:            eras.ConwayEraDesc,
			expectedWindow: 25920, // 3k/f
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				currentEra: tc.era,
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			result := ls.calculateStabilityWindow()
			if result != tc.expectedWindow {
				t.Errorf(
					"era %s: expected stability window %d, got %d",
					tc.era.Name,
					tc.expectedWindow,
					result,
				)
			}
		})
	}
}

// TestCalculateStabilityWindow_Integration tests the function in realistic scenarios
func TestCalculateStabilityWindow_Integration(t *testing.T) {
	t.Parallel()

	t.Run("Mainnet-like configuration", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 2160,
				"protocolMagic": 764824073
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 2160,
			"systemStart": "2017-09-23T21:44:51Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		// Test Byron era with mainnet params
		lsByron := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		resultByron := lsByron.calculateStabilityWindow()
		if resultByron != 4320 {
			t.Errorf(
				"Byron era: expected stability window 4320, got %d",
				resultByron,
			)
		}

		// Test Shelley era with mainnet params
		lsShelley := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		resultShelley := lsShelley.calculateStabilityWindow()
		// 3*2160/0.05 = 129600
		if resultShelley != 129600 {
			t.Errorf(
				"Shelley era: expected stability window 129600, got %d",
				resultShelley,
			)
		}
	})

	t.Run("Preview testnet configuration", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 432,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		lsShelley := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := lsShelley.calculateStabilityWindow()
		// 3*432/0.05 = 25920
		if result != 25920 {
			t.Errorf(
				"Preview testnet: expected stability window 25920, got %d",
				result,
			)
		}
	})
}

// TestCalculateStabilityWindow_LargeValues tests with large but valid values
func TestCalculateStabilityWindow_LargeValues(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 1000000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result := ls.calculateStabilityWindow()
	// 3*1000000/0.05 = 60000000
	expectedWindow := uint64(60000000)
	if result != expectedWindow {
		t.Errorf("expected stability window %d, got %d", expectedWindow, result)
	}
}

func newNonceReadyTestConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.5,
		"securityParam": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)
	return cfg
}

func newNonceReadyTestLedgerState(
	t *testing.T,
	eventBus *event.EventBus,
	tipSlot uint64,
) *LedgerState {
	t.Helper()

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             10,
			StartSlot:           1000,
			LengthInSlots:       100,
			EraId:               eras.ShelleyEraDesc.Id,
			Nonce:               nil,
			EvolvingNonce:       []byte{0x02},
			CandidateNonce:      []byte{0x03},
			LastEpochBlockNonce: []byte{0x04},
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: tipSlot,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newNonceReadyTestConfig(t),
			EventBus:          eventBus,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

func TestLedgerStateIsNearTipUsesStabilityWindow(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newNonceReadyTestConfig(t),
		},
		currentEra: eras.ShelleyEraDesc,
	}
	ls.syncUpstreamTipSlot.Store(1000)

	assert.False(t, ls.isNearTip(993), "gap above 3k/f should be catch-up")
	assert.True(t, ls.isNearTip(994), "gap equal to 3k/f should be near tip")
	assert.True(t, ls.isNearTip(1001), "local tip beyond upstream is near tip")
	assert.False(
		t,
		ls.isNearTipWithStabilityWindow(989, 10),
		"explicit window must reject a larger upstream gap",
	)
	assert.True(
		t,
		ls.isNearTipWithStabilityWindow(990, 10),
		"explicit window must accept an equal upstream gap",
	)
}

func TestSyncProgressDoesNotUseAdmittedQueueAsNetworkHeadAfterRestart(
	t *testing.T,
) {
	t.Parallel()

	activeConnID := testChainsyncConnId(6000, 3091)
	ls := &LedgerState{
		config: LedgerStateConfig{
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConnID
			},
			GetPeerSyncTargetFunc: func(
				ouroboros.ConnectionId,
			) (ochainsync.Tip, bool) {
				return ochainsync.Tip{
					Point:       ocommon.NewPoint(1000, []byte("network-head")),
					BlockNumber: 1000,
				}, true
			},
			CardanoNodeConfig: newNonceReadyTestConfig(t),
		},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(5, nil)},
	}
	ls.syncUpstreamTipSlot.Store(5)
	ls.publishActiveUpstream(activeConnID)
	ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
		ConnectionId:      activeConnID,
		SyncTarget:        ochainsync.Tip{Point: ocommon.NewPoint(1000, nil)},
		SyncTargetTrusted: true,
	})

	assert.Equal(t, uint64(5), ls.syncUpstreamTipSlot.Load(),
		"the admitted frontier remains available for bookkeeping")
	assert.Equal(t, uint64(1000), ls.UpstreamTipSlot(),
		"sync consumers must use the corroborated remote target")
	assert.InDelta(t, 0.005, ls.SyncProgress(), 0.000001)
	assert.False(t, ls.isNearTip(5),
		"a restarted node with only a few admitted headers remains in catch-up")
}

func TestUpstreamSyncTargetRequiresTrustedAdmissionAndActiveGeneration(
	t *testing.T,
) {
	t.Parallel()

	connA := testChainsyncConnId(6000, 3092)
	connB := testChainsyncConnId(6000, 3093)
	activeConn := connA
	targets := map[string]uint64{
		connIdKey(connA): 100,
		connIdKey(connB): 200,
	}
	ls := &LedgerState{
		config: LedgerStateConfig{
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId { return &activeConn },
			GetPeerSyncTargetFunc: func(connId ouroboros.ConnectionId) (ochainsync.Tip, bool) {
				return ochainsync.Tip{
					Point: ocommon.NewPoint(targets[connIdKey(connId)], nil),
				}, true
			},
		},
	}

	ls.publishActiveUpstream(connA)
	assert.Zero(
		t,
		ls.UpstreamTipSlot(),
		"active selection alone must not trust a target",
	)
	// Model the independent queues: a rejected peer-tip observation R is
	// delivered before ledger later admits header V. R must not be recovered
	// from mutable selector state when V is published.
	ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
		ConnectionId: connA,
		SyncTarget:   ochainsync.Tip{Point: ocommon.NewPoint(999, nil)},
	})
	assert.Zero(t, ls.UpstreamTipSlot())
	ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
		ConnectionId: connA,
		SyncTarget: ochainsync.Tip{
			Point:       ocommon.NewPoint(100, nil),
			BlockNumber: 101,
		},
		SyncTargetTrusted: true,
	})
	assert.Equal(t, uint64(100), ls.UpstreamTipSlot())
	upstreamTip, upstreamLive := ls.UpstreamSyncTip()
	assert.True(t, upstreamLive)
	assert.Equal(t, uint64(101), upstreamTip.BlockNumber)

	// A→B changes the authoritative active connection before the ledger has
	// processed the switch. The A snapshot must not be visible as B's target.
	activeConn = connB
	target, active := ls.UpstreamSyncStatus()
	assert.True(t, active)
	assert.Zero(t, target)
	upstreamTip, upstreamLive = ls.UpstreamSyncTip()
	assert.True(t, upstreamLive)
	assert.Zero(t, upstreamTip.BlockNumber)
	ls.publishActiveUpstream(connB)
	assert.Zero(t, ls.UpstreamTipSlot())
	ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
		ConnectionId: connB,
		SyncTarget: ochainsync.Tip{
			Point:       ocommon.NewPoint(200, nil),
			BlockNumber: 202,
		},
		SyncTargetTrusted: true,
	})
	assert.Equal(t, uint64(200), ls.UpstreamTipSlot())
	upstreamTip, upstreamLive = ls.UpstreamSyncTip()
	assert.True(t, upstreamLive)
	assert.Equal(t, uint64(202), upstreamTip.BlockNumber)

	// A deferred or rejected header never reaches the trusted publication path.
	ls.recordAdmittedHeaderFrontier(ChainsyncEvent{ConnectionId: connB}, false)
	assert.Equal(t, uint64(200), ls.UpstreamTipSlot())
}

func TestNextEpochNonceReadyCutoffSlot(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Shelley → TPraos: stabilityWindow = 3k/f = 3*432/0.05 = 25920
	// cutoff = epochStart + epochLength - 25920
	//        = 106963200 + 86400 - 25920 = 107023680
	cutoffSlot, ok := ls.nextEpochNonceReadyCutoffSlot(models.Epoch{
		EpochId:       1238,
		StartSlot:     106963200,
		LengthInSlots: 86400,
		EraId:         eras.ShelleyEraDesc.Id,
	})
	require.True(t, ok)
	assert.Equal(t, uint64(107023680), cutoffSlot)
}

func TestNextEpochNonceReadyEpoch(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.5,
		"securityParam": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	currentSlot := uint64(1095)
	provider := newMockSlotTimeProvider(
		time.Now().Add(-time.Duration(currentSlot)*time.Second),
		time.Second,
		100,
	)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	clock.nowFunc = func() time.Time {
		return provider.systemStart.Add(
			time.Duration(currentSlot) * time.Second,
		)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             10,
			StartSlot:           1000,
			LengthInSlots:       100,
			EraId:               eras.ShelleyEraDesc.Id,
			Nonce:               nil,
			EvolvingNonce:       []byte{0x02},
			CandidateNonce:      []byte{0x03},
			LastEpochBlockNonce: []byte{0x04},
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1095,
			},
		},
		slotClock: clock,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.syncUpstreamTipSlot.Store(1100)
	ls.publishSnapshotsLocked()

	readyEpoch, ok := ls.NextEpochNonceReadyEpoch()
	require.True(t, ok)
	assert.Equal(t, uint64(11), readyEpoch)
}

func TestComputeNextEpochNonceUsesImportedTipAnchor(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	tipNonce := bytes.Repeat([]byte{0x22}, 32)
	candidateNonce := bytes.Repeat([]byte{0x33}, 32)

	require.NoError(t, db.SetBlockNonce(
		bytes.Repeat([]byte{0x44}, 32),
		1050,
		tipNonce,
		false,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:        10,
			StartSlot:      1000,
			LengthInSlots:  100,
			Nonce:          bytes.Repeat([]byte{0x11}, 32),
			EvolvingNonce:  tipNonce,
			CandidateNonce: candidateNonce,
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1050,
			},
		},
		// currentTipBlockNonce is intentionally unset to mimic a snapshot
		// import where the in-memory tip-nonce cache hasn't been populated.
		// This forces computeEpochNonceForSlot past its in-memory short-circuit
		// and exercises the DB-resume anchor lookup against block_nonce rows.
		config: LedgerStateConfig{
			CardanoNodeConfig: newNonceReadyTestConfig(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	got := ls.computeNextEpochNonce(ls.currentEpoch, ls.currentEra)
	require.Equal(t, candidateNonce, got)
	require.NotEqual(t, tipNonce, got)
}

func TestNextEpochNonceReadyEpochNotReadyBeforeCutoff(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.5,
		"securityParam": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	currentSlot := uint64(1085)
	provider := newMockSlotTimeProvider(
		time.Now().Add(-time.Duration(currentSlot)*time.Second),
		time.Second,
		100,
	)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	clock.nowFunc = func() time.Time {
		return provider.systemStart.Add(
			time.Duration(currentSlot) * time.Second,
		)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             10,
			StartSlot:           1000,
			LengthInSlots:       100,
			EraId:               eras.ShelleyEraDesc.Id,
			Nonce:               nil,
			EvolvingNonce:       []byte{0x02},
			CandidateNonce:      []byte{0x03},
			LastEpochBlockNonce: []byte{0x04},
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1085,
			},
		},
		slotClock: clock,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.syncUpstreamTipSlot.Store(1100)
	ls.publishSnapshotsLocked()

	readyEpoch, ok := ls.NextEpochNonceReadyEpoch()
	require.False(t, ok)
	assert.Equal(t, uint64(0), readyEpoch)
}

func TestEmitNextEpochNonceReadyRequiresLedgerTipAtCutoff(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	_, evtCh := eventBus.Subscribe(event.EpochNonceReadyEventType)
	ls := newNonceReadyTestLedgerState(t, eventBus, 1085)

	ls.emitNextEpochNonceReady(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SlotTick{Slot: 1095, Epoch: 10},
		ls.currentEpoch,
		ls.currentEra,
		1085,
	)

	select {
	case evt := <-evtCh:
		t.Fatalf("unexpected nonce-ready event published: %#v", evt)
	case <-time.After(100 * time.Millisecond):
	}

	assert.Equal(t, uint64(0), ls.nextNonceReadyEpoch.Load())
}

func TestResetNextEpochNonceReadyAllowsReEmit(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	_, evtCh := eventBus.Subscribe(event.EpochNonceReadyEventType)
	ls := newNonceReadyTestLedgerState(t, eventBus, 1095)
	ls.nextNonceReadyEpoch.Store(11)
	ls.resetNextEpochNonceReady()

	ls.emitNextEpochNonceReady(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SlotTick{Slot: 1095, Epoch: 10},
		ls.currentEpoch,
		ls.currentEra,
		1095,
	)

	select {
	case evt := <-evtCh:
		readyEvent, ok := evt.Data.(event.EpochNonceReadyEvent)
		require.True(t, ok)
		assert.Equal(t, uint64(10), readyEvent.CurrentEpoch)
		assert.Equal(t, uint64(11), readyEvent.ReadyEpoch)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("expected nonce-ready event after rollback reset")
	}
}

func TestNextEpochNonceReadyCutoffSlotShortEpoch(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Shelley → 3k/f = 25920, which exceeds the 100-slot epoch, so the
	// cutoff degenerates to the epoch start.
	cutoffSlot, ok := ls.nextEpochNonceReadyCutoffSlot(models.Epoch{
		EpochId:       42,
		StartSlot:     1000,
		LengthInSlots: 100,
		EraId:         eras.ShelleyEraDesc.Id,
	})
	require.True(t, ok)
	assert.Equal(t, uint64(1000), cutoffSlot)
}

// TestDatabaseWorkerPoolBasic tests basic worker pool functionality
func TestDatabaseWorkerPoolBasic(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	// Use a nil database for testing - workers don't actually need a real one
	pool := NewDatabaseWorkerPool(nil, config)
	require.NotNil(t, pool)

	var executedCount atomic.Int32

	// Submit a simple operation
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			executedCount.Add(1)
			return nil
		},
		ResultChan: resultChan,
	})

	// Wait for result with timeout
	select {
	case result := <-resultChan:
		assert.NoError(t, result.Error)
		assert.Equal(t, int32(1), executedCount.Load())
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for operation result")
	}

	pool.Shutdown(5 * time.Second)
}

// TestDatabaseWorkerPoolOpFuncPanicReturnsWrappedError proves
// executeOperation follows the same panic contract as database.Txn.Do: a
// panic in OpFunc is recovered and delivered on ResultChan as an error
// wrapping database.ErrTxnPanic, rather than crashing the worker goroutine
// or leaving the submitter's ResultChan waiting forever.
func TestDatabaseWorkerPoolOpFuncPanicReturnsWrappedError(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)
	require.NotNil(t, pool)

	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			panic("opfunc boom")
		},
		ResultChan: resultChan,
	})

	select {
	case result := <-resultChan:
		require.ErrorIs(t, result.Error, database.ErrTxnPanic)
		require.ErrorContains(t, result.Error, "opfunc boom")
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for operation result")
	}

	// The pool itself must still be usable after a worker recovers a panic:
	// its goroutine must have kept running rather than dying with the panic.
	var executedCount atomic.Int32
	okResultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			executedCount.Add(1)
			return nil
		},
		ResultChan: okResultChan,
	})
	select {
	case result := <-okResultChan:
		require.NoError(t, result.Error)
		require.Equal(t, int32(1), executedCount.Load())
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for post-panic operation result")
	}

	pool.Shutdown(5 * time.Second)
}

// TestDatabaseWorkerPoolInFlightOperations tests that shutdown waits for in-flight operations
func TestDatabaseWorkerPoolInFlightOperations(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 2
	config.TaskQueueSize = 10

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32
	var wg sync.WaitGroup

	// Submit multiple operations
	for range 5 {
		wg.Add(1)
		resultChan := make(chan DatabaseResult, 1)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				// Simulate work with short delay
				time.Sleep(10 * time.Millisecond)
				completedCount.Add(1)
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result in goroutine
		go func(ch chan DatabaseResult) {
			defer wg.Done()
			result := <-ch
			// Error is expected if shutdown occurred before operation completed
			// But we should receive the error in the channel
			_ = result.Error
		}(resultChan)
	}

	// Wait for at least one operation to start processing
	require.Eventually(t, func() bool {
		return completedCount.Load() > 0
	}, testutil.AsyncWait, 5*time.Millisecond, "at least one operation should start")

	// Shutdown the pool - this should wait for all operations to complete
	pool.Shutdown(5 * time.Second)

	// Wait for all result handlers
	wg.Wait()

	// Verify all operations completed
	assert.Equal(
		t,
		int32(5),
		completedCount.Load(),
		"not all operations completed before shutdown returned",
	)
}

// TestDatabaseWorkerPoolShutdownWithErrors tests error handling during shutdown
func TestDatabaseWorkerPoolShutdownWithErrors(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 2
	config.TaskQueueSize = 10

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit operations, some will error
	for i := range 3 {
		resultChan := make(chan DatabaseResult, 1)
		operationIndex := i

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				time.Sleep(20 * time.Millisecond)
				completedCount.Add(1)
				if operationIndex == 1 {
					return fmt.Errorf("operation %d failed", operationIndex)
				}
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain results
		go func() {
			select {
			case <-resultChan:
			case <-time.After(10 * time.Second):
			}
		}()
	}

	// Shutdown should wait for all operations to complete
	pool.Shutdown(5 * time.Second)

	// Verify all operations completed even with errors
	assert.Equal(
		t,
		int32(3),
		completedCount.Load(),
		"not all operations completed",
	)
}

// TestDatabaseWorkerPoolQueueFull tests behavior when queue is full
func TestDatabaseWorkerPoolQueueFull(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 1 // Very small queue

	pool := NewDatabaseWorkerPool(nil, config)

	// Submit some operations
	for range 3 {
		resultChan := make(chan DatabaseResult, 1)
		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)
	}

	// Shutdown should complete successfully
	pool.Shutdown(5 * time.Second)
}

// TestDatabaseWorkerPoolSubmitAfterShutdown tests that submitting after shutdown fails
func TestDatabaseWorkerPoolSubmitAfterShutdown(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	// Shutdown the pool
	pool.Shutdown(5 * time.Second)

	// Try to submit an operation after shutdown
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			return nil
		},
		ResultChan: resultChan,
	})

	// Should get a shutdown error
	select {
	case result := <-resultChan:
		assert.Error(t, result.Error)
		assert.Contains(t, result.Error.Error(), "shut down")
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for error result")
	}
}

// TestDatabaseWorkerPoolShutdownDoesNotPanicWithInFlightOperations verifies that
// shutdown remains panic-free while operations are still queued or running.
func TestDatabaseWorkerPoolShutdownDoesNotPanicWithInFlightOperations(
	t *testing.T,
) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 2
	config.TaskQueueSize = 20

	pool := NewDatabaseWorkerPool(nil, config)

	// Barrier: workers block until release so Shutdown overlaps in-flight work.
	hold := make(chan struct{})
	var inFlight atomic.Int32

	for range 10 {
		resultChan := make(chan DatabaseResult, 1)
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				inFlight.Add(1)
				defer inFlight.Add(-1)
				<-hold
				return nil
			},
			ResultChan: resultChan,
		})
	}

	testutil.WaitForCondition(
		t,
		func() bool { return inFlight.Load() > 0 },
		testutil.AsyncWait,
		"at least one operation should be running",
	)

	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown(5 * time.Second)
		close(shutdownDone)
	}()

	close(hold)

	select {
	case <-shutdownDone:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for Shutdown")
	}
}

// TestDatabaseWorkerPoolConcurrency tests the pool under concurrent load
func TestDatabaseWorkerPoolConcurrency(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 5
	config.TaskQueueSize = 50

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit many operations
	numOperations := 20
	for range numOperations {
		resultChan := make(chan DatabaseResult, 1)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				completedCount.Add(1)
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result immediately
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)
	}

	// Shutdown pool - should wait for all operations
	pool.Shutdown(5 * time.Second)

	// All operations should complete
	assert.Equal(t, int32(numOperations), completedCount.Load())
}

// TestDatabaseWorkerPoolMultipleShutdowns tests that multiple shutdown calls are safe
func TestDatabaseWorkerPoolMultipleShutdowns(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	// Submit an operation
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			return nil
		},
		ResultChan: resultChan,
	})

	// Drain result
	<-resultChan

	// Call shutdown multiple times - should be safe
	pool.Shutdown(5 * time.Second)
	pool.Shutdown(5 * time.Second) // Should not panic
	pool.Shutdown(5 * time.Second) // Should not panic
}

// TestDatabaseWorkerPoolShutdownTimesOutOnSlowOperation tests that Shutdown
// returns an error promptly at drainTimeout, rather than blocking
// indefinitely, when an in-flight operation runs longer than the requested
// drain timeout.
func TestDatabaseWorkerPoolShutdownTimesOutOnSlowOperation(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	started := make(chan struct{})
	blockUntil := make(chan struct{})
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			close(started)
			<-blockUntil
			return nil
		},
		ResultChan: resultChan,
	})

	select {
	case <-started:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for operation to start")
	}

	shutdownStart := time.Now()
	err := pool.Shutdown(50 * time.Millisecond)
	elapsed := time.Since(shutdownStart)

	require.Error(
		t,
		err,
		"Shutdown should report an error when the drain timeout elapses before in-flight operations finish",
	)
	assert.Less(
		t,
		elapsed,
		2*time.Second,
		"Shutdown must return promptly at the drain timeout instead of blocking on the stuck operation",
	)

	// Unblock the stuck operation so it doesn't leak past the test.
	close(blockUntil)
	select {
	case <-resultChan:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for stuck operation to finally complete")
	}
}

// TestDatabaseWorkerPoolShutdownTimeoutSpawnsNoWaiterGoroutine guards against
// Shutdown's drain-timeout bound being reimplemented as a goroutine bridging
// a sync.WaitGroup to a timeout-selectable channel: WaitGroup.Wait can't be
// interrupted, so that goroutine (and the worker still running the stuck
// operation under it) would keep running for the operation's full remaining
// duration after Shutdown times out and returns. The timeout must wait for
// in-flight operations to drain without leaving a goroutine behind.
// The current implementation tracks in-flight operations with a
// mutex-guarded counter and a drained channel Shutdown selects directly, so
// no goroutine is ever spawned by the timeout path.
// Not t.Parallel: runtime.NumGoroutine is a process-wide measurement that
// concurrent tests perturb.
func TestDatabaseWorkerPoolShutdownTimeoutSpawnsNoWaiterGoroutine(
	t *testing.T,
) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	started := make(chan struct{})
	blockUntil := make(chan struct{})
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			close(started)
			<-blockUntil
			return nil
		},
		ResultChan: resultChan,
	})

	select {
	case <-started:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for operation to start")
	}

	// The stuck worker goroutine is already running at this point, so it's
	// part of the baseline count -- only a goroutine spawned by Shutdown
	// itself would show up as growth below. GC first so a transient
	// runtime/GC goroutine isn't baked into the baseline.
	runtime.GC()
	baseline := runtime.NumGoroutine()

	err := pool.Shutdown(50 * time.Millisecond)
	require.Error(t, err)

	// A single immediate snapshot is flaky: a short-lived runtime/GC
	// goroutine can transiently push the count above baseline with no
	// relation to Shutdown. Poll briefly instead, matching
	// storagetest.AssertNoGoroutineLeak's pattern -- since a leaked waiter
	// goroutine would persist for the stuck operation's full duration, it
	// would still be caught well within this deadline.
	deadline := time.Now().Add(2 * time.Second)
	for {
		after := runtime.NumGoroutine()
		if after <= baseline {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"Shutdown's timeout path must not leave behind a goroutine "+
					"of its own: baseline %d, now %d",
				baseline,
				after,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Unblock the stuck operation so it doesn't leak past the test.
	close(blockUntil)
	select {
	case <-resultChan:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout waiting for stuck operation to finally complete")
	}
}

// TestDatabaseWorkerPoolResultChannelFull tests handling of full result channels
func TestDatabaseWorkerPoolResultChannelFull(t *testing.T) {
	t.Parallel()

	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit operations
	for range 3 {
		resultChan := make(chan DatabaseResult, 1)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				completedCount.Add(1)
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)
	}

	// Shutdown should work
	pool.Shutdown(5 * time.Second)

	// All operations should complete
	assert.Equal(t, int32(3), completedCount.Load())
}

// TestTransitionToEra_ReturnsResultWithoutMutating tests that transitionToEra
// returns computed state without mutating LedgerState fields
func TestTransitionToEra_ReturnsResultWithoutMutating(t *testing.T) {
	t.Parallel()

	// Setup: Create genesis configs for the transition
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
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

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	// Create in-memory database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ByronEraDesc,
		currentPParams: nil, // Start with nil
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Capture original state
	originalEra := ls.currentEra
	originalPParams := ls.currentPParams

	// Execute transition in a transaction
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		result, err := ls.transitionToEra(
			txn,
			eras.ShelleyEraDesc.Id,
			0,   // startEpoch
			0,   // addedSlot
			nil, // currentPParams (Byron has none)
		)
		if err != nil {
			return err
		}

		// Verify result contains expected values
		assert.NotNil(t, result)
		assert.Equal(t, eras.ShelleyEraDesc.Id, result.NewEra.Id)
		assert.Equal(t, "Shelley", result.NewEra.Name)
		// Shelley transition creates protocol parameters
		assert.NotNil(t, result.NewPParams)

		// Verify LedgerState was NOT mutated
		assert.Equal(
			t,
			originalEra.Id,
			ls.currentEra.Id,
			"currentEra should not be mutated",
		)
		assert.Equal(
			t,
			originalPParams,
			ls.currentPParams,
			"currentPParams should not be mutated",
		)

		return nil
	})
	require.NoError(t, err)
}

// TestTransitionToEra_ChainedTransitions tests multiple era transitions in sequence
func TestTransitionToEra_ChainedTransitions(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
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

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Chain transitions from Byron -> Shelley -> Allegra
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Track working state as we chain transitions
		workingPParams := ls.currentPParams

		// Byron -> Shelley
		result1, err := ls.transitionToEra(
			txn,
			eras.ShelleyEraDesc.Id,
			0,
			0,
			workingPParams,
		)
		require.NoError(t, err)
		workingPParams = result1.NewPParams

		// Shelley -> Allegra
		result2, err := ls.transitionToEra(
			txn,
			eras.AllegraEraDesc.Id,
			1,
			432000,
			workingPParams,
		)
		require.NoError(t, err)

		// Verify final result
		assert.Equal(t, eras.AllegraEraDesc.Id, result2.NewEra.Id)
		assert.NotNil(t, result2.NewPParams)

		// Verify LedgerState still has original Byron era
		assert.Equal(t, eras.ByronEraDesc.Id, ls.currentEra.Id)

		return nil
	})
	require.NoError(t, err)
}

func TestTransitionToEraTranslatesConwayGovernanceWhenProtocolAlreadyDijkstra(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	fee := uint(1234)
	action := &conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			MinFeeA: &fee,
		},
		PolicyHash: []byte{0xaa, 0xbb},
	}
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	ratifiedEpoch := uint64(10)
	ratifiedSlot := uint64(200)
	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0xe1}, 32),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch: 9,
		ExpiresEpoch:  20,
		GovActionCbor: actionCbor,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AddedSlot:     100,
		AnchorURL:     "https://example.invalid/transition-translate",
		AnchorHash:    bytes.Repeat([]byte{0xe2}, 32),
		ReturnAddress: bytes.Repeat([]byte{0xe3}, 29),
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))

	newCborRat := func(num, denom int64) *cbor.Rat {
		return &cbor.Rat{Rat: big.NewRat(num, denom)}
	}
	newRat := func(num, denom int64) cbor.Rat {
		return cbor.Rat{Rat: big.NewRat(num, denom)}
	}
	currentPParams := &conway.ConwayProtocolParameters{
		A0:  newCborRat(0, 1),
		Rho: newCborRat(0, 1),
		Tau: newCborRat(0, 1),
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: dijkstra.MinProtocolVersionDijkstra,
		},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  newCborRat(1, 1),
			StepPrice: newCborRat(1, 1),
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    newRat(1, 2),
			CommitteeNormal:       newRat(1, 2),
			CommitteeNoConfidence: newRat(1, 2),
			HardForkInitiation:    newRat(1, 2),
			PpSecurityGroup:       newRat(1, 2),
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    newRat(1, 2),
			CommitteeNormal:       newRat(1, 2),
			CommitteeNoConfidence: newRat(1, 2),
			UpdateToConstitution:  newRat(1, 2),
			HardForkInitiation:    newRat(1, 2),
			PpNetworkGroup:        newRat(1, 2),
			PpEconomicGroup:       newRat(1, 2),
			PpTechnicalGroup:      newRat(1, 2),
			PpGovGroup:            newRat(1, 2),
			TreasuryWithdrawal:    newRat(1, 2),
		},
		MinFeeRefScriptCostPerByte: newCborRat(1, 1),
	}
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		activeEras:     eras.ErasWithDijkstra,
		currentPParams: currentPParams,
		config: LedgerStateConfig{
			CardanoNodeConfig: &cardano.CardanoNodeConfig{},
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		_, err := ls.transitionToEra(
			txn,
			eras.DijkstraEraDesc.Id,
			11,
			300,
			currentPParams,
		)
		return err
	})
	require.NoError(t, err)

	got, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	var translated dijkstra.DijkstraParameterChangeGovAction
	_, err = cbor.Decode(got.GovActionCbor, &translated)
	require.NoError(t, err)
	require.NotNil(t, translated.ParamUpdate.MinFeeA)
	require.Equal(t, uint(1234), *translated.ParamUpdate.MinFeeA)
	require.Equal(t, []byte{0xaa, 0xbb}, translated.PolicyHash)
}

// TestEpochRolloverResult_FieldsPopulated tests that EpochRolloverResult
// contains all expected fields after processEpochRollover
func TestEpochRolloverResult_FieldsPopulated(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
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
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    0, // Triggers initial epoch creation
			LengthInSlots: 0,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Execute epoch rollover for initial epoch
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		result, err := ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			ls.currentEra,
			ls.currentPParams,
			false,
		)
		require.NoError(t, err)

		// Verify result fields are populated
		assert.NotNil(t, result)
		assert.NotEmpty(
			t,
			result.NewEpochCache,
			"NewEpochCache should be populated",
		)
		assert.Equal(t, uint64(0), result.NewCurrentEpoch.EpochId)
		assert.Equal(t, false, result.CheckpointWrittenForEpoch)

		// Verify LedgerState was NOT mutated
		assert.Equal(t, uint64(0), ls.currentEpoch.EpochId)
		assert.Empty(t, ls.epochCache, "epochCache should not be mutated")

		return nil
	})
	require.NoError(t, err)
}

// TestEpochRollover_NoDeadlockDuringTransaction tests that epoch rollover
// does not hold LedgerState lock during database operations.
// This simulates the scenario that caused the original deadlock.
func TestEpochRollover_NoDeadlockDuringTransaction(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
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
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    0,
			LengthInSlots: 0,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// This test verifies that the pattern doesn't deadlock:
	// 1. Take RLock to capture snapshot
	// 2. Release RLock
	// 3. Execute transaction (which might need to acquire lock in recovery)
	// 4. Take Lock briefly to apply results
	// 5. Release Lock

	errChan := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)

		// Step 1: Capture snapshot with RLock
		ls.RLock()
		snapshotEra := ls.currentEra
		snapshotEpoch := ls.currentEpoch
		snapshotPParams := ls.currentPParams
		ls.RUnlock()

		// Step 2: Execute transaction WITHOUT holding lock
		var result *EpochRolloverResult
		txn := db.Transaction(true)
		err := txn.Do(func(txn *database.Txn) error {
			var err error
			result, err = ls.processEpochRollover(
				txn,
				snapshotEpoch,
				snapshotEra,
				snapshotPParams,
				false,
			)
			return err
		})
		if err != nil {
			errChan <- err
			return
		}

		// Step 3: Apply results with brief Lock
		ls.Lock()
		if result != nil {
			ls.epochCache = result.NewEpochCache
			ls.currentEpoch = result.NewCurrentEpoch
			ls.currentEra = result.NewCurrentEra
		}
		ls.Unlock()
	}()

	// If this test times out, we have a deadlock
	select {
	case <-done:
		// Success - no deadlock
		select {
		case err := <-errChan:
			require.NoError(t, err)
		default:
		}
	case <-time.After(testutil.AsyncWait):
		t.Fatal("deadlock detected - epoch rollover did not complete in time")
	}
}

// TestEpochRollover_ConcurrentReaders tests that the epoch rollover pattern
// allows concurrent readers during the transaction phase
func TestEpochRollover_ConcurrentReaders(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
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
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    0,
			LengthInSlots: 0,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var wg sync.WaitGroup
	readCount := atomic.Int32{}
	txnStarted := make(chan struct{})
	txnDone := make(chan struct{})
	rolloverErr := make(chan error, 1)

	// Start the epoch rollover goroutine
	wg.Go(func() {
		// Capture snapshot
		ls.RLock()
		snapshotEra := ls.currentEra
		snapshotEpoch := ls.currentEpoch
		snapshotPParams := ls.currentPParams
		ls.RUnlock()

		// Signal that transaction is starting
		close(txnStarted)

		// Execute transaction (simulates DB work)
		var result *EpochRolloverResult
		txn := db.Transaction(true)
		err := txn.Do(func(txn *database.Txn) error {
			// Add a small delay to give readers time to run
			time.Sleep(50 * time.Millisecond)
			var err error
			result, err = ls.processEpochRollover(
				txn,
				snapshotEpoch,
				snapshotEra,
				snapshotPParams,
				false,
			)
			return err
		})
		if err != nil {
			rolloverErr <- err
			close(txnDone)
			return
		}

		// Apply results
		ls.Lock()
		if result != nil {
			ls.epochCache = result.NewEpochCache
			ls.currentEpoch = result.NewCurrentEpoch
		}
		ls.Unlock()

		close(txnDone)
	})

	// Start multiple reader goroutines that try to read during the transaction
	for range 5 {
		wg.Go(func() {
			// Wait for transaction to start
			<-txnStarted

			// Try to read multiple times during the transaction
			for range 10 {
				select {
				case <-txnDone:
					return
				default:
					ls.RLock()
					_ = ls.currentEra   // Read era
					_ = ls.currentEpoch // Read epoch
					readCount.Add(1)
					ls.RUnlock()
					time.Sleep(5 * time.Millisecond)
				}
			}
		})
	}

	// Wait for all goroutines with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - check for rollover error
		select {
		case err := <-rolloverErr:
			require.NoError(t, err)
		default:
		}
		assert.Greater(
			t,
			readCount.Load(),
			int32(0),
			"readers should have been able to read during transaction",
		)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("timeout - possible deadlock with concurrent readers")
	}
}

// TestTransitionToEra_ErrorHandling tests error conditions in transitionToEra
func TestTransitionToEra_ErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("invalid era ID returns error", func(t *testing.T) {
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir: "",
		})
		require.NoError(t, err)

		ls := &LedgerState{
			db:         db,
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			},
		}

		txn := db.Transaction(true)
		err = txn.Do(func(txn *database.Txn) error {
			_, err := ls.transitionToEra(txn, 999, 0, 0, nil)
			return err
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown era ID 999")
	})
}

// makeTestBlock creates a test block with deterministic hash based on slot
func makeTestBlock(slot, id uint64) models.Block {
	// Create deterministic hash from slot
	slotBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(slotBytes, slot)
	hash := sha256.Sum256(slotBytes)
	return models.Block{
		ID:       id,
		Slot:     slot,
		Hash:     hash[:],
		Number:   id,
		Type:     1, // Shelley era type
		PrevHash: nil,
		Cbor:     []byte{0x80}, // minimal CBOR (empty array)
	}
}

// makeTestPoint creates a Point from a test block
func makeTestPoint(block models.Block) pcommon.Point {
	return pcommon.NewPoint(block.Slot, block.Hash)
}

// TestCleanupOrphanedBlobs_EmptyBlobStore verifies cleanup returns without
// error against a real (badger) blob store that has no stored blocks, so there
// are no orphaned blobs to remove. dbtest.NewDatabase always composes a badger
// blob store, and database.New now requires a non-nil blob store, so a
// no-blob-store database is no longer constructible.
func TestCleanupOrphanedBlobs_EmptyBlobStore(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		db: nil, // No database
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Create a database backed by a real (badger) blob store with no blocks.
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)

	ls.db = db

	// Cleanup should return nil when there are no orphaned blobs.
	err = ls.cleanupOrphanedBlobs(100)
	assert.NoError(t, err)
}

// TestCleanupOrphanedBlobs_NoOrphans tests cleanup when there are no orphaned blocks
func TestCleanupOrphanedBlobs_NoOrphans(t *testing.T) {
	t.Parallel()

	// Create an in-memory database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Store a few blocks at slots 1, 2, 3
	for slot := uint64(1); slot <= 3; slot++ {
		block := makeTestBlock(slot, slot)
		err = db.BlockCreate(block, nil)
		require.NoError(t, err)
	}

	// Cleanup with tip at slot 3 - no orphans expected
	err = ls.cleanupOrphanedBlobs(3)
	assert.NoError(t, err)

	// Verify all blocks still exist
	for slot := uint64(1); slot <= 3; slot++ {
		block := makeTestBlock(slot, slot)
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.NoError(t, err, "block at slot %d should still exist", slot)
	}
}

// TestCleanupOrphanedBlobs_WithOrphans tests cleanup when orphaned blocks exist
func TestCleanupOrphanedBlobs_WithOrphans(t *testing.T) {
	t.Parallel()

	// Create an in-memory database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Store blocks at slots 1-5
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		err = db.BlockCreate(block, nil)
		require.NoError(t, err)
	}

	// Cleanup with tip at slot 3 - blocks at slots 4 and 5 should be orphans
	err = ls.cleanupOrphanedBlobs(3)
	assert.NoError(t, err)

	// Verify blocks at slots 1-3 still exist
	for slot := uint64(1); slot <= 3; slot++ {
		block := makeTestBlock(slot, slot)
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.NoError(t, err, "block at slot %d should still exist", slot)
	}

	// Verify blocks at slots 4-5 were deleted
	for slot := uint64(4); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.Error(t, err, "block at slot %d should be deleted", slot)
	}
}

// TestCleanupOrphanedBlobs_SlotZero tests cleanup behavior when tip is at slot 0
func TestCleanupOrphanedBlobs_SlotZero(t *testing.T) {
	t.Parallel()

	// Create an in-memory database
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Store a block at slot 1 (would be orphan if tip is 0)
	block := makeTestBlock(1, 1)
	err = db.BlockCreate(block, nil)
	require.NoError(t, err)

	// Cleanup with tip at slot 0 - block at slot 1 should be deleted
	err = ls.cleanupOrphanedBlobs(0)
	assert.NoError(t, err)

	// Verify block at slot 1 was deleted
	_, err = database.BlockByPoint(db, makeTestPoint(block))
	assert.Error(t, err, "block at slot 1 should be deleted")
}

func TestIntersectPointsReturnsNoPointsWhenLedgerTipIsEmpty(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	txn := db.BlobTxn(true)
	err = txn.Do(func(txn *database.Txn) error {
		return db.Blob().Set(
			txn.Blob(),
			dbtypes.BlockBlobIndexKey(1),
			[]byte("bad"),
		)
	})
	require.NoError(t, err)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	assert.Nil(t, points)
}

func TestLoadMithrilTrustBoundaryLoadsPersistedHash(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	boundaryHash := bytes.Repeat([]byte{0x42}, 32)
	require.NoError(t, db.SetSyncState(
		mithrilLedgerSlotSyncKey,
		"42",
		nil,
	))
	require.NoError(t, db.SetSyncState(
		mithrilLedgerHashSyncKey,
		fmt.Sprintf("%x", boundaryHash),
		nil,
	))
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.loadMithrilTrustBoundary()

	require.NoError(t, err)
	require.Equal(t, uint64(42), ls.mithrilLedgerSlot)
	require.Equal(t, boundaryHash, ls.mithrilLedgerHash)
}

// TestLoadMithrilTrustBoundaryAbsentKeyStartsNotMithril proves that an
// absent mithril_ledger_slot sync-state key (a non-Mithril-bootstrapped DB)
// starts cleanly with no error.
func TestLoadMithrilTrustBoundaryAbsentKeyStartsNotMithril(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.loadMithrilTrustBoundary()

	require.NoError(t, err)
	require.Equal(t, uint64(0), ls.mithrilLedgerSlot)
}

// TestLoadMithrilTrustBoundaryMalformedSlotReturnsError: a malformed
// mithril_ledger_slot value must fail ledger start rather than being
// silently ignored as "not a Mithril DB". Before the fix,
// this only logged a Warn and left mithrilLedgerSlot at its zero value,
// which disables the gap-nonce heal (heal_mithril_gap_nonce.go) and removes
// the Mithril boundary exemption, so header verification later fails a
// VRF/nonce check that blames peers instead of surfacing the real cause.
func TestLoadMithrilTrustBoundaryMalformedSlotReturnsError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.SetSyncState(
		mithrilLedgerSlotSyncKey,
		"x",
		nil,
	))
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.loadMithrilTrustBoundary()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mithril_ledger_slot")
	assert.Equal(t, uint64(0), ls.mithrilLedgerSlot)
}

// TestLoadMithrilTrustBoundaryReadErrorReturnsError: a sync_state database
// read error must fail ledger start rather than being silently ignored as
// "not a Mithril DB".
func TestLoadMithrilTrustBoundaryReadErrorReturnsError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.SetSyncState(
		mithrilLedgerSlotSyncKey,
		"42",
		nil,
	))
	// Close the metadata store so the subsequent GetSyncState read fails
	// deterministically, mirroring the pattern used in
	// TestDeleteDeferredMarkerUnlessReadmitted_RestoreFailurePropagates.
	require.NoError(t, db.Metadata().Close())
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.loadMithrilTrustBoundary()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Mithril trust boundary")
	assert.Equal(t, uint64(0), ls.mithrilLedgerSlot)
}

// TestLoadMithrilTrustBoundaryEmptyRecordedSlotReturnsError: GetSyncState
// reports an absent key as "", so a mithril_ledger_slot row that exists and
// holds nothing must not be read as "not a Mithril DB". Every other
// fail-closed reader of the key (Database.MithrilTrustBoundarySlotStrict,
// the sqlstore minted-block floor, koiosparity) already rejects it.
func TestLoadMithrilTrustBoundaryEmptyRecordedSlotReturnsError(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.SetSyncState(mithrilLedgerSlotSyncKey, "", nil))
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.loadMithrilTrustBoundary()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mithril_ledger_slot")
	assert.Equal(t, uint64(0), ls.mithrilLedgerSlot)
}

// TestLedgerStateStartFailsOnMalformedMithrilTrustBoundary pins the
// propagation out of Start: a loader error must abort ledger startup, not
// be discarded at the call site.
func TestLedgerStateStartFailsOnMalformedMithrilTrustBoundary(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.SetSyncState(mithrilLedgerSlotSyncKey, "x", nil))
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		PromRegistry:      prometheus.NewRegistry(),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(ls.publishCancel)

	err = ls.Start(t.Context())

	require.ErrorContains(t, err, "Mithril trust boundary")
	require.ErrorContains(t, err, "mithril_ledger_slot")
	assert.Equal(t, uint64(0), ls.mithrilLedgerSlot)
}

func TestIntersectPointsIncludesPersistedMithrilBoundaryWhenRecentPointsEmpty(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	boundaryHash := bytes.Repeat([]byte{0x24}, 32)
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 42,
		mithrilLedgerHash: boundaryHash,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.Len(t, points, 1)
	assert.Equal(t, uint64(42), points[0].Slot)
	assert.Equal(t, boundaryHash, points[0].Hash)
}

func TestIntersectPointsUsesPrimaryChainWhenPrimaryChainIsAhead(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
	}
	ls.currentTip = ledgerTip

	points, err := ls.IntersectPoints(3)
	require.NoError(t, err)
	require.Len(t, points, 3)
	assert.Equal(t, blocks[4].Slot, points[0].Slot)
	assert.Equal(t, blocks[4].Hash, points[0].Hash)
	assert.Equal(t, blocks[3].Slot, points[1].Slot)
	assert.Equal(t, blocks[3].Hash, points[1].Hash)
	assert.Equal(t, blocks[2].Slot, points[2].Slot)
	assert.Equal(t, blocks[2].Hash, points[2].Hash)
}

func TestIntersectPointsUsesSparseLedgerTipSamples(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	blocks := make([]models.Block, 0, 256)
	for slot := uint64(1); slot <= 256; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	require.NotEmpty(t, blocks)
	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)
	require.Greater(t, len(points), ledgerIntersectDenseCount)
	assert.Equal(t, ledgerTipBlock.Slot, points[0].Slot)
	assert.Equal(t, ledgerTipBlock.Hash, points[0].Hash)

	pointSlots := make(map[uint64]struct{}, len(points))
	for _, point := range points {
		pointSlots[point.Slot] = struct{}{}
	}
	for _, slot := range []uint64{224, 192, 128, 1} {
		_, ok := pointSlots[slot]
		assert.True(t, ok, "missing sparse intersect point at slot %d", slot)
	}
}

func TestIntersectPointsIncludesMithrilTrustBoundary(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	blocks := make([]models.Block, 0, 256)
	for slot := uint64(1); slot <= 256; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	require.NotEmpty(t, blocks)
	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: 173,
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)

	boundarySlot := uint64(173)
	var boundaryPoint *ocommon.Point
	for _, point := range points {
		if point.Slot == boundarySlot {
			point := point
			boundaryPoint = &point
			break
		}
	}
	require.NotNil(t, boundaryPoint)
	assert.Equal(t, blocks[boundarySlot-1].Hash, boundaryPoint.Hash)
}

func TestIntersectPointsSkipsZeroMithrilTrustBoundary(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: 0,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, 0)
}

func TestIntersectPointsSkipsFutureMithrilTrustBoundary(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	boundarySlot := ledgerTipBlock.Slot + 1
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, boundarySlot)
}

func TestIntersectPointsSkipsMissingMithrilTrustBoundaryBlock(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	var blocks []models.Block
	for slot := uint64(1); slot <= 10; slot++ {
		if slot == 5 {
			continue
		}
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	require.NotEmpty(t, blocks)
	ledgerTipBlock := blocks[len(blocks)-1]
	boundarySlot := uint64(5)
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, boundarySlot)
}

func TestIntersectPointsSkipsMithrilTrustBoundaryOnLookupError(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	boundarySlot := uint64(5)
	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return db.Blob().Set(
			txn.Blob(),
			dbtypes.BlockHashIndexKey(blocks[boundarySlot-1].Hash),
			[]byte("bad"),
		)
	}))

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, boundarySlot)
}

func TestIntersectPointsUsesCanonicalMithrilTrustBoundary(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	blocks := make([]models.Block, 0, 64)
	for slot := uint64(1); slot <= 64; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	boundarySlot := uint64(20)
	canonicalBoundaryBlock := blocks[boundarySlot-1]
	nonCanonicalBoundaryBlock := makeTestBlock(boundarySlot, 1000)
	nonCanonicalBoundaryBlock.Hash = bytes.Repeat([]byte{0xff}, 32)
	nonCanonicalBoundaryBlock.PrevHash = append(
		[]byte(nil),
		blocks[boundarySlot-2].Hash...,
	)
	require.NoError(t, db.BlockCreate(nonCanonicalBoundaryBlock, nil))

	rawBoundaryBlock, err := database.BlockBeforeSlot(
		db,
		boundarySlot+1,
	)
	require.NoError(t, err)
	require.Equal(t, nonCanonicalBoundaryBlock.Hash, rawBoundaryBlock.Hash)

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)

	var boundaryPoint *ocommon.Point
	for _, point := range points {
		if point.Slot == boundarySlot {
			point := point
			boundaryPoint = &point
			break
		}
	}
	require.NotNil(t, boundaryPoint)
	assert.Equal(t, canonicalBoundaryBlock.Hash, boundaryPoint.Hash)
	assert.NotEqual(t, nonCanonicalBoundaryBlock.Hash, boundaryPoint.Hash)
}

func TestAuthoritativeLedgerBlockAtSlotDoesNotRequireMonotonicBlockIDs(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	blocks := make([]models.Block, 0, 64)
	for slot := uint64(1); slot <= 64; slot++ {
		id := slot
		switch slot {
		case 20:
			id = 50
		case 50:
			id = 20
		}
		block := makeTestBlock(slot, id)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{db: db}

	block, err := ls.authoritativeLedgerBlockAtSlot(
		20,
		makeTestPoint(ledgerTipBlock),
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), block.Slot)
	assert.Equal(t, blocks[19].Hash, block.Hash)
}

func TestIntersectPointsKeepsMithrilTrustBoundaryWhenPointListIsFull(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: 5,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.Len(t, points, 4)
	assert.Equal(t, uint64(10), points[0].Slot)
	assert.Equal(t, uint64(9), points[1].Slot)
	assert.Equal(t, uint64(8), points[2].Slot)
	assert.Equal(t, uint64(5), points[3].Slot)
}

func assertNoIntersectPointAtSlot(
	t *testing.T,
	points []ocommon.Point,
	slot uint64,
) {
	t.Helper()
	for _, point := range points {
		assert.NotEqual(t, slot, point.Slot)
	}
}

func TestIntersectPointsSkipsMissingDenseBlockIndex(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	blocks := make([]models.Block, 0, 40)
	for slot := uint64(1); slot <= 40; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append(
				[]byte(nil),
				blocks[len(blocks)-1].Hash...,
			)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	blockBlobIndexKey := dbtypes.BlockBlobIndexKey(39)
	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		indexBytes, err := db.Blob().Get(txn.Blob(), blockBlobIndexKey)
		require.NoError(t, err)
		require.NotNil(t, indexBytes)
		return db.Blob().Delete(
			txn.Blob(),
			blockBlobIndexKey,
		)
	}))

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(points), ledgerIntersectDenseCount)

	pointSlots := make(map[uint64]struct{}, len(points))
	for _, point := range points {
		pointSlots[point.Slot] = struct{}{}
	}
	_, hasMissingIndexSlot := pointSlots[39]
	assert.False(t, hasMissingIndexSlot)
	_, hasPreviousDenseSlot := pointSlots[38]
	assert.True(t, hasPreviousDenseSlot)
}

func TestChainDensityUsesCardanoNodeFragment(t *testing.T) {
	t.Parallel()

	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 3
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	blocks := []models.Block{
		makeTestBlock(10, 1),
		makeTestBlock(20, 2),
		makeTestBlock(100, 3),
		makeTestBlock(190, 4),
		makeTestBlock(210, 5),
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	tipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(tipBlock),
			BlockNumber: tipBlock.Number,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())

	density := ls.chainFragmentDensity(ls.currentTip, ls.SecurityParam())
	ls.Lock()
	ls.updateTipMetrics(density)
	ls.Unlock()

	// cardano-node computes density over the ChainDB fragment as:
	// (tip block - oldest fragment block) / (tip slot - oldest fragment slot).
	// With k=3 and tip block index 5, the oldest fragment block is index 2.
	assert.InDelta(
		t,
		3.0/190.0,
		promtestutil.ToFloat64(ls.metrics.density),
		1e-12,
	)
}

func TestLoadTipSeedsChainDensityFromPersistedFragment(t *testing.T) {
	t.Parallel()

	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 3
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	blocks := []models.Block{
		makeTestBlock(10, 1),
		makeTestBlock(20, 2),
		makeTestBlock(100, 3),
		makeTestBlock(190, 4),
		makeTestBlock(210, 5),
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	tipBlock := blocks[len(blocks)-1]
	require.NoError(
		t,
		db.SetBlockNonce(tipBlock.Hash, tipBlock.Slot, []byte{1}, false, nil),
	)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       makeTestPoint(tipBlock),
		BlockNumber: tipBlock.Number,
	}, nil))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())

	require.NoError(t, ls.loadTip())

	assert.InDelta(
		t,
		3.0/190.0,
		promtestutil.ToFloat64(ls.metrics.density),
		1e-12,
	)
}

func TestFragmentDensityIgnoresByronEbbBlockNumber(t *testing.T) {
	t.Parallel()

	assert.InDelta(t, 9.0/100.0, fragmentDensity(100, 10, 0, 0), 1e-12)
}

func TestReconcilePrimaryChainTipWithLedgerTipPreservesSelectedChain(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip())

	chainTip := cm.PrimaryChain().Tip()
	assert.Equal(t, blocks[len(blocks)-1].Slot, chainTip.Point.Slot)
	assert.Equal(t, blocks[len(blocks)-1].Number, chainTip.BlockNumber)
	assert.Equal(t, blocks[len(blocks)-1].Hash, chainTip.Point.Hash)
	assert.Equal(t, ledgerTip, ls.currentTip)

	for _, block := range blocks {
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.NoError(
			t,
			err,
			"block at slot %d should still exist",
			block.Slot,
		)
	}
}

// ---------------------------------------------------------------------------
// applyEraTransition / transitionInfo clearing tests
// ---------------------------------------------------------------------------

// babbagePParams returns a minimal *babbage.BabbageProtocolParameters with
// the given protocol major version.  Used to construct era transitions without
// going through the full genesis-loading machinery.
func babbagePParams(major uint) *babbage.BabbageProtocolParameters {
	return &babbage.BabbageProtocolParameters{ProtocolMajor: major}
}

func TestNewLedgerStateHardForkTransitionUsesConfiguredEraList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		enableDijkstra bool
		expected       bool
	}{
		{
			name:     "default era table gates off Dijkstra",
			expected: false,
		},
		{
			name:           "Dijkstra-enabled era table detects transition",
			enableDijkstra: true,
			expected:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			ls, err := NewLedgerState(LedgerStateConfig{
				Database:       db,
				ChainManager:   cm,
				Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
				EnableDijkstra: tt.enableDijkstra,
			})
			require.NoError(t, err)

			got := ls.isHardForkTransition(
				ProtocolVersion{Major: 10},
				ProtocolVersion{Major: 12},
			)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPrepareEpochCacheForStartupPreservesByronPrefix(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {"k": 432, "protocolMagic": 2},
		"blockVersionData": {"slotDuration": "20000"}
	}`
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

	newLedger := func(
		t *testing.T,
		explicitShelleyHardFork bool,
		experimentalHardForks bool,
		shelleyHardForkEpoch uint64,
	) *LedgerState {
		t.Helper()
		cfg := &cardano.CardanoNodeConfig{
			ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
		}
		require.NoError(t, loadByronGenesisForTest(t, cfg,
			strings.NewReader(byronGenesisJSON),
		))
		require.NoError(t, cfg.LoadShelleyGenesisFromReader(
			strings.NewReader(shelleyGenesisJSON),
		))
		if explicitShelleyHardFork {
			// ExperimentalHardForksEnabled is set independently: preview ships
			// TestShelleyHardForkAtEpoch with the flag false, and
			// CardanoNodeConfig.HardForkEpoch reports nothing in that case.
			if experimentalHardForks {
				cfg.ExperimentalHardForksEnabled = new(true)
			}
			cfg.TestShelleyHardForkAtEpoch = new(shelleyHardForkEpoch)
		}

		db := newTestDB(t)
		cm, err := chain.NewManager(db, nil)
		require.NoError(t, err)
		ls, err := NewLedgerState(LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		})
		require.NoError(t, err)
		require.NoError(t, ls.PrepareEpochCacheForStartup())
		return ls
	}

	t.Run(
		"real network retains Byron until its on-chain boundary",
		func(t *testing.T) {
			ls := newLedger(t, false, false, 0)
			require.Equal(t, eras.ByronEraDesc.Id, ls.currentEpoch.EraId)
			assert.Nil(t, ls.currentPParams)
			assert.Equal(t, uint64(0), ls.currentEpoch.StartSlot)
			assert.Equal(t, uint(4320), ls.currentEpoch.LengthInSlots)
			assert.Equal(t, uint(20000), ls.currentEpoch.SlotLength)
		},
	)

	t.Run(
		"explicit test hard fork still starts in Shelley",
		func(t *testing.T) {
			ls := newLedger(t, true, true, 0)
			require.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEpoch.EraId)
			assert.Equal(t, uint64(0), ls.currentEpoch.StartSlot)
			assert.Equal(t, uint(432000), ls.currentEpoch.LengthInSlots)
			assert.Equal(t, uint(1000), ls.currentEpoch.SlotLength)
		},
	)

	// preview's shipped shape: TestShelleyHardForkAtEpoch: 0 with
	// ExperimentalHardForksEnabled: False. Reading the declaration through
	// CardanoNodeConfig.HardForkEpoch hides it, because that accessor returns
	// (0, false) unless the experimental flag is set -- which forced a node
	// back to Byron on a network with no Byron prefix and left currentPParams
	// nil for every GetCurrentPParams consumer (api/utxorpc ReadParams
	// returned "current protocol parameters empty").
	t.Run(
		"explicit hard fork without experimental flag starts in Shelley",
		func(t *testing.T) {
			ls := newLedger(t, true, false, 0)
			require.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEpoch.EraId)
			assert.NotNil(
				t,
				ls.currentPParams,
				"a post-Byron start must expose protocol parameters",
			)
			assert.Equal(t, uint64(0), ls.currentEpoch.StartSlot)
			assert.Equal(t, uint(432000), ls.currentEpoch.LengthInSlots)
		},
	)

	// A nonzero declaration means Shelley arrives some epochs in, so epochs
	// 0..N-1 are Byron: that is a Byron prefix, not the absence of one. Only
	// an explicit epoch 0 marks a network that never had one.
	t.Run(
		"nonzero hard-fork epoch keeps the Byron start",
		func(t *testing.T) {
			ls := newLedger(t, true, false, 5)
			require.Equal(t, eras.ByronEraDesc.Id, ls.currentEpoch.EraId)
			assert.Nil(t, ls.currentPParams)
		},
	)
}

func TestPrepareEpochCacheForStartupUsesEmbeddedMainnetConfig(t *testing.T) {
	t.Parallel()

	cardanoConfig, err := cardano.LoadCardanoNodeConfigWithFallback(
		"mainnet/config.json",
		"mainnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	require.NotNil(t, cardanoConfig.ByronGenesis())
	require.NotNil(t, cardanoConfig.ShelleyGenesis())

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: cardanoConfig,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, ls.PrepareEpochCacheForStartup())

	require.Len(t, ls.epochCache, 1)
	assert.Equal(t, uint64(0), ls.currentEpoch.EpochId)
	assert.Equal(t, eras.ByronEraDesc.Id, ls.currentEpoch.EraId)
	assert.Equal(t, uint(20000), ls.currentEpoch.SlotLength)
	assert.Equal(t, uint(21600), ls.currentEpoch.LengthInSlots)
}

// newTestEpoch is a convenience builder for models.Epoch.
func newTestEpoch(
	id, startSlot uint64,
	lengthInSlots uint,
	eraId uint,
) models.Epoch {
	return models.Epoch{
		EpochId:       id,
		StartSlot:     startSlot,
		LengthInSlots: lengthInSlots,
		EraId:         eraId,
		SlotLength:    1000,
	}
}

// ---------------------------------------------------------------------------
// evaluateTransitionImpossible tests
// ---------------------------------------------------------------------------

// TestEvaluateTransitionImpossible_SetWhenSafeZoneReachesEpochEnd verifies
// that TransitionImpossible is set when tipSlot + safeZone >= epochEndSlot.
//
// Using Shelley-era parameters from newTestEraHistoryCfg:
//
//	securityParam=432, activeSlotsCoeff=0.05
//	safeZone = ceil(3*432/0.05) = 25_920
//	epoch: startSlot=100_000, length=432_000, end=532_000
//	tipSlot = 532_000 - 25_920 = 506_080 → safeEnd = 532_000 = epochEnd → Impossible
func TestEvaluateTransitionImpossible_SetWhenSafeZoneReachesEpochEnd(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochStart = uint64(100_000)
		epochLen   = uint(432_000)
		epochEnd   = uint64(532_000)
		safeZone   = uint64(25_920)
		// tipSlot such that tipSlot + safeZone == epochEnd (boundary case)
		tipSlot = epochEnd - safeZone // 506_080
	)

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra: requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(
			500,
			epochStart,
			epochLen,
			eras.ConwayEraDesc.Id,
		),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionImpossible, ls.transitionInfo.State,
		"when safeEndSlot == epochEndSlot, TransitionImpossible must be set")
}

// TestEvaluateTransitionImpossible_SetWhenSafeZoneExceedsEpochEnd verifies
// that TransitionImpossible is set when safeEndSlot > epochEndSlot.
func TestEvaluateTransitionImpossible_SetWhenSafeZoneExceedsEpochEnd(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochStart = uint64(100_000)
		epochLen   = uint(432_000)
		epochEnd   = uint64(532_000)
		// tipSlot well past the safe-zone boundary
		tipSlot = uint64(520_000)
	)

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra: requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(
			500,
			epochStart,
			epochLen,
			eras.ConwayEraDesc.Id,
		),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionImpossible, ls.transitionInfo.State)
}

// TestEvaluateTransitionImpossible_NotSetWhenSafeZoneInsideEpoch verifies
// that TransitionImpossible is NOT set when safeEndSlot < epochEndSlot.
func TestEvaluateTransitionImpossible_NotSetWhenSafeZoneInsideEpoch(
	t *testing.T,
) {
	t.Parallel()

	const (
		epochStart = uint64(100_000)
		epochLen   = uint(432_000)
		// tipSlot one slot before the boundary: safeEnd = epochEnd - 1
		tipSlot = uint64(506_079) // 532_000 - 25_920 - 1
	)

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra: requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(
			500,
			epochStart,
			epochLen,
			eras.ConwayEraDesc.Id,
		),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"safeEndSlot < epochEndSlot: TransitionImpossible must NOT be set")
}

// TestEvaluateTransitionImpossible_NoOpWhenTransitionKnown verifies that
// evaluateTransitionImpossible does not override a confirmed TransitionKnown.
func TestEvaluateTransitionImpossible_NoOpWhenTransitionKnown(t *testing.T) {
	t.Parallel()

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra: requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(
			500,
			100_000,
			432_000,
			eras.ConwayEraDesc.Id,
		),
		currentTip: ochainsync.Tip{
			// tipSlot past the safe-zone boundary → would normally trigger Impossible
			Point: ocommon.NewPoint(520_000, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionKnown(501),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State,
		"evaluateTransitionImpossible must not override TransitionKnown")
	assert.Equal(t, uint64(501), ls.transitionInfo.KnownEpoch)
}

// TestEvaluateTransitionImpossible_NoOpAlreadyImpossible verifies that the
// call is idempotent when TransitionImpossible is already set.
func TestEvaluateTransitionImpossible_NoOpAlreadyImpossible(t *testing.T) {
	t.Parallel()

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra: requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(
			500,
			100_000,
			432_000,
			eras.ConwayEraDesc.Id,
		),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(520_000, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionImpossible(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionImpossible, ls.transitionInfo.State)
}

// TestEvaluateTransitionImpossible_NoOpWhenEpochLengthZero verifies that a
// zero LengthInSlots (uninitialized epoch) is skipped safely.
func TestEvaluateTransitionImpossible_NoOpWhenEpochLengthZero(t *testing.T) {
	t.Parallel()

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra:   requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: models.Epoch{EpochId: 0, LengthInSlots: 0},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(999_999, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"zero-length epoch must not trigger TransitionImpossible")
}

// ---------------------------------------------------------------------------
// evaluateTriggerAtEpoch tests
// ---------------------------------------------------------------------------

// newTestLedgerStateWithTrigger builds a minimal LedgerState with the given
// currentEra / currentEpoch / initial transitionInfo, and the requested
// TestXHardForkAtEpoch override wired into the config (keyed on the
// successor era's lowercase name).
func newTestLedgerStateWithTrigger(
	t *testing.T,
	currentEraId uint,
	currentEpochId uint64,
	initialTI hardfork.TransitionInfo,
	nextEraLower string,
	overrideEpoch *uint64,
	experimentalEnabled bool,
) *LedgerState {
	t.Helper()
	cfg := newTestEraHistoryCfg(t)
	if experimentalEnabled {
		enabled := true
		cfg.ExperimentalHardForksEnabled = &enabled
	}
	switch nextEraLower {
	case "shelley":
		cfg.TestShelleyHardForkAtEpoch = overrideEpoch
	case "allegra":
		cfg.TestAllegraHardForkAtEpoch = overrideEpoch
	case "mary":
		cfg.TestMaryHardForkAtEpoch = overrideEpoch
	case "alonzo":
		cfg.TestAlonzoHardForkAtEpoch = overrideEpoch
	case "babbage":
		cfg.TestBabbageHardForkAtEpoch = overrideEpoch
	case "conway":
		cfg.TestConwayHardForkAtEpoch = overrideEpoch
	}
	return &LedgerState{
		currentEra:     requireEraDesc(t, currentEraId),
		currentEpoch:   newTestEpoch(currentEpochId, 0, 432_000, currentEraId),
		transitionInfo: initialTI,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// Happy path: in Byron, with ExperimentalHardForksEnabled and
// TestShelleyHardForkAtEpoch=5, and the current epoch before 5, the
// TransitionInfo is surfaced as TransitionKnown(5).
func TestEvaluateTriggerAtEpoch_SetsTransitionKnown(t *testing.T) {
	t.Parallel()

	target := uint64(5)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch)
}

// Without ExperimentalHardForksEnabled, the override is inert.
func TestEvaluateTriggerAtEpoch_InertWithoutExperimentalFlag(t *testing.T) {
	t.Parallel()

	target := uint64(5)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		"shelley", &target, false,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"override must be ignored without ExperimentalHardForksEnabled")
}

// When currentEpoch.EpochId >= target epoch, the trigger is not applied
// (the transition should have already occurred).
func TestEvaluateTriggerAtEpoch_NotSetWhenEpochReached(t *testing.T) {
	t.Parallel()

	target := uint64(5)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 5,
		hardfork.NewTransitionUnknown(),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// The last known era has no successor: the call is a no-op even if
// Test<Next>HardForkAtEpoch happens to be set (not meaningful).
func TestEvaluateTriggerAtEpoch_NoOpOnFinalEra(t *testing.T) {
	t.Parallel()

	target := uint64(100)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ConwayEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		// This test uses the default active era table, where Dijkstra is
		// gated off and Conway has no successor.
		"", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// AtEpoch override supersedes a prior TransitionImpossible: AtEpoch is
// authoritative info about a known upcoming transition and must override the
// safe-zone-derived "no transition in this epoch" verdict.
func TestEvaluateTriggerAtEpoch_OverridesTransitionImpossible(t *testing.T) {
	t.Parallel()

	target := uint64(10)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionImpossible(),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch)
}

// AtEpoch override replaces a TransitionKnown set for a different epoch.
// Mirrors Haskell's shelleyTriggerHardFork short-circuit: the AtEpoch config
// is the truth and bypasses pparams-vote inspection entirely.
func TestEvaluateTriggerAtEpoch_ReplacesDifferentKnownEpoch(t *testing.T) {
	t.Parallel()

	target := uint64(10)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionKnown(4),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch,
		"AtEpoch override must replace a stale TransitionKnown(other)")
}

// Idempotent when already TransitionKnown at the same epoch.
func TestEvaluateTriggerAtEpoch_IdempotentOnSameEpoch(t *testing.T) {
	t.Parallel()

	target := uint64(10)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionKnown(target),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch)
}

// No override configured at all: evaluateTriggerAtEpoch is a no-op.
func TestEvaluateTriggerAtEpoch_NoOpWithoutOverride(t *testing.T) {
	t.Parallel()

	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		"", nil, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// TestRolloverCommit_ResetsTransitionImpossible verifies that a plain epoch
// rollover (no HardFork, no era transition) resets TransitionImpossible to
// TransitionUnknown so the new epoch starts fresh.
func TestRolloverCommit_ResetsTransitionImpossible(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentPParams: babbagePParams(9),
		// Simulate state at end of epoch 500: TransitionImpossible was set
		// because the tip's safe zone reached the epoch end.
		transitionInfo: hardfork.NewTransitionImpossible(),
	}

	var eraTransitions []*EraTransitionResult
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{
			EpochId:       501,
			StartSlot:     532_000,
			LengthInSlots: 432_000,
		},
		NewCurrentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 501}},
		HardFork:          nil,
	}

	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	if rolloverResult != nil {
		ls.epochCache = rolloverResult.NewEpochCache
		ls.currentEpoch = rolloverResult.NewCurrentEpoch
		ls.currentEra = rolloverResult.NewCurrentEra
		ls.currentPParams = rolloverResult.NewCurrentPParams
		if len(eraTransitions) == 0 {
			ls.transitionInfo = hardfork.NewTransitionUnknown()
		}
	}
	if len(eraTransitions) == 0 && rolloverResult != nil &&
		rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(
			rolloverResult.NewCurrentEpoch.EpochId,
		)
	}
	ls.Unlock()

	assert.Equal(
		t,
		hardfork.TransitionUnknown,
		ls.transitionInfo.State,
		"plain epoch rollover must reset TransitionImpossible to TransitionUnknown",
	)
}

// TestApplyEraTransition_ClearsTransitionKnown verifies that
// applyEraTransition unconditionally clears a pending TransitionKnown, even
// when called outside of any epoch-rollover context (the "standalone
// era-transition block" case).
func TestApplyEraTransition_ClearsTransitionKnown(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionKnown(500),
	}

	result := &EraTransitionResult{
		NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewPParams: babbagePParams(9),
	}

	// Simulate a standalone era-transition path: apply under the lock,
	// no epoch rollover involved.
	ls.Lock()
	ls.applyEraTransition(result)
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"TransitionKnown must be cleared when the new era becomes active")
	assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
}

// TestApplyEraTransition_ClearsTransitionUnknown confirms that calling
// applyEraTransition when transitionInfo is already TransitionUnknown is a
// no-op for the State field (still TransitionUnknown).
func TestApplyEraTransition_ClearsTransitionUnknown(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionUnknown(),
	}

	result := &EraTransitionResult{
		NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewPParams: babbagePParams(9),
	}

	ls.Lock()
	ls.applyEraTransition(result)
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// TestApplyEraTransition_PreservesAndUpdatesFields verifies that
// applyEraTransition correctly rotates currentPParams → prevEraPParams
// and installs result.NewPParams / result.NewEra.
func TestApplyEraTransition_PreservesAndUpdatesFields(t *testing.T) {
	t.Parallel()

	oldPParams := babbagePParams(8)
	newPParams := babbagePParams(9)

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: lcommon.ProtocolParameters(oldPParams),
		transitionInfo: hardfork.NewTransitionKnown(500),
	}

	result := &EraTransitionResult{
		NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewPParams: lcommon.ProtocolParameters(newPParams),
	}

	ls.Lock()
	ls.applyEraTransition(result)
	ls.Unlock()

	assert.Equal(t, lcommon.ProtocolParameters(oldPParams), ls.prevEraPParams,
		"old pparams must be preserved as prevEraPParams")
	assert.Equal(t, lcommon.ProtocolParameters(newPParams), ls.currentPParams,
		"new pparams must become currentPParams")
	assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id,
		"currentEra must be updated to the new era")
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"transitionInfo must be cleared")
}

// TestApplyEraTransition_MultipleSteps_AllCleared verifies the chained-
// transition case (e.g. jumping two eras at once): each step clears
// transitionInfo, and the final state is TransitionUnknown.
func TestApplyEraTransition_MultipleSteps_AllCleared(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.AlonzoEraDesc.Id),
		currentPParams: babbagePParams(6),
		transitionInfo: hardfork.NewTransitionKnown(300),
	}

	steps := []*EraTransitionResult{
		{
			NewEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
			NewPParams: babbagePParams(8),
		},
		{
			NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
			NewPParams: babbagePParams(9),
		},
	}

	ls.Lock()
	for _, step := range steps {
		ls.applyEraTransition(step)
	}
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
	assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
}

// TestRolloverCommit_EraTransitionClearsTransitionInfo exercises the
// in-memory state update block (the rollover-commit path) with both
// eraTransitions and a rolloverResult to confirm that eraTransitions take
// precedence: TransitionKnown is cleared even when rolloverResult.HardFork
// is also set (should not happen in practice, but the logic must be safe).
func TestRolloverCommit_EraTransitionClearsTransitionInfo(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionKnown(499),
	}

	eraTransitions := []*EraTransitionResult{
		{
			NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
			NewPParams: babbagePParams(9),
		},
	}
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch:   models.Epoch{EpochId: 500},
		NewCurrentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 500}},
		HardFork: &HardForkInfo{
			OldVersion: ProtocolVersion{Major: 8},
			NewVersion: ProtocolVersion{Major: 9},
		},
	}

	// Replicate the rollover-commit block logic directly.
	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	ls.epochCache = rolloverResult.NewEpochCache
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	if len(eraTransitions) == 0 && rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(
			rolloverResult.NewCurrentEpoch.EpochId,
		)
	}
	ls.Unlock()

	assert.Equal(
		t,
		hardfork.TransitionUnknown,
		ls.transitionInfo.State,
		"era transition must clear transitionInfo even when rolloverResult.HardFork is set",
	)
}

// TestRolloverCommit_HardForkWithoutEraTransition verifies that
// TransitionKnown is set when rolloverResult.HardFork is non-nil and no era
// transition happened (the normal epoch-boundary version-bump window).
func TestRolloverCommit_HardForkWithoutEraTransition(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionUnknown(),
	}

	var eraTransitions []*EraTransitionResult // empty — no standalone transition
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch:   models.Epoch{EpochId: 500},
		NewCurrentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 500}},
		HardFork: &HardForkInfo{
			OldVersion: ProtocolVersion{Major: 8},
			NewVersion: ProtocolVersion{Major: 9},
		},
	}

	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	ls.epochCache = rolloverResult.NewEpochCache
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	if len(eraTransitions) == 0 && rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(
			rolloverResult.NewCurrentEpoch.EpochId,
		)
	}
	ls.Unlock()

	assert.Equal(
		t,
		hardfork.TransitionKnown,
		ls.transitionInfo.State,
		"version bump at epoch boundary without era transition must set TransitionKnown",
	)
	assert.Equal(t, uint64(500), ls.transitionInfo.KnownEpoch)
}

// TestRolloverCommit_NoHardFork_TransitionInfoUnchanged verifies that a plain
// epoch rollover (no HardFork, no era transition) leaves transitionInfo alone.
func TestRolloverCommit_NoHardFork_TransitionInfoUnchanged(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentPParams: babbagePParams(9),
		transitionInfo: hardfork.NewTransitionUnknown(),
	}

	var eraTransitions []*EraTransitionResult
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch:   models.Epoch{EpochId: 501},
		NewCurrentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 501}},
		HardFork:          nil,
	}

	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	ls.epochCache = rolloverResult.NewEpochCache
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	if len(eraTransitions) == 0 && rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(
			rolloverResult.NewCurrentEpoch.EpochId,
		)
	}
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"plain epoch rollover must not change transitionInfo")
}

func TestLatestOpCertSequenceTracksHighestObservedAndRollback(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	var poolID [28]byte
	for i := range poolID {
		poolID[i] = byte(i + 1)
	}
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolID[:]))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	sequence, found, err := ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(0), sequence)

	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 3, 10, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 7, 20, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 5, 30, nil))

	sequence, found, err = ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), sequence)

	require.NoError(t, db.RestorePoolStateAtSlot(15, nil))
	sequence, found, err = ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), sequence)
}

func TestLedgerProcessBlockTracksOpCertSequenceByIssuerVkeyHash(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	var issuerVkey lcommon.IssuerVkey
	for i := range issuerVkey {
		issuerVkey[i] = byte(i + 1)
	}
	pkh := lcommon.PoolKeyHash(issuerVkey.Hash())
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	block := &babbage.BabbageBlock{
		BlockHeader: &babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				Slot:       10,
				IssuerVkey: issuerVkey,
				OpCert: babbage.BabbageOpCert{
					SequenceNumber: 4,
				},
			},
		},
	}

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 10},
			block,
			false,
			false,
			false,
			nil,
			envelopeParent{},
			nil,
			eras.BabbageEraDesc,
			nil,
			nil,
			0,
			0,
			false,
		)
		return err
	}))

	var poolID [28]byte
	copy(poolID[:], pkh.Bytes())
	sequence, found, err := ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(4), sequence)
}

func TestLedgerProcessBlockRejectsCertRBWhenParentCannotBeResolved(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	certified, err := cbor.Encode(true)
	require.NoError(t, err)
	block := &dijkstra.DijkstraBlock{
		BlockBody: dijkstra.DijkstraBlockBody{
			LeiosCertificate: &dijkstra.DijkstraLeiosCertificate{
				Signers:             []byte{1},
				AggregatedSignature: make([]byte, 48),
			},
		},
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 2,
					Slot:        10,
					PrevHash: lcommon.NewBlake2b256(
						[]byte("missing-cert-rb-parent"),
					),
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{certified},
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EndorserBlockProvider: func(
				[]byte,
				uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, false
			},
			ValidateLeiosCertificate: func(
				uint64,
				[]byte,
				[]byte,
				[]byte,
			) error {
				return nil
			},
		},
	}

	err = db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: block.SlotNumber()},
			block,
			false,
			false,
			false,
			nil,
			envelopeParent{},
			nil,
			eras.DijkstraEraDesc,
			nil,
			nil,
			0,
			0,
			false,
		)
		return err
	})
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
}

// TestLedgerProcessBlockRejectsStandardDijkstraValidationFailure exercises
// the full standard-profile apply path. The transaction is invalid only
// because its fee is below the protocol minimum, so trusting the validation
// error would record it in metadata; the rejection must return a
// txValidationError and leave no transaction committed.
func TestLedgerProcessBlockRejectsStandardDijkstraValidationFailure(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	txCbor, err := cbor.Encode([]any{
		map[uint]any{
			0: cbor.Tag{Number: 258, Content: []any{}},
			1: []any{},
			2: uint64(0),
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := dijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)

	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	pparams.MinFeeB = 1
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  10,
				ByteLength: uint32(len(txCbor)),
			},
		},
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        10,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: 12,
					},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err = db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 10, Hash: []byte("dijkstra-validation")},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{},
			offsets,
			eras.DijkstraEraDesc,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
	require.Error(t, err)
	var validationErr *txValidationError
	require.ErrorAs(t, err, &validationErr)
	require.Contains(t, err.Error(), "fee")

	stored, err := db.Metadata().GetTransactionByHash(tx.Hash().Bytes(), nil)
	require.NoError(t, err)
	assert.Nil(t, stored, "rejected Dijkstra transaction must not be committed")
}

// TestStrictConsumedInputsEnabled pins the strict-consumed-inputs guard
// condition, including the P1 transition-batch case: the first batch whose
// blocks cross the tip cutoff is processed while reachedTip is still false (it
// is stored true only after that batch commits), so the per-block reachesTip
// signal must enable the guard on its own. Without it that transition batch
// could still recover an unapplied producer from the blob store.
func TestStrictConsumedInputsEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		shouldValidate bool
		reachedTip     bool
		reachesTip     bool
		want           bool
	}{
		{
			name:           "unvalidated application is never strict",
			shouldValidate: false,
			reachedTip:     true,
			reachesTip:     true,
			want:           false,
		},
		{
			name:           "validated at an established tip",
			shouldValidate: true,
			reachedTip:     true,
			reachesTip:     false,
			want:           true,
		},
		{
			name:           "validated transition batch before reachedTip stored",
			shouldValidate: true,
			reachedTip:     false,
			reachesTip:     true,
			want:           true,
		},
		{
			name:           "validated historical catch-up not yet at tip",
			shouldValidate: true,
			reachedTip:     false,
			reachesTip:     false,
			want:           false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{}
			ls.reachedTip.Store(tc.reachedTip)
			require.Equal(
				t,
				tc.want,
				ls.strictConsumedInputsEnabled(
					tc.shouldValidate,
					tc.reachesTip,
				),
			)
		})
	}
}

func TestLogLeiosEndorserBlockApplyResultDistinguishesEmptyBlock(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name     string
		applyTxs bool
		ebTxs    []cbor.RawMessage
		applied  int
		want     string
		notWant  []string
	}{
		{
			name:     "empty CIP block",
			applyTxs: true,
			want:     "Leios endorser block has no transactions",
			notWant: []string{
				"skipped already-applied Leios endorser block transactions",
				"stored Leios endorser block without applying to UTxO",
			},
		},
		{
			name:     "CIP deduplicated block",
			applyTxs: true,
			ebTxs:    []cbor.RawMessage{{0x80}},
			want:     "skipped already-applied Leios endorser block transactions",
			notWant:  []string{"Leios endorser block has no transactions"},
		},
		{
			name:  "Haskell deduplicated block",
			ebTxs: []cbor.RawMessage{{0x80}},
			want:  "skipped already-applied Leios endorser block transactions",
			notWant: []string{
				"Leios endorser block has no transactions",
				"stored Leios endorser block without applying to UTxO",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			ls := &LedgerState{
				config: LedgerStateConfig{
					LeiosApplyEndorserBlockTxs: tc.applyTxs,
					Logger: slog.New(slog.NewTextHandler(
						&logBuf,
						&slog.HandlerOptions{Level: slog.LevelDebug},
					)),
				},
			}

			ls.logLeiosEndorserBlockApplyResult(
				ocommon.Point{Slot: 10},
				20,
				tc.ebTxs,
				tc.applied,
			)

			logs := logBuf.String()
			assert.Contains(t, logs, tc.want)
			for _, notWant := range tc.notWant {
				assert.NotContains(t, logs, notWant)
			}
		})
	}
}

func TestLeiosValidationSessionRollsBackStagedCertificateWrites(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	credential := bytes.Repeat([]byte{0x61}, lcommon.Blake2b224Size)
	credentialHash := lcommon.NewBlake2b224(credential)
	tx := mockledger.NewTransactionBuilder().WithCertificates(
		&lcommon.RegistrationDrepCertificate{
			CertType: uint(lcommon.CertificateTypeRegistrationDrep),
			DrepCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: credentialHash,
			},
			Amount: 500,
		},
	)
	tx.WithId(bytes.Repeat([]byte{0x62}, lcommon.Blake2b256Size))
	tx.WithValid(true)

	ls := &LedgerState{
		db:             db,
		currentPParams: &conway.ConwayProtocolParameters{DRepDeposit: 500},
		slotClock: NewSlotClock(
			newMockSlotTimeProvider(time.Now(), time.Second, 100),
			DefaultSlotClockConfig(),
		),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err = ls.withTxValidationSession(nil, nil, true, func(
		_ func(lcommon.Transaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo) error,
		_ func() bool,
		applyTx txValidationApplyFunc,
	) error {
		return applyTx(
			tx,
			0,
			ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x63}, lcommon.Blake2b256Size)},
			uint(conway.EraIdConway),
			1,
		)
	})
	require.NoError(t, err)

	_, err = db.GetDrepByCredential(0, credential, true, nil)
	require.ErrorIs(t, err, models.ErrDrepNotFound)
}

// TestCloseReturnsErrorWhenDBWorkerPoolDoesNotShutdownInTime covers Close()'s
// database-worker-pool wait: a timeout there used to be logged as a Warn while
// Close() still returned nil, which let live restore/truncate's caller
// (closeStorageForLiveLifecycleOp) treat an unconfirmed drain as a green light
// to close and reopen the data directory.
func TestCloseReturnsErrorWhenDBWorkerPoolDoesNotShutdownInTime(t *testing.T) {
	origTimeout := CloseDBWorkerPoolShutdownTimeout
	CloseDBWorkerPoolShutdownTimeout = 10 * time.Millisecond
	t.Cleanup(func() { CloseDBWorkerPoolShutdownTimeout = origTimeout })

	pool := NewDatabaseWorkerPool(nil, DatabaseWorkerPoolConfig{
		WorkerPoolSize: 1,
		TaskQueueSize:  1,
	})
	release := make(chan struct{})
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			<-release
			return nil
		},
	})
	t.Cleanup(func() { close(release) })

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
		dbWorkerPool: pool,
	}

	err := ls.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database worker pool")
}

// TestCloseReturnsErrorWhenBlockProcessingPipelineDoesNotStopInTime covers
// the root cause of the "persistent chain index gap" liveness failure seen
// under TestLiveTruncateUnderRealForgingAndNetworking (real forging +
// networking, only reproducible under contention/slower hardware): Close
// previously never waited for ledgerProcessBlocks (the goroutine Start
// launches to apply incoming chainsync blocks) at all, since Start ran it
// against ctx directly rather than a child context Close could cancel. A
// block landing mid-write exactly as Close proceeded to shut down
// dbWorkerPool left the persisted block-ID index permanently inconsistent
// with the in-memory tip already advanced for it -- a corruption no retry
// recovers from, unlike a transient timing issue.
func TestCloseReturnsErrorWhenBlockProcessingPipelineDoesNotStopInTime(
	t *testing.T,
) {
	origTimeout := CloseProcessBlocksDrainTimeout
	CloseProcessBlocksDrainTimeout = 10 * time.Millisecond
	t.Cleanup(func() { CloseProcessBlocksDrainTimeout = origTimeout })

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.processBlocksCancel = func() {}
	// Simulate an in-flight ledgerProcessBlocks goroutine that outlives the
	// timeout -- e.g. mid-write on a block when Close is called.
	ls.processBlocksWG.Add(1)
	t.Cleanup(ls.processBlocksWG.Done)

	err := ls.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "block-processing pipeline")
}

// TestCloseDoesNotHoldBlockfetchContinuationMutexWhileWaiting verifies that
// Close releases the continuation scheduling mutex before waiting for the
// continuation WaitGroup. A worker may need that mutex to complete the request
// that lets it return, so holding it across the wait deadlocks shutdown.
//
// The invariant is asserted directly -- the mutex must be acquirable *while*
// Close is parked in the wait -- rather than by having a queued worker finish,
// which passes whether or not Close ever held the mutex.
// Not t.Parallel: this and the other Close* tests below swap the package-level
// Close*Timeout tunables.
func TestCloseDoesNotHoldBlockfetchContinuationMutexWhileWaiting(t *testing.T) {
	origTimeout := CloseBlockfetchDrainTimeout
	// Generous: the worker is released only after the assertion below, so this
	// bounds the failure mode rather than the happy path.
	CloseBlockfetchDrainTimeout = 30 * time.Second
	t.Cleanup(func() { CloseBlockfetchDrainTimeout = origTimeout })

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	schedulingDone := make(chan struct{})
	ls.blockfetchContinuationSchedulingHook = func() {
		close(schedulingDone)
	}

	// A continuation worker that stays registered until the test releases it,
	// so Close cannot leave its wait while the assertion runs. It deliberately
	// does not touch the mutex: the point is what Close holds, not what the
	// worker can acquire.
	proceed := make(chan struct{})
	ls.blockfetchContinuationWG.Go(func() {
		<-proceed
	})

	ls.blockfetchContinuationMu.Lock()
	closeDone := make(chan error, 1)
	go func() { closeDone <- ls.Close() }()
	require.Eventually(
		t,
		ls.closed.Load,
		testutil.AsyncWait,
		time.Millisecond,
		"Close did not begin before releasing the continuation mutex",
	)
	ls.blockfetchContinuationMu.Unlock()

	// The hook fires only after Close has completed the scheduling lock/unlock
	// pair. The worker remains registered, so Close must still be in its wait.
	testutil.RequireReceive(
		t,
		schedulingDone,
		testutil.AsyncWait,
		"Close did not release blockfetchContinuationMu before waiting",
	)
	require.True(
		t,
		ls.blockfetchContinuationMu.TryLock(),
		"Close held blockfetchContinuationMu while waiting for continuations",
	)
	ls.blockfetchContinuationMu.Unlock()

	// Only now let the worker finish, proving Close was genuinely still waiting
	// throughout the assertion above.
	close(proceed)
	err := testutil.RequireReceive(
		t,
		closeDone,
		testutil.AsyncWait,
		"Close did not finish after the continuation worker drained",
	)
	require.NoError(t, err)
}

// TestCloseWaitsForBlockProcessingPipelineToActuallyStop is the positive
// counterpart: a real Start/Close cycle (ledgerProcessBlocks genuinely
// running, not simulated) must not report a timeout, and Close must
// actually block until the goroutine has exited -- proving
// processBlocksCancel's child context, not ctx directly, is what Start
// wires ledgerProcessBlocks to run against.
func TestCloseWaitsForBlockProcessingPipelineToActuallyStop(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())

	processCtx, processCancel := context.WithCancel(t.Context())
	ls.processBlocksCancel = processCancel
	ls.processBlocksWG.Add(1)
	stopped := make(chan struct{})
	go func() {
		defer ls.processBlocksWG.Done()
		<-processCtx.Done()
		close(stopped)
	}()

	err := ls.Close()
	require.NoError(t, err)
	select {
	case <-stopped:
	default:
		t.Fatal("Close returned without processCtx actually being cancelled")
	}
}

func TestCloseReplayReturnsWhenPreviousCloseIsStillRunning(t *testing.T) {
	origTimeout := CloseResultReplayTimeout
	CloseResultReplayTimeout = 10 * time.Millisecond
	t.Cleanup(func() { CloseResultReplayTimeout = origTimeout })

	ls := &LedgerState{closeDone: make(chan struct{})}
	err := ls.Close()
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"previous ledger state close still in progress",
	)
}

// TestCloseStopsDecodePipelineBeforeWaitingForBlockProcessing covers the
// shutdown ordering required when block processing is draining the decode
// pipeline's Results channel. That drain has no context select after a batch
// is submitted, so stopping the pipeline must close Results before Close
// waits for the block-processing goroutine.
func TestCloseStopsDecodePipelineBeforeWaitingForBlockProcessing(t *testing.T) {
	origTimeout := CloseProcessBlocksDrainTimeout
	CloseProcessBlocksDrainTimeout = time.Second
	t.Cleanup(func() { CloseProcessBlocksDrainTimeout = origTimeout })

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
		blockPipeline: pipeline.NewBlockPipeline(
			pipeline.WithDecodeWorkers(1),
		),
	}
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	ls.processBlocksCancel = func() {}
	ls.processBlocksWG.Go(func() {
		for range ls.blockPipeline.Results() {
		}
	})

	require.NoError(t, ls.Close())
}

// TestReconstructTransitionInfoIgnoresStaleShelleyPParamsUnderByron pins the
// Byron guard as a backstop rather than a redundancy.
//
// It is not covered by the currentPParams == nil check that follows it. The
// reachable shape is a rollback into Byron: rollbackChainAndStateDeferred sets
// currentEra to Byron and then calls this function, and before the ppComputed
// change it skipped the currentPParams assignment whenever the recomputed
// value was nil -- which is exactly what Byron computes. That left a Shelley
// value in place under a Byron era, and without this guard
// reconstructTransitionInfo would read the Shelley protocol version out of it
// and fabricate a transition at epoch zero.
//
// The rollback path itself is not driven here; that needs a chain fixture
// spanning the Byron-Shelley boundary. This asserts the guard holds for the
// state that path can produce.
func TestReconstructTransitionInfoIgnoresStaleShelleyPParamsUnderByron(
	t *testing.T,
) {
	t.Parallel()

	shelleyPParams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor: 2,
		ProtocolMinor: 0,
	}

	ls := &LedgerState{
		currentEra: eras.ByronEraDesc,
		// The stale value a rollback into Byron used to leave behind.
		currentPParams: shelleyPParams,
		transitionInfo: hardfork.NewTransitionUnknown(),
	}

	ls.reconstructTransitionInfo()

	require.Equal(
		t,
		hardfork.NewTransitionUnknown(),
		ls.transitionInfo,
		"a Shelley pparams value under a Byron era must not be read as a transition",
	)
}

// TestWarnOnPreByronPrefixEpochCache pins the detection of a database written
// before the Byron prefix was preserved at startup. The startup fix only
// applies to an empty database, so an operator who already began a preprod or
// mainnet from-genesis sync keeps epoch 0 tagged Shelley at slot 0 and sees the
// same overlay rejection as before -- with nothing to say the binary already
// carries the fix. The warning is the only signal, so it needs to fire exactly
// on that shape.
func TestWarnOnPreByronPrefixEpochCache(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {"k": 432, "protocolMagic": 2},
		"blockVersionData": {"slotDuration": "20000"}
	}`

	newLedger := func(
		t *testing.T, withByron, shelleyAtGenesis bool,
	) (*LedgerState, *bytes.Buffer) {
		t.Helper()
		cfg := &cardano.CardanoNodeConfig{}
		if withByron {
			require.NoError(t, loadByronGenesisForTest(t, cfg,
				strings.NewReader(byronGenesisJSON),
			))
		}
		if shelleyAtGenesis {
			cfg.TestShelleyHardForkAtEpoch = new(uint64)
		}
		var logs bytes.Buffer
		return &LedgerState{
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(slog.NewTextHandler(
					&logs, &slog.HandlerOptions{Level: slog.LevelWarn},
				)),
			},
		}, &logs
	}

	const warning = "database predates Byron prefix preservation"

	t.Run("stale shape warns", func(t *testing.T) {
		ls, logs := newLedger(t, true, false)
		ls.epochCache = []models.Epoch{
			{EpochId: 0, EraId: eras.ShelleyEraDesc.Id},
			{EpochId: 1, EraId: eras.ShelleyEraDesc.Id},
		}
		ls.warnOnPreByronPrefixEpochCache()
		assert.Contains(t, logs.String(), warning)
	})

	t.Run("stale shape warns once per process", func(t *testing.T) {
		// loadEpochs runs twice on startup, from PrepareEpochCacheForStartup
		// and again from Start, and both take the populated-cache branch on a
		// database that already has epochs. An operator in exactly the
		// situation this diagnoses should not see it twice.
		ls, logs := newLedger(t, true, false)
		ls.epochCache = []models.Epoch{
			{EpochId: 0, EraId: eras.ShelleyEraDesc.Id},
		}
		ls.warnOnPreByronPrefixEpochCache()
		ls.warnOnPreByronPrefixEpochCache()
		assert.Equal(t, 1, strings.Count(logs.String(), warning))
	})

	t.Run("byron epoch zero is silent", func(t *testing.T) {
		ls, logs := newLedger(t, true, false)
		ls.epochCache = []models.Epoch{
			{EpochId: 0, EraId: eras.ByronEraDesc.Id},
			{EpochId: 4, EraId: eras.ShelleyEraDesc.Id},
		}
		ls.warnOnPreByronPrefixEpochCache()
		assert.NotContains(t, logs.String(), warning)
	})

	t.Run("shelley declared at genesis is silent", func(t *testing.T) {
		// preview's shape: no Byron prefix to preserve, so epoch 0 being
		// Shelley is correct rather than stale.
		ls, logs := newLedger(t, true, true)
		ls.epochCache = []models.Epoch{
			{EpochId: 0, EraId: eras.ShelleyEraDesc.Id},
		}
		ls.warnOnPreByronPrefixEpochCache()
		assert.NotContains(t, logs.String(), warning)
	})

	t.Run("no byron genesis is silent", func(t *testing.T) {
		ls, logs := newLedger(t, false, false)
		ls.epochCache = []models.Epoch{
			{EpochId: 0, EraId: eras.ShelleyEraDesc.Id},
		}
		ls.warnOnPreByronPrefixEpochCache()
		assert.NotContains(t, logs.String(), warning)
	})

	t.Run("empty cache is silent", func(t *testing.T) {
		ls, logs := newLedger(t, true, false)
		ls.warnOnPreByronPrefixEpochCache()
		assert.NotContains(t, logs.String(), warning)
	})
}

// TestUpstreamSyncStatusReachableStates pins every (target, active) pair the
// real LedgerState can return from UpstreamSyncStatus, which is the value the
// forge staleness gate reads.
//
// It exists because a gate was written against a state this type cannot
// produce. An earlier revision fell back to the admitted-header frontier when
// UpstreamSyncStatus returned a zero target, on the belief that a live upstream
// with no published target reported (0, false). It reports (0, true), so the
// fallback was written for a state that never occurs. It passed review only
// because a test double could express (0, false) alongside a non-zero admitted
// frontier, which is the one combination the adapter cannot produce.
//
// (0, true) is now doubly worth pinning. It used to be unreachable at the
// staleness gate as well, because the sync gate refused every slot on
// upstreamActive && upstreamTip == 0; that blanket refusal was replaced with
// a bound on the local tip's lag, so a node at tip passes it and the staleness
// gate does see this pair. What keeps the bound quiet there is its own
// upstreamTarget > newestKnown term -- see
// TestForgeUpstreamStalenessIgnoresUnknownUpstreamTarget -- which is only
// sound while this test holds that the target really is 0 and not something
// substituted for it.
//
// Assert the adapter's own outputs, not a double's: a double is only evidence
// about the double.
func TestUpstreamSyncStatusReachableStates(t *testing.T) {
	conn := testChainsyncConnId(6000, 3094)
	activeConn := conn
	live := true
	ls := &LedgerState{
		config: LedgerStateConfig{
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				if !live {
					return nil
				}
				return &activeConn
			},
		},
	}

	// Live upstream, no target published -- the state the removed fallback
	// was written for. It is (0, TRUE), not (0, false).
	ls.advanceUpstreamTipSlot(318)
	target, active := ls.UpstreamSyncStatus()
	assert.Zero(t, target)
	assert.True(
		t,
		active,
		"a live upstream with no published target is (0, true); the "+
			"pre-existing sync gate refuses this slot before the stale-tip "+
			"gate runs, so no stale-tip branch may be written for it",
	)
	upstreamTip, upstreamLive := ls.UpstreamSyncTip()
	assert.True(t, upstreamLive)
	assert.Zero(t, upstreamTip.BlockNumber)

	ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
		ConnectionId: conn,
		SyncTarget: ochainsync.Tip{
			Point:       ocommon.NewPoint(319, nil),
			BlockNumber: 320,
		},
		SyncTargetTrusted: true,
	})
	target, active = ls.UpstreamSyncStatus()
	assert.Equal(t, uint64(319), target)
	assert.True(t, active)
	upstreamTip, upstreamLive = ls.UpstreamSyncTip()
	assert.True(t, upstreamLive)
	assert.Equal(t, uint64(320), upstreamTip.BlockNumber)
	assert.Equal(t, uint64(319), upstreamTip.Point.Slot)

	// No live upstream -- (0, false).
	live = false
	target, active = ls.UpstreamSyncStatus()
	assert.Zero(t, target)
	assert.False(t, active)
	upstreamTip, upstreamLive = ls.UpstreamSyncTip()
	assert.False(t, upstreamLive)
	assert.Zero(t, upstreamTip.BlockNumber)
}

func TestSetForgedBlockChecker(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	assert.Nil(t, ls.config.ForgedBlockChecker)

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: {0x01},
		},
	}
	ls.SetForgedBlockChecker(checker)

	assert.NotNil(t, ls.config.ForgedBlockChecker)

	hash, ok := ls.config.ForgedBlockChecker.WasForgedByUs(1000)
	assert.True(t, ok)
	assert.Equal(t, []byte{0x01}, hash)
}

// newActiveSlotCoeffLedgerState builds a minimal LedgerState whose Shelley
// genesis carries the given activeSlotsCoeff JSON literal.
func newActiveSlotCoeffLedgerState(
	t *testing.T,
	coeffJSON string,
) *LedgerState {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": `+coeffJSON+`,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	return &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// TestActiveSlotCoeffRatIsExactGenesisRational pins that the leader-check
// coefficient accessor returns the genesis value exactly, and that the float64
// accessor does not.
//
// A Shelley genesis "activeSlotsCoeff": 0.05 decodes to exactly 1/20.
// ActiveSlotCoeff() divides the numerator and denominator as float64, and the
// nearest binary64 value to 0.05 is strictly GREATER than 1/20, so a threshold
// derived from it is strictly larger than the reference node's — a node using it
// can only over-claim leader slots, never miss any. That is the one-sided
// signature of the phantom leader slots seen in the field, so the direction
// is pinned here even though
// the magnitude (~5.6e-17 relative) is far too small to account for the three
// phantom slots per epoch reported there.
func TestActiveSlotCoeffRatIsExactGenesisRational(t *testing.T) {
	t.Parallel()

	ls := newActiveSlotCoeffLedgerState(t, "0.05")

	exact := ls.ActiveSlotCoeffRat()
	require.NotNil(t, exact)
	require.Equal(t, 0, exact.Cmp(big.NewRat(1, 20)),
		"ActiveSlotCoeffRat must return the genesis value exactly")

	approx := new(big.Rat).SetFloat64(ls.ActiveSlotCoeff())
	require.NotNil(t, approx)
	require.Equal(t, 1, approx.Cmp(exact),
		"the float64 accessor must be strictly greater than 1/20, which is "+
			"why the leader check must not use it")
}

// TestActiveSlotCoeffRatReturnsCopy proves callers cannot mutate shared genesis
// state through the returned pointer. big.Rat is mutable, and the leader
// schedule hands this value to the consensus package.
func TestActiveSlotCoeffRatReturnsCopy(t *testing.T) {
	t.Parallel()

	ls := newActiveSlotCoeffLedgerState(t, "0.05")

	first := ls.ActiveSlotCoeffRat()
	require.NotNil(t, first)
	first.SetInt64(7)

	second := ls.ActiveSlotCoeffRat()
	require.NotNil(t, second)
	require.Equal(t, 0, second.Cmp(big.NewRat(1, 20)),
		"mutating a returned coefficient must not corrupt the genesis value")
}

// TestActiveSlotCoeffRatWithoutGenesis returns nil rather than a degenerate
// zero value, so callers can fall back explicitly.
func TestActiveSlotCoeffRatWithoutGenesis(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	require.Nil(t, ls.ActiveSlotCoeffRat())
}

// pausingLedgerReadIterator is a ledgerReadIterator whose Next calls block
// waiting on resume immediately before returning the result at index
// pauseAtIndex, closing paused first so a test can observe that the call is
// in flight. It exists to put ledgerReadChainIterator's gather loop in a
// known, held-open state -- "already fetched some raw blocks, about to
// fetch/hand off more" -- so a concurrent goroutine can probe whether
// blockPipelineGatherMutex is held during that window.
type pausingLedgerReadIterator struct {
	ctx                 context.Context
	results             []*chain.ChainIteratorResult
	pauseAtIdx          int
	calls               int
	paused              chan struct{}
	resume              chan struct{}
	blockingNextStarted chan struct{}
}

func (p *pausingLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	idx := p.calls
	p.calls++
	if idx == p.pauseAtIdx {
		close(p.paused)
		<-p.resume
	}
	if idx < len(p.results) {
		return p.results[idx], nil
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	close(p.blockingNextStarted)
	// Mirror a real blocking iterator call: cancellation releases the call.
	// blockingNextStarted lets the test order cancellation after entry without
	// using a wall-clock delay as its sequencing mechanism.
	<-p.ctx.Done()
	return nil, p.ctx.Err()
}

// TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather verifies that
// rollback coordination holds the gather mutex across iterator gathering:
// drainBlockPipelineBeforeRollback only waits for work
// already Submitted to blockPipeline, so a rollback landing while
// ledgerReadChainIterator has already pulled raw blocks off the chain
// iterator into its local batch, but has not yet reached decodeReadChainBatch
// (Submit), would previously go unnoticed -- WaitForDrain sees nothing
// pending and returns immediately.
//
// blockPipelineGatherMutex closes that window by having the reader hold its
// read lock for the whole gather-then-submit span. This test proves the
// reader actually holds it there (not just after Submit): while the
// scripted iterator's second Next call is deliberately paused -- i.e. one
// raw block already gathered, mid-way through gathering the next -- a
// concurrent TryLock for the write side (the lock rollbackChainAndStateDeferred
// takes) must fail. Once the reader delivers its batch and the mutex is no
// longer needed, TryLock must succeed.
func TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather(t *testing.T) {
	t.Parallel()

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	iter := &pausingLedgerReadIterator{
		ctx: ctx,
		results: []*chain.ChainIteratorResult{
			{Point: point1, Block: block1},
			{Point: point2, Block: block2},
		},
		// Pause immediately before the second Next call, i.e. after the
		// first raw block has already been appended to the reader's
		// local batch and it is about to fetch more.
		pauseAtIdx:          1,
		paused:              make(chan struct{}),
		resume:              make(chan struct{}),
		blockingNextStarted: make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	resultCh := make(chan readChainResult)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.paused, testutil.AsyncWait,
		"reader never reached the paused mid-gather point",
	)

	// The reader is mid-gather with one raw block already collected. The
	// write-side lock rollbackChainAndStateDeferred takes must not be obtainable
	// right now.
	require.False(
		t,
		ls.blockPipelineGatherMutex.TryLock(),
		"blockPipelineGatherMutex.Lock() succeeded while the reader was "+
			"mid-gather -- a concurrent rollback could truncate the chain "+
			"while stale raw blocks are still about to be submitted",
	)

	close(iter.resume)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its gathered batch",
	)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 2)

	// The gather-plus-submit span has ended; the write lock must now be
	// obtainable.
	require.Eventually(t, func() bool {
		if ls.blockPipelineGatherMutex.TryLock() {
			ls.blockPipelineGatherMutex.Unlock()
			return true
		}
		return false
	}, testutil.AsyncWait, 5*time.Millisecond,
		"blockPipelineGatherMutex remained held after the batch was "+
			"delivered",
	)

	close(result.done)
	testutil.RequireReceive(
		t, iter.blockingNextStarted, testutil.AsyncWait,
		"reader never entered the blocking iterator call",
	)
	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

func TestBlockReferenceScriptLimitAdmission(t *testing.T) {
	for _, over := range []bool{false, true} {
		name := "at limit"
		if over {
			name = "over limit"
		}
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				0,
				bytes.Repeat([]byte{1}, 28),
				nil,
			)
			require.NoError(t, err)
			block := &conway.ConwayBlock{
				BlockHeader: &conway.ConwayBlockHeader{},
			}
			block.BlockHeader.Body.BlockNumber = 1
			block.BlockHeader.Body.Slot = 1
			block.BlockHeader.Body.ProtoVersion.Major = 11
			for i := range 6 {
				size := int(conway.MaxRefScriptSizePerBlock / 6)
				if i == 5 {
					size += int(conway.MaxRefScriptSizePerBlock % 6)
					if over {
						size++
					}
				}
				require.Less(t, uint64(size), conway.MaxRefScriptSizePerTx)
				txID := bytes.Repeat([]byte{byte(i + 1)}, 32)
				input := shelley.ShelleyTransactionInput{
					TxId: lcommon.NewBlake2b256(txID),
				}
				output := &babbage.BabbageTransactionOutput{
					OutputAddress: address,
					TxOutScriptRef: &lcommon.ScriptRef{
						Type:   lcommon.ScriptRefTypePlutusV3,
						Script: make(lcommon.PlutusV3Script, size),
					},
				}
				encoded, err := cbor.Encode(output)
				require.NoError(t, err)
				require.NoError(
					t,
					db.Transaction(true).Do(func(txn *database.Txn) error {
						if err := db.CreateUtxo(txn, &models.Utxo{TxId: txID, OutputIdx: 0, AddedSlot: 0}); err != nil {
							return err
						}
						return db.Blob().SetUtxo(txn.Blob(), txID, 0, encoded)
					}),
				)
				block.TransactionBodies = append(
					block.TransactionBodies,
					conway.ConwayTransactionBody{
						TxReferenceInputs: cbor.NewSetType(
							[]shelley.ShelleyTransactionInput{input},
							false,
						),
					},
				)
				block.TransactionWitnessSets = append(
					block.TransactionWitnessSets,
					conway.ConwayTransactionWitnessSet{},
				)
			}
			encodedBlock, err := cbor.EncodeGeneric(block)
			require.NoError(t, err)
			block.SetCbor(encodedBlock)
			bodySize, err := serializedBlockBodySize(block)
			require.NoError(t, err)
			block.BlockHeader.Body.BlockBodySize = bodySize
			encodedBlock, err = cbor.EncodeGeneric(block)
			require.NoError(t, err)
			block.SetCbor(encodedBlock)
			pp := &conway.ConwayProtocolParameters{
				MaxBlockBodySize:   100000,
				MaxBlockHeaderSize: 100000,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 11,
				},
			}
			sentinel := errors.New("transaction validator reached")
			era := eras.ConwayEraDesc
			era.ValidateTxFunc = func(lcommon.Transaction, uint64, lcommon.LedgerState, lcommon.ProtocolParameters) error {
				return sentinel
			}
			nodeConfig := newTestShelleyGenesisCfg(t)
			nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
			ls := &LedgerState{
				db:             db,
				activeEras:     []eras.EraDesc{era, eras.DijkstraEraDesc},
				currentEra:     era,
				currentPParams: pp,
				config: LedgerStateConfig{
					Logger:            testLogger(),
					CardanoNodeConfig: nodeConfig,
				},
			}
			ls.metrics.init(prometheus.NewRegistry())
			ls.publishSnapshotsLocked()
			for path, run := range map[string]func() error{
				"imported_previous_era": func() error {
					currentParams := &dijkstra.DijkstraProtocolParameters{
						ConwayProtocolParameters: *pp,
						MaxRefScriptSizePerBlock: 1,
					}
					currentParams.ProtocolVersion.Major = dijkstra.MinProtocolVersionDijkstra
					return db.Transaction(true).Do(func(txn *database.Txn) error {
						_, err := ls.ledgerProcessBlock(txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, eras.DijkstraEraDesc, currentParams, pp, 0, 0, false)
						return err
					})
				},
				"imported": func() error {
					return db.Transaction(true).Do(func(txn *database.Txn) error {
						_, err := ls.ledgerProcessBlock(txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, era, pp, nil, 0, 0, false)
						return err
					})
				},
				"forged": func() error { return ls.validateForgedTxs(block) },
			} {
				t.Run(path, func(t *testing.T) {
					err := run()
					if over {
						var limit lcommon.RefScriptSizePerBlockTooLargeError
						require.ErrorAs(
							t,
							err,
							&limit,
							"aggregate reference-script limit must reject before transaction validation",
						)
						require.Equal(
							t,
							conway.MaxRefScriptSizePerBlock+1,
							limit.BlockSize,
						)
					} else {
						require.ErrorIs(t, err, sentinel, "exact block limit must reach later transaction validation")
					}
				})
			}
		})
	}
}

const testByronGenesisJSON = `{
  "avvmDistr": {},
  "blockVersionData": {
    "heavyDelThd": "0", "maxBlockSize": "1",
    "maxHeaderSize": "1", "maxProposalSize": "1",
    "maxTxSize": "1", "mpcThd": "0", "scriptVersion": 0,
    "slotDuration": "20000",
    "softforkRule": {"initThd": "0", "minThd": "0", "thdDecrement": "0"},
    "txFeePolicy": {"multiplier": "0", "summand": "0"},
    "unlockStakeEpoch": "0", "updateImplicit": "0",
    "updateProposalThd": "0", "updateVoteThd": "0"
  },
  "protocolConsts": {"k": 432, "protocolMagic": 2},
  "startTime": 0, "bootStakeholders": {},
  "heavyDelegation": {}, "nonAvvmBalances": {}
}`

func testByronGenesisJSONForK(k uint64) string {
	return strings.Replace(
		testByronGenesisJSON,
		`"k": 432`,
		`"k": `+strconv.FormatUint(k, 10),
		1,
	)
}

func completeTestByronGenesisJSON(t testing.TB, input string) string {
	t.Helper()
	var base map[string]json.RawMessage
	var overrides map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(testByronGenesisJSON), &base))
	require.NoError(t, json.Unmarshal([]byte(input), &overrides))
	for key, value := range overrides {
		if key == "blockVersionData" || key == "protocolConsts" {
			var defaults map[string]json.RawMessage
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(base[key], &defaults))
			require.NoError(t, json.Unmarshal(value, &fields))
			maps.Copy(defaults, fields)
			merged, err := json.Marshal(defaults)
			require.NoError(t, err)
			base[key] = merged
			continue
		}
		base[key] = value
	}
	merged, err := json.Marshal(base)
	require.NoError(t, err)
	return string(merged)
}

// TestLedgerProcessBlockAllowsSyntheticByronBlocksWithPlaceholderCbor keeps
// structured test blocks out of the decoded-wire size-validation boundary.
// A placeholder Cbor value is not sufficient to apply Byron genesis limits;
// concrete gouroboros Byron blocks are covered by the envelope tests.
func TestLedgerProcessBlockAllowsSyntheticByronBlocksWithPlaceholderCbor(
	t *testing.T,
) {
	db := newTestDB(t)
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	block := &envelopeTestBlock{
		header: &envelopeTestHeader{
			cbor:   []byte{0x80},
			slot:   1,
			number: 1,
			era:    byron.EraByron,
		},
		cbor: []byte{0x82, 0x80, 0x80},
	}

	err := db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 1, Hash: block.Hash().Bytes()},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			nil,
			eras.ByronEraDesc,
			&shelley.ShelleyProtocolParameters{},
			nil,
			0,
			0,
			false,
		)
		return err
	})
	require.NoError(t, err)
}

// shrinkCleanupConsumedUtxosInterval makes the periodic cleanup timer fire on
// a test timescale. The package var is restored on cleanup, so these tests
// must not run in parallel with each other.
func shrinkCleanupConsumedUtxosInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := cleanupConsumedUtxosInterval
	cleanupConsumedUtxosInterval = d
	t.Cleanup(func() { cleanupConsumedUtxosInterval = prev })
}

// newCleanupTimerFireSignal returns a hook that reports each timer fire on the
// returned channel. The send is non-blocking so an unread fire never stalls
// the timer callback itself.
func newCleanupTimerFireSignal() (func(), <-chan struct{}) {
	fires := make(chan struct{}, 64)
	return func() {
		select {
		case fires <- struct{}{}:
		default:
		}
	}, fires
}

// drainCleanupTimerFires discards every fire already buffered. Close has
// stopped and drained the timer by the time this is called, so all of them
// happened before it returned; only a fire arriving afterwards is a defect.
// Draining fully -- rather than discarding a single fire -- is what keeps the
// absence assertion from depending on how many intervals elapsed while the
// test was between receives.
func drainCleanupTimerFires(fires <-chan struct{}) {
	for {
		select {
		case <-fires:
		default:
			return
		}
	}
}

// TestCleanupConsumedUtxos_TimerStopsOnClose covers a
// shutdown leak: the cleanup timer callback re-arms itself via
// scheduleCleanupConsumedUtxos, so a Close that does not stop it leaves a
// self-perpetuating timer running against a database its owner closes
// immediately after Close returns (LedgerState does not own the database --
// see the note at the end of Close).
// Not t.Parallel: shrinkCleanupConsumedUtxosInterval swaps the package-level
// cleanupConsumedUtxosInterval, which every concurrent LedgerState in this
// package would observe.
func TestCleanupConsumedUtxos_TimerStopsOnClose(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	ls.scheduleCleanupConsumedUtxos()
	// Two fires prove the callback re-armed itself at least once, so the
	// absence check below is measuring a stopped timer rather than one that
	// simply never started.
	testutil.RequireReceive(
		t, fires, testutil.AsyncWait,
		"cleanup timer must fire while the ledger state is open",
	)
	testutil.RequireReceive(
		t, fires, testutil.AsyncWait,
		"cleanup timer must re-arm itself while the ledger state is open",
	)

	require.NoError(t, ls.Close())

	drainCleanupTimerFires(fires)

	// 200ms is ~40 shrunken intervals. The assertion is that a stopped timer
	// fires zero times, not that a running one fires within a deadline, so
	// runner load cannot turn this into a flake.
	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"cleanup timer must not fire after Close returns",
	)
}

// TestCleanupConsumedUtxos_CloseWaitsForActiveCallback covers the drain half
// of the acceptance criteria. Stopping a time.Timer does not wait for an
// AfterFunc callback that has already started, so Close must join the
// in-flight run; otherwise it returns while cleanup is still issuing database
// work, and the owner closes the database out from under it.
func TestCleanupConsumedUtxos_CloseWaitsForActiveCallback(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	// The run hook, not the timer-fired hook: this must block inside the
	// region that has already registered with the drain, which is what Close
	// is required to wait for.
	ls.cleanupConsumedUtxosRunHook = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	ls.scheduleCleanupConsumedUtxos()
	testutil.RequireReceive(
		t, entered, testutil.AsyncWait,
		"cleanup timer callback must start before Close is called",
	)

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- ls.Close() }()

	testutil.RequireNoReceive(
		t, closeReturned, 200*time.Millisecond,
		"Close must not return while a cleanup callback is in flight",
	)

	close(release)
	err := testutil.RequireReceive(
		t, closeReturned, testutil.AsyncWait,
		"Close must return once the in-flight cleanup callback finishes",
	)
	require.NoError(t, err)
}

// TestCleanupConsumedUtxos_NoDatabaseWorkAfterClose covers the second
// acceptance criterion for the path that has no timer at all: the epoch
// transition fires cleanup as a bare `go ls.cleanupConsumedUtxos()`
// (state.go), which can lose the race with shutdown. Stopping the timer alone
// does not constrain that goroutine.
//
// TestCleanupConsumedUtxos_CoreModePrunes is the positive control: the same
// seeded row and tip are deleted by the same call on an open ledger state, so
// a passing result here cannot come from cleanup being inert.
func TestCleanupConsumedUtxos_NoDatabaseWorkAfterClose(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xC5}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	require.NoError(t, ls.Close())

	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(
		t, post,
		"cleanup must not begin database work after Close returns",
	)
}

// TestCleanupConsumedUtxos_RepeatedCloseIsSafe covers the repeated-close half
// of the third acceptance criterion. A drain built on sync.WaitGroup is easy
// to get wrong on the second call, and Close is genuinely called twice on the
// live restore/truncate path.
func TestCleanupConsumedUtxos_RepeatedCloseIsSafe(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	ls.scheduleCleanupConsumedUtxos()
	testutil.RequireReceive(
		t, fires, testutil.AsyncWait,
		"cleanup timer must fire while the ledger state is open",
	)

	require.NoError(t, ls.Close())
	require.NoError(t, ls.Close(), "repeated Close must remain a no-op")

	drainCleanupTimerFires(fires)
	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"cleanup timer must stay stopped across repeated Close calls",
	)
}

// TestCleanupConsumedUtxos_ScheduleAfterCloseDoesNotArm covers the re-arm
// window directly: the timer callback calls scheduleCleanupConsumedUtxos
// after running cleanup, so a callback that was already in flight when Close
// stopped the timer would otherwise install a fresh one behind Close's back.
func TestCleanupConsumedUtxos_ScheduleAfterCloseDoesNotArm(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	require.NoError(t, ls.Close())
	ls.scheduleCleanupConsumedUtxos()

	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"scheduling cleanup after Close must not arm a timer",
	)
}

// newTestDBForCleanup builds an in-memory Database in the requested storage
// mode. An empty mode defaults to core (per Database.New).
func newTestDBForCleanup(t *testing.T, mode string) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     "",
		StorageMode: mode,
	})
	require.NoError(t, err)
	return db
}

// seedSpentUtxoForCleanup writes a single spent UTxO row directly. The
// row is "consumed at deletedSlot but still well within the periodic
// cleanup eligibility window" — matching what a normal block application
// would produce after the consumed input was soft-marked.
func seedSpentUtxoForCleanup(
	t *testing.T,
	db *database.Database,
	txId []byte,
	outputIdx uint32,
	addedSlot, deletedSlot uint64,
) {
	t.Helper()
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		return db.CreateUtxo(txn, &models.Utxo{
			TxId:        txId,
			OutputIdx:   outputIdx,
			AddedSlot:   addedSlot,
			DeletedSlot: deletedSlot,
			Amount:      types.Uint64(1),
		})
	}))
}

// newLedgerStateForCleanup wires the minimum surface area
// cleanupConsumedUtxos needs: db, currentTip, currentEra, and a logger.
// CardanoNodeConfig is intentionally left nil so
// calculateStabilityWindowForEra returns the default
// (blockfetchBatchSlotThresholdDefault = 50000); the tip slot is then
// chosen well past that window so consumed-UTxO cleanup is eligible to
// run in core mode. The upstream tip is initialized to the local tip so the
// test represents a node that is near the network tip.
func newLedgerStateForCleanup(
	db *database.Database,
	tipSlot uint64,
) *LedgerState {
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, nil),
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.syncUpstreamTipSlot.Store(tipSlot)
	return ls
}

// TestCleanupConsumedUtxos_CoreModePrunes asserts the pre-existing
// invariant that core mode hard-deletes consumed UTxO rows once the
// stability window has passed. Without this baseline, the API-mode
// retention test below could pass by accident if the cleanup loop were
// silently dead for both modes.
func TestCleanupConsumedUtxos_CoreModePrunes(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xA1}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	pre, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, pre, "seed must succeed before cleanup")

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(
		t, post,
		"core mode must hard-delete consumed UTxO rows after stability "+
			"window",
	)
}

// TestCleanupConsumedUtxos_PersistsPruneFloor covers the durable marker
// checkUtxoRetentionWindow relies on (ledger/queries.go): every run that
// actually prunes must durably record the floor it used, so a later pin
// check can reject against it even if the tip subsequently moves in a way
// that would otherwise make a freshly-computed floor look more lenient
// ( review -- see persistConsumedUtxoPruneFloor's doc
// comment for the rollback and era-transition cases this closes).
func TestCleanupConsumedUtxos_PersistsPruneFloor(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	const tipSlot = 100_000 // > 50_000 default stability window

	ls := newLedgerStateForCleanup(db, tipSlot)

	before, err := ls.readConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	assert.Zero(t, before, "no floor before cleanup has ever run")

	ls.cleanupConsumedUtxos()

	after, err := ls.readConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	assert.Equal(
		t, uint64(50_000), after,
		"must persist tipSlot minus the default stability window",
	)
}

func TestCleanupConsumedUtxos_DoesNotWaitForChainsyncMutex(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()

	done := make(chan struct{})
	go func() {
		ls.cleanupConsumedUtxos()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"consumed UTxO cleanup must not acquire chainsyncMutex",
	)
}

// TestCleanupConsumedUtxos_SkipsDeleteWhenPruneFloorPersistFails covers the
// ordering invariant persistConsumedUtxoPruneFloor's doc comment depends on:
// a failure to durably record the floor must abort this run before any row
// is actually hard-deleted, not just be logged and ignored. Otherwise real
// rows could be pruned with no durable record that floor was ever used,
// letting a later pinned query at or above that floor see no persisted
// floor to reject against and silently answer "absent" for a ref that was
// actually there. The test drops the
// sync_state table (via a raw connection to the same file) so SetSyncState
// fails while the utxo table -- and so UtxosDeleteConsumed -- stays fully
// functional, isolating the failure to exactly the call this test cares
// about.
func TestCleanupConsumedUtxos_SkipsDeleteWhenPruneFloorPersistFails(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeCore,
	})
	require.NoError(t, err)

	txId := bytes.Repeat([]byte{0xD2}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DROP TABLE sync_state")
	require.NoError(t, err)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(
		t, post,
		"cleanup must not delete rows when persisting the prune floor "+
			"fails, since that would prune without any durable record "+
			"that pruning happened",
	)
}

func TestCleanupConsumedUtxos_ProcessesOneBoundedBatch(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		for idx := 0; idx <= cleanupConsumedUtxoBatchSize; idx++ {
			txID := bytes.Repeat([]byte{0}, 32)
			binary.BigEndian.PutUint32(txID[:4], uint32(idx+1))
			if err := db.CreateUtxo(txn, &models.Utxo{
				TxId:        txID,
				OutputIdx:   0,
				AddedSlot:   1_000,
				DeletedSlot: 5_000,
				Amount:      types.Uint64(1),
			}); err != nil {
				return err
			}
		}
		return nil
	}))

	ls := newLedgerStateForCleanup(db, 100_000)
	ls.cleanupConsumedUtxos()

	remaining, err := db.Metadata().GetUtxosDeletedBeforeSlot(
		50_000,
		cleanupConsumedUtxoBatchSize+1,
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, remaining, 1, "one eligible row must remain for a later run")

	ls.cleanupConsumedUtxos()
	remaining, err = db.Metadata().GetUtxosDeletedBeforeSlot(
		50_000,
		cleanupConsumedUtxoBatchSize+1,
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, remaining, "the next run must resume the bounded cleanup")
}

func TestCleanupConsumedUtxos_DefersDuringCatchup(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xA3}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000
		upstreamTip uint64 = 200_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.syncUpstreamTipSlot.Store(upstreamTip)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, post, "cleanup must defer while the ledger is catching up")
}

// TestCleanupConsumedUtxos_RunsWithoutKnownUpstreamTip covers the
// distinction the catch-up deferral has to make: an upstream tip of 0 means
// unknown, not "infinitely far behind". A node that has never connected to a
// peer -- or that lost its last active connection, which zeroes the value in
// chainsync.go -- would otherwise defer cleanup for as long as it stays
// peerless, growing the utxo table without bound in core mode, and silently:
// no error, no crash. Cleanup ran off the local tip alone before the deferral
// existed, so that is the behavior an unknown upstream tip falls back to.
func TestCleanupConsumedUtxos_RunsWithoutKnownUpstreamTip(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xB4}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		// Far past the 50_000 default stability window, so the only
		// reason to retain the row would be the deferral itself.
		tipSlot uint64 = 10_000_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	pre, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, pre, "seed must succeed before cleanup")

	ls := newLedgerStateForCleanup(db, tipSlot)
	// No peer has ever reported a tip.
	ls.syncUpstreamTipSlot.Store(0)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(
		t, post,
		"an unknown upstream tip must not defer cleanup: the local tip "+
			"is already far past the stability window",
	)
}

// TestCleanupConsumedUtxos_APIModeRetains covers API
// storage mode: the periodic cleanup must leave
// spent UTxO metadata rows in place so historical transaction queries
// can resolve input / collateral / reference-input associations via
// spent_at_tx_id, collateral_by_tx_id, and referenced_by_tx_id.
func TestCleanupConsumedUtxos_APIModeRetains(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeAPI)
	txId := bytes.Repeat([]byte{0xA2}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, post,
		"API mode must retain spent UTxO row past the cleanup threshold "+
			"so historical transaction queries can still resolve input / "+
			"collateral / reference-input associations",
	)
	assert.Equal(
		t, deletedSlot, post.DeletedSlot,
		"retained row must keep deleted_slot as the spent-state encoding",
	)

	// Live-UTxO queries must still filter the retained row out: it has
	// a non-zero deleted_slot so it is no longer part of the active set.
	live, err := db.Metadata().GetUtxo(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, live,
		"live UTxO view must continue to exclude spent rows in API mode")
}

// TestDrepRegistrationReportsAbsenceWithoutAPublishedSnapshot covers the other
// way the fallback has nothing to report: no consensus snapshot has been
// published yet, so there are no current parameters to read. Kept separate
// from the pre-Conway case because newStakeRefundTestView reaches this branch
// and never the era one -- the era field it sets is never read.
func TestDrepRegistrationReportsAbsenceWithoutAPublishedSnapshot(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	require.Nil(
		t,
		lv.ls.loadConsensusSnapshot(),
		"this fixture must leave the snapshot unpublished for this test to mean what it says",
	)
	cred := drepRefundTestCredential(0xe8)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(t, reg.Deposit)

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

func (r *epochBoundaryPhaseRecorder) Enabled(
	context.Context,
	slog.Level,
) bool {
	return true
}

func (r *epochBoundaryPhaseRecorder) Handle(
	_ context.Context,
	rec slog.Record,
) error {
	if rec.Message != "epoch rollover phase" {
		return nil
	}
	var name string
	var seconds float64
	rec.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "phase":
			name = a.Value.String()
		case "duration_seconds":
			seconds = a.Value.Float64()
		}
		return true
	})
	r.mu.Lock()
	r.phases = append(r.phases, epochBoundaryPhase{
		name:     name,
		duration: time.Duration(seconds * float64(time.Second)),
	})
	r.mu.Unlock()
	return nil
}

func (r *epochBoundaryPhaseRecorder) WithAttrs([]slog.Attr) slog.Handler {
	return r
}

func (r *epochBoundaryPhaseRecorder) WithGroup(string) slog.Handler {
	return r
}

// TestHealEmptyLabNoncesFoldsExtraEntropy covers the startup lab-recovery path,
// which recomputes a stored epoch's nonce from chain data. That epoch is in the
// past, so its extraEntropy comes from the protocol parameters recorded for it
// rather than from a forecast.
func TestHealEmptyLabNoncesFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	const (
		entropyEpoch uint64 = 259
		prevEpoch    uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	candidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	boundaryHash := mustDecodeHex(t, mainnetEpoch259Nonce)
	boundaryPrevHash := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     mary.BlockTypeMary,
	}, nil))

	_, entropyCbor := maryPParamsWithExtraEntropy(t, entropy)
	require.NoError(t, db.SetPParams(
		entropyCbor, 200, entropyEpoch, eras.MaryEraDesc.Id, nil,
	))

	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 150,
		epochCache: []models.Epoch{
			{
				EpochId:             prevEpoch,
				StartSlot:           100,
				LengthInSlots:       100,
				EraId:               eras.MaryEraDesc.Id,
				Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
				CandidateNonce:      mustDecodeHex(t, mainnetEpoch259Nonce),
				LastEpochBlockNonce: carriedLab,
			},
			{
				EpochId:        entropyEpoch,
				StartSlot:      200,
				LengthInSlots:  100,
				EraId:          eras.MaryEraDesc.Id,
				CandidateNonce: candidate,
				// NeutralNonce-collapsed (wrong) nonce: eta == candidateNonce.
				Nonce:               append([]byte(nil), candidate...),
				LastEpochBlockNonce: nil, // corrupted: empty lab
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	withoutEntropy, err := lcommon.CalculateEpochNonce(
		candidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.NotEqual(
		t,
		withoutEntropy.Bytes(),
		ls.epochCache[1].Nonce,
		"recomputed epoch nonce must not be the extraEntropy-free value",
	)
	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(ls.epochCache[1].Nonce),
		"recomputed epoch nonce must fold the epoch's recorded extraEntropy",
	)
}

func TestEraTransitionPathAllowsPrimeBoundaryPair(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	path, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		eras.BabbageEraDesc.Id,
		true,
	)
	require.True(t, ok)
	require.Equal(
		t,
		[]uint{eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id},
		path,
	)
}

func TestEraTransitionPathRejectsLargerJump(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	path, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		eras.ConwayEraDesc.Id,
		true,
	)
	require.False(t, ok)
	require.Nil(t, path)
}

func TestBoundaryEraForBlockUsesSuccessorHeaderEra(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		7,
		true,
	)
	require.Equal(t, eras.BabbageEraDesc.Id, target)
	require.True(t, allowTwoTransitions)
}

func TestBoundaryEraForBlockDoesNotAdvanceFromHeaderAlone(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.AlonzoEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		eras.BabbageEraDesc.MinMajorVersion,
		true,
	)
	require.Equal(
		t,
		eras.AlonzoEraDesc.Id,
		target,
		"an Alonzo block remains Alonzo even when its header advertises protocol major 7",
	)
	require.False(t, allowTwoTransitions)
}

func TestBoundaryEraForBlockRejectsNonAdjacentHeaderEra(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		eras.ConwayEraDesc.MinMajorVersion,
		true,
	)
	require.Equal(t, eras.AlonzoEraDesc.Id, target)
	require.False(t, allowTwoTransitions)
}

func TestEraAdvancementRejectsRawTwoStepBodyJumpWithoutHeaderElevation(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.BabbageEraDesc.Id,
		eras.BabbageEraDesc.MinMajorVersion,
		true,
	)
	require.Equal(t, eras.BabbageEraDesc.Id, target)
	require.False(t, allowTwoTransitions)

	_, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		target,
		allowTwoTransitions,
	)
	require.False(
		t,
		ok,
		"a raw two-era body jump must not skip the omitted era",
	)
}

// newBoundaryRolloverLedger builds a LedgerState positioned at the end of a
// Shelley epoch, with the persisted epoch record the rollover needs. The
// returned pparams carry Shelley's protocol major, so a snapshot captured
// before a boundary's era transitions records a different major than one
// captured after them.
func newBoundaryRolloverLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()

	const shelleyGenesisJSON = `{
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

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

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
		currentEpoch.EraId, currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))

	rat := func() *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(1, 2)} }
	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ShelleyEraDesc,
		currentEpoch: currentEpoch,
		currentPParams: &shelley.ShelleyProtocolParameters{
			ProtocolMajor:    shelley.MinProtocolVersionShelley,
			MinFeeA:          44,
			A0:               rat(),
			Rho:              rat(),
			Tau:              rat(),
			Decentralization: rat(),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return ls, db
}

// TestBoundaryEraTransitionsSnapshotRecordsFinalProtocolVersion drives a
// two-era boundary the way ledgerProcessBlocksFromSource does: the rollover
// runs first so source-era pparam updates are enacted, then the remaining era
// transitions are applied. The authoritative mark snapshot must be captured
// once, after those transitions, so its protocol version is the one the new
// epoch actually runs at. Capturing it at the end of the rollover records the
// source era's major instead, and that value is durable.
func TestBoundaryEraTransitionsSnapshotRecordsFinalProtocolVersion(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newBoundaryRolloverLedger(t)

	var captures []event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			captures = append(captures, evt)
			return nil
		},
	)

	transitionPath, ok := ls.eraTransitionPath(
		eras.ShelleyEraDesc.Id,
		eras.MaryEraDesc.Id,
		true,
	)
	require.True(t, ok)
	require.Len(t, transitionPath, 2)

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			ls.currentEra,
			ls.currentPParams,
			true,
		)
		if err != nil {
			return err
		}
		require.True(t, result.BoundarySnapshotDeferred,
			"a multi-era boundary must defer the mark snapshot capture")
		require.Empty(t, captures,
			"the rollover must not capture the mark snapshot before the "+
				"boundary's era transitions have run")

		transitions, err := ls.applyBoundaryEraTransitions(
			txn, ls.currentEpoch, transitionPath, result,
		)
		if err != nil {
			return err
		}
		require.Len(t, transitions, 2)
		return nil
	}))

	if result == nil {
		t.Fatal("epoch rollover returned no result")
	}
	require.Len(t, captures, 1,
		"the deferred capture must run exactly once, not be re-run")
	require.Equal(
		t,
		uint(mary.MinProtocolVersionMary),
		captures[0].ProtocolVersion,
		"the mark snapshot must record the protocol major of the era the "+
			"new epoch runs at, not the era the rollover started in",
	)
	require.Equal(t, eras.MaryEraDesc.Id, result.NewCurrentEra.Id)
	require.Equal(t, eras.MaryEraDesc.Id, result.NewCurrentEpoch.EraId)
	require.False(t, result.BoundarySnapshotDeferred,
		"the deferred capture must be marked as taken")

	// The event the caller publishes after commit is built from the same
	// result, so the durable row and the event must agree.
	require.Equal(
		t,
		captures[0].ProtocolVersion,
		ls.protocolMajorForEvent(
			result.NewCurrentPParams, result.NewCurrentEra,
		),
	)
}

func TestBoundaryEraTransitionUsesTargetEraTiming(t *testing.T) {
	t.Parallel()

	ls, db := newBoundaryRolloverLedger(t)

	sourceEra := ls.currentEra
	sourceEra.EpochLengthFunc = func(
		*cardano.CardanoNodeConfig,
	) (uint, uint, error) {
		return 20_000, 21_600, nil
	}
	ls.currentEra = sourceEra

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			sourceEra,
			ls.currentPParams,
			true,
		)
		if err != nil {
			return err
		}
		_, err = ls.applyBoundaryEraTransitions(
			txn,
			ls.currentEpoch,
			[]uint{eras.AllegraEraDesc.Id},
			result,
		)
		return err
	}))
	if result == nil {
		t.Fatal("epoch rollover returned no result")
	}

	wantSlotLength, wantEpochLength, err := eras.AllegraEraDesc.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	require.NoError(t, err)
	require.Equal(t, wantSlotLength, result.NewCurrentEpoch.SlotLength)
	require.Equal(t, wantEpochLength, result.NewCurrentEpoch.LengthInSlots)
	require.Equal(t, wantSlotLength, result.SchedulerIntervalMs)

	var cachedEpoch *models.Epoch
	for i := range result.NewEpochCache {
		if result.NewEpochCache[i].EpochId == result.NewCurrentEpoch.EpochId {
			cachedEpoch = &result.NewEpochCache[i]
			break
		}
	}
	require.NotNil(t, cachedEpoch)
	require.Equal(t, wantSlotLength, cachedEpoch.SlotLength)
	require.Equal(t, wantEpochLength, cachedEpoch.LengthInSlots)

	persistedEpoch, err := db.GetEpoch(result.NewCurrentEpoch.EpochId, nil)
	require.NoError(t, err)
	require.NotNil(t, persistedEpoch)
	require.Equal(t, wantSlotLength, persistedEpoch.SlotLength)
	require.Equal(t, wantEpochLength, persistedEpoch.LengthInSlots)
}

// TestSingleEraBoundaryRolloverCapturesSnapshotInRollover covers the common
// path: with no era transitions deferred, the rollover still captures the mark
// snapshot itself, at its own era's protocol version.
func TestSingleEraBoundaryRolloverCapturesSnapshotInRollover(t *testing.T) {
	t.Parallel()

	ls, db := newBoundaryRolloverLedger(t)

	var captures []event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			captures = append(captures, evt)
			return nil
		},
	)

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			ls.currentEra,
			ls.currentPParams,
			false,
		)
		return err
	}))

	if result == nil {
		t.Fatal("epoch rollover returned no result")
	}
	require.False(t, result.BoundarySnapshotDeferred)
	require.Len(t, captures, 1)
	require.Equal(
		t,
		uint(shelley.MinProtocolVersionShelley),
		captures[0].ProtocolVersion,
	)
	require.Equal(t, eras.ShelleyEraDesc.Id, result.NewCurrentEra.Id)
}

func TestProtocolParamsForSlot_UnavailableShape(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		cfg    *cardano.CardanoNodeConfig
		era    eras.EraDesc
		params lcommon.ProtocolParameters
	}{
		{
			name: "missing config", era: eras.ShelleyEraDesc,
			params: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		},
		{
			name: "invalid config", cfg: &cardano.CardanoNodeConfig{},
			era:    eras.ShelleyEraDesc,
			params: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		},
		{
			name: "current era unavailable", cfg: newAllegraAtEpoch1Cfg(t),
			era: eras.DijkstraEraDesc, params: &dijkstra.DijkstraProtocolParameters{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{
				currentEra: tc.era,
				currentEpoch: models.Epoch{
					EpochId: 0, StartSlot: 0, LengthInSlots: 75, EraId: tc.era.Id,
				},
				currentPParams: tc.params,
				config:         LedgerStateConfig{CardanoNodeConfig: tc.cfg},
			}
			ls.publishSnapshotsLocked()
			require.Same(t, tc.params, ls.ProtocolParamsForSlot(74),
				"current-epoch parameters do not require a forecast")
			require.Nil(t, ls.ProtocolParamsForSlot(75),
				"a future epoch with unavailable shape must not use current parameters")
		})
	}
	// The existing boundary test exercises a valid scheduled Shelley-to-Allegra
	// forecast; unavailable-shape handling must retain that path.
}

func TestProtocolParamsForSlot_UnavailableTransition(t *testing.T) {
	t.Parallel()

	for _, missingSuccessor := range []bool{false, true} {
		name := "hard fork error"
		if missingSuccessor {
			name = "missing successor"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{
				currentEra: eras.ShelleyEraDesc,
				currentEpoch: models.Epoch{
					EpochId: 0, StartSlot: 0, LengthInSlots: 75,
					EraId: eras.ShelleyEraDesc.Id,
				},
				currentPParams: &babbage.BabbageProtocolParameters{},
				config: LedgerStateConfig{
					CardanoNodeConfig: newAllegraAtEpoch1Cfg(t),
					Logger:            slog.New(slog.DiscardHandler),
				},
			}
			if missingSuccessor {
				ls.currentPParams = &shelley.ShelleyProtocolParameters{ProtocolMajor: 2}
				ls.activeEras = []eras.EraDesc{eras.ShelleyEraDesc}
			}
			ls.publishSnapshotsLocked()
			require.Same(t, ls.currentPParams, ls.ProtocolParamsForSlot(74))
			require.Nil(t, ls.ProtocolParamsForSlot(75),
				"an unresolved scheduled transition must not return pre-fork parameters")
		})
	}
}

func TestProtocolParamsForSlot_PendingUpdateFailure(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	// A selected but undecodable proposal is an error, not an absent update.
	require.NoError(t, db.SetPParamUpdate([]byte{0xaa}, []byte{0xff}, 50, 0, nil))
	pparams := &shelley.ShelleyProtocolParameters{ProtocolMajor: 2}
	ls := &LedgerState{
		db: db, currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId: 0, StartSlot: 0, LengthInSlots: 100,
			EraId: eras.ShelleyEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: newShelleyUpdateQuorum1Cfg(t),
			Logger:            slog.New(slog.DiscardHandler),
		},
	}
	ls.publishSnapshotsLocked()
	require.Same(t, pparams, ls.ProtocolParamsForSlot(99))
	require.Nil(t, ls.ProtocolParamsForSlot(100),
		"a failed pending update must not return stale parameters")
}

// shelleyOnlyGenesisCfg returns a config with a Shelley genesis and no Byron
// genesis. ByronGenesisFile is optional (config/cardano/node.go loads it only
// when non-empty) and a Shelley-only config without it is a supported shape
// (see the setEpochCache era-start comment in state.go), but
// eras.BuildShapeForEras still builds Byron era params for every config, so no
// hard-fork shape -- and therefore no forecast -- can be built from one.
func shelleyOnlyGenesisCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))
	require.Nil(t, cfg.ByronGenesis())
	return cfg
}

// TestConsensusModeForEpoch_UnresolvableShapeFailsClosed pins the
// forward-looking era walk as fail-closed. An unavailable shape breaks the
// walk, and answering with the CURRENT era's mode for a future epoch reports
// exactly what a scheduled hard fork changes. The control fixes Babbage at
// epoch 501 under the same current era (Alonzo, TPraos), so the two cases
// differ by mode and not merely by error: with the shape resolvable the
// forecast is CPraos.
func TestConsensusModeForEpoch_UnresolvableShapeFailsClosed(t *testing.T) {
	t.Parallel()

	newLedger := func(cfg *cardano.CardanoNodeConfig) *LedgerState {
		enabled := true
		cfg.ExperimentalHardForksEnabled = &enabled
		babbage := uint64(501)
		cfg.TestBabbageHardForkAtEpoch = &babbage
		ls := &LedgerState{
			epochCache: []models.Epoch{{
				EpochId:       500,
				StartSlot:     100_000,
				SlotLength:    1_000,
				LengthInSlots: 432_000,
				EraId:         eras.AlonzoEraDesc.Id,
			}},
			currentEra: eras.AlonzoEraDesc,
			currentEpoch: models.Epoch{
				EpochId:       500,
				StartSlot:     100_000,
				LengthInSlots: 432_000,
			},
			currentTip: ochainsync.Tip{
				Point: ocommon.NewPoint(200_000, []byte("tip")),
			},
			config: LedgerStateConfig{CardanoNodeConfig: cfg},
		}
		ls.publishSnapshotsLocked()
		return ls
	}

	// Control: with a resolvable shape the walk crosses the scheduled
	// Babbage boundary and reports CPraos for the future epoch.
	control := newLedger(newTestEraHistoryCfg(t))
	mode, err := control.ConsensusModeForEpoch(600)
	require.NoError(t, err)
	assert.Equal(t, consensus.ConsensusModeCPraos, mode,
		"the scheduled Babbage fork must be reflected in the forecast")

	broken := newLedger(shelleyOnlyGenesisCfg(t))
	_, shapeErr := broken.eraShapeWithError()
	require.Error(t, shapeErr, "the premise: no shape can be built")

	// An epoch the cache already covers is not a forecast and still answers.
	mode, err = broken.ConsensusModeForEpoch(500)
	require.NoError(t, err)
	assert.Equal(t, consensus.ConsensusModeTPraos, mode)

	// The current epoch and earlier read applied state and still answer.
	mode, err = broken.ConsensusModeForEpoch(499)
	require.NoError(t, err)
	assert.Equal(t, consensus.ConsensusModeTPraos, mode)

	// The future epoch needs the walk, so it must fail closed instead of
	// reporting Alonzo's TPraos across the scheduled Babbage boundary.
	_, err = broken.ConsensusModeForEpoch(600)
	require.Error(t, err,
		"a future-epoch consensus mode must fail closed without a shape")
}

func TestEmptyGenesisCommitteeValidation(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = map[string]int{}
	require.NoError(t, ls.createGenesisBlock())
	require.Zero(t, committeeMemberRowCount(t, db))
	pp := &conway.ConwayProtocolParameters{}
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()
	lv := ls.NewView(nil)

	for _, tag := range []uint{
		lcommon.CredentialTypeAddrKeyHash,
		lcommon.CredentialTypeScriptHash,
	} {
		cold := committeeTestCredential(0xe1)
		cold.CredType = tag
		hot := committeeTestCredential(0xe2)
		hot.CredType = tag
		certs := []lcommon.Certificate{
			&lcommon.AuthCommitteeHotCertificate{
				CertType:       uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: cold,
				HotCredential:  hot,
			},
			&lcommon.ResignCommitteeColdCertificate{
				CertType: uint(
					lcommon.CertificateTypeResignCommitteeCold,
				),
				ColdCredential: cold,
			},
		}
		for _, cert := range certs {
			t.Run(
				fmt.Sprintf("tag %d/%s", tag, certificateName(cert)),
				func(t *testing.T) {
					tx := &conway.ConwayTransaction{
						TxIsValid: true,
						Body: conway.ConwayTransactionBody{
							TxCertificates: []lcommon.CertificateWrapper{{
								Type: cert.Type(), Certificate: cert,
							}},
						},
					}
					err := eras.ValidateTxConway(tx, 0, lv, pp)
					var notMember conway.NotCommitteeMemberError
					require.ErrorAs(t, err, &notMember)
					require.Equal(t, cold.Credential, notMember.Credential)
					require.Equal(t, certificateName(cert), notMember.Operation)
				},
			)
		}

		voterType := uint8(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash)
		if tag == lcommon.CredentialTypeScriptHash {
			voterType = lcommon.VoterTypeConstitutionalCommitteeHotScriptHash
		}
		voter := &lcommon.Voter{Type: voterType, Hash: hot.Credential}
		tx := &conway.ConwayTransaction{
			TxIsValid: true,
			Body: conway.ConwayTransactionBody{
				TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
			},
		}
		t.Run(fmt.Sprintf("tag %d/vote", tag), func(t *testing.T) {
			err := eras.ValidateTxConway(tx, 0, lv, pp)
			var unknown conway.UnknownVoterError
			require.ErrorAs(t, err, &unknown)
			require.Equal(t, *voter, unknown.Voter)
		})

		// GOVCERT permits a proposed member even when no committee is seated.
		storeCommitteeUpdateProposal(t, db, byte(0xe3+tag), cold, 90)
		for _, cert := range certs {
			tx := &conway.ConwayTransaction{
				TxIsValid: true,
				Body: conway.ConwayTransactionBody{
					TxCertificates: []lcommon.CertificateWrapper{{
						Type: cert.Type(), Certificate: cert,
					}},
				},
			}
			err := eras.ValidateTxConway(tx, 0, lv, pp)
			var notMember conway.NotCommitteeMemberError
			require.False(t, errors.As(err, &notMember), "%v", err)
			var lookup conway.CommitteeMemberLookupError
			require.False(t, errors.As(err, &lookup), "%v", err)
		}
	}
}

func TestEmptyGenesisCommitteeReferenceResignation(t *testing.T) {
	t.Parallel()

	root, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err)
	vector, err := conformance.DecodeTestVector(filepath.Join(
		root,
		"eras",
		"conway",
		"impl",
		"dump",
		"Conway.Imp.ConwayImpSpec_-_Version_10.GOVCERT.fails_for.resigning_a_nonexistent_CC_member_hotkey",
		"1",
	))
	require.NoError(t, err)
	initial, err := conformance.ParseInitialState(vector.InitialState)
	require.NoError(t, err)
	require.Len(t, vector.Events, 1)
	event := vector.Events[0]
	require.Equal(t, conformance.EventTypeTransaction, event.Type)
	require.False(t, event.Success)
	tx, err := conway.NewConwayTransactionFromCbor(event.TxBytes)
	require.NoError(t, err)
	require.Len(t, tx.Certificates(), 1)
	cert, ok := tx.Certificates()[0].(*lcommon.ResignCommitteeColdCertificate)
	require.True(t, ok)
	for credential := range initial.CommitteeMembersByCredential {
		require.NotEqual(
			t,
			cert.ColdCredential.Credential,
			credential.Credential,
		)
	}
	pp, err := conformance.NewPParamsLoaderFromTestdata(root).
		LoadForVector(vector, initial)
	require.NoError(t, err)

	ls, _ := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = map[string]int{}
	require.NoError(t, ls.createGenesisBlock())
	ls.currentPParams = pp
	ls.currentEpoch = models.Epoch{EpochId: initial.CurrentEpoch}
	ls.publishSnapshotsLocked()
	// The reference rejects this unknown cold credential with a seated
	// committee. Removing that committee cannot make the credential eligible.
	// Check that same GOVCERT error with the production empty-genesis view;
	// this is a rule-level projection, not a replay of the full vector state.
	err = eras.ValidateTxConway(tx, event.Slot, ls.NewView(nil), pp)
	var notMember conway.NotCommitteeMemberError
	require.ErrorAs(t, err, &notMember)
	require.Equal(t, cert.ColdCredential.Credential, notMember.Credential)
	require.Equal(t, "resign", notMember.Operation)
}

// hardForkRatifyFixture reproduces the exact Preview Plomin hard-fork
// incident at a real epoch-rollover level: a HardForkInitiation
// proposal, 49 SPO votes' worth of yes/no stake collapsed into two pools
// carrying the real observed mark[740]/mark[741]/mark[742] ratios
// (0.4779/0.4757/0.6283), and a single seated CC member voting yes with a
// 1/1 quorum -- matching the live incident's committee_member/
// auth_committee_hot state. Still at protocol major 9 (bootstrap), so the
// DRep gate is bypassed entirely and only the SPO gate governs ratification,
// exactly as it did on the real network.
type hardForkRatifyFixture struct {
	ls       *LedgerState
	db       *database.Database
	proposal *models.GovernanceProposal
	pparams  *conway.ConwayProtocolParameters
}

const (
	hfrYesPool    = "hfr-yes-pool-2222222222222"
	hfrSilentPool = "hfr-silent-pool-11111111111"
)

func newHardForkRatifyFixture(t *testing.T) *hardForkRatifyFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	// currentEpoch is epoch 741, already on disk; the fixture's first
	// rollover call transitions the boundary into 742.
	currentEpoch := newTestEpoch(741, 74_100, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))

	// PV9: still bootstrap, the real incident's protocol version.
	pparams := donationTestConwayPParams(9)
	pparams.MinCommitteeSize = 1

	// mark[740]/mark[741]/mark[742], the exact ratios measured on
	// Preview. Yes stake is hfrYesPool's explicit Yes vote; the remainder is
	// hfrSilentPool, which casts no vote at all -- HardForkInitiation always
	// keeps a silent pool's stake in the active denominator as implicit No
	// (tallySPOVotes), matching cardano-ledger's checkDisallowedVotes-adjacent
	// SPO semantics for this action type.
	for _, row := range []struct {
		epoch    uint64
		yesStake uint64
	}{
		{740, 4_779}, // ratio 0.4779, below the 0.51 threshold
		{741, 4_757}, // ratio 0.4757, below the 0.51 threshold
		{742, 6_283}, // ratio 0.6283, clears the 0.51 threshold
	} {
		require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
			&models.PoolStakeSnapshot{
				Epoch:        row.epoch,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  []byte(hfrYesPool),
				TotalStake:   types.Uint64(row.yesStake),
			},
			nil,
		))
		require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
			&models.PoolStakeSnapshot{
				Epoch:        row.epoch,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  []byte(hfrSilentPool),
				TotalStake:   types.Uint64(10_000 - row.yesStake),
			},
			nil,
		))
	}

	// Committee: one seated member, 1/1 quorum -- matches the live
	// incident's committee_member/auth_committee_hot state exactly (issue
	// text: "committee_member holds one seated member ... auth_committee_hot
	// maps it to ... the CC yes ratio is 1/1").
	coldCredential := repeatByte(28, 0xC1)
	hotCredential := repeatByte(28, 0xC2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: 1000,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = 10
	action.ProtocolVersion.Minor = 0
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0x49),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 737,
		ExpiresEpoch:  767,
		// Deposit 0 keeps ratificationEnactmentPrecondition's return-address
		// decode (which real proposals need) out of scope here -- this test
		// is about the SPO stake epoch selection, not deposit handling.
		Deposit:       0,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/plomin",
		AnchorHash:    repeatByte(32, 0x4a),
		GovActionCbor: actionCbor,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: []byte(hfrYesPool),
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return &hardForkRatifyFixture{
		ls:       ls,
		db:       db,
		proposal: loaded,
		pparams:  pparams,
	}
}

func (f *hardForkRatifyFixture) rollover(
	t *testing.T,
	currentEpoch models.Epoch,
	pparams lcommon.ProtocolParameters,
) *EpochRolloverResult {
	t.Helper()
	var result *EpochRolloverResult
	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = f.ls.processEpochRollover(
			txn,
			currentEpoch,
			eras.ConwayEraDesc,
			pparams,
			false,
		)
		return rolloverErr
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	// RATIFY runs after the boundary commits; its marks are durable once
	// the decision settles.
	require.NoError(t, f.ls.WaitEpochBoundaryJob(t.Context()))
	return result
}

// A hard fork is found only after the SNAP point has registered the deferred
// capture, so the boundary must withdraw it when it keeps the capture itself;
// a leftover entry stops the epoch-transition fallback from writing mark[743]
// if the boundary's own capture does not persist.
func TestHardForkBoundaryDiscardsDeferredSnapshotCapture(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyFixture(t)
	ratifyResult := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), ratifyResult.NewCurrentEpoch.EpochId)

	noop := func(*database.Txn, event.EpochTransitionEvent) error { return nil }
	f.ls.SetEpochBoundarySnapshotStakeHook(noop)
	f.ls.SetEpochBoundarySnapshotHook(noop)
	f.ls.SetCurrentBoundarySPOStakeHook(
		func(
			*database.Txn, event.EpochTransitionEvent,
		) ([]*models.PoolStakeSnapshot, error) {
			return nil, nil
		},
	)
	var captured, discarded []uint64
	f.ls.SetDeferredEpochBoundarySnapshotHooks(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			captured = append(captured, evt.NewEpoch)
			return nil
		},
		func(epoch uint64) { discarded = append(discarded, epoch) },
		func(
			*database.Txn, event.EpochTransitionEvent,
		) (DeferredBoundarySnapshot, error) {
			return nil, errors.New("hard-fork boundary deferred its snapshot")
		},
	)

	enactResult := f.rollover(t, ratifyResult.NewCurrentEpoch, f.pparams)
	require.Equal(t, uint64(743), enactResult.NewCurrentEpoch.EpochId)
	require.Equal(t, []uint64{743}, captured,
		"the SNAP point must register the capture before the hard fork is known")
	require.Equal(t, []uint64{743}, discarded)
}

// TestHealEmptyLabNoncesRepairsAndRecomputes verifies that healEmptyLabNonces
// restores an epoch's empty LastEpochBlockNonce from its boundary block's
// PrevHash and recomputes that epoch's nonce from the PREVIOUS epoch's carried
// lab (η(E) = candidate(E) ⭒ lab(E-1), the cardano-ledger assembly — NOT the
// epoch's own lab, which would shift eta by one epoch). A pre-fix
// BlockBeforeSlot endorser-block collision could persist an empty lab,
// collapsing the next epoch's nonce to the NeutralNonce identity
// (η == candidateNonce) and failing every leader-VRF check in that epoch (the
// Dijkstra/Leios at-tip wedge).
func TestHealEmptyLabNoncesRepairsAndRecomputes(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// The last block of the epoch preceding epoch 5 (slot < 200). Its PrevHash
	// is the lab value epoch 5 must recover to.
	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	// Epoch 4 is covered by the Mithril trust boundary: its imported carried
	// lab is trusted verbatim and feeds epoch 5's nonce.
	carriedLab := bytes.Repeat([]byte{0xfa}, 32)
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 150,
		epochCache: []models.Epoch{
			{
				EpochId:             4,
				StartSlot:           100,
				LengthInSlots:       100,
				Nonce:               bytes.Repeat([]byte{0xee}, 32),
				CandidateNonce:      bytes.Repeat([]byte{0xed}, 32),
				LastEpochBlockNonce: carriedLab,
			},
			{
				EpochId:        5,
				StartSlot:      200,
				LengthInSlots:  100,
				CandidateNonce: candidate,
				// NeutralNonce-collapsed (wrong) nonce: η == candidateNonce.
				Nonce:               append([]byte(nil), candidate...),
				LastEpochBlockNonce: nil, // corrupted: empty lab
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	// Epoch 5's lab is recovered from the boundary block's PrevHash.
	require.Equal(
		t,
		boundaryPrevHash,
		ls.epochCache[1].LastEpochBlockNonce,
		"empty lab must be restored from the boundary block's PrevHash",
	)

	// Epoch 5's nonce is recomputed as candidateNonce ⭒ epoch 4's carried lab,
	// no longer the NeutralNonce-collapsed value.
	want, err := lcommon.CalculateEpochNonce(candidate, carriedLab, nil)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), ls.epochCache[1].Nonce)
	require.NotEqual(
		t,
		candidate,
		ls.epochCache[1].Nonce,
		"epoch nonce must no longer be the NeutralNonce-collapsed candidate",
	)
	// The one-epoch-shifted assembly (candidate ⭒ epoch 5's OWN lab) must NOT
	// be produced — that is the divergence.
	shifted, err := lcommon.CalculateEpochNonce(
		candidate,
		boundaryPrevHash,
		nil,
	)
	require.NoError(t, err)
	require.NotEqual(
		t,
		shifted.Bytes(),
		ls.epochCache[1].Nonce,
		"epoch nonce must not mix the candidate with the epoch's own lab",
	)
}

// TestHealEmptyLabNoncesBoundsToRecentEpochs verifies the repair only touches
// the recent window: repairing every historical epoch on each restart is one
// block lookup per epoch and needlessly slow, and older labs never feed a
// runtime nonce, so an epoch older than the window must be left untouched even
// when it has a repairable (empty) lab.
func TestHealEmptyLabNoncesBoundsToRecentEpochs(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// A single boundary block precedes every epoch's start slot, so any epoch
	// that is actually processed repairs its empty lab to this PrevHash.
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       1,
		Slot:     50,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     6,
	}, nil))

	const n = healLabNonceRecentEpochs + 3
	epochs := make([]models.Epoch, n)
	for i := range epochs {
		epochs[i] = models.Epoch{
			EpochId:        uint64(i + 1),
			StartSlot:      uint64((i + 1) * 100),
			LengthInSlots:  100,
			Nonce:          bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce: bytes.Repeat([]byte{0xaa}, 32),
			// LastEpochBlockNonce left empty (repairable)
		}
	}
	ls := &LedgerState{
		db:         db,
		epochCache: epochs,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	require.Empty(
		t,
		ls.epochCache[0].LastEpochBlockNonce,
		"epoch older than the recent window must be left untouched, not repaired",
	)
	require.Equal(t, boundaryPrevHash, ls.epochCache[n-1].LastEpochBlockNonce,
		"the most recent epoch must still be repaired")
}

// TestHealEmptyLabNoncesRepairsOldestInWindowNonce verifies the bounded scan
// includes one predecessor epoch so the oldest in-window epoch's nonce — which
// mixes the PREVIOUS epoch's lab — is repaired from a verified predecessor lab
// rather than left stale. Without the predecessor the first in-window nonce
// check has no verified previous lab and is skipped.
func TestHealEmptyLabNoncesRepairsOldestInWindowNonce(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	prevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       1,
		Slot:     50,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: prevHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	const n = healLabNonceRecentEpochs + 3
	epochs := make([]models.Epoch, n)
	for i := range epochs {
		epochs[i] = models.Epoch{
			EpochId:        uint64(i + 1),
			StartSlot:      uint64((i + 1) * 100),
			LengthInSlots:  100,
			CandidateNonce: candidate,
			// NeutralNonce-collapsed (wrong) nonce and empty lab, both repairable.
			Nonce: append([]byte(nil), candidate...),
		}
	}
	ls := &LedgerState{
		db:         db,
		epochCache: epochs,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	// The oldest in-window epoch is scanned right after the predecessor whose
	// lab the scan verifies. Its nonce must be recomputed as candidate ⭒ the
	// predecessor's repaired lab (prevHash), not left as the collapsed candidate.
	oldest := n - healLabNonceRecentEpochs
	want, err := lcommon.CalculateEpochNonce(candidate, prevHash, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		want.Bytes(),
		ls.epochCache[oldest].Nonce,
		"oldest in-window epoch nonce must be repaired from the verified predecessor lab",
	)
	require.NotEqual(
		t,
		candidate,
		ls.epochCache[oldest].Nonce,
		"oldest in-window epoch nonce must no longer be the collapsed candidate",
	)
}

// TestHealEmptyLabNoncesLeavesValidRecordsUntouched verifies the recovery is a
// no-op when no epoch has a repairable lab mismatch — it must not perturb
// correct state.
func TestHealEmptyLabNoncesLeavesValidRecordsUntouched(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	lab := bytes.Repeat([]byte{0xcc}, 32)
	nonce := bytes.Repeat([]byte{0xdd}, 32)
	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{
				EpochId:             6,
				StartSlot:           300,
				LengthInSlots:       100,
				LastEpochBlockNonce: lab,
				Nonce:               nonce,
				CandidateNonce:      bytes.Repeat([]byte{0xaa}, 32),
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	require.Equal(t, lab, ls.epochCache[0].LastEpochBlockNonce)
	require.Equal(t, nonce, ls.epochCache[0].Nonce)
}

func TestHealEmptyLabNoncesLeavesParentHashLabUntouched(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	nonce, err := lcommon.CalculateEpochNonce(candidate, boundaryPrevHash, nil)
	require.NoError(t, err)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               nonce.Bytes(),
			CandidateNonce:      candidate,
			LastEpochBlockNonce: boundaryPrevHash,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce)
	require.NotEqual(t, boundaryHash, epochs[0].LastEpochBlockNonce)
	require.Equal(t, nonce.Bytes(), epochs[0].Nonce)
}

// TestHealEmptyLabNoncesRepairsLabWhenCandidateMissing verifies that the lab
// repair does not depend on a stored candidate nonce: the lab feeds the NEXT
// boundary's eta directly, so leaving it in a stale shape just because this
// epoch's nonce cannot be re-verified would wedge the next rollover. The
// nonce itself must be left untouched (no candidate to recompute it from).
func TestHealEmptyLabNoncesRepairsLabWhenCandidateMissing(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	cfg := newConwayBootstrapStabilityCfg(t)
	oldLab := bytes.Repeat([]byte{0x99}, 32)
	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: oldLab,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce,
		"stale lab must be repaired even without a stored candidate")
	require.Equal(t, oldNonce, epochs[0].Nonce,
		"nonce must be untouched when no candidate is stored")
	require.Empty(t, epochs[0].CandidateNonce)
}

// TestHealEmptyLabNoncesRepairsEmptyLabWithoutCandidate mirrors the test above
// for an empty (rather than stale) lab.
func TestHealEmptyLabNoncesRepairsEmptyLabWithoutCandidate(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: nil,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce,
		"empty lab must be repaired even without a stored candidate")
	require.Equal(t, oldNonce, epochs[0].Nonce,
		"nonce must be untouched when no candidate is stored")
}

func TestHealEmptyLabNoncesSkipsMissingCandidateBeforeBoundaryLookup(
	t *testing.T,
) {
	t.Parallel()

	oldLab := bytes.Repeat([]byte{0x99}, 32)
	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: oldLab,
		},
	}
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	require.NotPanics(t, func() {
		repaired := ls.healEmptyLabNoncesInPlace(epochs)
		require.False(t, repaired)
	})
	require.Equal(t, oldLab, epochs[0].LastEpochBlockNonce)
	require.Equal(t, oldNonce, epochs[0].Nonce)
}

// TestHealEmptyLabNoncesLeavesFirstPraosEpochLabNeutral verifies the heal does
// not "repair" the first Praos epoch's nil lab to the last pre-Praos (Byron)
// block's parent hash. cardano-ledger initializes praosStateLastEpochBlockNonce
// to NeutralNonce at the Praos start (initialChainDepState csLabNonce =
// NeutralNonce), and the initial-epoch branch of calculateEpochNonce stores a
// nil lab — rewriting it would diverge the next boundary's eta on any chain
// with a Byron era (mainnet, preprod).
func TestHealEmptyLabNoncesLeavesFirstPraosEpochLabNeutral(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Last Byron block before the Shelley start at slot 200. Without the
	// first-Praos guard, its PrevHash would be written as the lab.
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     1, // Byron main
	}, nil))

	genesisNonce := bytes.Repeat([]byte{0x11}, 32)
	epochs := []models.Epoch{
		{
			// Pre-Praos (Byron) epoch: no nonce, no candidate, no lab.
			EpochId:       3,
			StartSlot:     100,
			LengthInSlots: 100,
		},
		{
			// First Praos epoch: nonce/candidate are the genesis nonce, lab is
			// Neutral (nil).
			EpochId:        4,
			StartSlot:      200,
			LengthInSlots:  100,
			Nonce:          append([]byte(nil), genesisNonce...),
			CandidateNonce: append([]byte(nil), genesisNonce...),
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Empty(t, epochs[0].LastEpochBlockNonce)
	require.Empty(t, epochs[1].LastEpochBlockNonce,
		"first Praos epoch's lab must stay NeutralNonce (nil)")
	require.Equal(t, genesisNonce, epochs[1].Nonce,
		"first Praos epoch's nonce must stay the genesis nonce")
}

func TestHealEmptyLabNoncesTrustsMithrilCoveredEpoch(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	importedLab := bytes.Repeat([]byte{0x99}, 32)
	importedNonce := bytes.Repeat([]byte{0xdd}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               importedNonce,
			CandidateNonce:      candidate,
			LastEpochBlockNonce: importedLab,
		},
	}
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 350,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Equal(t, importedLab, epochs[0].LastEpochBlockNonce)
	require.Equal(t, importedNonce, epochs[0].Nonce)
	require.NotEqual(t, boundaryHash, epochs[0].LastEpochBlockNonce)
}

func TestHealEmptyLabNoncesInPlaceRepairsReloadedEpochs(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	// Epoch 5 is Mithril-trusted; its carried lab feeds epoch 6's nonce.
	carriedLab := bytes.Repeat([]byte{0xfa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             5,
			StartSlot:           200,
			LengthInSlots:       100,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce:      bytes.Repeat([]byte{0xed}, 32),
			LastEpochBlockNonce: carriedLab,
		},
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               append([]byte(nil), candidate...),
			CandidateNonce:      candidate,
			LastEpochBlockNonce: nil,
		},
	}
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 250,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	want, err := lcommon.CalculateEpochNonce(candidate, carriedLab, nil)
	require.NoError(t, err)
	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[1].LastEpochBlockNonce)
	require.Equal(t, want.Bytes(), epochs[1].Nonce)
}

func TestLoadEpochsRefreshesCurrentEpochAfterHealing(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Last block of epoch 4 (before slot 200): its PrevHash is epoch 5's lab.
	epoch5BoundaryPrevHash := bytes.Repeat([]byte{0x44}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       2,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x02}, 32),
		PrevHash: epoch5BoundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   2,
		Type:     6,
	}, nil))
	// Last block of epoch 5 (before slot 300): its PrevHash is epoch 6's lab.
	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetEpoch(
		200,
		5,
		bytes.Repeat([]byte{0x55}, 32),
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		300,
		6,
		append([]byte(nil), candidate...),
		nil,
		candidate,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))

	// Epoch 6's nonce mixes its candidate with epoch 5's carried lab (repaired
	// to the epoch-5 boundary block's PrevHash), not with epoch 6's own lab.
	want, err := lcommon.CalculateEpochNonce(
		candidate,
		epoch5BoundaryPrevHash,
		nil,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	txn := db.Transaction(true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.loadEpochs(txn)
	}))

	require.Equal(t, want.Bytes(), ls.epochCache[1].Nonce)
	require.Equal(t, want.Bytes(), ls.currentEpoch.Nonce)
	require.Equal(t, want.Bytes(), ls.EpochNonce(6))
	require.NotEqual(t, candidate, ls.EpochNonce(6))
}

// rollbackWindowFixture reproduces the state a node holds while a rollback's
// metadata truncation is still in flight.
//
// rollbackChainAndStateDeferred rewinds ls.chain (which physically removes the
// rolled-away block rows) before calling ls.rollback, and ls.rollback only
// assigns ls.currentTip once TruncateAfterSlot has committed. On a large
// metadata database that truncation takes tens of seconds, and for that whole
// window the chain tip is the rollback point while ls.currentTip still names
// the block the chain rollback already deleted.
type rollbackWindowFixture struct {
	ls          *LedgerState
	blocks      []models.Block
	rollbackTo  models.Block
	staleLedger ochainsync.Tip
}

func newRollbackWindowFixture(t *testing.T) *rollbackWindowFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 5}))

	tipBlock := blocks[len(blocks)-1]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(tipBlock),
		BlockNumber: tipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip

	// Rewind the chain exactly as rollbackChainAndStateDeferred does, and
	// deliberately do NOT run ls.rollback: this is the in-flight-truncation
	// window.
	rollbackTo := blocks[2]
	require.NoError(t, ls.chain.Rollback(makeTestPoint(rollbackTo)))

	return &rollbackWindowFixture{
		ls:          ls,
		blocks:      blocks,
		rollbackTo:  rollbackTo,
		staleLedger: ledgerTip,
	}
}

// TestRollbackWindowPreconditions pins the exact state the bug depends on, so
// a future change that makes the window unreachable fails here loudly rather
// than silently turning the regression tests below into tautologies.
func TestRollbackWindowPreconditions(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// The chain has been rewound to the rollback point...
	chainTip := f.ls.chain.Tip()
	require.Equal(t, f.rollbackTo.Slot, chainTip.Point.Slot)

	// ...but the ledger tip still names the block that rewind deleted.
	f.ls.RLock()
	ledgerTip := f.ls.currentTip
	f.ls.RUnlock()
	require.Equal(t, f.staleLedger.Point.Slot, ledgerTip.Point.Slot)
	require.Greater(t, ledgerTip.Point.Slot, chainTip.Point.Slot)

	// The ledger tip's block row is gone, which is what makes
	// authoritativeRecentChainPoints bail out.
	_, err := database.BlockByPoint(f.ls.db, ledgerTip.Point)
	require.ErrorIs(t, err, models.ErrBlockNotFound)

	// And the "chain is usable" guard is false, so IntersectPoints takes the
	// authoritative path rather than the chain path.
	require.False(t, f.ls.primaryChainTipAtOrAheadOfLedgerTip())
}

// TestAuthoritativeRecentChainPointsFallsBackToChainTipWhenLedgerTipMissing
// asserts the ledger never reports "I have no chain points" while it demonstrably
// holds a chain. Before the fix this returned an empty slice with a nil error,
// which buildDefaultChainsyncIntersectPoints turned into an origin-only
// MsgFindIntersect.
func TestAuthoritativeRecentChainPointsFallsBackToChainTipWhenLedgerTipMissing(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		points,
		"ledger reported no chain points while holding a chain tip at slot %d",
		f.ls.chain.Tip().Point.Slot,
	)

	// The newest point offered must be the rollback point (the chain tip),
	// not the deleted ledger tip.
	assert.Equal(t, f.rollbackTo.Slot, points[0].Slot)
	assert.Equal(t, f.rollbackTo.Hash, points[0].Hash)

	// No point may name a block that no longer exists.
	for _, point := range points {
		assert.LessOrEqual(
			t,
			point.Slot,
			f.rollbackTo.Slot,
			"offered a point above the rollback point",
		)
	}

	// Recent points below the tip must still be walked, so a peer that has
	// also rewound can intersect deeper than the tip.
	require.Greater(t, len(points), 1, "expected recent points below the tip")
	assert.Equal(t, f.blocks[1].Slot, points[1].Slot)
}

// TestIntersectPointsDoesNotCollapseToEmptyDuringRollbackWindow is the
// end-to-end ledger-level assertion: the entry point chainsync actually calls
// must not return an empty set in this window.
func TestIntersectPointsDoesNotCollapseToEmptyDuringRollbackWindow(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	points, err := f.ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		points,
		"IntersectPoints returned nothing during an in-flight rollback; "+
			"chainsync would offer origin only",
	)
	assert.Equal(t, f.rollbackTo.Slot, points[0].Slot)
	assert.Equal(t, f.rollbackTo.Hash, points[0].Hash)
}

// TestIntersectPointsStillEmptyAtOriginWithNoChain guards the fallback from
// over-reaching: a node that genuinely has no chain must still report no
// points, so a fresh node syncs from origin as designed.
func TestIntersectPointsStillEmptyAtOriginWithNoChain(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 5}))
	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	assert.Empty(t, points)
}

// TestAuthoritativeRecentChainPointsIgnoresChainTipAheadOfLedgerTip pins the
// boundary of the fallback introduced for the rollback window. A chain tip at
// or ahead of the ledger tip is unapplied forward work -- possibly a fork that
// does not descend from the ledger tip at all -- and must NOT be offered as an
// intersect point, which is the invariant the primary-chain ancestor check
// exists to protect. Only a chain tip strictly below the ledger tip,
// the signature of an in-flight rewind, qualifies.
func TestAuthoritativeRecentChainPointsIgnoresChainTipAheadOfLedgerTip(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	// Extend the rewound chain past the (missing) ledger tip with a fork
	// block, so the chain tip is now ahead of the ledger tip.
	forkHash := bytes.Repeat([]byte{0xfe}, 32)
	require.NoError(t, f.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        f.staleLedger.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: f.staleLedger.BlockNumber + 1,
			Type:        1,
			PrevHash:    f.rollbackTo.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.Greater(
		t,
		f.ls.chain.Tip().Point.Slot,
		f.staleLedger.Point.Slot,
	)

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.NoError(t, err)
	assert.Empty(
		t,
		points,
		"must not offer unapplied forward chain state as intersect points",
	)
}

// countingWarnHandler counts Warn records carrying a given message.
type countingWarnHandler struct {
	slog.Handler
	message string
	count   *atomic.Int64
}

func (h countingWarnHandler) Handle(
	ctx context.Context,
	record slog.Record,
) error {
	if record.Level == slog.LevelWarn && record.Message == h.message {
		h.count.Add(1)
	}
	return nil
}

func (h countingWarnHandler) Enabled(
	_ context.Context,
	level slog.Level,
) bool {
	return level >= slog.LevelWarn
}

// TestRecentChainPointsFallbackAnchorPropagatesStorageError verifies a real
// storage failure is surfaced rather than silently degraded into "no anchor".
// Swallowing it would turn a transient database fault into an origin-only
// intersect list, i.e. a request that the peer replay the chain from genesis.
func TestRecentChainPointsFallbackAnchorPropagatesStorageError(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// Sanity: the anchor resolves while the database is healthy.
	block, ok, err := f.ls.recentChainPointsFallbackAnchor(f.staleLedger)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, f.rollbackTo.Slot, block.Slot)

	require.NoError(t, dbtest.CloseDatabase(f.ls.db))

	_, ok, err = f.ls.recentChainPointsFallbackAnchor(f.staleLedger)
	require.Error(t, err, "storage failure must not be swallowed")
	assert.False(t, ok)
	assert.NotErrorIs(
		t,
		err,
		models.ErrBlockNotFound,
		"a real storage fault must not be reported as a missing block",
	)
}

// TestAuthoritativeRecentChainPointsPropagatesAnchorStorageError verifies the
// propagated error reaches the caller instead of becoming an empty point list.
func TestAuthoritativeRecentChainPointsPropagatesAnchorStorageError(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)
	require.NoError(t, dbtest.CloseDatabase(f.ls.db))

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.Error(t, err)
	assert.Nil(t, points)
}

// TestIntersectAnchorFallbackWarnIsThrottled verifies the anchor-fallback
// warning does not flood. authoritativeRecentChainPoints runs on every
// chainsync client start, and during the truncation window peer governance
// reconnects roughly once a second across every peer, so an unthrottled
// warning would emit hundreds of lines for a single rollback.
func TestIntersectAnchorFallbackWarnIsThrottled(t *testing.T) {
	f := newRollbackWindowFixture(t)

	var warns atomic.Int64
	f.ls.config.Logger = slog.New(countingWarnHandler{
		Handler: slog.NewJSONHandler(io.Discard, nil),
		message: "ledger tip block missing, anchoring intersect points on primary chain tip",
		count:   &warns,
	})

	for range 50 {
		points, err := f.ls.authoritativeRecentChainPoints(4)
		require.NoError(t, err)
		require.NotEmpty(t, points)
	}

	assert.Equal(
		t,
		int64(1),
		warns.Load(),
		"anchor-fallback warning must be throttled, not emitted per call",
	)
}

// TestRollbackWindowIntersectAnchorReportsRollbackPoint verifies the exported
// anchor, which chainsync relies on, names the rollback point during the
// window.
func TestRollbackWindowIntersectAnchorReportsRollbackPoint(t *testing.T) {
	f := newRollbackWindowFixture(t)

	point, ok, err := f.ls.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, f.rollbackTo.Slot, point.Slot)
	assert.Equal(t, f.rollbackTo.Hash, point.Hash)
}

// TestRollbackWindowIntersectAnchorAbsentWhenLedgerTipPresent verifies the
// anchor is offered only inside the window: a self-consistent ledger whose tip
// row is readable needs no rescue.
func TestRollbackWindowIntersectAnchorAbsentWhenLedgerTipPresent(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// Move the ledger tip onto a block that still exists.
	f.ls.Lock()
	f.ls.currentTip = ochainsync.Tip{
		Point:       makeTestPoint(f.rollbackTo),
		BlockNumber: f.rollbackTo.Number,
	}
	f.ls.Unlock()

	_, ok, err := f.ls.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestPersistTipAfterForgedBlockUpdatesPersistedTip verifies that
// persistTipAfterForgedBlock actually advances database.GetTip to match
// the forged block -- forgeBlock's own ls.chain.AddBlock call only
// updates ls.chain's in-memory tip, not the persisted one, unlike the
// normal chainsync/forged-block batch pipeline (which calls db.SetTip
// itself). Without this call, a dev-mode-forged block is written to the
// blob/metadata block tables but invisible to anything relying on the
// persisted tip -- dingoctl's `database info`, a live Truncate's
// deletion boundary, and BlockForger's leader-election check all read
// stale data, and a later Truncate can never reach (and clean up) such a
// block, eventually surfacing as a "persistent chain index gap" error.
func TestPersistTipAfterForgedBlockUpdatesPersistedTip(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	block := newRecordForgedBlockTestBlock(42, 7)
	require.NoError(t, ls.persistTipAfterForgedBlock(block))

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(42), tip.Point.Slot)
	require.Equal(t, block.Hash().Bytes(), tip.Point.Hash)
	require.Equal(t, uint64(7), tip.BlockNumber)
}

func TestLedgerProcessBlockRunsPhase1ForPhase2InvalidTransaction(
	t *testing.T,
) {
	t.Parallel()

	const (
		blockSlot     = uint64(10)
		invalidBefore = uint64(11)
	)
	db := newTestDB(t)
	// This uses the public block-application path. The Dijkstra validator owns
	// the protocol-defined phase-two skip; this regression checks that Dingo
	// still invokes it for phase-one validation.
	// Key 8 is the upstream invalid-before/lower-bound field. Deliberately omit
	// key 3 (invalid-hereafter) so this regression is independent of the
	// separately owned upstream upper-bound implementation.
	txCbor, err := cbor.Encode([]any{
		map[uint]any{
			0: cbor.Tag{Number: 258, Content: []any{}},
			1: []any{},
			2: uint64(0),
			8: invalidBefore,
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := dijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	tx.TxIsValid = false

	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	// Fixture CBOR is bounded well below uint32.
	byteLength := uint32(len(txCbor)) // #nosec G115
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  blockSlot,
				ByteLength: byteLength,
			},
		},
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        blockSlot,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: 12,
					},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			InvalidTransactions: []uint{0},
			Transactions:        []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)

	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err = db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: blockSlot, Hash: []byte("phase-1-invalid-tx")},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{},
			offsets,
			eras.DijkstraEraDesc,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
	var outsideValidityIntervalErr allegra.OutsideValidityIntervalUtxoError
	require.ErrorAs(
		t,
		err,
		&outsideValidityIntervalErr,
		"phase-2-invalid transactions must still run phase-1 rules",
	)
	require.Equal(
		t,
		invalidBefore,
		outsideValidityIntervalErr.ValidityIntervalStart,
	)
	require.Equal(t, blockSlot, outsideValidityIntervalErr.Slot)
}

// A restart that made progress is not backed off at all, and is never stuck.
func TestLedgerPipelineBackoffProgressResets(t *testing.T) {
	t.Parallel()

	for _, consecutive := range []int{0, -1} {
		backoff, stuck := ledgerPipelineBackoff(consecutive)
		require.Zero(t, backoff)
		require.False(t, stuck)
	}
}

// Transient failures back off gently and are not reported as stuck: the
// pipeline restarting a handful of times is normal (a rollback racing the
// iterator, a peer dropping mid-batch) and must not raise an operator alarm.
func TestLedgerPipelineBackoffTransientFailuresAreNotStuck(t *testing.T) {
	t.Parallel()

	prev := time.Duration(0)
	for consecutive := 1; consecutive < noProgressStuckThreshold; consecutive++ {
		backoff, stuck := ledgerPipelineBackoff(consecutive)
		require.False(t, stuck,
			"%d consecutive restarts should still be transient", consecutive)
		require.LessOrEqual(t, backoff, noProgressBackoffMax,
			"transient backoff must stay under the normal ceiling")
		require.GreaterOrEqual(t, backoff, prev,
			"backoff must be monotonic")
		prev = backoff
	}
	require.Equal(t, noProgressBackoffMax, prev,
		"backoff should reach the normal ceiling before the stuck threshold")
}

// A deterministic failure -- a canonical block the node rejects every time --
// never stops repeating. Capping at the transient ceiling means retrying it
// forever at that rate, which is what turned a single rejected block into a
// node that spun every two seconds indefinitely. Past the threshold the
// pipeline is declared stuck and the wait escalates well beyond the transient
// ceiling.
func TestLedgerPipelineBackoffDeterministicFailureEscalates(t *testing.T) {
	t.Parallel()

	_, stuck := ledgerPipelineBackoff(noProgressStuckThreshold)
	require.True(t, stuck, "the stuck threshold should report stuck")

	// Escalates past the transient ceiling rather than sitting on it.
	longRun, stuck := ledgerPipelineBackoff(noProgressStuckThreshold + 20)
	require.True(t, stuck)
	require.Greater(t, longRun, noProgressBackoffMax,
		"a stuck pipeline must back off further than a transient one")
	require.LessOrEqual(t, longRun, noProgressStuckBackoffMax,
		"backoff must stay bounded by the stuck ceiling")

	// And is bounded no matter how long it stays stuck.
	forever, stuck := ledgerPipelineBackoff(1_000_000)
	require.True(t, stuck)
	require.Equal(t, noProgressStuckBackoffMax, forever)
}

// Monotonic across the transient/stuck boundary: the escalation must not dip.
func TestLedgerPipelineBackoffIsMonotonic(t *testing.T) {
	t.Parallel()

	prev := time.Duration(0)
	for consecutive := range noProgressStuckThreshold + 64 {
		backoff, _ := ledgerPipelineBackoff(consecutive)
		require.GreaterOrEqual(t, backoff, prev,
			"backoff dipped at %d consecutive restarts", consecutive)
		prev = backoff
	}
}

// Rejected blocks and unavailable certified blocks share the same no-progress
// budget. The latter has a minimum retry delay, but it must still enter the
// escalating cap; otherwise a deterministic rejection can spin forever at its
// fixed cadence.
func TestLedgerPipelineRetryDelayIsBounded(t *testing.T) {
	t.Parallel()

	minimum := 250 * time.Millisecond
	previous := time.Duration(0)
	for consecutive := range noProgressStuckThreshold + 64 {
		delay, stuck := ledgerPipelineRetryDelay(consecutive, minimum)
		require.GreaterOrEqual(t, delay, minimum)
		require.GreaterOrEqual(t, delay, previous,
			"retry delay dipped at %d consecutive rejections", consecutive)
		if consecutive < noProgressStuckThreshold {
			require.False(t, stuck)
		}
		previous = delay
	}

	stuckDelay, stuck := ledgerPipelineRetryDelay(
		noProgressStuckThreshold,
		minimum,
	)
	require.True(t, stuck)
	require.Greater(t, stuckDelay, minimum,
		"a deterministic rejection must leave its minimum retry cadence")
	forever, stuck := ledgerPipelineRetryDelay(1_000_000, minimum)
	require.True(t, stuck)
	require.Equal(t, noProgressStuckBackoffMax, forever,
		"rejection retry delay must have a finite upper bound")
}

func newPipelineLoopLedger(t *testing.T) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	return ls
}

// TestLedgerProcessBlocksStopsRetryingOnUnrepairableFailure covers the terminal
// recovery half. Recovery raises errHaltLedgerPipeline once it has
// established that no local replay can change a block's verdict; the restart
// loop must then stop rather than restart into the same block forever, and must
// leave a terminal signal behind for an operator.
func TestLedgerProcessBlocksStopsRetryingOnUnrepairableFailure(t *testing.T) {
	t.Parallel()

	ls := newPipelineLoopLedger(t)

	var attempts atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.ledgerProcessBlocksWithAttempt(
			t.Context(),
			func(context.Context) error {
				attempts.Add(1)
				return fmt.Errorf(
					"process block batch: %w",
					errHaltLedgerPipeline,
				)
			},
		)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"an unrepairable validation failure must stop the ledger pipeline",
	)

	assert.Equal(
		t,
		int64(1),
		attempts.Load(),
		"a halted pipeline must not run another attempt",
	)
	assert.Equal(
		t,
		1.0,
		promtestutil.ToFloat64(ls.metrics.pipelineHalted),
		"a halted pipeline must report its terminal state",
	)
}

// TestLedgerProcessBlocksKeepsRetryingRecoverableFailures is the negative case:
// an ordinary failure must keep restarting the pipeline. Treating every failure
// as terminal would turn a transient database or peer problem into an outage.
func TestLedgerProcessBlocksKeepsRetryingRecoverableFailures(t *testing.T) {
	t.Parallel()

	ls := newPipelineLoopLedger(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var attempts atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.ledgerProcessBlocksWithAttempt(
			ctx,
			func(context.Context) error {
				attempts.Add(1)
				return errors.New("transient read failure")
			},
		)
	}()
	testutil.WaitForCondition(
		t,
		func() bool { return attempts.Load() >= 3 },
		testutil.AsyncWait,
		"a recoverable failure must keep restarting the pipeline",
	)
	assert.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.pipelineHalted),
		"a retrying pipeline must not report itself halted",
	)

	cancel()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"the pipeline loop must exit when its context is cancelled",
	)
}

func TestStopStuckLedgerPipelineDoesNotInvokeFatalCallback(t *testing.T) {
	t.Parallel()

	ls := newPipelineLoopLedger(t)
	var fatalErr error
	ls.config.FatalErrorFunc = func(err error) {
		fatalErr = err
	}
	progress := pipelineProgress{
		consecutiveNoProgress: noProgressStuckThreshold,
		lastTipSlot:           123,
	}

	ls.stopStuckLedgerPipeline(errors.New("rejected block"), progress)
	assert.NoError(t, fatalErr)
}

// TestProtocolParamsForSlot_ForecastsBumpAtBoundarySlot is the deterministic
// mechanism behind ConsensusAtEachFork's Allegra 1-slot drift in the eras
// DevNet (dingo observes Allegra at slot 75; cardano-producer observes it
// at slot 76).
//
// After d8d01df ("ensure era transitions bump protocol versions") the
// forger reads pparams via ProtocolParamsForSlot, which projects the
// active era forward through any TestXHardForkAtEpoch override. So if a
// dingo node is leader for the boundary slot (slot 75 == start of epoch
// 1) under a config where Allegra is scheduled at epoch 1, it forges in
// Allegra even though the in-memory ledger state still reads Shelley —
// the boundary-crossing block is itself the trigger. Its sole-producer
// rationale is sound (otherwise a single-producer network never crosses
// the fork at all), but it makes the boundary slot's era kind depend on
// who happens to be leader for that slot:
//
//   - dingo leader at slot 75: dingo forges Allegra at slot 75. Its own
//     chain therefore observes "first Allegra block" at slot 75.
//   - cardano-producer leader at slot 75: cardano-node — which does not
//     forecast pparams across a scheduled fork the same way — produces a
//     Shelley boundary block, and the next leader slot in epoch 1 is
//     where its chain first sees an Allegra block.
//
// Whichever node loses the boundary-slot leader election has its first
// Allegra observation pushed to the next leader slot in epoch 1. With
// VRF leader randomness on a small test pool, the cardano-producer side
// of that race is ~1 slot late on average, exactly the drift we observe.
// This test proves the dingo half of the mechanism: at the boundary
// slot, ProtocolParamsForSlot returns Allegra pparams; one slot earlier
// it still returns Shelley pparams.
func TestProtocolParamsForSlot_ForecastsBumpAtBoundarySlot(t *testing.T) {
	t.Parallel()

	cfg := newAllegraAtEpoch1Cfg(t)

	// Concrete Shelley pparams as if we were mid-epoch-0 with the
	// genesis protocol version. major=2 ⇒ Shelley.
	pparams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor: 2,
		ProtocolMinor: 0,
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 75,
			SlotLength:    1000,
			EraId:         eras.ShelleyEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// Slot 74: last slot of epoch 0. Still Shelley by every measure —
	// the schedule's trigger fires AT epoch 1, not before.
	got74 := ls.ProtocolParamsForSlot(74)
	got74Major := got74.(*shelley.ShelleyProtocolParameters).ProtocolMajor
	require.Equalf(
		t,
		uint(2),
		got74Major,
		"slot 74 (last slot of epoch 0) must still report "+
			"Shelley pparams (major=2); got major=%d",
		got74Major,
	)

	// Slot 75: first slot of epoch 1. With Allegra scheduled at
	// epoch 1, ProtocolParamsForSlot walks Shelley.NextEraTrigger=
	// AtEpoch(1) ≤ slotEpoch(1) and applies AllegraEraDesc.HardForkFunc,
	// returning Allegra pparams (major=3). The forger sees major=3,
	// extractPParamsLimits selects the Allegra block layout, and the
	// boundary-slot block is forged as Allegra.
	got75 := ls.ProtocolParamsForSlot(75)
	got75Major := got75.(*shelley.ShelleyProtocolParameters).ProtocolMajor
	require.Equalf(
		t,
		uint(3),
		got75Major,
		"slot 75 (first slot of epoch 1, scheduled Allegra fork) "+
			"must report Allegra pparams (major=3); got major=%d. "+
			"This is the proximate cause of the Allegra drift in "+
			"ConsensusAtEachFork: any node that forges this slot "+
			"will produce an Allegra block at it.",
		got75Major,
	)
}

// TestProtocolParamsForSlot_UsesMultiEraEpochs ensures the target epoch is
// resolved from the complete era history. Dividing an absolute slot by the
// current era's epoch length loses the epochs occupied by a Byron prefix and
// can therefore miss a scheduled fork at the first future Shelley boundary.
func TestProtocolParamsForSlot_UsesMultiEraEpochs(t *testing.T) {
	t.Parallel()

	const (
		byronEpochs       = 2
		byronEpochLength  = uint(100)
		shelleyEpoch      = uint64(207)
		shelleyEpochLen   = uint(432)
		byronEndSlot      = uint64(byronEpochs) * uint64(byronEpochLength)
		currentEpochStart = byronEndSlot + (shelleyEpoch-2)*uint64(
			shelleyEpochLen,
		)
		boundarySlot = currentEpochStart + uint64(shelleyEpochLen)
	)

	cfg := newMultiEraForecastCfg(t, shelleyEpoch+1)
	epochCache := make([]models.Epoch, 0, int(shelleyEpoch)+1)
	for epoch := range uint64(byronEpochs) {
		epochCache = append(epochCache, models.Epoch{
			EpochId:       epoch,
			StartSlot:     epoch * uint64(byronEpochLength),
			SlotLength:    20_000,
			LengthInSlots: byronEpochLength,
			EraId:         eras.ByronEraDesc.Id,
		})
	}
	for epoch := uint64(2); epoch <= shelleyEpoch; epoch++ {
		epochCache = append(epochCache, models.Epoch{
			EpochId:       epoch,
			StartSlot:     byronEndSlot + (epoch-2)*uint64(shelleyEpochLen),
			SlotLength:    1_000,
			LengthInSlots: shelleyEpochLen,
			EraId:         eras.ShelleyEraDesc.Id,
		})
	}

	ls := &LedgerState{
		epochCache:   epochCache,
		currentEra:   eras.ShelleyEraDesc,
		currentEpoch: epochCache[len(epochCache)-1],
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(
			boundarySlot-1, []byte("tip"),
		)},
		currentPParams: &shelley.ShelleyProtocolParameters{
			ProtocolMajor: 2,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	got := ls.ProtocolParamsForSlot(boundarySlot)
	gotShelley, ok := got.(*shelley.ShelleyProtocolParameters)
	require.True(t, ok)
	require.Equal(
		t,
		uint(3),
		gotShelley.ProtocolMajor,
		"the first Shelley slot after a Byron prefix must forecast the scheduled fork",
	)
}

// TestProtocolParamsForSlot_FallbackProjectsFromCurrentEpoch verifies the
// bounded-summary error path. A slot beyond the forecast horizon still needs
// an epoch estimate, and that estimate must retain the absolute epoch offset
// introduced by earlier eras.
func TestProtocolParamsForSlot_FallbackProjectsFromCurrentEpoch(t *testing.T) {
	t.Parallel()

	const (
		byronEpochs      = uint64(2)
		byronEpochLength = uint64(100)
		currentEpoch     = uint64(207)
		shelleyEpochLen  = uint64(432)
	)
	currentStart := byronEpochs*byronEpochLength +
		(currentEpoch-byronEpochs)*shelleyEpochLen
	targetEpoch := currentEpoch + 100
	targetSlot := currentStart + (targetEpoch-currentEpoch)*shelleyEpochLen
	cfg := newMultiEraForecastCfg(t, targetEpoch)

	ls := &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 0, StartSlot: 0, SlotLength: 20_000,
				LengthInSlots: uint(
					byronEpochLength,
				), EraId: eras.ByronEraDesc.Id},
			{EpochId: 1, StartSlot: byronEpochLength, SlotLength: 20_000,
				LengthInSlots: uint(
					byronEpochLength,
				), EraId: eras.ByronEraDesc.Id},
			{EpochId: currentEpoch, StartSlot: currentStart, SlotLength: 1_000,
				LengthInSlots: uint(
					shelleyEpochLen,
				), EraId: eras.ShelleyEraDesc.Id},
		},
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId: currentEpoch, StartSlot: currentStart,
			SlotLength: 1_000, LengthInSlots: uint(shelleyEpochLen),
			EraId: eras.ShelleyEraDesc.Id,
		},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(
			currentStart-1, []byte("tip"),
		)},
		currentPParams: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	_, err := ls.SlotToEpoch(targetSlot)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the target must exercise the bounded-summary fallback")

	got, ok := ls.ProtocolParamsForSlot(targetSlot).(*shelley.ShelleyProtocolParameters)
	require.True(t, ok)
	require.Equal(t, uint(3), got.ProtocolMajor,
		"fallback epoch projection must preserve the Byron epoch offset")
}

func newMultiEraForecastCfg(
	t *testing.T,
	forkEpoch uint64,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"blockVersionData": { "slotDuration": "20000" },
		"protocolConsts": { "k": 1 }
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.4,
		"securityParam": 1,
		"slotLength": 1,
		"epochLength": 432,
		"systemStart": "2026-01-01T00:00:00Z"
	}`)))
	enabled := true
	cfg.ExperimentalHardForksEnabled = &enabled
	cfg.TestAllegraHardForkAtEpoch = &forkEpoch
	return cfg
}

// TestProtocolParamsForSlot_ForecastsPendingPParamUpdateAtNormalBoundary is
// the normal-boundary counterpart of the era-fork forecast test above, and
// the regression guard: Preview launches federated (Shelley
// genesis decentralisationParam = 1) and drops decentralization below 1 at
// the epoch 1->2 boundary through an ordinary on-chain protocol-parameter
// update, not an era hard fork. Before the fix, ProtocolParamsForSlot
// forecast future-epoch params by applying only era HardForkFuncs, so it
// returned the pre-boundary d = 1 for the next epoch. The genesis-overlay
// check then classified the first Praos block of the new epoch (on an
// irregular slot) as a non-active overlay slot and rejected it, deadlocking
// a from-genesis sync at the boundary: entering the new epoch requires
// accepting that block, which requires the post-update d, which only became
// available after entering the epoch.
//
// The pending update was proposed by a transaction the node already applied,
// so it is in ledger state (a PParamUpdate row keyed to the target epoch)
// before the rollover ticks into that epoch. ProtocolParamsForSlot now
// applies it in the forecast, mirroring the rollover's enactment, so the
// next epoch's slots see the lowered d without the row being persisted yet.
func TestProtocolParamsForSlot_ForecastsPendingPParamUpdateAtNormalBoundary(
	t *testing.T,
) {
	t.Parallel()

	cfg := newShelleyUpdateQuorum1Cfg(t)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	// Seed a pending pparam-update proposal submitted in epoch 0 that lowers
	// decentralization from 1 to 1/2, from a single genesis-key delegate
	// (shelley genesis updateQuorum = 1). Per the Shelley update system a
	// proposal carries its submission epoch (0) and is enacted as epoch 1's
	// parameters at the epoch 0->1 boundary.
	updateCbor, err := cbor.Encode(map[uint64]any{
		12: cbor.Rat{Rat: big.NewRat(1, 2)},
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, // genesis key delegate hash
		updateCbor,
		50, // slot within epoch 0 (the submission epoch)
		0,  // submission epoch (enacted for epoch 1 at the 0->1 boundary)
		nil,
	))

	// Concrete Shelley pparams as if mid-epoch-0, fully federated (d = 1).
	pparams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor:    2,
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 100,
			SlotLength:    1000,
			EraId:         eras.ShelleyEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// Slot 50: still in epoch 0 (the current epoch). The forecast returns
	// the current params unchanged, so d is still 1.
	got50 := ls.ProtocolParamsForSlot(50)
	d50 := got50.(*shelley.ShelleyProtocolParameters).Decentralization
	require.NotNil(t, d50)
	require.Equalf(
		t,
		0,
		d50.Cmp(big.NewRat(1, 1)),
		"slot 50 (epoch 0, current epoch) must still report d=1; got %s",
		d50.RatString(),
	)

	// Slot 150: first-epoch-ahead slot (epoch 1). The pending update
	// enacted at the epoch 0->1 boundary lowers d to 1/2, and the forecast
	// must reflect it BEFORE the ledger has ticked into epoch 1.
	got150 := ls.ProtocolParamsForSlot(150)
	d150 := got150.(*shelley.ShelleyProtocolParameters).Decentralization
	require.NotNil(t, d150)
	require.Equalf(
		t,
		0,
		d150.Cmp(big.NewRat(1, 2)),
		"slot 150 (epoch 1) must report the forecast-lowered d=1/2 from "+
			"the pending pparam update; got %s. A stale d=1 here is the "+
			"#3061 overlay-rejection deadlock.",
		d150.RatString(),
	)

	// The forecast is pure: it must not mutate the shared snapshot's
	// current params (era update functions mutate their pointer in place).
	snapD := ls.GetCurrentPParams().(*shelley.ShelleyProtocolParameters).
		Decentralization
	require.NotNil(t, snapD)
	require.Equalf(
		t,
		0,
		snapD.Cmp(big.NewRat(1, 1)),
		"forecast must not mutate snapshot currentPParams; d is now %s",
		snapD.RatString(),
	)
}

// newShelleyUpdateQuorum1Cfg builds a CardanoNodeConfig whose era transitions
// are all version-gated (no scheduled TriggerAtEpoch fork), so the era-fork
// forecast walk is a no-op and the pending-pparam-update forecast is exercised
// in isolation. updateQuorum = 1 lets a single genesis-key proposal enact.
func newShelleyUpdateQuorum1Cfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.05,
		"epochLength": 100,
		"slotLength": 1,
		"updateQuorum": 1
	}`)))
	return cfg
}

// newAllegraAtEpoch1Cfg builds a CardanoNodeConfig that mirrors the eras
// DevNet's testnet.yaml as far as the era-shape forecast is concerned:
// experimental hard forks are enabled and Allegra is scheduled at epoch 1
// (slot 75 with epochLength=75). All other forks are left as AtVersion so
// the forecast walks at most one step.
func newAllegraAtEpoch1Cfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	enabled := true
	allegraEpoch := uint64(1)
	cfg.ExperimentalHardForksEnabled = &enabled
	cfg.TestAllegraHardForkAtEpoch = &allegraEpoch
	return cfg
}

// TestProtocolParamsForSlot_ConcurrentPostForkCallsDoNotRaceOnCostModels
// guards against a concurrent map write crash, not just a -race warning.
// ProtocolParamsForSlot forecasts across a scheduled fork by calling
// HardForkFunc directly on the published snapshot's currentPParams; if a
// HardForkFunc wrapper shares its input's CostModels map instead of cloning
// it (the shape gouroboros's UpgradePParams produces — it copies the
// pparams struct but not the map), concurrent forecasts for the same
// post-fork slot become concurrent writes into that one shared map, which
// Go's runtime terminates the process for rather than reporting as a
// data race. HardForkBabbage (and Conway/Dijkstra) must clone CostModels
// before writing to it for this to be safe.
func TestProtocolParamsForSlot_ConcurrentPostForkCallsDoNotRaceOnCostModels(
	t *testing.T,
) {
	t.Parallel()

	cfg := newAlonzoBabbageAtEpoch1Cfg(t)

	pparams := &alonzo.AlonzoProtocolParameters{
		ProtocolMajor: eras.AlonzoEraDesc.MaxMajorVersion,
		CostModels: map[uint][]int64{
			0: {1, 2, 3},
		},
	}

	ls := &LedgerState{
		currentEra: eras.AlonzoEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 75,
			SlotLength:    1000,
			EraId:         eras.AlonzoEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	const goroutines = 16
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			got := ls.ProtocolParamsForSlot(75)
			babbagePParams, ok := got.(*babbage.BabbageProtocolParameters)
			require.True(t, ok)
			require.NotEmpty(t, babbagePParams.CostModels)
		})
	}
	wg.Wait()
}

// newAlonzoBabbageAtEpoch1Cfg schedules Babbage at epoch 1 (slot 75 with
// epochLength=75), mirroring newAllegraAtEpoch1Cfg's shape but for the
// CostModels-bearing Alonzo->Babbage transition.
func newAlonzoBabbageAtEpoch1Cfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	enabled := true
	babbageEpoch := uint64(1)
	cfg.ExperimentalHardForksEnabled = &enabled
	cfg.TestBabbageHardForkAtEpoch = &babbageEpoch
	return cfg
}

// shrinkGatherCoalesceRetryInterval overrides the package-level
// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam for the
// duration of the calling test and restores it on cleanup, so these tests
// must not run in parallel with each other (see cleanup_consumed_utxos's
// shrinkCleanupConsumedUtxosInterval for the same pattern).
func shrinkGatherCoalesceRetryInterval(
	t *testing.T,
	interval time.Duration,
	attempts int,
) {
	t.Helper()
	prevInterval := gatherCoalesceRetryInterval
	prevAttempts := gatherCoalesceMaxAttempts
	gatherCoalesceRetryInterval = interval
	gatherCoalesceMaxAttempts = attempts
	t.Cleanup(func() {
		gatherCoalesceRetryInterval = prevInterval
		gatherCoalesceMaxAttempts = prevAttempts
	})
}

// scriptedGapLedgerReadIterator scripts a fixed sequence of non-blocking
// Next outcomes. A nil entry simulates the iterator momentarily having
// nothing ready (chain.ErrIteratorChainTip on a non-blocking probe) without
// the chain having actually stopped growing -- e.g. the goroutine that
// appends blocks to ls.chain is a beat behind this reader. Once the script
// is exhausted, a non-blocking call keeps returning ErrIteratorChainTip and
// a blocking call waits on ctx.Done(), matching a real iterator genuinely
// caught up to a still-open chain tip.
type scriptedGapLedgerReadIterator struct {
	ctx    context.Context
	script []*chain.ChainIteratorResult
	idx    int
}

func (s *scriptedGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	if s.idx < len(s.script) {
		next := s.script[s.idx]
		s.idx++
		if next == nil {
			return nil, chain.ErrIteratorChainTip
		}
		return next, nil
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

// TestLedgerReadChainIteratorCoalescesGapsDuringBulkReplay is a regression
// test for confirmed premature-flush defect: the gather loop
// used to flush a batch the moment a non-blocking iter.Next(false) returned
// chain.ErrIteratorChainTip, even with only one block gathered and 49 more
// blocks about to arrive. On the harness that produced the issue, this
// fragmented an intended 50-block batch into ~6-9 block commits.
//
// This scripts five blocks separated by momentary gaps (including a run of
// three consecutive gaps) with no upstream tip configured, so isNearTip is
// false throughout (bulk-replay/no-known-upstream default) and the
// coalescing wait applies. Before the fix, the first gap alone flushed a
// batch of 1; after it, all five blocks land in one batch.
func TestLedgerReadChainIteratorCoalescesGapsDuringBulkReplay(t *testing.T) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 5)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)
	block3, point3 := buildDecodableTestBlock(t, 30, 3)
	block4, point4 := buildDecodableTestBlock(t, 40, 4)
	block5, point5 := buildDecodableTestBlock(t, 50, 5)

	script := []*chain.ChainIteratorResult{
		{Point: point1, Block: block1},
		nil,
		{Point: point2, Block: block2},
		nil,
		nil,
		{Point: point3, Block: block3},
		nil,
		{Point: point4, Block: block4},
		nil,
		nil,
		nil,
		{Point: point5, Block: block5},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	// Zero-value config: config.GetActiveConnectionFunc is nil and
	// syncUpstreamTipSlot defaults to 0, so UpstreamTipSlot() returns 0 and
	// isNearTip is false for every slot -- the "still catching up, or
	// upstream unknown" default (see isNearTip's doc comment).
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered a batch",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(
		t,
		result.blocks,
		5,
		"gaps during bulk replay should be coalesced into one batch instead "+
			"of flushing on the very first empty non-blocking check",
	)
	close(result.done)
}

// TestLedgerReadChainIteratorNearTipFlushesSingleBlockPromptly confirms the
// coalescing wait added for does not regress live tip-following
// latency: once isNearTip is true, a solitary new block must still commit
// immediately rather than wait for a batch that will never fill.
//
// The retry interval is deliberately set far larger (minutes) than the
// receive deadline (seconds) below, so this does not race a tight timing
// window against scheduler jitter: a correct implementation returns near-
// instantly regardless of load, while a regressed one that started waiting
// would still be asleep by the time the deadline below expires.
func TestLedgerReadChainIteratorNearTipFlushesSingleBlockPromptly(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 1)

	block, point := buildDecodableTestBlock(t, 100, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx:    ctx,
		script: []*chain.ChainIteratorResult{{Point: point, Block: block}},
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	// Upstream tip at slot 1: the block's slot (100) is at or past it, so
	// isNearTip reports "caught up" (see nearUpstreamTip).
	ls.advanceUpstreamTipSlot(1)

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"single block at live tip did not commit promptly -- looks like it "+
			"waited on a coalescing batch that will never fill",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)
}

// farUpstreamTipSlot is far enough past the single-digit slots these tests
// build blocks at to sit well outside the stability window a nil
// CardanoNodeConfig falls back to (blockfetchBatchSlotThresholdDefault,
// 50000), so isNearTip reports "still catching up" against a KNOWN upstream
// tip rather than against the unknown-upstream default.
const farUpstreamTipSlot = 1_000_000

// pausingGapLedgerReadIterator returns one block, then parks inside the Next
// call that reports the first chain-tip gap: it closes gapEntered and waits
// on resume before returning chain.ErrIteratorChainTip. Parking there leaves
// ledgerReadChainIterator holding blockPipelineGatherMutex's read lock with
// one block already gathered and holding no ledger lock, which is the only
// point from which a test can both release the reader into the coalescing
// branch and control what other locks are held when it gets there.
type pausingGapLedgerReadIterator struct {
	ctx        context.Context
	first      *chain.ChainIteratorResult
	calls      int
	gapEntered chan struct{}
	resume     chan struct{}
}

func (p *pausingGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	idx := p.calls
	p.calls++
	if idx == 0 {
		return p.first, nil
	}
	// Next is called serially from the reader goroutine, so indexing is a
	// deterministic way to name the first gap without a sync.Once.
	if idx == 1 {
		close(p.gapEntered)
		<-p.resume
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	<-p.ctx.Done()
	return nil, p.ctx.Err()
}

// tryLockGatherMutex reports whether blockPipelineGatherMutex's write lock --
// the one rollbackChainAndStateDeferred takes -- is obtainable right now,
// releasing it again if it is, so it can be polled.
func tryLockGatherMutex(ls *LedgerState) bool {
	if ls.blockPipelineGatherMutex.TryLock() {
		ls.blockPipelineGatherMutex.Unlock()
		return true
	}
	return false
}

// TestLedgerReadChainIteratorHoldsGatherMutexAcrossCoalesceWait pins the
// safety property the coalescing branch rests on unlike the
// genuinely-blocking wait for a still-empty batch, the coalescing wait keeps
// blockPipelineGatherMutex's read lock held, because rawBatch already holds
// real gathered blocks a concurrent rollback must not race ahead of.
//
// TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather does not reach
// here: its scripted reader pauses inside Next, never inside this wait, so
// releasing the lock across the wait leaves the whole ledger package green.
// This test closes that gap by probing for the write lock while the reader
// is inside the wait -- a release there makes the probe succeed.
func TestLedgerReadChainIteratorHoldsGatherMutexAcrossCoalesceWait(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	//
	// One attempt, so the pass makes exactly one coalescing wait, and an
	// interval long enough that the probe below fits comfortably inside
	// that single wait rather than racing its end.
	const coalesceWait = 500 * time.Millisecond
	shrinkGatherCoalesceRetryInterval(t, coalesceWait, 1)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &pausingGapLedgerReadIterator{
		ctx:        ctx,
		first:      &chain.ChainIteratorResult{Point: point, Block: block},
		gapEntered: make(chan struct{}),
		resume:     make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	resultCh := make(chan readChainResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.gapEntered, testutil.AsyncWait,
		"reader never reached the chain-tip gap that triggers coalescing",
	)
	// Releasing the iterator sends the reader straight into the coalescing
	// wait: one block is gathered, the batch is under capacity, no attempt
	// has been spent, and the tip is far away.
	close(iter.resume)

	require.Never(
		t,
		func() bool { return tryLockGatherMutex(ls) },
		coalesceWait/2,
		2*time.Millisecond,
		"blockPipelineGatherMutex.Lock() succeeded while the reader was "+
			"inside the coalescing wait holding blocks it has not yet "+
			"submitted -- a concurrent rollback would drain an empty "+
			"blockPipeline and proceed ahead of them",
	)
	// Half the wait has elapsed at most, so the batch cannot have been
	// delivered yet. A delivery here would mean the probe above ran after
	// the pass ended rather than during the wait.
	select {
	case <-resultCh:
		t.Fatal(
			"batch was delivered before the coalescing wait elapsed -- the " +
				"probe above did not cover the wait",
		)
	default:
	}

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its coalesced batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)

	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

// TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan pins the other
// half of that bound. ARCHITECTURE.md states a worst case for how long a
// rollback blocked on blockPipelineGatherMutex waits, derived purely from
// batchSize, gatherCoalesceMaxAttempts and gatherCoalesceRetryInterval. That
// figure only holds if nothing inside the held span can block on anything
// else. ls.isNearTip reaches calculateStabilityWindow, which takes ls.RLock,
// and Go's RWMutex parks a reader behind a pending writer -- so evaluating it
// inside the span folds an unbounded block-apply wait into the stated bound.
//
// Here a block apply's ls.Lock() is taken while the reader sits in the gather
// span, and the gather pass must still finish and release the gather mutex.
func TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 1)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &pausingGapLedgerReadIterator{
		ctx:        ctx,
		first:      &chain.ChainIteratorResult{Point: point, Block: block},
		gapEntered: make(chan struct{}),
		resume:     make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	resultCh := make(chan readChainResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.gapEntered, testutil.AsyncWait,
		"reader never reached the chain-tip gap that triggers coalescing",
	)

	// The reader is parked inside Next holding the gather read lock and no
	// ledger lock, so this is obtainable now. Holding it across the resume
	// below is what a concurrent block apply does.
	ls.Lock()
	ledgerLockHeld := true
	defer func() {
		if ledgerLockHeld {
			ls.Unlock()
		}
	}()

	close(iter.resume)

	require.Eventually(
		t,
		func() bool { return tryLockGatherMutex(ls) },
		testutil.AsyncWait,
		5*time.Millisecond,
		"gather pass never released blockPipelineGatherMutex while the "+
			"ledger write lock was held -- it takes ls.RLock inside the "+
			"gather span, so the documented coalescing bound also includes "+
			"however long a block apply holds the ledger lock",
	)

	ls.Unlock()
	ledgerLockHeld = false

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its coalesced batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)

	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

// gatherSpanLockProbe records, for every LedgerStateConfig callback
// UpstreamTipSlot makes, whether blockPipelineGatherMutex was already held at
// the moment of the call. TryLock cannot block, so calling it from the reader
// goroutine that may itself hold the read lock is safe: a held read lock
// simply makes it fail.
type gatherSpanLockProbe struct {
	mu             sync.Mutex
	activeConnCals int
	connLiveCalls  int
	insideSpan     int
}

func (p *gatherSpanLockProbe) record(ls *LedgerState, activeConn bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if activeConn {
		p.activeConnCals++
	} else {
		p.connLiveCalls++
	}
	if !tryLockGatherMutex(ls) {
		p.insideSpan++
	}
}

func (p *gatherSpanLockProbe) counts() (int, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeConnCals, p.connLiveCalls, p.insideSpan
}

// TestLedgerReadChainIteratorTakesNoConnectionLocksInsideGatherSpan closes the
// half of the bound that
// TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan cannot see.
// That test leaves GetActiveConnectionFunc nil, so UpstreamTipSlot
// falls through to the syncUpstreamTipSlot atomic and never reaches the node's
// real wiring. Under that wiring UpstreamTipSlot is not an atomic read at all:
// GetActiveConnectionFunc is node_ledger_config.go's closure into
// withLiveChainsyncState (liveLifecycleMu) and chainsync State.GetClientConnId
// (clientConnIdMutex), and ConnectionLiveFunc reaches
// ConnectionManager.GetConnectionById (connectionsMutex). Evaluating it inside
// the span that holds blockPipelineGatherMutex therefore folds three more
// mutexes into a figure ARCHITECTURE.md derives purely from batchSize and the
// gatherCoalesce* settings.
//
// The upstream tip is read once per gather pass, alongside the stability
// window and before the pass takes any gather read lock, so these callbacks
// must never run with that lock held.
func TestLedgerReadChainIteratorTakesNoConnectionLocksInsideGatherSpan(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 5)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	// One gap between two blocks, so the pass enters the coalescing branch --
	// and therefore evaluates the near-tip term -- with a block already
	// gathered and the gather read lock held.
	script := []*chain.ChainIteratorResult{
		{Point: point1, Block: block1},
		nil,
		{Point: point2, Block: block2},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	probe := &gatherSpanLockProbe{}
	connId := testRecycleConnId()
	ls := &LedgerState{}
	ls.config = LedgerStateConfig{
		Logger: testLogger(),
		GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			probe.record(ls, true)
			return &connId
		},
		ConnectionLiveFunc: func(ouroboros.ConnectionId) bool {
			probe.record(ls, false)
			return true
		},
	}

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered a batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 2)
	close(result.done)

	activeConnCalls, connLiveCalls, insideSpan := probe.counts()
	// Without these the assertion below would hold vacuously: a pass that
	// never reads the upstream tip at all never takes the locks either.
	require.Positive(
		t,
		activeConnCalls,
		"gather pass never read the upstream tip, so this test asserts nothing",
	)
	require.Positive(
		t,
		connLiveCalls,
		"gather pass never checked whether the upstream connection is live, "+
			"so this test asserts nothing",
	)
	require.Zero(
		t,
		insideSpan,
		"UpstreamTipSlot ran with blockPipelineGatherMutex held, so the "+
			"liveLifecycleMu, clientConnIdMutex and connectionsMutex "+
			"acquisitions it makes are inside the span whose wait "+
			"ARCHITECTURE.md bounds from batchSize and the gatherCoalesce* "+
			"settings alone",
	)
}

// TestLedgerReadChainIteratorSkipsCoalesceAfterReachingTip covers the case
// isNearTip alone cannot see. UpstreamTipSlot returns 0 whenever no live
// upstream connection is selected, and isNearTipWithStabilityWindow folds an
// unknown upstream into "not near" -- so a node that has already caught up
// and then loses its upstream would start paying the coalescing wait again,
// gather lock held, including for its own forged blocks. reachedTip latches
// once the node first reaches the stability window and never clears, so it
// distinguishes "catching up and not yet connected" from "was at tip, lost
// the upstream".
//
// The retry interval here is minutes against a seconds-long receive
// deadline, so a regression cannot pass by winning a timing race: a correct
// implementation returns near-instantly, a regressed one is still asleep.
func TestLedgerReadChainIteratorSkipsCoalesceAfterReachingTip(t *testing.T) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 10)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx:    ctx,
		script: []*chain.ChainIteratorResult{{Point: point, Block: block}, nil},
	}

	// No upstream tip: UpstreamTipSlot returns 0 and isNearTip is false for
	// every slot, exactly as during bulk replay. reachedTip is what tells
	// the two apart.
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.reachedTip.Store(true)

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"a node that already reached tip and then lost its upstream waited "+
			"on the bulk-replay coalescing batch instead of committing",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)
}

// TestLedgerReadChainIteratorCommitBatchBlocksSkipsEmptyPasses pins the
// dingo_ledger_commit_batch_blocks histogram to actual submissions. A gather
// pass whose very first non-blocking probe returns chain.ErrIteratorChainTip
// gathers nothing -- the coalescing wait does not apply, because there is no
// partial batch to protect -- yet it still delivers a zero-block result
// downstream. Observing those would accumulate zeros in the lowest bucket of
// the distribution the histogram exists to measure, exactly during the bulk
// replay where the premature-flush symptom is read off it.
func TestLedgerReadChainIteratorCommitBatchBlocksSkipsEmptyPasses(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 2)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	// The leading nil is consumed by the first, non-blocking probe, so that
	// pass gathers no blocks at all and flushes an empty result. The two
	// blocks then arrive on the following pass.
	script := []*chain.ChainIteratorResult{
		nil,
		{Point: point1, Block: block1},
		{Point: point2, Block: block2},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.metrics.init(prometheus.NewRegistry())

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	empty := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered the empty pass",
	)
	require.NoError(t, empty.err)
	require.Empty(t, empty.blocks)
	require.Zero(
		t,
		readCommitBatchBlocksSampleCount(t, ls),
		"an empty gather pass must not be recorded as a commit",
	)
	close(empty.done)

	batch := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered the gathered batch",
	)
	require.NoError(t, batch.err)
	require.Len(t, batch.blocks, 2)
	count, sum := readCommitBatchBlocks(t, ls)
	require.Equal(t, uint64(1), count)
	require.InDelta(t, 2.0, sum, 0.0001)
	close(batch.done)
}

func readCommitBatchBlocks(
	t *testing.T,
	ls *LedgerState,
) (uint64, float64) {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, ls.metrics.commitBatchBlocks.Write(metric))
	return metric.GetHistogram().GetSampleCount(),
		metric.GetHistogram().GetSampleSum()
}

func readCommitBatchBlocksSampleCount(
	t *testing.T,
	ls *LedgerState,
) uint64 {
	t.Helper()
	count, _ := readCommitBatchBlocks(t, ls)
	return count
}

// countingGapLedgerReadIterator returns one block and then reports a
// chain-tip gap on every later non-blocking call, recording how many such
// probes were made and how many of them ran while blockPipelineGatherMutex
// was held. A real iterator's Next takes chain.Chain's tip mutex and the
// chain manager's read lock (chain.Chain.iterNext), so each in-span probe is
// one acquisition of the lock the block-append path holds -- the term
// ARCHITECTURE.md's coalescing bound has to account for separately from the
// sleeping, because unlike the near-tip terms it cannot be hoisted out of
// the span.
type countingGapLedgerReadIterator struct {
	ctx    context.Context
	first  *chain.ChainIteratorResult
	ls     *LedgerState
	mu     sync.Mutex
	calls  int
	probes int
	inSpan int
}

func (c *countingGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	c.mu.Lock()
	idx := c.calls
	c.calls++
	c.mu.Unlock()
	if idx == 0 {
		return c.first, nil
	}
	if !blocking {
		// Probe for the write lock before answering, so the sample
		// describes the span the reader is actually inside when it
		// makes this call rather than the moment after it returns.
		held := !tryLockGatherMutex(c.ls)
		c.mu.Lock()
		c.probes++
		if held {
			c.inSpan++
		}
		c.mu.Unlock()
		return nil, chain.ErrIteratorChainTip
	}
	<-c.ctx.Done()
	return nil, c.ctx.Err()
}

func (c *countingGapLedgerReadIterator) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.probes, c.inSpan
}

// TestLedgerReadChainIteratorBoundsChainProbesPerCoalesceGap pins the
// multiplier in the coalescing bound ARCHITECTURE.md states. The sleeping is
// bounded by the gatherCoalesce* settings, but each retry also re-probes the
// iterator while blockPipelineGatherMutex is still held, and that probe
// reaches chain.Chain.iterNext's c.mutex -- the lock addBlockInternal and
// addRawBlocks hold to advance the tip. The number of those acquisitions is
// what the documented worst case multiplies by, so it is the part a later
// change can silently inflate.
//
// A gap that never resolves must therefore cost exactly
// gatherCoalesceMaxAttempts+1 probes: the one that first reports the tip,
// plus one per retry the budget allows. Dropping the
// coalesceAttempts < gatherCoalesceMaxAttempts term in place makes the
// reader probe forever and never deliver the batch.
func TestLedgerReadChainIteratorBoundsChainProbesPerCoalesceGap(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	const attempts = 4
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, attempts)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	iter := &countingGapLedgerReadIterator{
		ctx:   ctx,
		first: &chain.ChainIteratorResult{Point: point, Block: block},
		ls:    ls,
	}

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	// The whole pass is attempts*1ms of sleeping, so a deadline in seconds
	// separates "flushed after exhausting the budget" from "still retrying"
	// without racing scheduler jitter.
	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"reader never flushed its batch -- the coalesce budget did not "+
			"stop the retry loop, so the probe count the documented bound "+
			"multiplies by is unbounded",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)

	probes, inSpan := iter.counts()
	require.Equal(
		t, attempts+1, probes,
		"a gap that never resolves must cost one chain probe to find the "+
			"tip plus one per allowed retry; ARCHITECTURE.md's worst case "+
			"multiplies batchSize by gatherCoalesceMaxAttempts on that basis",
	)
	require.Equal(
		t, probes, inSpan,
		"every coalesce probe must run with blockPipelineGatherMutex held "+
			"-- that is what makes each one an acquisition of the chain tip "+
			"lock inside the span, and what the bound has to account for",
	)
}

// TestWindowedRewindConvergesWhilePrimaryChainExtends pins the descent
// schedule in rollbackPrimaryChainInSecurityParamWindows against a primary
// chain that keeps growing underneath it, which is what a recovery rewind
// races with on a syncing node: blockfetch appends to the chain under
// chainsyncMutex while the ledger pipeline runs recovery under
// transactionEventMutex, so nothing serialises the two.
//
// The function used to read the chain tip once and then derive every
// intermediate target as snapshot-n*window. One block appended after that
// snapshot makes the next target window+1 below the chain's live tip, and
// Chain.Rollback refuses it as exceeding K. The whole rewind then fails, the
// pipeline restarts, and recovery recomputes the same doomed schedule against
// a tip that has grown further --, where that loop ran for nine
// hours and 1150 restarts without the chain ever being truncated.
//
// Each step must therefore be derived from the chain's live tip, so it is a
// legal K-bounded rollback by construction no matter how far the chain has
// advanced since the rewind began.
func TestWindowedRewindConvergesWhilePrimaryChainExtends(t *testing.T) {
	t.Parallel()

	const (
		securityParam = 8
		blockCount    = 240
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: securityParam}),
	)
	pc := cm.PrimaryChain()
	raw := seedTestChain(t, pc, "windowed-race", blockCount)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, securityParam),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	// SecurityParam() is era-derived; without this the Byron fallback window
	// dwarfs the chain and no intermediate step is ever taken.
	ls.currentEra = eras.ShelleyEraDesc
	require.Equal(
		t,
		securityParam,
		ls.SecurityParam(),
		"rewind window must match the chain manager's k",
	)

	// Extend the primary chain by one block on every windowed-rewind loop
	// iteration, mimicking blockfetch appending while recovery descends.
	// beforeWindowedRewindStep runs synchronously inside
	// rollbackPrimaryChainInSecurityParamWindows at the top of every loop
	// iteration, so an append here is guaranteed to land during a step the
	// rewind is actually taking -- not merely likely to, the way a
	// separately scheduled goroutine racing the rewind would only
	// probabilistically overlap it, with no guarantee it is ever scheduled
	// between the rewind starting and returning. One block per step is
	// slower than the window each step covers, so a rewind that re-reads
	// the live tip still converges.
	var stepCount int
	ls.beforeWindowedRewindStep = func() {
		stepCount++
		tip := pc.Tip()
		next := chain.RawBlock{
			Slot: tip.Point.Slot + 1,
			Hash: testHashBytes(
				fmt.Sprintf("windowed-race-append-%d", stepCount),
			),
			BlockNumber: tip.BlockNumber + 1,
			Type:        1,
			PrevHash:    tip.Point.Hash,
			Cbor:        []byte{0x80},
		}
		require.NoError(t, pc.AddRawBlocks([]chain.RawBlock{next}))
	}

	target := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	committed, rewindErr := ls.rollbackPrimaryChainInSecurityParamWindows(
		target,
	)

	require.NotErrorIs(
		t,
		rewindErr,
		chain.ErrRollbackExceedsSecurityParam,
		"a windowed step must stay within K of the chain's live tip",
	)
	require.NoError(t, rewindErr)
	require.True(
		t,
		committed,
		"a descent that reached its target committed its steps",
	)
	// The hook is called from inside the rewind's own loop, so every call it
	// receives is by construction a step the rewind is actively taking; the
	// loop must take at least one such step to reach a target this far
	// behind the seeded chain. This is therefore a guaranteed fact about the
	// run rather than a probability the scheduler could fail to realize.
	require.Positive(
		t,
		stepCount,
		"the windowed rewind must take at least one step that extends the chain",
	)
}

func TestRollbackRequeuesRewardPrecompute(t *testing.T) {
	t.Parallel()

	for _, crossEpoch := range []bool{false, true} {
		name := "same epoch"
		if crossEpoch {
			name = "previous epoch"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			nonce := testHashBytes("reward-epoch")
			epochLength := uint(100)
			if crossEpoch {
				epochLength = 15
			}
			require.NoError(t, ls.db.SetEpoch(
				0, 3, nonce, nil, nil, nil,
				eras.ShelleyEraDesc.Id, 1000, epochLength, nil,
			))
			pp, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
				ProtocolMajor: 7,
			})
			require.NoError(t, err)
			require.NoError(t, ls.db.SetPParams(
				pp, 0, 3, eras.ShelleyEraDesc.Id, nil,
			))
			epochID := uint64(3)
			if crossEpoch {
				epochID = 4
				require.NoError(t, ls.db.SetEpoch(
					15, epochID, testHashBytes("rolled-away-epoch"),
					nil, nil, nil, eras.ShelleyEraDesc.Id, 1000, 15, nil,
				))
			}
			epoch, err := ls.db.Metadata().GetEpoch(epochID, nil)
			require.NoError(t, err)
			ls.currentEpoch = *epoch
			ls.currentEra = eras.ShelleyEraDesc
			// Keep the worker occupied so the replacement event remains
			// observable after the real rollback path returns.
			ls.rewardPrecomputeRunning = true
			ls.rewardPrecomputePending = &event.EpochTransitionEvent{
				NewEpoch: epochID + 1,
			}
			ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
				epochEvent: event.EpochTransitionEvent{NewEpoch: epochID + 1},
				cutoffSlot: 100,
			}

			require.NoError(t, ls.rollbackWithBlocks(
				fixture.ancestorTip.Point, nil, false,
			))

			require.Zero(t, ls.rewardInputRollbackActive.Load())
			require.Equal(t, uint64(2), ls.rewardInputGeneration.Load())
			ls.rewardPrecomputeMu.Lock()
			defer ls.rewardPrecomputeMu.Unlock()
			pending := ls.rewardPrecomputePending
			require.NotNil(t, pending, "rollback must replace invalidated work")
			require.Equal(t, uint64(3), pending.NewEpoch,
				"replacement must calculate the surviving epoch's rewards")
			require.Equal(
				t,
				fixture.ancestorTip.Point.Slot,
				pending.BoundarySlot,
				"capture must use the surviving applied tip",
			)
			require.Equal(t, nonce, pending.EpochNonce)
			require.Nil(t, ls.rewardPrecomputeRetry,
				"a rolled-away prefilter retry must not replace fresh work")
		})
	}
}

func TestRollbackTransactionFailureRestoresRewardPrecompute(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, ls.db.SetEpoch(
		0, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1000, 100, nil,
	))
	epoch, err := ls.db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	ls.rewardPrecomputeRunning = true
	queued := &event.EpochTransitionEvent{
		NewEpoch:     4,
		BoundarySlot: 300,
		EpochNonce:   nonce,
	}
	ls.rewardPrecomputePending = queued
	retry := &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{NewEpoch: 4},
		cutoffSlot: 300,
		generation: ls.rewardInputGeneration.Load(),
	}
	ls.rewardPrecomputeRetry = retry
	transactionErr := errors.New("injected rollback transaction failure")
	failLedgerRollbackAfterChainTruncation(t, ls, transactionErr)

	err = ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, transactionErr)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	require.NotNil(t, ls.rewardPrecomputePending,
		"a failed rollback must restore the queued transition")
	require.Equal(t, queued.NewEpoch, ls.rewardPrecomputePending.NewEpoch)
	require.Equal(
		t,
		queued.BoundarySlot,
		ls.rewardPrecomputePending.BoundarySlot,
	)
	require.Equal(t, queued.EpochNonce, ls.rewardPrecomputePending.EpochNonce)
	require.NotNil(t, ls.rewardPrecomputeRetry,
		"a failed rollback must restore the deferred prefilter retry")
	require.Equal(t, retry.cutoffSlot, ls.rewardPrecomputeRetry.cutoffSlot)
	require.Equal(t, retry.epochEvent.NewEpoch,
		ls.rewardPrecomputeRetry.epochEvent.NewEpoch)
	require.Equal(t, ls.rewardInputGeneration.Load(),
		ls.rewardPrecomputeRetry.generation,
		"the restored retry must use the new stable generation")
}

func TestRollbackRewardPrecomputePersistsReusableOutputs(t *testing.T) {
	t.Parallel()

	for _, protocolMajor := range []uint{6, 7} {
		t.Run(fmt.Sprintf("protocol %d", protocolMajor), func(t *testing.T) {
			t.Parallel()
			seed, db := seedRewardPrecomputeTimingState(t, protocolMajor)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			require.NoError(
				t,
				cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
			)
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
			cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
			require.NoError(t, err)
			ancestor := chain.RawBlock{
				Slot: cutoff + 1, Hash: testHashBytes("reward-ancestor"),
				BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
			}
			current := chain.RawBlock{
				Slot: cutoff + 2, Hash: testHashBytes("reward-current"),
				PrevHash:    ancestor.Hash,
				BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
			}
			require.NoError(t, cm.PrimaryChain().AddRawBlocks(
				[]chain.RawBlock{ancestor, current},
			))
			for _, block := range []chain.RawBlock{ancestor, current} {
				require.NoError(t, db.SetBlockNonce(
					block.Hash, block.Slot, nonce, true, nil,
				))
			}
			ls.currentTip = ochainsync.Tip{
				Point:       ocommon.NewPoint(current.Slot, current.Hash),
				BlockNumber: current.BlockNumber,
			}
			require.NoError(t, db.SetTip(ls.currentTip, nil))

			require.NoError(t, ls.rollbackWithBlocks(
				ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
			))
			ls.rewardPrecomputeWG.Wait()

			outputs, err := db.Metadata().GetRewardPoolOutputs(1, nil)
			require.NoError(t, err)
			require.Len(t, outputs, 1,
				"rollback must replace discarded work before the next boundary")
			require.Equal(t, ancestor.Slot, outputs[0].CapturedSlot)
			require.Equal(t, uint64(1_200), outputs[0].BoundarySlot)
			txn := db.Transaction(false)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				app, ok, err := ls.precomputedStakeRewardApplication(
					txn,
					4,
					1_200,
				)
				require.NoError(t, err)
				require.True(t, ok, "next boundary must reuse the replacement")
				require.NotNil(t, app)
				return nil
			}))
		})
	}
}

func TestRollbackDoesNotRestartRewardsWithoutRestoredState(t *testing.T) {
	t.Parallel()

	for _, noop := range []bool{false, true} {
		t.Run(fmt.Sprintf("no-op %t", noop), func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			// An unknown surviving era forces the post-commit state reload
			// to fail; a no-op rollback must not reach that reload at all.
			require.NoError(t, ls.db.SetEpoch(
				0, 3, testHashBytes("unknown-era"), nil, nil, nil,
				255, 1000, 100, nil,
			))
			ls.rewardPrecomputeRunning = true
			pending := &event.EpochTransitionEvent{NewEpoch: 3}
			ls.rewardPrecomputePending = pending
			point := fixture.ancestorTip.Point
			if noop {
				point = fixture.currentTip.Point
			}

			err := ls.rollbackWithBlocks(point, nil, false)
			if noop {
				require.NoError(t, err)
				require.Same(t, pending, ls.rewardPrecomputePending)
				require.Zero(t, ls.rewardInputGeneration.Load())
			} else {
				require.ErrorContains(t, err, "unknown era ID 255")
				require.Nil(t, ls.rewardPrecomputePending,
					"failed reload must not schedule rewards against stale state")
				require.Equal(t, uint64(2), ls.rewardInputGeneration.Load())
			}
			require.Zero(t, ls.rewardInputRollbackActive.Load())
		})
	}
}

func TestCommittedRollbackWithFloorFailureRequeuesRewardPrecompute(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, ls.db.SetEpoch(
		0, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1000, 100, nil,
	))
	pp, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 7,
	})
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		pp, 0, 3, eras.ShelleyEraDesc.Id, nil,
	))
	epoch, err := ls.db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	// Keep the worker occupied so the replacement stays observable.
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputePending = &event.EpochTransitionEvent{NewEpoch: 4}
	floorErr := errors.New("injected durable floor lookup failure")
	base := ls.db
	failing, err := database.New(
		base.Config(),
		database.Stores{
			Blob: base.Blob(),
			Metadata: floorLookupFailingMetadataStore{
				MetadataStore: base.Metadata(),
				err:           floorErr,
			},
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, failing.Close()) })
	ls.db = failing

	err = ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, floorErr)
	_, committed := errors.AsType[*rollbackCommittedError](err)
	require.True(
		t,
		committed,
		"the truncation committed before the floor check",
	)
	require.Equal(t, fixture.ancestorTip, ls.currentTip)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	pending := ls.rewardPrecomputePending
	require.NotNil(t, pending,
		"a committed rollback must replace the work it invalidated")
	require.Equal(t, uint64(3), pending.NewEpoch)
	require.Equal(t, fixture.ancestorTip.Point.Slot, pending.BoundarySlot)
	require.Equal(t, nonce, pending.EpochNonce)
}

func TestRollbackFailureDuringCloseDoesNotRestoreRewardPrecompute(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputePending = &event.EpochTransitionEvent{
		NewEpoch:   4,
		EpochNonce: testHashBytes("reward-epoch"),
	}
	ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{NewEpoch: 4},
		cutoffSlot: 300,
	}
	injected := errors.New("injected rollback failure during close")
	ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		// Close marks the ledger closed and discards queued precompute
		// work while this transaction is still open.
		ls.closed.Store(true)
		ls.rewardPrecomputeMu.Lock()
		ls.rewardPrecomputePending = nil
		ls.rewardPrecomputeRetry = nil
		ls.rewardPrecomputeMu.Unlock()
		return ochainsync.Tip{}, nil, injected
	}

	err := ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, injected)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	require.Nil(t, ls.rewardPrecomputePending,
		"a closed ledger must not re-arm the transition Close discarded")
	require.Nil(t, ls.rewardPrecomputeRetry,
		"a closed ledger must not re-arm the retry Close discarded")
}

func TestLedgerStateStartQueuesStartupRewardPrecompute(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	nonce := []byte{0x30, 0x93, 0x65, 0x6a}
	require.NoError(t, db.SetEpoch(
		0, 0, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 7,
		ProtocolMinor: 0,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.ShelleyEraDesc.Id, nil,
	))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ls.Close()
	})

	// Occupy the precompute worker slot before Start. queueRewardPrecompute
	// hands the event to a worker goroutine that clears
	// rewardPrecomputePending under the same mutex, so a worker that reaches
	// the mutex before the hook leaves nothing for the hook to observe. With
	// the slot taken the queued round stays pending, which is what Start owes
	// the in-progress epoch.
	ls.rewardPrecomputeMu.Lock()
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputeMu.Unlock()

	startupQueued := make(chan struct{})
	ls.startupRewardPrecomputeHook = func() {
		ls.rewardPrecomputeMu.Lock()
		pending := ls.rewardPrecomputePending
		var queued event.EpochTransitionEvent
		if pending != nil {
			queued = *pending
		}
		ls.rewardPrecomputeMu.Unlock()
		require.NotNil(
			t, pending, "Start must queue the established current epoch",
		)
		require.Equal(t, uint64(0), queued.NewEpoch)
		require.Equal(t, nonce, queued.EpochNonce)
		close(startupQueued)
	}

	_ = ls.Start(t.Context())
	testutil.RequireReceive(t, startupQueued, 2*time.Second, "startup precompute queued")
}

const (
	// Deliberately contestedSlot-1: the ancestor search range is half-open, so
	// an ancestor immediately below the contested slot is the boundary case.
	sameSlotAncestorSlot  = 19
	sameSlotContestedSlot = 20
)

// sameSlotCompetitorFixture holds a ledger whose applied tip is a block at
// sameSlotContestedSlot, with one UTxO produced at sameSlotAncestorSlot and
// consumed by that applied block.
type sameSlotCompetitorFixture struct {
	ls            *LedgerState
	db            *database.Database
	appliedTip    ochainsync.Tip
	ancestorPoint ocommon.Point
	survivingHash []byte
	spentTxId     []byte
}

func newSameSlotCompetitorFixture(
	t *testing.T,
) *sameSlotCompetitorFixture {
	t.Helper()
	return newSameSlotCompetitorFixtureOpts(t, true)
}

// newSameSlotCompetitorFixtureOpts builds the fixture, optionally omitting the
// ancestor's recorded nonce so that no applied ancestor exists below the
// contested slot.
func newSameSlotCompetitorFixtureOpts(
	t *testing.T,
	seedAncestorNonce bool,
) *sameSlotCompetitorFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	ancestorHash := testHashBytes("3678-ancestor")
	survivingHash := testHashBytes("3678-surviving")
	competitorHash := testHashBytes("3678-competitor")

	// The primary chain holds the ancestor and the block at the contested slot
	// that survives chain selection. The ledger's applied tip is a *different*
	// block at that same slot -- an abandoned same-slot competitor whose effects
	// were applied to the UTxO set before chain selection moved off it. This is
	// the shape enforceDurableTipFloor repairs: it hands rollback the durable
	// applied floor while currentTip names the same-slot competitor.
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			{
				Slot:        sameSlotAncestorSlot,
				Hash:        ancestorHash,
				BlockNumber: 1,
				Type:        1,
				Cbor:        []byte{0x80},
			},
			{
				Slot:        sameSlotContestedSlot,
				Hash:        survivingHash,
				BlockNumber: 2,
				Type:        1,
				PrevHash:    ancestorHash,
				Cbor:        []byte{0x80},
			},
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	// Block nonces record which blocks were applied.
	// latestLedgerPrimaryChainAncestor reads them to find the newest applied
	// ancestor below the contested slot.
	if seedAncestorNonce {
		require.NoError(
			t,
			db.SetBlockNonce(
				ancestorHash,
				sameSlotAncestorSlot,
				[]byte("nonce-3678-ancestor"),
				true,
				nil,
			),
		)
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			survivingHash,
			sameSlotContestedSlot,
			[]byte("nonce-3678-surviving"),
			false,
			nil,
		),
	)

	// The competitor was applied before chain selection moved off it, so its
	// nonce is recorded. That recorded nonce is what distinguishes an applied
	// same-slot competitor, whose effects are in the UTxO set, from a merely
	// in-memory tip that was never applied.
	require.NoError(
		t,
		db.SetBlockNonce(
			competitorHash,
			sameSlotContestedSlot,
			[]byte("nonce-3678-competitor"),
			false,
			nil,
		),
	)

	appliedTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(sameSlotContestedSlot, competitorHash),
		BlockNumber: 2,
	}
	require.NoError(t, db.SetTip(appliedTip, nil))
	ls.currentTip = appliedTip
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	// One UTxO produced at the ancestor slot and consumed by the applied
	// block at the contested slot, exactly as a normal block application
	// leaves it: the row survives, soft-deleted with deleted_slot set to the
	// consuming block's slot.
	spentTxId := testHashBytes("3678-utxo-producer")
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		return db.CreateUtxo(txn, &models.Utxo{
			TxId:        spentTxId,
			OutputIdx:   0,
			AddedSlot:   sameSlotAncestorSlot,
			DeletedSlot: sameSlotContestedSlot,
			Amount:      types.Uint64(1_000_000),
		})
	}))

	return &sameSlotCompetitorFixture{
		ls:            ls,
		db:            db,
		appliedTip:    appliedTip,
		ancestorPoint: ocommon.NewPoint(sameSlotAncestorSlot, ancestorHash),
		survivingHash: survivingHash,
		spentTxId:     spentTxId,
	}
}

// inputInLiveSet reports whether the consumed UTxO is present in the live UTxO
// set, using the same database.UtxoByRef lookup that LedgerView.UtxoById
// delegates to. That lookup applies the deleted_slot filter, so it is the
// predicate that decides Conway bad-inputs and, through it, the consumed term
// of value conservation.
//
// Presence is judged on ErrUtxoNotFound rather than on a nil error: a row
// seeded directly into metadata carries no blob CBOR (models.Utxo.Cbor is not
// persisted by CreateUtxo), so the decode step of UtxoById cannot succeed for a
// synthetic UTxO. Only ErrUtxoCborUnavailable, alongside a nil error, is read
// as the row being in the live set. Any other error -- including a genuine
// decode failure -- is a lookup failure rather than an answer about live-set
// membership, and is returned so the test fails loudly instead of counting as
// present.
func (f *sameSlotCompetitorFixture) inputInLiveSet(t *testing.T) bool {
	t.Helper()

	var live bool
	txn := f.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, err := f.db.UtxoByRef(f.spentTxId, 0, txn)
		switch {
		case err == nil,
			errors.Is(err, database.ErrUtxoCborUnavailable):
			live = true
			return nil
		case errors.Is(err, database.ErrUtxoNotFound):
			live = false
			return nil
		default:
			// Any other error is a real lookup failure, not an answer about
			// live-set membership. Return it so the test fails instead of
			// reading it as "present".
			return err
		}
	}))
	return live
}

// TestRollbackSameSlotCompetitorRestoresConsumedUtxo covers.
//
// A rollback target that shares the applied tip's slot but carries a different
// hash used to fall through to database.TruncateAfterSlot's slot-only UTxO
// predicates (added_slot > slot, deleted_slot > slot). Nothing at the contested
// slot matched, so the UTxOs the abandoned block consumed stayed soft-deleted
// with no row left to restore them, while the tip was reported as repaired.
//
// The next block that legitimately spends such an input cannot resolve it,
// which Conway reports under the bad-inputs rule and, because value
// conservation sums consumed over only the inputs that did resolve, under the
// value-not-conserved rule in the same pass -- both from the one divergence.
// The numbers those rules carry in the diagnostic are upstream positions and
// move on a gouroboros bump, so they are named here rather than pinned.
//
// This drives LedgerState.rollback, the entry point every recovery path uses,
// and asserts live-set membership through the database.UtxoByRef lookup that
// LedgerView.UtxoById delegates to, rather than querying deleted_slot directly.
func TestRollbackSameSlotCompetitorRestoresConsumedUtxo(t *testing.T) {
	t.Parallel()

	fixture := newSameSlotCompetitorFixture(t)

	// While the applied block at the contested slot stands, its consumed
	// input is correctly unresolvable.
	require.False(
		t,
		fixture.inputInLiveSet(t),
		"consumed input should not be in the live set before the rollback",
	)

	require.NoError(
		t,
		fixture.ls.rollback(
			ocommon.NewPoint(
				sameSlotContestedSlot,
				fixture.survivingHash,
			),
		),
	)

	// The contested slot must be truncated whole, so the input the abandoned
	// block consumed is live again and resolvable at the validated point.
	require.True(
		t,
		fixture.inputInLiveSet(t),
		"consumed input must be restored to the live set after rolling back past the contested slot",
	)

	// The ledger must sit at an applied point, not at the competitor it was
	// handed, so the block at the contested slot can be re-applied.
	require.Equal(
		t,
		fixture.ancestorPoint,
		fixture.ls.currentTip.Point,
		"tip should be redirected to the applied ancestor below the contested slot",
	)
}

// TestRollbackSameSlotCompetitorWithoutAncestorFailsLoudly covers the other
// half of acceptance criteria: when the contested slot cannot be
// truncated because no applied ancestor below it can be found, the rollback
// must fail with a persistent diagnostic instead of reporting a repair that
// left the UTxO set diverged.
func TestRollbackSameSlotCompetitorWithoutAncestorFailsLoudly(t *testing.T) {
	t.Parallel()

	// No ancestor nonce, so no applied block exists below the contested slot.
	fixture := newSameSlotCompetitorFixtureOpts(t, false)

	err := fixture.ls.rollback(
		ocommon.NewPoint(sameSlotContestedSlot, fixture.survivingHash),
	)
	require.ErrorIs(t, err, ErrNoAppliedAncestorBelowContestedSlot)

	// The tip must not move, so the failure stays visible to the recovery
	// caller instead of being reported as a completed repair.
	require.Equal(
		t,
		fixture.appliedTip.Point,
		fixture.ls.currentTip.Point,
		"tip must not move when the contested slot cannot be truncated",
	)
}

// TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault covers the
// actual code path the regression test exercises:
// HardForkBabbage fabricates a PlutusV2 cost model whenever the previous
// era's params don't have one -- real for any Alonzo genesis, since the
// AlonzoGenesisCostModels format predates PlutusV2 entirely and never has a
// slot for it.
func TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault(
	t *testing.T,
) {
	t.Parallel()

	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.True(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent covers a pparams
// value that already carries a real (non-fabricated) PlutusV2 entry before
// the transition -- HardForkBabbage's own guard (`if _, hasV2 :=
// ret.CostModels[1]; !hasV2`) leaves it untouched, so nothing was injected.
func TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent(t *testing.T) {
	t.Parallel()

	realV2 := []int64{9, 9, 9}
	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: realV2},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.False(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault covers
// a hypothetical newly-added key 1 whose value does not match
// eras.DefaultPlutusV2CostModel -- only the exact known fabricated value
// counts as synthetic, not "any new key 1."
func TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault(
	t *testing.T,
) {
	t.Parallel()

	before := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: {999}},
	}

	assert.False(t, injectedSyntheticV2CostModel(before, after))
}

// TestGetCurrentPParamsForReporting_OmitsSyntheticV2CostModel covers
// reporting coverage: withoutSyntheticV2CostModel
// originally had a single call site (queries.go's LocalStateQuery handler),
// while every other interface reporting current parameters --
// api/blockfrost, api/utxorpc, api/mesh -- read GetCurrentPParams()
// unfiltered and would still report a synthetic PlutusV2 entry a real
// cardano-node never has. GetCurrentPParamsForReporting is the shared
// accessor all of those now use; this proves its filtering behavior
// directly, independent of which specific API surface calls it.
func TestGetCurrentPParamsForReporting_OmitsSyntheticV2CostModel(t *testing.T) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = true
	ls.publishSnapshotsLocked()

	reported := ls.GetCurrentPParamsForReporting()
	pp, ok := reported.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.NotContains(t, pp.CostModels, uint(1),
		"the reporting accessor must omit the synthetic PlutusV2 cost model,"+
			" matching every other reporting surface")

	// GetCurrentPParams (used by internal validation, block-building, Leios
	// committee parameters, and governance-action decoding) must be
	// completely unaffected.
	internal, ok := ls.GetCurrentPParams().(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, internal.CostModels, uint(1),
		"GetCurrentPParams must keep the default for internal validation")
}

// TestGetCurrentPParamsForReporting_IncludesRealV2CostModel covers the other
// half: once the synthetic marker is cleared, the reporting accessor must
// return the same value GetCurrentPParams does.
func TestGetCurrentPParamsForReporting_IncludesRealV2CostModel(t *testing.T) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = false
	ls.publishSnapshotsLocked()

	reported := ls.GetCurrentPParamsForReporting()
	pp, ok := reported.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, pp.CostModels, uint(1))
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1])
}

// TestSyntheticV2CostModelPersistence_RoundTripsAcrossRestart covers
// PR review: LedgerState.syntheticV2CostModel must
// survive a restart via persistSyntheticV2CostModel/loadSyntheticV2CostModel,
// not silently reconstruct as false (the zero value) regardless of the
// chain's real history.
func TestSyntheticV2CostModelPersistence_RoundTripsAcrossRestart(t *testing.T) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))

	// Not yet persisted: a fresh database reads back false, same as an
	// explicit false would.
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel)

	require.NoError(t, ls.persistSyntheticV2CostModel(true, nil))
	// Simulate a restart: a fresh in-memory value, restored from the same
	// database.
	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.True(t, ls.syntheticV2CostModel,
		"restored value must survive the simulated restart")

	require.NoError(t, ls.persistSyntheticV2CostModel(false, nil))
	ls.syntheticV2CostModel = true
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel,
		"a later persisted false must also survive the simulated restart")
}

// TestResolveSyntheticV2CostModel_BootstrapsFromValueWhenMarkerAbsent covers
// pre-marker databases: a database that predates
// this marker (or one that was reset by
// database.RecomputeSyntheticV2CostModelMarkerAfterTruncate) must not
// silently behave as "not synthetic" -- it must compare the current PlutusV2
// cost model directly against the known synthetic default instead.
func TestResolveSyntheticV2CostModel_BootstrapsFromValueWhenMarkerAbsent(
	t *testing.T,
) {
	t.Parallel()

	stillSynthetic := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	}
	assert.True(t, resolveSyntheticV2CostModel("", stillSynthetic),
		"an absent marker with the exact synthetic default present must"+
			" resolve to still-synthetic")

	realData := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: {9, 9, 9}},
	}
	assert.False(t, resolveSyntheticV2CostModel("", realData),
		"an absent marker with a value that differs from the synthetic"+
			" default must resolve to real, not synthetic")

	noV2 := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	assert.False(t, resolveSyntheticV2CostModel("", noV2),
		"an absent marker with no PlutusV2 key at all must resolve to"+
			" not-synthetic")

	assert.False(t, resolveSyntheticV2CostModel("", nil),
		"an absent marker with nil pparams must resolve to not-synthetic")
}

// TestResolveSyntheticV2CostModel_ExplicitValueWins covers the common case:
// an explicitly persisted marker value is trusted directly, regardless of
// what pp happens to contain.
func TestResolveSyntheticV2CostModel_ExplicitValueWins(t *testing.T) {
	t.Parallel()

	realData := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	}
	assert.True(t, resolveSyntheticV2CostModel("true", realData))
	assert.False(t, resolveSyntheticV2CostModel("false", realData))
}

// TestMarkRealV2CostModelObserved_KeepsEarliestConfirmationAcrossMultipleUpdates
// verifies that a chain that enacts more than one real PlutusV2 cost-model
// update over its life does not have
// its cleared-epoch marker overwritten by the later update -- doing so would
// make RecomputeSyntheticV2CostModelMarkerAfterTruncate incorrectly reset
// the marker to synthetic on a rollback that crosses back past only the
// LATEST update but not an EARLIER one, even though the earlier real value
// still survives on the truncated chain.
func TestMarkRealV2CostModelObserved_KeepsEarliestConfirmationAcrossMultipleUpdates(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newExpiryRollbackTestLedger(t, false, 0)

	// First real update confirmed at epoch 5 (slot 500).
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.markRealV2CostModelObserved(5, txn)
	}))

	// A second real update (e.g. a later governance-enacted cost-model
	// change) confirmed at epoch 10 (slot 1000) must not overwrite the
	// epoch-5 confirmation.
	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.markRealV2CostModelObserved(10, txn)
	}))

	clearedEpoch, cleared, err := database.SyntheticV2CostModelClearedEpoch(
		db, nil,
	)
	require.NoError(t, err)
	require.True(t, cleared)
	require.Equal(
		t,
		uint64(5),
		clearedEpoch,
		"the earliest confirmation must be kept, not overwritten by the later one",
	)

	// Roll back to slot 700 (epoch 7): after the first real update, before
	// the second. The surviving chain's PlutusV2 cost model is still real
	// (from the first update), so the marker must NOT be reset to synthetic.
	require.NoError(
		t,
		database.RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 700),
	)

	value, err := db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "false", value,
		"the surviving chain still has real data from the first update"+
			" and must not be reported as synthetic")
}

// TestRollbackRestore_LeavesRealPreExistingModelCorrectlyResolvedAsNotSynthetic
// covers pre-marker databases: on a database that
// predates these markers entirely, a real PlutusV2 cost model already in
// force (differing from the known synthetic default) can still pick up a
// clearedEpoch marker from the first update tracked AFTER these markers
// existed, even though the model was already real long before that epoch.
// A rollback crossing that epoch must not force the marker to "true" --
// doing so would misreport a real, already-in-force model as synthetic.
// Deleting it instead (RecomputeSyntheticV2CostModelMarkerAfterTruncate)
// lets resolveSyntheticV2CostModel's absent-marker fallback re-derive the
// correct answer from the live value.
func TestRollbackRestore_LeavesRealPreExistingModelCorrectlyResolvedAsNotSynthetic(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	// differs from eras.DefaultPlutusV2CostModel
	realNonDefaultV2 := []int64{1, 2, 3}

	// A persisted epoch table is needed for EpochBySlot to resolve the
	// rollback slot below (epochs 0-9, 100 slots each).
	for i := range uint64(10) {
		require.NoError(t, db.SetEpoch(
			i*100, i, nil, nil, nil, nil, 1, 1000, 100, nil,
		))
	}

	// A clearedEpoch marker exists (from the first tracked update after
	// these markers were introduced), even though the real model has
	// actually been in force since before that epoch.
	require.NoError(t, database.SetSyntheticV2CostModelClearedEpoch(db, nil, 5))
	require.NoError(
		t,
		db.SetSyncState(database.SyntheticV2CostModelSyncKey, "false", nil),
	)

	// Roll back to before epoch 5.
	require.NoError(
		t,
		database.RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 0),
	)

	// The boolean marker must be absent, not forced to "true".
	value, err := db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, value)

	// A fresh load against the surviving (real, non-default) pparams value
	// must resolve to "not synthetic", not be misreported as synthetic.
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: realNonDefaultV2},
	}
	assert.False(t, resolveSyntheticV2CostModel(value, pp),
		"a real, non-default model already in force must not be reported"+
			" as synthetic just because a later marker briefly existed")
}

// TestTransitionToEraFrom_PersistsSyntheticMarkerInSameTransactionAsPParams
// verifies that the synthetic-cost-model marker is written in the same database
// transaction as the pparams update it describes, not committed
// separately afterward. If they were in different transactions, a crash
// between the two commits could leave a stale marker on restart. This is
// proven here by rolling the transaction back entirely: since both writes
// share one transaction, rollback must undo both together, and a fresh
// read must see neither.
func TestTransitionToEraFrom_PersistsSyntheticMarkerInSameTransactionAsPParams(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}

	txn := db.Transaction(true)
	result, err := ls.transitionToEraFrom(
		txn,
		eras.BabbageEraDesc.Id,
		1,
		100,
		prev,
		eras.AlonzoEraDesc.Id,
	)
	require.NoError(t, err)
	require.True(t, result.InjectedSyntheticV2CostModel)
	require.NoError(t, txn.Rollback())

	// Nothing must be durable: neither the pparams write nor the marker,
	// since they shared one now-rolled-back transaction.
	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel,
		"a rolled-back transaction must not leave the marker persisted")
	rolledBackPParams, err := db.GetPParams(
		1, eras.BabbageEraDesc.Id, eras.DecodePParamsBabbage, nil,
	)
	require.NoError(t, err)
	assert.Nil(t, rolledBackPParams,
		"a rolled-back transaction must not leave the pparams write persisted")

	// The same sequence, committed instead, must persist both together.
	txn = db.Transaction(true)
	result, err = ls.transitionToEraFrom(
		txn,
		eras.BabbageEraDesc.Id,
		1,
		100,
		prev,
		eras.AlonzoEraDesc.Id,
	)
	require.NoError(t, err)
	require.True(t, result.InjectedSyntheticV2CostModel)
	require.NoError(t, txn.Commit())

	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.True(t, ls.syntheticV2CostModel,
		"a committed transaction must persist the marker")
	committedPParams, err := db.GetPParams(
		1, eras.BabbageEraDesc.Id, eras.DecodePParamsBabbage, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, committedPParams,
		"a committed transaction must persist the pparams write")
}

// awaitTransitionInfo blocks until evaluateHardForkInitiationStability's
// async tally goroutine commits a new transitionInfo, then returns it
// under a read lock so the caller's assertions race-free against the
// goroutine's write. Use this whenever a test expects the helper to
// promote transitionInfo (Unknown/Impossible -> Known); paths that
// short-circuit before spawning the goroutine don't need it.
func awaitTransitionInfo(
	t *testing.T,
	ls *LedgerState,
	want hardfork.TransitionState,
) hardfork.TransitionInfo {
	t.Helper()
	require.Eventually(t, func() bool {
		ls.RLock()
		defer ls.RUnlock()
		return ls.transitionInfo.State == want
	}, testutil.AsyncWait, 5*time.Millisecond,
		"transitionInfo.State did not reach %v", want)
	ls.RLock()
	defer ls.RUnlock()
	return ls.transitionInfo
}

func awaitHFIEvalIdle(t *testing.T, ls *LedgerState) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !ls.hfiStabilityEvalInFlight.Load()
	}, testutil.AsyncWait, 5*time.Millisecond,
		"HFI stability evaluation did not become idle")
}

// stabilityFixtureEpoch parameters: Shelley-style 432_000-slot epoch,
// safeZone = ceil(3*432/0.05) = 25_920, voting deadline distance from
// epoch end is 2 * safeZone = 51_840. So an epoch ending at slot
// 532_000 has its voting deadline at slot 480_160.
const (
	stabilityFixtureEpochID    uint64 = 500
	stabilityFixtureEpochStart uint64 = 100_000
	stabilityFixtureEpochLen   uint   = 432_000
	stabilityFixtureEpochEnd   uint64 = stabilityFixtureEpochStart +
		uint64(stabilityFixtureEpochLen)
	stabilityFixtureVotingDeadline uint64 = stabilityFixtureEpochEnd - 2*25_920
)

// stabilityFixtureLedgerState assembles a LedgerState wired with
// real-shaped Shelley genesis (so calculateStabilityWindowForEra returns
// the expected 25_920) and a file-backed SQLite DB. The caller seeds proposal /
// vote rows on db, sets currentTip and transitionInfo, and invokes
// evaluateHardForkInitiationStability.
//
// Setting currentPParams to Conway pparams with the supplied major
// version exercises the post-Conway code path inside the helper.
// Bootstrap (major 9) waives the DRep threshold but preserves the
// action-specific SPO and committee thresholds, so ratifiable fixtures must
// include both voting bodies.
func stabilityFixtureLedgerState(
	t *testing.T,
	major uint,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.ProtocolVersion.Major = major
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: newTestEpoch(
			stabilityFixtureEpochID,
			stabilityFixtureEpochStart,
			stabilityFixtureEpochLen,
			eras.ConwayEraDesc.Id,
		),
		currentPParams: &pparams,
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	return ls, db
}

// seedRatifiableBootstrapHardForkInitiation primes the DB so that the
// governance ratifiability helper returns a non-nil result when the
// bootstrap (major 9) ratification rule is in effect — a HardForkInitiation
// proposal in the active set plus the required CC and SPO yes votes.
func seedRatifiableBootstrapHardForkInitiation(
	t *testing.T,
	db *database.Database,
	currentEpoch uint64,
	targetMajor uint,
) *models.GovernanceProposal {
	t.Helper()
	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = targetMajor
	action.ProtocolVersion.Minor = 0
	cborBytes, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0xAA),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: currentEpoch - 1,
		ExpiresEpoch:  currentEpoch + 10,
		Deposit:       1_000,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/anchor",
		AnchorHash:    repeatByte(32, 0xEE),
		GovActionCbor: cborBytes,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	drepCred := repeatByte(28, 0xBB)
	stakeCred := repeatByte(28, 0xCC)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
		AddedSlot:  1,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       repeatByte(32, 0x01),
		OutputIdx:  0,
		StakingKey: stakeCred,
		AddedSlot:  1,
		Amount:     types.Uint64(1_000),
	}))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: drepCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	coldCred := repeatByte(28, 0xCE)
	hotCred := repeatByte(28, 0xCF)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: coldCred, ExpiresEpoch: currentEpoch + 10},
	}, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCred, hotCred, 1, 1)
	require.NoError(t, err)
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	poolCred := repeatByte(28, 0xDD)
	// governance.predictedBoundaryStakeEpochFor(currentEpoch) resolves to
	// currentEpoch itself: the mid-epoch check tallies the SPO
	// vote against mark[currentEpoch], the last mark durably written at the
	// boundary that opened the currently active epoch. The boundary it
	// predicts will instead tally mark[currentEpoch+1], which SNAP does not
	// capture until that boundary runs.
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        currentEpoch,
			SnapshotType: models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:  poolCred,
			TotalStake:   types.Uint64(1_000),
		},
		nil,
	))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: poolCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	return loaded
}

// TestEvaluateHardForkInitiationStability_PreDeadline_NoChange pins the
// "votes can still flip the outcome" guard: while the current tip is
// before the voting deadline (epochEnd - 2*stabilityWindow), the helper
// must not surface the upcoming transition even if the in-flight
// proposal currently meets thresholds — a yet-to-arrive No vote could
// still defeat it.
func TestEvaluateHardForkInitiationStability_PreDeadline_NoChange(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9 /* bootstrap */)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	// Tip one slot before the voting deadline.
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline-1,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"pre-deadline must not promote to TransitionKnown")
}

// TestEvaluateHardForkInitiationStability_PostDeadline_Ratifiable_SetsKnown
// is the core happy-path: after the voting deadline, with a ratifiable
// HardForkInitiation in flight, the helper must set TransitionKnown for
// the epoch the boundary will fire (currentEpoch + 1).
func TestEvaluateHardForkInitiationStability_PostDeadline_Ratifiable_SetsKnown(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9 /* bootstrap */)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline, []byte("tip")),
	}

	ls.evaluateHardForkInitiationStability()

	got := awaitTransitionInfo(t, ls, hardfork.TransitionKnown)
	assert.Equal(t, stabilityFixtureEpochID+1, got.KnownEpoch,
		"target epoch is the next epoch boundary")
}

// TestEvaluateHardForkInitiationStability_PostDeadline_NotRatifiable_NoChange
// pins the negative side: post-deadline without a ratifiable proposal,
// transitionInfo stays Unknown. (No proposal seeded; helper returns nil.)
func TestEvaluateHardForkInitiationStability_PostDeadline_NotRatifiable_NoChange(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := stabilityFixtureLedgerState(t, 9)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	awaitHFIEvalIdle(t, ls)
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"no ratifiable proposal means transitionInfo stays Unknown")
}

// TestEvaluateHardForkInitiationStability_PreConwayPParams_NoOp pins the
// short-circuit when the chain is pre-Conway: no governance state
// machine exists, so the helper must not even attempt the DB lookup
// (and certainly must not promote transitionInfo).
func TestEvaluateHardForkInitiationStability_PreConwayPParams_NoOp(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	// Even with a "ratifiable" proposal seeded, swapping pparams to nil
	// (or any non-Conway type) makes the helper short-circuit before
	// it hits the proposal store.
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentPParams = nil
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	awaitHFIEvalIdle(t, ls)
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"pre-Conway pparams must short-circuit without promotion")
}

// TestEvaluateHardForkInitiationStability_AlreadyKnownForSameEpoch_Idempotent
// pins the short-circuit when transitionInfo already reports the same
// upcoming boundary. The function must not redundantly mutate state
// (a redundant mutation is harmless to behaviour but makes per-block
// invocations noisier than necessary).
func TestEvaluateHardForkInitiationStability_AlreadyKnownForSameEpoch_Idempotent(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}
	ls.transitionInfo = hardfork.NewTransitionKnown(stabilityFixtureEpochID + 1)

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, stabilityFixtureEpochID+1, ls.transitionInfo.KnownEpoch)
}

// TestEvaluateHardForkInitiationStability_PreservesKnownFromOtherSource
// pins the deference to higher-priority sources of TransitionKnown.
// Both evaluateTriggerAtEpoch (test override) and reconstructTransitionInfo
// (pparams-bump detection) may set Known for a specific epoch. The
// mid-epoch governance detector must not clobber that decision even if
// it would otherwise fire, so on-chain HFI ratifiability cannot
// override an operator-configured TestXHardForkAtEpoch boundary.
func TestEvaluateHardForkInitiationStability_PreservesKnownFromOtherSource(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}
	const externalTargetEpoch = stabilityFixtureEpochID + 7
	ls.transitionInfo = hardfork.NewTransitionKnown(externalTargetEpoch)

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(
		t,
		uint64(externalTargetEpoch),
		ls.transitionInfo.KnownEpoch,
		"a Known target set elsewhere must not be overwritten by mid-epoch detection",
	)
}

// TestEvaluateHardForkInitiationStability_IntraEraHFI_DoesNotSetKnown
// pins the era-boundary gate: TransitionKnown signals an upcoming era
// transition, not just any pparams bump. A HardForkInitiation that
// proposes a new ProtocolVersion still inside the current era's
// version range (e.g. Plomin's pv9 → pv10, both Conway) is an
// intra-era bump and must not be surfaced as TransitionKnown — clients
// would otherwise see era-history responses claiming the era ends at
// epoch+1 when in fact the era continues.
//
// The check matches the era-filter the boundary path's
// IsHardForkTransition applies, so mid-epoch detection and the
// boundary's enactment dispatch agree on what counts as a transition.
func TestEvaluateHardForkInitiationStability_IntraEraHFI_DoesNotSetKnown(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9 /* Conway, bootstrap */)
	// Target major 10 — still in Conway (Conway covers pv9-pv10).
	// A ratifiable proposal here represents an intra-era pparams
	// bump, not an era transition.
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 10,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	awaitHFIEvalIdle(t, ls)
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"intra-era HardForkInitiation must not be surfaced as TransitionKnown")
}

// TestEvaluateHardForkInitiationStability_UpgradesImpossibleToKnown pins
// the priority order: TransitionKnown is strictly more informative than
// TransitionImpossible (the latter only says "no transition this epoch
// before safe-zone end", the former says "transition will happen at
// epoch+1"). When both could apply, Known wins.
func TestEvaluateHardForkInitiationStability_UpgradesImpossibleToKnown(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}
	ls.transitionInfo = hardfork.NewTransitionImpossible()

	ls.evaluateHardForkInitiationStability()

	got := awaitTransitionInfo(t, ls, hardfork.TransitionKnown)
	assert.Equal(t, stabilityFixtureEpochID+1, got.KnownEpoch)
}

// Slot grid for the prune-floor fixture. The Shelley genesis used here
// (k=432, f=0.05) gives a stability window W of 3k/f = 25920 slots, so with a
// tip at pruneFixtureTipSlot the consumed-UTxO sweep prunes everything with
// deleted_slot <= pruneFixtureFloorSlot (140000-W).
//
// The at-tip rewind schedule then steps the ledger tip 140000 -> 114080 ->
// 88160. Attempt 2 asks for W/2 below the tip (127040) and findRewindPoint
// resolves that to the nearest committed block, 114080; attempt 3 asks for a
// full W below the *new* tip, 88160. That recomputation from the lowered tip
// is what carries the descent past the floor -- the per-attempt cap is a
// stability window, but the cumulative descent is not.
const (
	pruneFixtureStabilityWindow = 25_920
	pruneFixtureRootSlot        = 10_000
	pruneFixtureProducerSlot    = 50_000
	pruneFixtureDeepRewindSlot  = 88_160
	pruneFixtureConsumerSlot    = 110_000
	pruneFixtureFloorSlot       = 114_080
	pruneFixtureRetainedSlot    = 120_000
	pruneFixtureTipSlot         = 140_000
)

type prunedUtxoFixture struct {
	ls *LedgerState
	db *database.Database
	// prunedTxId is consumed at pruneFixtureConsumerSlot, at or below the
	// prune floor, so its row is hard-deleted by the consumed-UTxO sweep.
	prunedTxId []byte
	// retainedTxId is consumed above the prune floor, so its row survives the
	// sweep and rollback can still restore it. It is the control that keeps a
	// failure of the pruned probe from being read as a dead fixture.
	retainedTxId []byte
}

func newPrunedUtxoFixture(t *testing.T, mithrilLedgerSlot uint64) *prunedUtxoFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 1000}),
	)

	type fixtureBlock struct {
		slot   uint64
		number uint64
		hash   []byte
	}
	blocks := []fixtureBlock{
		{pruneFixtureRootSlot, 1, testHashBytes("3766-root")},
		{pruneFixtureProducerSlot, 2, testHashBytes("3766-producer")},
		{pruneFixtureDeepRewindSlot, 3, testHashBytes("3766-deep")},
		{pruneFixtureConsumerSlot, 4, testHashBytes("3766-consumer")},
		{pruneFixtureFloorSlot, 5, testHashBytes("3766-floor")},
		{pruneFixtureTipSlot, 6, testHashBytes("3766-tip")},
	}
	rawBlocks := make([]chain.RawBlock, 0, len(blocks))
	for i, b := range blocks {
		var prevHash []byte
		if i > 0 {
			prevHash = blocks[i-1].hash
		}
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        b.slot,
			Hash:        b.hash,
			BlockNumber: b.number,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(rawBlocks))

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	// Every fixture block was applied, so each carries a recorded nonce.
	for _, b := range blocks {
		require.NoError(
			t,
			db.SetBlockNonce(b.hash, b.slot, []byte("nonce-3766"), false, nil),
		)
	}

	// One epoch covering the whole fixture grid, so the era reload that
	// follows every rollback keeps the ledger in Conway and the stability
	// window at 3k/f rather than falling back to the Byron default.
	require.NoError(t, db.SetEpoch(
		0,
		1,
		[]byte("nonce-3766-epoch"),
		[]byte("evolving-3766"),
		[]byte("candidate-3766"),
		[]byte("last-3766"),
		eras.ConwayEraDesc.Id,
		1,
		1_000_000,
		nil,
	))

	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(pruneFixtureTipSlot, blocks[len(blocks)-1].hash),
		BlockNumber: blocks[len(blocks)-1].number,
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip
	ls.currentEra = eras.ConwayEraDesc
	ls.mithrilLedgerSlot = mithrilLedgerSlot
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()
	ls.syncUpstreamTipSlot.Store(pruneFixtureTipSlot)

	f := &prunedUtxoFixture{
		ls:           ls,
		db:           db,
		prunedTxId:   testHashBytes("3766-utxo-pruned"),
		retainedTxId: testHashBytes("3766-utxo-retained"),
	}
	seed := func(txId []byte, addedSlot, deletedSlot uint64) {
		mdTxn := db.MetadataTxn(true)
		require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
			return db.CreateUtxo(txn, &models.Utxo{
				TxId:        txId,
				OutputIdx:   0,
				AddedSlot:   addedSlot,
				DeletedSlot: deletedSlot,
				Amount:      types.Uint64(1_000_000),
			})
		}))
	}
	seed(f.prunedTxId, pruneFixtureProducerSlot, pruneFixtureConsumerSlot)
	seed(f.retainedTxId, pruneFixtureProducerSlot, pruneFixtureRetainedSlot)
	return f
}

// inLiveSet mirrors the probe used by the rollback tests: it asks
// the database.UtxoByRef lookup that LedgerView.UtxoById delegates to, so it
// exercises the deleted_slot filter that decides Conway bad-inputs and, through
// it, the consumed term of value conservation. A row seeded straight into
// metadata carries no blob CBOR, so ErrUtxoCborUnavailable counts as present;
// any error other than ErrUtxoNotFound is a lookup failure and fails the test.
func (f *prunedUtxoFixture) inLiveSet(t *testing.T, txId []byte) bool {
	t.Helper()
	var live bool
	txn := f.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, err := f.db.UtxoByRef(txId, 0, txn)
		switch {
		case err == nil, errors.Is(err, database.ErrUtxoCborUnavailable):
			live = true
			return nil
		case errors.Is(err, database.ErrUtxoNotFound):
			live = false
			return nil
		default:
			return err
		}
	}))
	return live
}

// driveAtTipRecovery runs the production at-tip recovery entry point the
// requested number of times against one persistent failure identity, which is
// what escalates the rewind schedule.
func (f *prunedUtxoFixture) driveAtTipRecovery(t *testing.T, rounds int) {
	t.Helper()
	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(
			pruneFixtureTipSlot+1,
			testHashBytes("3766-failing"),
		),
		TxHash: testHashBytes("3766-failing-tx"),
		Cause:  errors.New("bad input(s)"),
	}
	for i := range rounds {
		handled, err := f.ls.recoverAtTipFromTxValidationError(validationErr)
		require.NoError(t, err, "recovery round %d", i+1)
		require.True(t, handled, "recovery round %d", i+1)
	}
}

// assertLiveSetConsistentAtTip checks the invariant a rollback must preserve:
// at the ledger tip recovery settled on, every seeded output produced at or
// below that tip and consumed above it is resolvable, and every output already
// consumed at or below it is not. The second half is what keeps the fix from
// being "make lookups more permissive": refusing the rewind must not resurrect
// a genuinely spent output.
func (f *prunedUtxoFixture) assertLiveSetConsistentAtTip(t *testing.T) {
	t.Helper()
	tipSlot := f.ls.currentTip.Point.Slot
	for _, probe := range []struct {
		name        string
		txId        []byte
		addedSlot   uint64
		deletedSlot uint64
	}{
		{"pruned", f.prunedTxId, pruneFixtureProducerSlot, pruneFixtureConsumerSlot},
		{"retained", f.retainedTxId, pruneFixtureProducerSlot, pruneFixtureRetainedSlot},
	} {
		live := f.inLiveSet(t, probe.txId)
		switch {
		case probe.deletedSlot <= tipSlot:
			require.False(
				t,
				live,
				"%s output was consumed at slot %d, at or below ledger tip %d, and must not be in the live set",
				probe.name,
				probe.deletedSlot,
				tipSlot,
			)
		case probe.addedSlot <= tipSlot:
			require.True(
				t,
				live,
				"%s output produced at slot %d and consumed at slot %d must be in the live set at ledger tip %d",
				probe.name,
				probe.addedSlot,
				probe.deletedSlot,
				tipSlot,
			)
		}
	}
}

// TestAtTipRecoveryRewindBelowConsumedUtxoPruneFloor covers.
//
// cleanupConsumedUtxos hard-deletes consumed UTxO rows whose deleted_slot is at
// or below tip-stabilityWindow. database.TruncateAfterSlot restores consumed
// UTxOs with an UPDATE (deleted_slot > slot), so a rollback below that prune
// floor cannot restore anything the sweep already removed -- and used to report
// the ledger repaired anyway. The at-tip recovery rewind schedule reaches such
// a target because each escalating attempt rewinds a further stability window
// below the *current* tip while the prune floor stays fixed at the highest tip
// the node reached.
//
// Blocks the node applied cleanly then become unapplyable: their inputs resolve
// to nothing, which Conway reports as bad inputs and, because value
// conservation sums consumed over only the inputs that resolve, as value not
// conserved with consumed 0 in the same pass.
//
// The recovery schedule here walks 140000 -> 114080 -> 88160, and 88160 is
// below the 114080 sweep floor.
func TestAtTipRecoveryRewindBelowConsumedUtxoPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)

	require.Equal(
		t,
		uint64(pruneFixtureStabilityWindow),
		f.ls.calculateStabilityWindow(),
		"fixture slot grid assumes a 3k/f stability window",
	)

	// Production consumed-UTxO sweep at the highest tip the node reached.
	f.ls.cleanupConsumedUtxos()
	require.False(
		t,
		f.inLiveSet(t, f.prunedTxId),
		"output consumed at or below the prune floor must be hard-deleted by the sweep",
	)
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(pruneFixtureFloorSlot),
		floor,
		"the sweep must record how deep it removed spent rows",
	)

	f.driveAtTipRecovery(t, 3)

	// The live UTxO set must agree with the point recovery settled on.
	f.assertLiveSetConsistentAtTip(t)
	// ...which it can only do if the tip was never rewound below the point
	// from which the consumed-UTxO sweep can still restore state.
	require.GreaterOrEqual(
		t,
		f.ls.currentTip.Point.Slot,
		uint64(pruneFixtureFloorSlot),
		"recovery rewound the ledger below the consumed UTxO prune floor",
	)
	require.Positive(
		t,
		promtestutil.ToFloat64(f.ls.metrics.atTipRecoveryPruneFloorClamped),
		"the refused rewind must be visible to an operator",
	)
}

// TestRollbackBelowConsumedUtxoPruneFloorIsRefused pins the backstop every
// rewind path funnels through. Callers other than at-tip recovery -- a peer
// rollback, the durable-tip-floor repair, replay recovery -- reach
// LedgerState.rollback directly, and it must refuse before mutating anything
// rather than move the tip and report a repair it cannot perform.
func TestRollbackBelowConsumedUtxoPruneFloorIsRefused(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()
	tipBefore := f.ls.currentTip

	err := f.ls.rollback(
		ocommon.NewPoint(
			pruneFixtureDeepRewindSlot,
			testHashBytes("3766-deep"),
		),
	)
	require.ErrorIs(t, err, ErrRollbackBelowUtxoPruneFloor)
	require.Equal(
		t,
		tipBefore.Point,
		f.ls.currentTip.Point,
		"a refused rollback must leave the ledger tip where it was",
	)

	// A target at or above the floor is still allowed: the floor refuses the
	// rewinds it cannot restore, not every rewind.
	require.NoError(
		t,
		f.ls.rollback(
			ocommon.NewPoint(
				pruneFixtureFloorSlot,
				testHashBytes("3766-floor"),
			),
		),
	)
	require.Equal(
		t,
		uint64(pruneFixtureFloorSlot),
		f.ls.currentTip.Point.Slot,
	)
	require.True(
		t,
		f.inLiveSet(t, f.retainedTxId),
		"an output consumed above the prune floor must still be restored by an allowed rollback",
	)
}

// TestConsumedUtxoPruneFloorIsReadFromTheDatabase pins where the floor comes
// from. It is deliberately not mirrored in memory: a mirror is only refreshed
// after the sweep's transaction commits, so between commit and refresh it
// reports a lower floor than the database holds, and a rollback admitted on
// that stale value is exactly the divergence the floor exists to refuse.
func TestConsumedUtxoPruneFloorIsReadFromTheDatabase(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)

	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Zero(t, floor, "nothing has been swept yet")

	f.ls.cleanupConsumedUtxos()

	floor, err = f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(pruneFixtureFloorSlot), floor)

	// A floor written by another writer -- a prior run, or the sweep's own
	// transaction before any in-process cache could observe it -- is honored
	// immediately, with no reload step.
	require.NoError(t, f.db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(pruneFixtureRetainedSlot, 10),
		nil,
	))
	below, seen, err := f.ls.rollbackBelowConsumedUtxoPruneFloor(
		ocommon.NewPoint(pruneFixtureFloorSlot, nil),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(pruneFixtureRetainedSlot), seen)
	require.True(
		t,
		below,
		"the check must read the persisted floor, not a cached copy",
	)

	// A malformed value fails closed rather than reading as "nothing swept".
	require.NoError(t, f.db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey, "not-a-slot", nil,
	))
	_, _, err = f.ls.rollbackBelowConsumedUtxoPruneFloor(
		ocommon.NewPoint(pruneFixtureFloorSlot, nil),
	)
	require.Error(t, err)
	require.ErrorIs(
		t,
		f.ls.rollback(
			ocommon.NewPoint(
				pruneFixtureDeepRewindSlot,
				testHashBytes("3766-deep"),
			),
		),
		strconv.ErrSyntax,
		"an unreadable floor must refuse the rollback",
	)
}

// TestRollbackChainAndStateRefusesRedirectBelowPruneFloor covers the ordering
// hazard between the same-slot competitor redirect and the prune
// floor. rollbackChainAndStateDeferred truncates the primary chain and only then
// synchronizes the ledger. A target sitting exactly on the floor whose hash
// differs from the applied tip resolves to an applied ancestor strictly below
// the floor, so checking the unresolved point would admit it here and refuse it
// only after chain.Rollback had already run, splitting the chain from the
// ledger.
func TestRollbackChainAndStateRefusesRedirectBelowPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()

	// Put the applied ledger tip on a same-slot competitor at the floor, with
	// a recorded nonce so the redirect treats it as genuinely applied.
	competitorHash := testHashBytes("3766-floor-competitor")
	require.NoError(t, f.db.SetBlockNonce(
		competitorHash,
		pruneFixtureFloorSlot,
		[]byte("nonce-3766-competitor"),
		false,
		nil,
	))
	competitorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(pruneFixtureFloorSlot, competitorHash),
		BlockNumber: 5,
	}
	require.NoError(t, f.db.SetTip(competitorTip, nil))
	f.ls.currentTip = competitorTip
	f.ls.publishSnapshotsLocked()

	chainTipBefore := f.ls.chain.Tip().Point

	// The target's slot equals the floor, so an unresolved check passes; the
	// redirect resolves it below the floor.
	target := ocommon.NewPoint(
		pruneFixtureFloorSlot,
		testHashBytes("3766-floor"),
	)
	resolved, err := f.ls.resolveRollbackTarget(target, competitorTip)
	require.NoError(t, err)
	require.Less(
		t,
		resolved.Slot,
		uint64(pruneFixtureFloorSlot),
		"fixture must produce a redirect below the floor",
	)

	require.ErrorIs(
		t,
		f.ls.rollbackChainAndStateDeferred(target, nil),
		ErrRollbackBelowUtxoPruneFloor,
	)
	require.Equal(
		t,
		chainTipBefore,
		f.ls.chain.Tip().Point,
		"the primary chain must not be truncated for a refused rollback",
	)
	require.Equal(
		t,
		competitorTip.Point,
		f.ls.currentTip.Point,
		"the ledger tip must not move for a refused rollback",
	)
}

// TestUtxoPruningDeferredForCatchup pins both of utxoPruningDeferredForCatchup's
// defer conditions, plus the two cases that must NOT defer: it is the one
// change here that widens what Acquire accepts, so a mirror that drifts
// from cleanupConsumedUtxos' own two defer conditions fails open rather
// than closed. checkUtxoRetentionWindow calls this to decide whether to
// accept a point below the ordinary stability-window floor, so a false
// positive here (deferring when it shouldn't) would let Acquire accept a
// point cleanupConsumedUtxos might have already pruned.
func TestUtxoPruningDeferredForCatchup(t *testing.T) {
	t.Parallel()

	t.Run("no upstream tracked at all: not deferred", func(t *testing.T) {
		t.Parallel()
		ls := &LedgerState{}
		require.False(t, ls.utxoPruningDeferredForCatchup(1000, 50))
	})

	t.Run("active upstream, target not yet known: deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6101, 3291)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		// UpstreamSyncStatus is (0, true) here: a live active connection
		// with no admitted target yet -- "still syncing," per that
		// function's own doc comment.
		require.True(t, ls.utxoPruningDeferredForCatchup(1000, 50))
	})

	t.Run("active upstream, known target, far behind: deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6102, 3292)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		ls.publishActiveUpstream(connA)
		ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
			ConnectionId:      connA,
			SyncTarget:        ochainsync.Tip{Point: ocommon.NewPoint(1000, nil)},
			SyncTargetTrusted: true,
		})
		require.Equal(t, uint64(1000), ls.UpstreamTipSlot())
		// tipSlot 100 is 900 slots behind upstream's 1000, well outside a
		// 50-slot stability window.
		require.True(t, ls.utxoPruningDeferredForCatchup(100, 50))
	})

	t.Run("active upstream, known target, caught up: not deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6103, 3293)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		ls.publishActiveUpstream(connA)
		ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
			ConnectionId:      connA,
			SyncTarget:        ochainsync.Tip{Point: ocommon.NewPoint(1000, nil)},
			SyncTargetTrusted: true,
		})
		require.Equal(t, uint64(1000), ls.UpstreamTipSlot())
		// tipSlot 980 is within a 50-slot stability window of upstream's
		// 1000 -- caught up, pruning must proceed normally.
		require.False(t, ls.utxoPruningDeferredForCatchup(980, 50))
	})
}

func TestValidationReferenceSlotPrefersCurrentSlotWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, nil)
	if got != 125 {
		t.Fatalf("expected current slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsCurrentWhenEqual(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 125, nil)
	if got != 125 {
		t.Fatalf("expected shared slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotFallsBackToTipOnError(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, errors.New("clock unavailable"))
	if got != 100 {
		t.Fatalf("expected tip slot 100 on error, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsTipWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 100, nil)
	if got != 125 {
		t.Fatalf("expected tip slot 125, got %d", got)
	}
}

func TestHistoricalBlockValidationSkipsMithrilCoveredBlocks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		validationEnabled bool
		trustedReplay     bool
		chainsyncState    ChainsyncState
		blockSlot         uint64
		cutoffSlot        uint64
		mithrilLedgerSlot uint64
		shouldValidate    bool
		reachedTipRegion  bool
	}{
		{
			name:              "historical validation inside Mithril boundary",
			validationEnabled: true,
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         100,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			reachedTipRegion:  true,
		},
		{
			name:              "historical validation outside Mithril boundary",
			validationEnabled: true,
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         101,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			shouldValidate:    true,
			reachedTipRegion:  true,
		},
		{
			name:              "tip window inside Mithril boundary",
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         100,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			reachedTipRegion:  true,
		},
		{
			name:              "tip window outside Mithril boundary",
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         101,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			shouldValidate:    true,
			reachedTipRegion:  true,
		},
		{
			name:           "trusted replay",
			trustedReplay:  true,
			chainsyncState: SyncingChainsyncState,
			blockSlot:      100,
			cutoffSlot:     50,
		},
		{
			name:           "before tip window",
			chainsyncState: SyncingChainsyncState,
			blockSlot:      49,
			cutoffSlot:     50,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			shouldValidate, reachedTipRegion := historicalBlockValidationDecision(
				testCase.validationEnabled,
				testCase.trustedReplay,
				testCase.chainsyncState,
				testCase.blockSlot,
				testCase.cutoffSlot,
				testCase.mithrilLedgerSlot,
			)
			require.Equal(t, testCase.shouldValidate, shouldValidate)
			require.Equal(t, testCase.reachedTipRegion, reachedTipRegion)
		})
	}
}

// TestValidateChainSelectionHeaderCryptoDoesNotAdvanceEpochCache proves that,
// like ValidateBlockHeaderCrypto, verifying a header for chain selection
// never mutates the shared epoch cache -- an unauthenticated peer header must
// not be able to influence shared ledger state as a side effect of being
// observed for density.
func TestValidateChainSelectionHeaderCryptoDoesNotAdvanceEpochCache(
	t *testing.T,
) {
	t.Parallel()

	const futureSlot = uint64(1001)
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("tip"))},
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 1_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         []byte{0x01},
		}},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(
		&mockBabbageBlock{slot: futureSlot},
	)
	require.Error(t, err)
	assert.Len(
		t,
		ls.loadConsensusSnapshot().epochCache,
		1,
		"chain-selection header validation must not advance the shared epoch cache",
	)
}

// TestVerifyPointQueryable_UtxoFloorOnly_Rejected covers a gap the other
// TestVerifyPointQueryable* cases leave open: deleting the
// checkUtxoRetentionWindow call, or the queryShelleyCurrentProtocolParams
// call, from VerifyPointQueryable leaves every existing
// TestVerifyPointQueryable* case green --
// PastRetentionFloor_Rejected is rejected by the stake floor either way, and
// no fixture has a UTxO-floor rejection as its only failure. Identical to
// TestVerifyPointQueryable_WithinAllFloors_Accepted (on chain, within the
// stake/era windows, no CardanoNodeConfig so the network_state floor never
// runs) except for a durably persisted consumed-UTxO prune floor (400) above
// the pinned slot (350) -- only checkUtxoRetentionWindow can reject this
// point.
func TestVerifyPointQueryable_UtxoFloorOnly_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))
	require.NoError(t, ls.persistConsumedUtxoPruneFloor(400, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}
