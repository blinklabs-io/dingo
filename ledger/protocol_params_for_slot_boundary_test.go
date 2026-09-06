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
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

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
	const (
		byronEpochs       = 2
		byronEpochLength  = uint(100)
		shelleyEpoch      = uint64(207)
		shelleyEpochLen   = uint(432)
		byronEndSlot      = uint64(byronEpochs) * uint64(byronEpochLength)
		currentEpochStart = byronEndSlot + (shelleyEpoch-2)*uint64(shelleyEpochLen)
		boundarySlot      = currentEpochStart + uint64(shelleyEpochLen)
	)

	cfg := newMultiEraForecastCfg(t, shelleyEpoch+1)
	epochCache := make([]models.Epoch, 0, int(shelleyEpoch)+1)
	for epoch := uint64(0); epoch < byronEpochs; epoch++ {
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
	require.Equal(t, uint(3), gotShelley.ProtocolMajor,
		"the first Shelley slot after a Byron prefix must forecast the scheduled fork")
}

// TestProtocolParamsForSlot_FallbackProjectsFromCurrentEpoch verifies the
// bounded-summary error path. A slot beyond the forecast horizon still needs
// an epoch estimate, and that estimate must retain the absolute epoch offset
// introduced by earlier eras.
func TestProtocolParamsForSlot_FallbackProjectsFromCurrentEpoch(t *testing.T) {
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
				LengthInSlots: uint(byronEpochLength), EraId: eras.ByronEraDesc.Id},
			{EpochId: 1, StartSlot: byronEpochLength, SlotLength: 20_000,
				LengthInSlots: uint(byronEpochLength), EraId: eras.ByronEraDesc.Id},
			{EpochId: currentEpoch, StartSlot: currentStart, SlotLength: 1_000,
				LengthInSlots: uint(shelleyEpochLen), EraId: eras.ShelleyEraDesc.Id},
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

func newMultiEraForecastCfg(t *testing.T, forkEpoch uint64) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadByronGenesisFromReader(strings.NewReader(`{
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
// the regression guard for issue #3061. Preview launches federated (Shelley
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
	cfg := newShelleyUpdateQuorum1Cfg(t)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	// Seed a pending pparam-update proposal submitted in epoch 0 that lowers
	// decentralization from 1 to 1/2, from a single genesis-key delegate
	// (shelley genesis updateQuorum = 1). Per the Shelley update system a
	// proposal carries its submission epoch (0) and is enacted as epoch 1's
	// parameters at the epoch 0->1 boundary.
	updateCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameterUpdate{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 2)},
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
	require.NoError(t, cfg.LoadByronGenesisFromReader(strings.NewReader(`{
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
	require.NoError(t, cfg.LoadByronGenesisFromReader(strings.NewReader(`{
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
	require.NoError(t, cfg.LoadByronGenesisFromReader(strings.NewReader(`{
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
