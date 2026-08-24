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
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestEraTransitionPathAllowsPrimeBoundaryPair(t *testing.T) {
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

func TestEraTransitionsRunAfterSourceEraPParamEnactment(t *testing.T) {
	path := []uint{eras.BabbageEraDesc.Id}
	before, after := splitEraTransitionsForRollover(path)

	require.Empty(t, before,
		"successor transitions must not replace the source era before rollover")
	require.Equal(t, path, after,
		"the successor transition must run after source-era pparam enactment")
}

func TestEraTransitionPathRejectsLargerJump(t *testing.T) {
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
	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.AlonzoEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		eras.BabbageEraDesc.MinMajorVersion,
		true,
	)
	require.Equal(t, eras.AlonzoEraDesc.Id, target,
		"an Alonzo block remains Alonzo even when its header advertises protocol major 7")
	require.False(t, allowTwoTransitions)
}

func TestBoundaryEraForBlockRejectsNonAdjacentHeaderEra(t *testing.T) {
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

	wantSlotLength, wantEpochLength, err := eras.AllegraEraDesc.EpochLengthFunc(ls.config.CardanoNodeConfig)
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
