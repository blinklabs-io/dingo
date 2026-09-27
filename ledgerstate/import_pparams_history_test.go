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
	"context"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lalonzo "github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
)

const previewHistoricalPParamsSnapshotEpoch = uint64(1397)

// A Preview snapshot in epoch 1397 seeds Mark/Set/Go reward bases for
// 1397/1396/1395. The first boundary into 1398 consumes the Go basis and
// evaluates epoch 1396's block performance, so both the snapshot's previous
// and current parameters must survive the import as distinct historical rows.
func TestImportPParamsPersistsPreviewRewardHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, previous := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, previous)

	require.NoError(t, importPParams(context.Background(), cfg))

	previousRows, err := db.Metadata().GetPParams(1396, EraConway, nil)
	require.NoError(t, err)
	require.Len(t, previousRows, 1)
	require.Equal(t, previous, previousRows[0].Cbor)
	require.Equal(t, uint64(1396), previousRows[0].Epoch)

	currentRows, err := db.Metadata().GetPParams(1397, EraConway, nil)
	require.NoError(t, err)
	require.Len(t, currentRows, 1)
	require.Equal(t, current, currentRows[0].Cbor)
	require.Equal(t, uint64(1397), currentRows[0].Epoch)
	require.NotEqual(t, previousRows[0].Cbor, currentRows[0].Cbor,
		"current parameters must not substitute for the historical epoch")
}

// Without historical parameters the pparams phase still persists the usable
// current row; deciding whether the Go basis can run without them belongs to
// snapshot seeding, which fails the import in that case.
func TestImportPParamsStoresCurrentWithoutUnavailableHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, _ := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, nil)

	for range 2 {
		require.NoError(t, importPParams(context.Background(), cfg))

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Empty(t, previousRows)

		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, currentRows, 1)
		require.Equal(t, current, currentRows[0].Cbor)
	}
}

// TestImportPParamsReentryUsesStoredCrossEraHistory models a Babbage snapshot
// in the first Babbage epoch. Its translated previous parameters cannot be
// downgraded to Alonzo, so a valid Alonzo row a prior pass stored is what
// satisfies epoch 1396, and re-entry must leave it in place.
func TestImportPParamsReentryUsesStoredCrossEraHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedRewardBasisMarkers(t, db, 1395)
	current, err := cbor.Encode(testBabbagePParams())
	require.NoError(t, err)
	translatedPreviousParams := testBabbagePParams()
	translatedPreviousParams.MinFeeA++
	translatedPrevious, err := cbor.Encode(translatedPreviousParams)
	require.NoError(t, err)
	storedPrevious, err := cbor.Encode(testAlonzoPParams())
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetPParams(
		storedPrevious, 99_999, 1396, EraAlonzo, nil,
	))
	cfg := previewPParamsImportConfig(db, current, translatedPrevious)
	cfg.State.EraIndex = EraBabbage
	cfg.State.EraBoundEpoch = previewHistoricalPParamsSnapshotEpoch
	cfg.State.EraBounds = cfg.State.EraBounds[:EraBabbage+1]
	cfg.State.EraBounds[EraBabbage] = EraBound{
		Slot:  100_000,
		Epoch: previewHistoricalPParamsSnapshotEpoch,
	}

	for range 2 {
		require.NoError(t, importPParams(context.Background(), cfg))
		require.NoError(t, validateImportedRewardPParams(cfg, nil, 1395))

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraAlonzo, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, previousRows, 1)
		require.Equal(t, storedPrevious, previousRows[0].Cbor)
		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraBabbage, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, currentRows, 1)
		require.Equal(t, current, currentRows[0].Cbor)
	}
}

// TestValidateImportedRewardPParamsFailsWhenPreviousEraCannotBeRecovered keeps
// the fail-closed rule for the one gap the snapshot cannot fill: without a
// stored Alonzo row, epoch 1396 of a first-Babbage-epoch snapshot has no
// parameters, since downgradeBabbagePParams needs d and extraEntropy.
func TestValidateImportedRewardPParamsFailsWhenPreviousEraCannotBeRecovered(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	current, err := cbor.Encode(testBabbagePParams())
	require.NoError(t, err)
	cfg := previewPParamsImportConfig(db, current, current)
	cfg.State.EraIndex = EraBabbage
	cfg.State.EraBoundEpoch = previewHistoricalPParamsSnapshotEpoch
	cfg.State.EraBounds = cfg.State.EraBounds[:EraBabbage+1]
	cfg.State.EraBounds[EraBabbage] = EraBound{
		Slot:  100_000,
		Epoch: previewHistoricalPParamsSnapshotEpoch,
	}
	require.NoError(t, importPParams(context.Background(), cfg))

	require.NoError(t, validateImportedRewardPParams(cfg, nil, 1396))
	err = validateImportedRewardPParams(cfg, nil, 1395)
	require.ErrorIs(t, err, errRewardPParamsUnavailable)
	require.ErrorContains(t, err,
		"historical protocol parameters for epoch 1396 are unavailable in era Alonzo")
	require.ErrorContains(t, err, "Babbage-to-Alonzo")
	rows, err := db.Metadata().GetPParams(1396, EraAlonzo, nil)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestImportPParamsDowngradesTranslatedCrossEraHistory models an import in
// the first Conway epoch. The hard fork translated GovState's previous
// parameters to Conway while epoch 1396 is still Babbage, so the row that
// epoch's reward round reads is the ledger's downgrade of that payload.
func TestImportPParamsDowngradesTranslatedCrossEraHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, _ := distinctConwayPParams(t)
	babbageParams, translatedParams := translatedBabbagePParams()
	translatedPrevious, err := cbor.Encode(translatedParams)
	require.NoError(t, err)
	cfg := previewPParamsImportConfig(db, current, translatedPrevious)
	cfg.State.EraBoundEpoch = previewHistoricalPParamsSnapshotEpoch
	cfg.State.EraBounds[EraConway] = EraBound{
		Slot:  100_000,
		Epoch: previewHistoricalPParamsSnapshotEpoch,
	}

	var firstRowID uint
	for pass := range 2 {
		require.NoError(t, importPParams(context.Background(), cfg))
		require.NoError(t, validateImportedRewardPParams(cfg, nil, 1395))

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraBabbage, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, previousRows, 1)
		require.Equal(t, uint64(1396), previousRows[0].Epoch)
		decoded, decodeErr := eras.DecodePParamsBabbage(previousRows[0].Cbor)
		require.NoError(t, decodeErr)
		wantPrevious := *babbageParams
		wantPrevious.CostModels = translatedParams.CostModels
		require.Equal(t, &wantPrevious, decoded)
		if pass == 0 {
			firstRowID = previousRows[0].ID
		}
		require.Equal(t, firstRowID, previousRows[0].ID,
			"re-entry must not rewrite an already-satisfying historical row")

		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, currentRows, 1)
		require.Equal(t, current, currentRows[0].Cbor)
	}
}

// TestImportSnapShotsSeedsGoBasisInFirstEpochOfEra imports the real snapshot
// fixture positioned in the first Conway epoch, whose Go round reads the
// Babbage epoch before it. The snapshot's translated prevPParams supply that
// epoch, so the basis seeds and the Babbage row is written.
func TestImportSnapShotsSeedsGoBasisInFirstEpochOfEra(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.GreaterOrEqual(t, state.Epoch, uint64(2))
	current, _ := distinctConwayPParams(t)
	babbageParams, translatedParams := translatedBabbagePParams()
	translatedPrevious, err := cbor.Encode(translatedParams)
	require.NoError(t, err)
	state.PParamsData = current
	state.PrevPParamsData = translatedPrevious
	state.EraIndex = EraConway
	state.EraBoundEpoch = state.Epoch
	state.EraBoundSlot = state.Tip.Slot
	state.EraBounds = previewEraBounds()
	state.EraBounds[EraConway] = EraBound{
		Slot:  state.Tip.Slot,
		Epoch: state.Epoch,
	}
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: state,
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	_, err = importCertState(
		context.Background(), cfg, state.Tip.Slot, noProgress,
	)
	require.NoError(t, err)
	require.NoError(t, importSnapShots(
		context.Background(), cfg, state.Tip.Slot, noProgress, false,
	))
	goBasis, err := db.Metadata().GetRewardSnapshot(
		state.Epoch-2, "mark", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, goBasis)
	goPools, err := db.Metadata().GetRewardPoolInputs(state.Epoch-2, nil)
	require.NoError(t, err)
	require.NotEmpty(t, goPools)

	require.NoError(t, importPParams(context.Background(), cfg))
	previousRows, err := db.Metadata().GetPParams(
		state.Epoch-1, EraBabbage, nil,
	)
	require.NoError(t, err)
	require.Len(t, previousRows, 1)
	decoded, err := eras.DecodePParamsBabbage(previousRows[0].Cbor)
	require.NoError(t, err)
	wantPrevious := *babbageParams
	wantPrevious.CostModels = translatedParams.CostModels
	require.Equal(t, &wantPrevious, decoded)
	currentRows, err := db.Metadata().GetPParams(
		state.Epoch, EraConway, nil,
	)
	require.NoError(t, err)
	require.Len(t, currentRows, 1)
}

func TestImportSnapShotsPreservesAuthoritativeRewardBasis(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: state,
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	_, err = importCertState(
		context.Background(), cfg, state.Tip.Slot, noProgress,
	)
	require.NoError(t, err)

	const retainedPoolCount = uint64(999)
	require.NoError(t, db.Metadata().SaveRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:          state.Epoch - 2,
			SnapshotType:   "mark",
			TotalPoolCount: retainedPoolCount,
			Authoritative:  true,
		},
		nil,
	))
	require.NoError(t, importSnapShots(
		context.Background(), cfg, state.Tip.Slot, noProgress, false,
	))

	basis, err := db.Metadata().GetRewardSnapshot(
		state.Epoch-2, "mark", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, basis)
	require.True(t, basis.Authoritative)
	require.Equal(t, retainedPoolCount, basis.TotalPoolCount)
}

func seedPreviewRewardBases(t *testing.T, db *database.Database) {
	t.Helper()
	seedRewardBasisMarkers(t, db, 1395, 1396, 1397)
}

func seedRewardBasisMarkers(
	t *testing.T,
	db *database.Database,
	epochs ...uint64,
) {
	t.Helper()
	for _, epoch := range epochs {
		require.NoError(t, db.Metadata().SaveRewardSnapshot(
			&models.RewardSnapshot{
				Epoch:        epoch,
				SnapshotType: "mark",
			},
			nil,
		))
	}
}

func distinctConwayPParams(t *testing.T) (current, previous []byte) {
	t.Helper()
	currentParams := testConwayPParams()
	previousParams := *currentParams
	previousParams.MinFeeA++

	var err error
	current, err = cbor.Encode(currentParams)
	require.NoError(t, err)
	previous, err = cbor.Encode(&previousParams)
	require.NoError(t, err)
	return current, previous
}

func previewPParamsImportConfig(
	db *database.Database,
	current []byte,
	previous []byte,
) ImportConfig {
	return ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			PParamsData:     current,
			PrevPParamsData: previous,
			Epoch:           previewHistoricalPParamsSnapshotEpoch,
			EraIndex:        EraConway,
			EraBoundEpoch:   1200,
			EraBoundSlot:    100_000,
			EraBounds:       previewEraBounds(),
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}
}

func previewEraBounds() []EraBound {
	// Preview starts several historical eras at epoch zero. The last bound at
	// or before a target epoch therefore has to win, just as it does for a real
	// imported telescope.
	return []EraBound{
		{Slot: 0, Epoch: 0}, // Byron
		{Slot: 0, Epoch: 0}, // Shelley
		{Slot: 0, Epoch: 0}, // Allegra
		{Slot: 0, Epoch: 0}, // Mary
		{Slot: 0, Epoch: 0}, // Alonzo
		{Slot: 0, Epoch: 0}, // Babbage
		{Slot: 0, Epoch: 0}, // Conway
	}
}

func testAlonzoPParams() *lalonzo.AlonzoProtocolParameters {
	babbageParams := testBabbagePParams()
	return &lalonzo.AlonzoProtocolParameters{
		MinFeeA:              babbageParams.MinFeeA,
		MinFeeB:              babbageParams.MinFeeB,
		MaxBlockBodySize:     babbageParams.MaxBlockBodySize,
		MaxTxSize:            babbageParams.MaxTxSize,
		MaxBlockHeaderSize:   babbageParams.MaxBlockHeaderSize,
		KeyDeposit:           babbageParams.KeyDeposit,
		PoolDeposit:          babbageParams.PoolDeposit,
		MaxEpoch:             babbageParams.MaxEpoch,
		NOpt:                 babbageParams.NOpt,
		A0:                   babbageParams.A0,
		Rho:                  babbageParams.Rho,
		Tau:                  babbageParams.Tau,
		Decentralization:     &cbor.Rat{Rat: big.NewRat(0, 1)},
		ProtocolMajor:        6,
		MinPoolCost:          babbageParams.MinPoolCost,
		AdaPerUtxoByte:       34482,
		CostModels:           babbageParams.CostModels,
		ExecutionCosts:       babbageParams.ExecutionCosts,
		MaxTxExUnits:         babbageParams.MaxTxExUnits,
		MaxBlockExUnits:      babbageParams.MaxBlockExUnits,
		MaxValueSize:         babbageParams.MaxValueSize,
		CollateralPercentage: babbageParams.CollateralPercentage,
		MaxCollateralInputs:  babbageParams.MaxCollateralInputs,
	}
}
