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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

const previewHistoricalPParamsSnapshotEpoch = uint64(1397)

// A Preview snapshot in epoch 1397 seeds Mark/Set/Go reward bases for
// 1397/1396/1395. The first boundary into 1398 consumes the Go basis and
// evaluates epoch 1396's block performance, so both the snapshot's previous
// and current parameters must survive the import as distinct historical rows.
func TestImportPParamsPersistsPreviewRewardHistory(t *testing.T) {
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

// An imported Go basis whose historical parameters are unavailable must be
// left ineligible by snapshot seeding. The pparams phase still persists the
// usable current parameters instead of turning one skipped reward round into a
// permanently failing bootstrap.
func TestImportPParamsStoresCurrentWithoutUnavailableHistory(t *testing.T) {
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

func TestImportPParamsReentryUsesStoredCrossEraHistory(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	// Model the first Conway epoch. GovState's previous field has already been
	// translated to Conway, while reward performance for E-1 still needs a
	// Babbage row. A prior pass stored that exact historical row.
	seedRewardBasisMarkers(t, db, 1395)
	current, translatedPrevious := distinctConwayPParams(t)
	storedPrevious, err := cbor.Encode(testBabbagePParams())
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetPParams(
		storedPrevious, 99_999, 1396, EraBabbage, nil,
	))
	cfg := previewPParamsImportConfig(db, current, translatedPrevious)
	cfg.State.EraBoundEpoch = previewHistoricalPParamsSnapshotEpoch
	cfg.State.EraBounds[EraConway] = EraBound{
		Slot:  100_000,
		Epoch: previewHistoricalPParamsSnapshotEpoch,
	}

	for range 2 {
		require.NoError(t, importPParams(context.Background(), cfg))

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraBabbage, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, previousRows, 1)
		require.Equal(t, storedPrevious, previousRows[0].Cbor)
		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, currentRows, 1,
			"re-entry must not duplicate an already-satisfying current row")
		require.Equal(t, current, currentRows[0].Cbor)
	}
}

func TestImportPParamsSkipsTranslatedCrossEraHistory(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, previous := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, previous)
	// Model an import at the first Conway epoch. GovState's previous payload
	// is translated to the current-era shape, but the performance epoch is
	// still Babbage. Persisting it as Babbage would not be exact, so reject it.
	cfg.State.EraBoundEpoch = previewHistoricalPParamsSnapshotEpoch
	cfg.State.EraBounds[EraConway] = EraBound{
		Slot:  100_000,
		Epoch: previewHistoricalPParamsSnapshotEpoch,
	}

	require.NoError(t, importPParams(context.Background(), cfg))

	previousRows, queryErr := db.Metadata().GetPParams(
		1396, EraBabbage, nil,
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

func TestImportSnapShotsSkipsGoBasisWithoutCrossEraHistory(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.GreaterOrEqual(t, state.Epoch, uint64(2))
	current, translatedPrevious := distinctConwayPParams(t)
	state.PParamsData = current
	state.PrevPParamsData = translatedPrevious
	state.EraIndex = EraConway
	state.EraBoundEpoch = state.Epoch
	state.EraBoundSlot = state.Tip.Slot
	state.EraBounds = previewEraBounds()
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
	// Reproduce the partial state left by the old importer: the provisional Go
	// basis committed before the pparams phase discovered that its translated
	// previous payload could not be decoded as the old era.
	require.NoError(t, importSnapShots(
		context.Background(), cfg, state.Tip.Slot, noProgress, false,
	))
	preexistingGo, err := db.Metadata().GetRewardSnapshot(
		state.Epoch-2, "mark", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, preexistingGo)
	require.False(t, preexistingGo.Authoritative)
	preexistingPools, err := db.Metadata().GetRewardPoolInputs(
		state.Epoch-2, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, preexistingPools)

	state.EraBounds[EraConway] = EraBound{
		Slot:  state.Tip.Slot,
		Epoch: state.Epoch,
	}
	require.NoError(t, importSnapShots(
		context.Background(), cfg, state.Tip.Slot, noProgress, false,
	))

	goBasis, err := db.Metadata().GetRewardSnapshot(
		state.Epoch-2, "mark", nil,
	)
	require.NoError(t, err)
	require.Nil(t, goBasis,
		"the Go basis cannot be consumed without old-era historical pparams")
	goPools, err := db.Metadata().GetRewardPoolInputs(state.Epoch-2, nil)
	require.NoError(t, err)
	require.Empty(t, goPools)
	goStake, err := db.Metadata().GetRewardStakeInputs(state.Epoch-2, nil)
	require.NoError(t, err)
	require.Empty(t, goStake)
	for _, epoch := range []uint64{state.Epoch - 1, state.Epoch} {
		basis, queryErr := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, queryErr)
		require.NotNil(t, basis,
			"epoch %d does not depend on unavailable old-era history", epoch)
	}

	require.NoError(t, importPParams(context.Background(), cfg))
	previousRows, err := db.Metadata().GetPParams(
		state.Epoch-1, EraBabbage, nil,
	)
	require.NoError(t, err)
	require.Empty(t, previousRows)
	currentRows, err := db.Metadata().GetPParams(
		state.Epoch, EraConway, nil,
	)
	require.NoError(t, err)
	require.Len(t, currentRows, 1)
}

func TestImportSnapShotsPreservesAuthoritativeRewardBasis(t *testing.T) {
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
