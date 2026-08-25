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

// If the imported Go reward basis is present, omitting previous pparams is an
// incomplete import, not permission to stamp the current payload onto the
// historical epoch. Repeating the failed phase must return the same error and
// leave both rows absent, proving validation happens before the atomic write.
func TestImportPParamsFailsClosedWithoutRequiredHistory(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, _ := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, nil)

	want := "historical protocol parameters for epoch 1396 are required " +
		"by the imported reward basis for epoch 1395"
	for range 2 {
		err := importPParams(context.Background(), cfg)
		require.EqualError(t, err, want)

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Empty(t, previousRows)

		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Empty(t, currentRows,
			"the current row must not partially commit before history fails")
	}
}

func TestImportPParamsGoBasisRequiresCurrentParameters(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	// The imported Go basis alone is enough to make both sides of the first
	// post-import reward calculation mandatory: E-1 for performance and E for
	// calculation. Deliberately omit the Set basis and current pparams.
	seedRewardBasisMarkers(t, db, 1395)
	_, previous := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, nil, previous)

	want := "protocol parameters for epoch 1397 are required " +
		"by the imported reward basis for epoch 1395"
	for range 2 {
		err := importPParams(context.Background(), cfg)
		require.EqualError(t, err, want)

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Empty(t, previousRows,
			"historical pparams must not partially commit before current fails")
		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Empty(t, currentRows)
	}
}

func TestImportPParamsValidatesHistoryAgainstItsEpochEra(t *testing.T) {
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

	err = importPParams(context.Background(), cfg)
	require.ErrorContains(t, err,
		"validating historical protocol parameters for epoch 1396")
	require.ErrorContains(t, err, "Babbage")

	previousRows, queryErr := db.Metadata().GetPParams(
		1396, EraBabbage, nil,
	)
	require.NoError(t, queryErr)
	require.Empty(t, previousRows)
	currentRows, queryErr := db.Metadata().GetPParams(
		1397, EraConway, nil,
	)
	require.NoError(t, queryErr)
	require.Empty(t, currentRows,
		"cross-era validation must happen before either row is written")
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
