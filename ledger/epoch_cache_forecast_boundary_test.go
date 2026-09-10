// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/require"
)

const (
	forecastBoundaryEpoch      = uint64(4)
	forecastByronEpochLength   = uint(21_600)
	forecastShelleyEpochLength = uint(432_000)
	forecastBoundaryStartSlot  = uint64(64_800)
	forecastWithinEraStartSlot = uint64(43_200)
)

func newEpochCacheForecastLedger(
	t *testing.T,
	epoch models.Epoch,
	transition hardfork.TransitionInfo,
	configuredBoundary bool,
) *LedgerState {
	t.Helper()
	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = strings.Repeat("01", 32)
	if configuredBoundary {
		enabled := true
		boundary := forecastBoundaryEpoch
		cfg.ExperimentalHardForksEnabled = &enabled
		cfg.TestShelleyHardForkAtEpoch = &boundary
	}
	ls := &LedgerState{
		currentEpoch:   epoch,
		currentEra:     eras.ByronEraDesc,
		epochCache:     []models.Epoch{epoch},
		transitionInfo: transition,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

func TestAdvanceEpochCacheRejectsHardForkBoundary(t *testing.T) {
	require.NotEqual(t, forecastByronEpochLength, forecastShelleyEpochLength,
		"fixture must expose the previous-era length overlap")
	lastByronEpoch := models.Epoch{
		EpochId:       forecastBoundaryEpoch - 1,
		StartSlot:     forecastBoundaryStartSlot,
		LengthInSlots: forecastByronEpochLength,
		SlotLength:    20_000,
		EraId:         eras.ByronEraDesc.Id,
	}

	for _, tc := range []struct {
		name               string
		transition         hardfork.TransitionInfo
		configuredBoundary bool
	}{
		{
			name:       "confirmed transition",
			transition: hardfork.NewTransitionKnown(forecastBoundaryEpoch),
		},
		{
			name:               "configured epoch trigger",
			transition:         hardfork.NewTransitionUnknown(),
			configuredBoundary: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls := newEpochCacheForecastLedger(
				t, lastByronEpoch, tc.transition, tc.configuredBoundary,
			)

			err := ls.advanceEpochCache()
			require.ErrorContains(t, err, "hard-fork boundary")
			require.ErrorIs(t, err, errEpochCacheForecastBoundary)
			require.Len(t, ls.loadConsensusSnapshot().epochCache, 1,
				"forecast must not publish a previous-era boundary row")

			_, err = ls.epochForSlot(
				lastByronEpoch.StartSlot + uint64(lastByronEpoch.LengthInSlots),
			)
			require.Error(t, err,
				"post-fork slot must remain uncovered until full rollover")
		})
	}
}

func TestAdvanceEpochCachePreservesWithinEraForecast(t *testing.T) {
	lastByronEpoch := models.Epoch{
		EpochId:       forecastBoundaryEpoch - 2,
		StartSlot:     forecastWithinEraStartSlot,
		LengthInSlots: forecastByronEpochLength,
		SlotLength:    20_000,
		EraId:         eras.ByronEraDesc.Id,
	}
	ls := newEpochCacheForecastLedger(
		t,
		lastByronEpoch,
		hardfork.NewTransitionKnown(forecastBoundaryEpoch),
		true,
	)

	require.NoError(t, ls.advanceEpochCache())
	cache := ls.loadConsensusSnapshot().epochCache
	require.Len(t, cache, 2)
	forecast := cache[1]
	require.Equal(t, forecastBoundaryEpoch-1, forecast.EpochId)
	require.Equal(t, eras.ByronEraDesc.Id, forecast.EraId)
	require.Equal(t, forecastByronEpochLength, forecast.LengthInSlots)

	got, err := ls.epochForSlot(forecast.StartSlot)
	require.NoError(t, err)
	require.Equal(t, forecast, got)
}

func TestHeaderVerificationEpochDefersAtHardForkBoundary(t *testing.T) {
	lastByronEpoch := models.Epoch{
		EpochId:       forecastBoundaryEpoch - 1,
		StartSlot:     forecastBoundaryStartSlot,
		LengthInSlots: forecastByronEpochLength,
		SlotLength:    20_000,
		EraId:         eras.ByronEraDesc.Id,
	}
	ls := newEpochCacheForecastLedger(
		t,
		lastByronEpoch,
		hardfork.NewTransitionKnown(forecastBoundaryEpoch),
		false,
	)

	_, err := ls.headerVerificationEpoch(
		lastByronEpoch.StartSlot+uint64(lastByronEpoch.LengthInSlots),
		true,
	)
	require.ErrorContains(t, err, "hard-fork boundary")
	require.ErrorIs(t, err, errHeaderVerificationDeferred,
		"boundary wait must not be classified as an honest-peer fault")
	require.Len(t, ls.loadConsensusSnapshot().epochCache, 1)
}
