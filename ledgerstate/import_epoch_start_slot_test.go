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
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// mark, set and go span three epochs, so an import landing in the first two
// epochs of a new era has set or go sitting in the era before it -- with a
// different epoch length and a different boundary slot. Deriving their start
// from the current era's bound puts the window edge in the wrong place, and
// for an epoch wholly before that bound it lands past the epoch entirely:
// every registration made during it then counts as pre-epoch, so the latest
// one wins and the epoch is seeded with parameters that only took effect
// afterwards. That is precisely the one-epoch-early error
// GetPoolRegistrationsEffectiveForEpoch exists to avoid, reintroduced at the
// window instead of the query.
func TestImportedEpochStartSlotUsesTheEpochsOwnEra(t *testing.T) {
	// Era 0 runs epochs 0..9 from slot 0 with 100-slot epochs; era 1 starts
	// at epoch 10, slot 1000, with 500-slot epochs.
	cfg := ImportConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 0, Epoch: 0},
				{Slot: 1000, Epoch: 10},
			},
			EraIndex:      1,
			EraBoundEpoch: 10,
			EraBoundSlot:  1000,
			Epoch:         11,
		},
		EpochLength: func(era uint) (uint, uint, error) {
			switch era {
			case 0:
				return 1, 100, nil
			case 1:
				return 1, 500, nil
			}
			return 0, 0, errors.New("unknown era")
		},
	}

	for _, c := range []struct {
		name  string
		epoch uint64
		want  uint64
	}{
		{"mark, in the current era", 11, 1500},
		{"set, the current era's first epoch", 10, 1000},
		{"go, in the previous era", 9, 900},
		{"an earlier epoch of the previous era", 3, 300},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := importedEpochStartSlot(cfg, c.epoch)
			require.True(t, ok, "this epoch is covered by the era bounds")
			require.Equal(t, c.want, got)
		})
	}
}

// With no era bounds to consult, the current era's boundary is all there is.
// It stays a usable window edge -- registrations before it fall on the
// pre-epoch side, where the most recent wins -- but it is a fallback, not the
// epoch's real start, so it must not be reached when bounds are available.
func TestImportedEpochStartSlotFallsBackWithoutEraBounds(t *testing.T) {
	cfg := ImportConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			EraIndex:      1,
			EraBoundEpoch: 10,
			EraBoundSlot:  1000,
			Epoch:         11,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	for _, c := range []struct {
		epoch uint64
		want  uint64
	}{
		{11, 1500},
		{10, 1000},
	} {
		got, ok := importedEpochStartSlot(cfg, c.epoch)
		require.True(t, ok)
		require.Equal(t, c.want, got)
	}
	// Epoch 9 began before this era did, and without bounds there is nothing
	// describing where. The era boundary is not a stand-in: it follows the
	// epoch, so every registration made during it would count as pre-epoch.
	_, ok := importedEpochStartSlot(cfg, 9)
	require.False(t, ok,
		"an epoch the current era's arithmetic cannot reach has no window")
}

// Bounds that do not reach back to genesis leave an epoch with no era to
// measure from, and neither available guess is safe. The current era's
// boundary sits after the epoch, so every registration made during it counts
// as pre-epoch and the newest wins. Widening to zero is the mirror image:
// they all look in-epoch, so the pool's earliest registration wins and a
// re-registration made before the target epoch is ignored. Both seed rewards
// from parameters that were not in force, so neither is offered.
func TestImportedEpochStartSlotHasNoWindowBelowTheFirstEraBound(t *testing.T) {
	cfg := ImportConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			EraBounds:     []EraBound{{Slot: 1000, Epoch: 10}},
			EraIndex:      0,
			EraBoundEpoch: 10,
			EraBoundSlot:  1000,
			Epoch:         11,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	_, ok := importedEpochStartSlot(cfg, 9)
	require.False(t, ok,
		"an epoch before every known era bound has no window: the boundary "+
			"that follows it is too late, and widening to zero is too early")
	got, ok := importedEpochStartSlot(cfg, 11)
	require.True(t, ok, "epochs the bounds do cover are unaffected")
	require.Equal(t, uint64(1500), got)
}
