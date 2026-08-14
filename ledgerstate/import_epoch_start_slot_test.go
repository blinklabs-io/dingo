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
			require.Equal(t, c.want, importedEpochStartSlot(cfg, c.epoch))
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
	require.Equal(t, uint64(1500), importedEpochStartSlot(cfg, 11))
	require.Equal(t, uint64(1000), importedEpochStartSlot(cfg, 10))
	require.Equal(t, uint64(1000), importedEpochStartSlot(cfg, 9),
		"without bounds there is nothing better than the era boundary")
}

// Bounds that do not reach back to genesis leave an epoch with no era to
// measure from. The current era's boundary is not a usable stand-in there:
// it sits after the epoch, so every registration made during that epoch
// would count as pre-epoch and the most recent would win -- the same
// one-epoch-early error, arrived at from the other side.
func TestImportedEpochStartSlotWidensBelowTheFirstEraBound(t *testing.T) {
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
	require.Equal(t, uint64(0), importedEpochStartSlot(cfg, 9),
		"an epoch before every known era bound must widen the window, not "+
			"borrow a boundary that follows it")
	require.Equal(t, uint64(1500), importedEpochStartSlot(cfg, 11),
		"epochs the bounds do cover are unaffected")
}
