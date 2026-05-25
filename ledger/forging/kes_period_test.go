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

package forging

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

func synthGenesis(slotsPerKES, maxKES int, slotLen time.Duration, systemStart time.Time) *shelley.ShelleyGenesis {
	rat := big.NewRat(int64(slotLen), int64(time.Second))
	return &shelley.ShelleyGenesis{
		SystemStart:       systemStart,
		SlotsPerKESPeriod: slotsPerKES,
		MaxKESEvolutions:  maxKES,
		SlotLength:        common.GenesisRat{Rat: rat},
	}
}

func TestCurrentKESPeriod_BeforeSystemStart(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(100, 5, time.Second, systemStart)
	got, err := CurrentKESPeriod(g, systemStart.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CurrentKESPeriod: %v", err)
	}
	if got != 0 {
		t.Errorf("got %d, want 0 (before SystemStart)", got)
	}
}

func TestCurrentKESPeriod_HappyPath(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(100, 5, time.Second, systemStart)
	// 250 slots elapsed at 1s/slot, 100 slots per period → period 2.
	got, err := CurrentKESPeriod(g, systemStart.Add(250*time.Second))
	if err != nil {
		t.Fatalf("CurrentKESPeriod: %v", err)
	}
	if got != 2 {
		t.Errorf("got %d, want 2", got)
	}
}

func TestCurrentKESPeriod_FractionalSlotLength(t *testing.T) {
	// Devnet style: 50ms slots, 200 slots per period.
	// At 50_000ms elapsed → 1000 slots → period 5.
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := &shelley.ShelleyGenesis{
		SystemStart:       systemStart,
		SlotsPerKESPeriod: 200,
		SlotLength:        common.GenesisRat{Rat: big.NewRat(1, 20)},
	}
	got, err := CurrentKESPeriod(g, systemStart.Add(50*time.Second))
	if err != nil {
		t.Fatalf("CurrentKESPeriod: %v", err)
	}
	if got != 5 {
		t.Errorf("got %d, want 5", got)
	}
}

func TestCurrentKESPeriod_PeriodBoundaryExclusive(t *testing.T) {
	// At exactly 100 slots elapsed (the start of period 1) we should be in
	// period 1, not 0 or 2.
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(100, 5, time.Second, systemStart)
	got, err := CurrentKESPeriod(g, systemStart.Add(100*time.Second))
	if err != nil {
		t.Fatalf("CurrentKESPeriod: %v", err)
	}
	if got != 1 {
		t.Errorf("got %d, want 1 (exact boundary)", got)
	}
}

func TestCurrentKESPeriod_NilGenesis(t *testing.T) {
	_, err := CurrentKESPeriod(nil, time.Now())
	if err == nil {
		t.Fatal("expected error for nil genesis")
	}
}

func TestCurrentKESPeriod_ZeroSlotsPerKES(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(0, 5, time.Second, systemStart)
	_, err := CurrentKESPeriod(g, systemStart)
	if err == nil {
		t.Fatal("expected error for zero SlotsPerKESPeriod")
	}
	if !strings.Contains(err.Error(), "slotsPerKESPeriod") {
		t.Errorf("error should mention slotsPerKESPeriod, got: %v", err)
	}
}

func TestCurrentKESPeriod_ZeroSlotLength(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := &shelley.ShelleyGenesis{
		SystemStart:       systemStart,
		SlotsPerKESPeriod: 100,
		SlotLength:        common.GenesisRat{Rat: big.NewRat(0, 1)},
	}
	_, err := CurrentKESPeriod(g, systemStart.Add(time.Hour))
	if err == nil {
		t.Fatal("expected error for zero SlotLength")
	}
	if !strings.Contains(err.Error(), "slotLength") {
		t.Errorf("error should mention slotLength, got: %v", err)
	}
}

func TestCurrentKESPeriod_NilSlotLengthRat(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := &shelley.ShelleyGenesis{
		SystemStart:       systemStart,
		SlotsPerKESPeriod: 100,
		SlotLength:        common.GenesisRat{Rat: nil},
	}
	_, err := CurrentKESPeriod(g, systemStart.Add(time.Hour))
	if err == nil {
		t.Fatal("expected error for nil SlotLength.Rat")
	}
}
