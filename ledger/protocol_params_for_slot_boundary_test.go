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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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
