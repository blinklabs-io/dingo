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
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestProtocolParamsForSlot_UnavailableShape(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    *cardano.CardanoNodeConfig
		era    eras.EraDesc
		params lcommon.ProtocolParameters
	}{
		{
			name: "missing config", era: eras.ShelleyEraDesc,
			params: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		},
		{
			name: "invalid config", cfg: &cardano.CardanoNodeConfig{},
			era:    eras.ShelleyEraDesc,
			params: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		},
		{
			name: "current era unavailable", cfg: newAllegraAtEpoch1Cfg(t),
			era: eras.DijkstraEraDesc, params: &dijkstra.DijkstraProtocolParameters{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				currentEra: tc.era,
				currentEpoch: models.Epoch{
					EpochId: 0, StartSlot: 0, LengthInSlots: 75, EraId: tc.era.Id,
				},
				currentPParams: tc.params,
				config:         LedgerStateConfig{CardanoNodeConfig: tc.cfg},
			}
			ls.publishSnapshotsLocked()
			require.Same(t, tc.params, ls.ProtocolParamsForSlot(74),
				"current-epoch parameters do not require a forecast")
			require.Nil(t, ls.ProtocolParamsForSlot(75),
				"a future epoch with unavailable shape must not use current parameters")
		})
	}
	// The existing boundary test exercises a valid scheduled Shelley-to-Allegra
	// forecast; unavailable-shape handling must retain that path.
}

func TestProtocolParamsForSlot_UnavailableTransition(t *testing.T) {
	for _, missingSuccessor := range []bool{false, true} {
		name := "hard fork error"
		if missingSuccessor {
			name = "missing successor"
		}
		t.Run(name, func(t *testing.T) {
			ls := &LedgerState{
				currentEra: eras.ShelleyEraDesc,
				currentEpoch: models.Epoch{
					EpochId: 0, StartSlot: 0, LengthInSlots: 75,
					EraId: eras.ShelleyEraDesc.Id,
				},
				currentPParams: &babbage.BabbageProtocolParameters{},
				config: LedgerStateConfig{
					CardanoNodeConfig: newAllegraAtEpoch1Cfg(t),
					Logger:            slog.New(slog.DiscardHandler),
				},
			}
			if missingSuccessor {
				ls.currentPParams = &shelley.ShelleyProtocolParameters{ProtocolMajor: 2}
				ls.activeEras = []eras.EraDesc{eras.ShelleyEraDesc}
			}
			ls.publishSnapshotsLocked()
			require.Same(t, ls.currentPParams, ls.ProtocolParamsForSlot(74))
			require.Nil(t, ls.ProtocolParamsForSlot(75),
				"an unresolved scheduled transition must not return pre-fork parameters")
		})
	}
}

func TestProtocolParamsForSlot_PendingUpdateFailure(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	// A selected but undecodable proposal is an error, not an absent update.
	require.NoError(t, db.SetPParamUpdate([]byte{0xaa}, []byte{0xff}, 50, 0, nil))
	pparams := &shelley.ShelleyProtocolParameters{ProtocolMajor: 2}
	ls := &LedgerState{
		db: db, currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId: 0, StartSlot: 0, LengthInSlots: 100,
			EraId: eras.ShelleyEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: newShelleyUpdateQuorum1Cfg(t),
			Logger:            slog.New(slog.DiscardHandler),
		},
	}
	ls.publishSnapshotsLocked()
	require.Same(t, pparams, ls.ProtocolParamsForSlot(99))
	require.Nil(t, ls.ProtocolParamsForSlot(100),
		"a failed pending update must not return stale parameters")
}

func TestGenesisOverlayRejectsUnavailableProtocolParams(t *testing.T) {
	for _, available := range []bool{false, true} {
		name := "unavailable"
		if available {
			name = "available"
		}
		t.Run(name, func(t *testing.T) {
			cfg := newGenesisDelegateShelleyGenesisCfg(t,
				strings.Repeat("22", 28), strings.Repeat("33", 32))
			epoch := models.Epoch{
				EpochId: 0, StartSlot: 0, LengthInSlots: 75,
				EraId: eras.BabbageEraDesc.Id,
			}
			ls := &LedgerState{
				currentEra: eras.BabbageEraDesc, currentEpoch: epoch,
				epochCache: []models.Epoch{epoch},
				config:     LedgerStateConfig{CardanoNodeConfig: cfg},
			}
			if available {
				ls.currentPParams = &babbage.BabbageProtocolParameters{}
			}
			ls.publishSnapshotsLocked()
			handled, err := ls.verifyGenesisDelegateHeader(
				&mockBabbageBlock{slot: 50}, false)
			if available {
				require.NoError(t, err)
				require.False(t, handled,
					"Babbage parameters correctly disable the genesis overlay")
				return
			}
			require.ErrorContains(t, err, "protocol parameters unavailable")
			require.True(t, handled,
				"unavailable parameters must not fall through as a non-overlay slot")
		})
	}
}
