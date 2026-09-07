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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// shelleyOnlyGenesisCfg returns a config with a Shelley genesis and no Byron
// genesis. ByronGenesisFile is optional (config/cardano/node.go loads it only
// when non-empty) and a Shelley-only config without it is a supported shape
// (see the setEpochCache era-start comment in state.go), but
// eras.BuildShapeForEras still builds Byron era params for every config, so no
// hard-fork shape -- and therefore no forecast -- can be built from one.
func shelleyOnlyGenesisCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))
	require.Nil(t, cfg.ByronGenesis())
	return cfg
}

// newShelleyOnlyForecastLedger builds a LedgerState whose epoch cache covers
// slots [100_000, 532_000) but whose config cannot produce a hard-fork shape.
func newShelleyOnlyForecastLedger(t testing.TB) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     100_000,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         []byte("nonce"),
		}},
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       500,
			StartSlot:     100_000,
			LengthInSlots: 432_000,
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(200_000, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: shelleyOnlyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// TestHeaderVerificationEpoch_ForecastBuildFailureDeferred pins the
// classification of a hard-fork summary that cannot be BUILT: the era shape,
// the genesis behind it, and the epoch cache are local inputs, so the failure
// says nothing about the header. ouroboros/chainsync.go routes every
// non-deferred header error to ConnectionRecycleRequestedEvent, so returning
// the build failure unwrapped recycles the honest peer that served the header
// and stalls the node at every epoch boundary.
func TestHeaderVerificationEpoch_ForecastBuildFailureDeferred(t *testing.T) {
	ls := newShelleyOnlyForecastLedger(t)

	// Confirm the premise: the config genuinely cannot build a summary.
	_, sumErr := ls.HardForkSummary()
	require.Error(t, sumErr, "Shelley-only config must not build a shape")

	// A slot past the cached epoch forces the summary path.
	_, err := ls.headerVerificationEpoch(532_000, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errHeaderVerificationDeferred,
		"an unbuildable forecast must not be reported as a peer fault")
	require.True(t, IsHeaderVerificationDeferred(err))
}
