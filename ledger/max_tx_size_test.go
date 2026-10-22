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
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func eraDesc(t *testing.T, eraList []eras.EraDesc, id uint) eras.EraDesc {
	t.Helper()
	desc := eras.GetEraByIdIn(eraList, id)
	require.NotNil(t, desc)
	return *desc
}

func TestLedgerStateMaxTxSize(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.publishSnapshotsLocked()
	_, ok := ls.MaxTxSize(conway.EraIdConway)
	require.False(t, ok, "unknown pparams must not report a limit")

	ls = &LedgerState{
		currentEra:     eraDesc(t, eras.Eras, conway.EraIdConway),
		currentPParams: &conway.ConwayProtocolParameters{MaxTxSize: 16_384},
	}
	ls.publishSnapshotsLocked()
	got, ok := ls.MaxTxSize(conway.EraIdConway)
	require.True(t, ok)
	require.Equal(t, uint(16_384), got)
	// Without previous-era parameters the ledger validates a previous-era
	// transaction with the current ones, which Babbage validation refuses
	// before any size rule, so no limit applies.
	_, ok = ls.MaxTxSize(babbage.EraIdBabbage)
	require.False(t, ok)

	dpp := &dijkstra.DijkstraProtocolParameters{}
	dpp.MaxTxSize = 32_768
	active := eras.ActiveEras(true)
	ls = &LedgerState{
		activeEras:     active,
		currentEra:     eraDesc(t, active, dijkstra.EraIdDijkstra),
		currentPParams: dpp,
	}
	ls.publishSnapshotsLocked()
	got, ok = ls.MaxTxSize(dijkstra.EraIdDijkstra)
	require.True(t, ok)
	require.Equal(t, uint(32_768), got)
}

// A transaction of the previous era is validated with the previous era's
// parameters, so its limit is the previous MaxTxSize even when the current
// one is smaller. Eras the ledger would refuse outright report no limit, so
// the ledger's era-mismatch rejection is the one a submitter sees.
func TestLedgerStateMaxTxSizeUsesTransactionEraParameters(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentEra:     eraDesc(t, eras.Eras, conway.EraIdConway),
		currentPParams: &conway.ConwayProtocolParameters{MaxTxSize: 16_384},
		prevEraPParams: &babbage.BabbageProtocolParameters{MaxTxSize: 32_768},
	}
	ls.publishSnapshotsLocked()

	got, ok := ls.MaxTxSize(conway.EraIdConway)
	require.True(t, ok)
	require.Equal(t, uint(16_384), got)

	got, ok = ls.MaxTxSize(babbage.EraIdBabbage)
	require.True(t, ok)
	require.Equal(t, uint(32_768), got)

	_, ok = ls.MaxTxSize(alonzo.EraIdAlonzo)
	require.False(t, ok, "an era two back is refused by the ledger")
	_, ok = ls.MaxTxSize(dijkstra.EraIdDijkstra)
	require.False(t, ok, "a later era is refused by the ledger")
}

// Whenever the ledger would accept a transaction without reaching a MaxTxSize
// rule for the resolved parameters, no limit is reported.
func TestLedgerStateMaxTxSizeUnknownWhenLedgerSkipsSizeRule(t *testing.T) {
	t.Parallel()

	conwayEra := eraDesc(t, eras.Eras, conway.EraIdConway)
	// Parameters of another era's type fail Conway validation.
	ls := &LedgerState{
		currentEra:     conwayEra,
		currentPParams: &shelley.ShelleyProtocolParameters{MaxTxSize: 16_384},
	}
	ls.publishSnapshotsLocked()
	_, ok := ls.MaxTxSize(conway.EraIdConway)
	require.False(t, ok, "mismatched pparams type")

	// An era without a ValidateTxFunc accepts every transaction.
	bare := conwayEra
	bare.ValidateTxFunc = nil
	ls = &LedgerState{
		activeEras:     []eras.EraDesc{bare},
		currentEra:     bare,
		currentPParams: &conway.ConwayProtocolParameters{MaxTxSize: 16_384},
	}
	ls.publishSnapshotsLocked()
	_, ok = ls.MaxTxSize(conway.EraIdConway)
	require.False(t, ok, "era without validation")

	// Configured Dijkstra validation skipping accepts every Dijkstra
	// transaction.
	dpp := &dijkstra.DijkstraProtocolParameters{}
	dpp.MaxTxSize = 32_768
	active := eras.ActiveEras(true)
	ls = &LedgerState{
		activeEras:     active,
		currentEra:     eraDesc(t, active, dijkstra.EraIdDijkstra),
		currentPParams: dpp,
		config:         LedgerStateConfig{SkipDijkstraTxValidation: true},
	}
	ls.publishSnapshotsLocked()
	_, ok = ls.MaxTxSize(dijkstra.EraIdDijkstra)
	require.False(t, ok, "Dijkstra validation skipped")
}
