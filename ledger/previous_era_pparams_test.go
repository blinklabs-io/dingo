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
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// A network that starts in Dijkstra records no previous-era parameters, so a
// Conway transaction used to be validated with Dijkstra parameters and failed
// with ErrIncompatibleProtocolParams.
func TestValidationPParamsConwayTxOnDijkstraWithoutPreviousParams(t *testing.T) {
	t.Parallel()

	dpp := &dijkstra.DijkstraProtocolParameters{}
	dpp.MaxTxSize = 32_768
	active := eras.ActiveEras(true)
	snap := txValidationSnapshot{
		currentEra:     eraDesc(t, active, dijkstra.EraIdDijkstra),
		currentPParams: dpp,
		eraList:        active,
	}

	pp, isCurrent := snap.validationPParams(conway.EraIdConway)
	require.False(t, isCurrent)
	require.Same(t, &dpp.ConwayProtocolParameters, pp)

	pp, isCurrent = snap.validationPParams(dijkstra.EraIdDijkstra)
	require.True(t, isCurrent)
	require.Same(t, dpp, pp)

	// Recorded previous-era parameters still win.
	recorded := &conway.ConwayProtocolParameters{}
	snap.prevEraPParams = recorded
	pp, isCurrent = snap.validationPParams(conway.EraIdConway)
	require.False(t, isCurrent)
	require.Same(t, recorded, pp)

	// No derivation for other eras.
	snap.prevEraPParams = nil
	pp, isCurrent = snap.validationPParams(babbage.EraIdBabbage)
	require.True(t, isCurrent)
	require.Same(t, dpp, pp)
}
