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

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// A network that starts in Dijkstra records no previous-era parameters, so
// historical Conway block validation uses Dijkstra's embedded Conway params.
func TestPreviousEraPParamsConwayOnDijkstraWithoutRecordedParams(t *testing.T) {
	t.Parallel()

	dpp := &dijkstra.DijkstraProtocolParameters{}
	dpp.MaxTxSize = 32_768
	pp := previousEraPParams(
		conway.EraIdConway,
		dijkstra.EraIdDijkstra,
		dpp,
		nil,
	)
	require.Same(t, &dpp.ConwayProtocolParameters, pp)

	pp = previousEraPParams(
		dijkstra.EraIdDijkstra,
		dijkstra.EraIdDijkstra,
		dpp,
		nil,
	)
	require.Nil(t, pp)

	// Recorded previous-era parameters still win.
	recorded := &conway.ConwayProtocolParameters{}
	pp = previousEraPParams(
		conway.EraIdConway,
		dijkstra.EraIdDijkstra,
		dpp,
		recorded,
	)
	require.Same(t, recorded, pp)

	// No derivation for other eras.
	pp = previousEraPParams(
		babbage.EraIdBabbage,
		dijkstra.EraIdDijkstra,
		dpp,
		nil,
	)
	require.Nil(t, pp)
}
