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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// Both values come from the snapshot published last, never one from each of
// two publications.
func TestCurrentEpochAndPParamsReadOneSnapshot(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{}
	first := &dijkstra.DijkstraProtocolParameters{LeiosCommitteeSize: 1}
	second := &dijkstra.DijkstraProtocolParameters{LeiosCommitteeSize: 2}
	ls.consensus.Store(&consensusSnapshot{
		currentEpoch:   models.Epoch{EpochId: 7},
		currentPParams: first,
	})
	epoch, pparams := ls.CurrentEpochAndPParams()
	require.Equal(t, uint64(7), epoch)
	require.Same(t, first, pparams)

	ls.consensus.Store(&consensusSnapshot{
		currentEpoch:   models.Epoch{EpochId: 8},
		currentPParams: second,
	})
	epoch, pparams = ls.CurrentEpochAndPParams()
	require.Equal(t, uint64(8), epoch)
	require.Same(t, second, pparams)
}
