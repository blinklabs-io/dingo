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

	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func TestLedgerStateMaxTxSize(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.publishSnapshotsLocked()
	_, ok := ls.MaxTxSize()
	require.False(t, ok, "unknown pparams must not report a limit")

	ls = &LedgerState{
		currentPParams: &conway.ConwayProtocolParameters{MaxTxSize: 16_384},
	}
	ls.publishSnapshotsLocked()
	got, ok := ls.MaxTxSize()
	require.True(t, ok)
	require.Equal(t, uint(16_384), got)

	dpp := &dijkstra.DijkstraProtocolParameters{}
	dpp.MaxTxSize = 32_768
	ls = &LedgerState{currentPParams: dpp}
	ls.publishSnapshotsLocked()
	got, ok = ls.MaxTxSize()
	require.True(t, ok)
	require.Equal(t, uint(32_768), got)
}
