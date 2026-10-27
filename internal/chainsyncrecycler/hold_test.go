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

package chainsyncrecycler

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
)

// While network ingest is held the local tip moves only with external
// batches and peers' headers are dropped on purpose, so neither a stalled
// client, an impatient one, nor a plateau is evidence of a stuck connection.
// The tick leaves every peer alone and restarts the plateau clock, so the
// release does not inherit the time spent held.
func TestTickHoldSuspendsRecycling(t *testing.T) {
	t.Parallel()
	connId := testConnId(1)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true, held: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			stalledClient(connId, false),
			stalledClient(testConnId(2), false),
		},
		activeConn: &active,
		impatient:  []ouroboros.ConnectionId{connId},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-time.Hour))
	st.recycleAt[connId.String()] = now.Add(-time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	assert.Empty(t, pub.all(), "a held tick must not recycle or resync")
	assert.Zero(t, state.stalledChecks, "a held tick must not mark stalls")
	assert.Empty(t, st.recycleAt, "a held tick drops pending recycles")
	assert.Equal(t, now, st.lastProgressAt, "the plateau clock restarts")
}
