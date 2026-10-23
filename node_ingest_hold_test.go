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

package dingo

import (
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func holdTestConnId(port int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port},
	}
}

// The recycler, the chain-selection handlers and the health and lifecycle
// polls all briefly take liveLifecycleMu. The hold must reach chainsync state
// regardless: a lost hold leaves nothing marked, a lost release marks every
// peer that reconnects afterwards and the node stops following the chain, and
// a lost read lets a marked connection through.
func TestNetworkIngestHoldTrackerIgnoresLifecycleContention(t *testing.T) {
	t.Parallel()
	n := &Node{config: Config{cfg: &internalconfig.Config{}}}
	n.publishChainsyncState(chainsync.NewState(nil, nil))
	before := holdTestConnId(3001)
	after := holdTestConnId(3002)
	require.True(t, n.chainsyncState.AddClientConnId(before))
	tracker := n.ledgerStateConfig().NetworkIngestHoldTracker
	require.NotNil(t, tracker)

	n.liveLifecycleMu.Lock()
	tracker.SetNetworkIngestHeld(true)
	assert.True(t, tracker.ConnectionIngestHeld(before),
		"a hold taken under contention must mark existing clients")
	tracker.SetNetworkIngestHeld(false)
	require.True(t, n.chainsyncState.AddClientConnId(after))
	n.liveLifecycleMu.Unlock()

	assert.True(t, n.chainsyncState.ConnectionIngestHeld(before))
	assert.False(t, n.chainsyncState.ConnectionIngestHeld(after),
		"a release under contention must stop marking new clients")
}

// A live Restore/Truncate rebuilds chainsync state. One rebuilt while ingest
// is held must mark the clients it tracks, or they deliver unheld.
func TestNetworkIngestHoldSurvivesChainsyncStateRebuild(t *testing.T) {
	t.Parallel()
	n := &Node{config: Config{cfg: &internalconfig.Config{}}}
	n.publishChainsyncState(chainsync.NewState(nil, nil))
	tracker := n.ledgerStateConfig().NetworkIngestHoldTracker
	tracker.SetNetworkIngestHeld(true)

	n.publishChainsyncState(nil)
	n.publishChainsyncState(chainsync.NewState(nil, nil))
	conn := holdTestConnId(3003)
	require.True(t, n.chainsyncState.AddClientConnId(conn))
	assert.True(t, tracker.ConnectionIngestHeld(conn),
		"a state rebuilt during a hold must inherit it")

	tracker.SetNetworkIngestHeld(false)
	later := holdTestConnId(3004)
	require.True(t, n.chainsyncState.AddClientConnId(later))
	assert.False(t, tracker.ConnectionIngestHeld(later))
}
