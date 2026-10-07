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

package ouroboros

import (
	"fmt"
	"testing"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const capacityTestSlots = 3

func newCapacityTestOuroboros(
	t *testing.T,
	isRoot func(ouroboros.ConnectionId) bool,
) *Ouroboros {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.chainsyncState = dchainsync.NewStateWithConfig(
		bus,
		nil,
		dchainsync.Config{
			MaxClients:   capacityTestSlots,
			StallTimeout: dchainsync.DefaultStallTimeout,
			IsRoot:       isRoot,
		},
	)
	return o
}

// A peer that is eligible for ingress but finds every slot taken still gets a
// chainsync client, as an observer that can be promoted when a slot frees.
func TestRegisterTrackedChainsyncClientAtCapacityObserves(t *testing.T) {
	t.Parallel()
	o := newCapacityTestOuroboros(t, nil)
	for i := range capacityTestSlots {
		connId := newTestConnId(
			"127.0.0.1:6000",
			fmt.Sprintf("10.0.0.%d:3001", i+1),
		)
		require.True(t, o.registerTrackedChainsyncClient(connId, true, true))
	}
	late := newTestConnId("127.0.0.1:6000", "10.0.0.9:3001")

	require.True(
		t,
		o.registerTrackedChainsyncClient(late, true, true),
		"a capacity rejection must not leave the connection without a client",
	)

	observer, tracked := o.chainsyncState.ClientObservabilityOnly(late)
	require.True(t, tracked)
	assert.True(t, observer)
	assert.Equal(t, capacityTestSlots, o.chainsyncState.ClientConnCount())
}

// With every slot held by discovered peers, a configured root registers as an
// eligible client and sync can start on it.
func TestRegisterTrackedChainsyncClientRootTakesSlotFromDiscoveredPeers(
	t *testing.T,
) {
	t.Parallel()
	root := newTestConnId("127.0.0.1:6000", "10.0.0.100:3001")
	o := newCapacityTestOuroboros(t, func(c ouroboros.ConnectionId) bool {
		return c == root
	})
	for i := range capacityTestSlots {
		connId := newTestConnId(
			"127.0.0.1:6000",
			fmt.Sprintf("10.0.0.%d:3001", i+1),
		)
		require.True(t, o.registerTrackedChainsyncClient(connId, true, true))
	}

	require.True(t, o.registerTrackedChainsyncClient(root, true, true))

	observer, tracked := o.chainsyncState.ClientObservabilityOnly(root)
	require.True(t, tracked)
	assert.False(t, observer, "the root must hold an eligible slot")
	assert.Equal(t, capacityTestSlots, o.chainsyncState.ClientConnCount())
}
