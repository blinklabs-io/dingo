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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// TestConnectionRecycleSubscriptionsRemainLossless verifies the production
// wiring of both hops of the connection-recycle stream, not EventBus in
// isolation: the ledger-to-connmanager translation and the connmanager handler
// subscription.
//
// A recycle request cannot be replayed. Each publisher raises exactly one per
// connection and then keeps its own "already asked" flag set -- the leios-fetch
// backfill's markProtocolDead is the clearest case, where a dropped request
// leaves a connection whose leios-fetch protocol can never answer again in the
// pool for the rest of its life (dingo #3552). Detaching either subscriber
// under backpressure would silently strip requests out of the stream, so both
// stay attached until they drain.
func TestConnectionRecycleSubscriptionsRemainLossless(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()

	started := make(chan struct{})
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	defer release()

	// Enough to fill both hops' buffers, so the ledger-side publisher can only
	// finish if an event was dropped or a subscriber was detached.
	const total = 2*event.DefaultSubscriberBuffer + 8
	allHandled := make(chan struct{})
	var handled atomic.Int32
	n := &Node{eventBus: bus}
	n.subscribeConnectionRecycleRequests(func(evt event.Event) {
		if _, ok := evt.Data.(connmanager.ConnectionRecycleRequestedEvent); !ok {
			return
		}
		if handled.Add(1) == 1 {
			close(started)
			<-releaseCh
		}
		if handled.Load() == total {
			close(allHandled)
		}
	})
	n.subscribeLedgerConnectionRecycleTranslation()

	publishRecycle := func(i int) {
		bus.Publish(
			ledger.ConnectionRecycleRequestedEventType,
			event.NewEvent(
				ledger.ConnectionRecycleRequestedEventType,
				ledger.ConnectionRecycleRequestedEvent{
					Reason: "leios_fetch_request_slot_abandoned_" +
						strconv.Itoa(i),
				},
			),
		)
	}

	publishRecycle(0)
	testutil.RequireReceive(
		t,
		started,
		time.Second,
		"connmanager recycle handler did not begin",
	)

	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 1; i < total; i++ {
			publishRecycle(i)
		}
	}()

	// The ordinary EventBus subscriber timeout is five seconds. A regression to
	// the detaching policy on either hop would let this publish finish at that
	// bound with the surplus recycle requests discarded.
	testutil.RequireNoReceive(
		t,
		published,
		event.RemoteDeliverTimeout+time.Second,
		"a connection-recycle subscription detached instead of retaining the stream",
	)

	release()
	testutil.RequireReceive(
		t,
		published,
		10*time.Second,
		"recycle publisher did not resume after the handler drained",
	)
	testutil.RequireReceive(
		t,
		allHandled,
		10*time.Second,
		"connmanager recycle handler did not receive every retained request",
	)
	require.Equal(t, int32(total), handled.Load())
}
