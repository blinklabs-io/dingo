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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// TestBlockfetchSubscriptionRemainsLosslessPastDeliveryTimeout verifies the
// production LedgerState subscription policy, not only EventBus in isolation.
// A blockfetch event cannot be silently omitted: the subscription remains
// attached until the handler drains or its lifecycle closes it.
func TestBlockfetchSubscriptionRemainsLosslessPastDeliveryTimeout(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()

	started := make(chan struct{})
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	defer release()

	const total = blockfetchCommitBatchSize + 2
	allHandled := make(chan struct{})
	var handled atomic.Int32
	ls := &LedgerState{config: LedgerStateConfig{EventBus: bus}}
	ls.subscribeBlockfetchEvents(func(event.Event) {
		if handled.Add(1) == 1 {
			close(started)
			<-releaseCh
		}
		if handled.Load() == total {
			close(allHandled)
		}
	})

	bus.Publish(BlockfetchEventType, event.NewEvent(BlockfetchEventType, 0))
	testutil.RequireReceive(
		t,
		started,
		time.Second,
		"blockfetch handler did not begin",
	)

	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 1; i < total; i++ {
			bus.Publish(
				BlockfetchEventType,
				event.NewEvent(BlockfetchEventType, i),
			)
		}
	}()

	// The ordinary EventBus subscriber timeout is five seconds. A regression to
	// the detaching policy would make this publish finish at that bound instead
	// of retaining the blockfetch stream until the handler can drain it.
	testutil.RequireNoReceive(
		t,
		published,
		event.RemoteDeliverTimeout+time.Second,
		"blockfetch subscription detached instead of preserving the stream",
	)

	release()
	testutil.RequireReceive(
		t,
		published,
		5*time.Second,
		"blockfetch publisher did not resume after the handler drained",
	)
	testutil.RequireReceive(
		t,
		allHandled,
		5*time.Second,
		"blockfetch handler did not receive every retained event",
	)
}
