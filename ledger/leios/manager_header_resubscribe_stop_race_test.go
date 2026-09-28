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

package leios

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/stretchr/testify/require"
)

// TestReplacementHeaderSubscriptionIsUndoneWhenStopWins is the regression test
// for the header-stream resubscribe racing Stop.
//
// replaceHeaderStream checks the lifecycle, releases m.mu to subscribe, then
// takes it again to record the subscription. A Stop landing in that gap has
// already snapshotted and unsubscribed m.subs, which cannot contain the id
// being created, so recording it afterwards left a subscriber that no
// goroutine drains -- the event loop is on its way out. The header stream uses
// SubscriberBackpressureBlock precisely so the bus will not drop its events
// under load, so the orphan is not harmless: the next publisher on this event
// type blocks on it rather than having its event dropped, which stalls whoever
// is publishing chain header lifecycle events.
//
// The interleaving is reproduced exactly rather than by racing goroutines: the
// subscription is created, then Stop runs to completion, then the registration
// step is invoked -- which is the ordering the bug requires.
func TestReplacementHeaderSubscriptionIsUndoneWhenStopWins(t *testing.T) {
	fixture := newManagerFixture(t)
	mgr := fixture.mgr
	require.NoError(t, mgr.Start(context.Background()))

	// The replacement subscription, created while the manager was still
	// running, as replaceHeaderStream creates it.
	subId, ch := mgr.subscribeHeaderStream()
	require.NotNil(t, ch)

	// Stop wins the race. It unsubscribes the subscriptions recorded in
	// m.subs, which cannot include the one just created.
	require.NoError(t, mgr.Stop())
	require.True(
		t,
		fixture.eventBus.HasSubscribers(chain.ChainHeaderEventType),
		"fixture must leave the replacement subscription attached, or there is no race to test",
	)

	// The registration step now runs, as it does on the far side of the
	// subscribe call in replaceHeaderStream.
	require.False(
		t,
		mgr.registerReplacementHeaderSubscription(subId),
		"a manager that stopped must not adopt the replacement stream",
	)

	require.False(
		t,
		fixture.eventBus.HasSubscribers(chain.ChainHeaderEventType),
		"the replacement subscription must be undone, not left for a publisher to block on",
	)

	mgr.mu.Lock()
	subs := mgr.subs
	mgr.mu.Unlock()
	require.Empty(
		t,
		subs,
		"a stopped manager must not carry a header subscription",
	)
}

// TestReplaceHeaderStreamAdoptsTheReplacementWhileRunning is the other side of
// the branch: with no Stop in flight the replacement is adopted and recorded,
// so the test above is pinning a distinction rather than a constant.
func TestReplaceHeaderStreamAdoptsTheReplacementWhileRunning(t *testing.T) {
	fixture := newManagerFixture(t)
	mgr := fixture.mgr
	require.NoError(t, mgr.Start(context.Background()))
	t.Cleanup(func() { _ = mgr.Stop() })

	ch, ok := mgr.replaceHeaderStream()
	require.True(t, ok)
	require.NotNil(t, ch)

	mgr.mu.Lock()
	var headerSubs int
	for _, sub := range mgr.subs {
		if sub.eventType == chain.ChainHeaderEventType {
			headerSubs++
		}
	}
	mgr.mu.Unlock()
	require.Equal(
		t,
		1,
		headerSubs,
		"the replacement must take the place of the previous header subscription, not add to it",
	)
	require.True(
		t,
		fixture.eventBus.HasSubscribers(chain.ChainHeaderEventType),
	)
}
