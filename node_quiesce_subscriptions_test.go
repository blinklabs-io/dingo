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
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/stretchr/testify/require"
)

func TestQuiesceBoundsSubscriptionDrains(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		topic  event.EventType
		assign func(*Node, event.EventSubscriberId)
	}{
		{"parity", event.EpochTransitionEventType, func(n *Node, id event.EventSubscriberId) { n.koiosParitySubId = id }},
		{"chainsync", chainsync.ClientRemoveRequestedEventType, func(n *Node, id event.EventSubscriberId) { n.chainsyncClientRemoveSubId = id }},
		{"recycle", connmanager.ConnectionRecycleRequestedEventType, func(n *Node, id event.EventSubscriberId) { n.connManagerRecycleSubId = id }},
		{"vote emitted", leios.VoteEmittedEventType, func(n *Node, id event.EventSubscriberId) { n.leiosVoteEmittedSubId = id }},
		{"vote received", leios.VoteReceivedEventType, func(n *Node, id event.EventSubscriberId) { n.leiosVoteReceivedSubId = id }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eb := event.NewEventBus(nil, nil)
			defer eb.Close()
			entered, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			n := &Node{config: NewConfig(WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))), eventBus: eb}
			tc.assign(n, eb.SubscribeFunc(tc.topic, func(event.Event) {
				close(entered)
				<-release
			}))
			eb.Publish(tc.topic, event.NewEvent(tc.topic, nil))
			testutil.RequireReceive(t, entered, time.Second, "handler did not start")
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			done := make(chan error, 1)
			go func() { done <- n.quiesceForLiveLifecycleOp(ctx) }()
			err := testutil.RequireReceive(t, done, time.Second, "quiesce blocked")
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, errStorageDrainUnconfirmed)
		})
	}
}
