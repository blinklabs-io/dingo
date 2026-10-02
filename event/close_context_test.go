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

package event

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestCloseContextAbandonsBlockedHandlerAndNamesItsType(t *testing.T) {
	t.Parallel()

	const stuckType EventType = "test.close_context_stuck"
	bus := NewEventBus(nil, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseDone bool
	releaseHandler := func() {
		if !releaseDone {
			releaseDone = true
			close(release)
		}
	}
	defer releaseHandler()
	bus.SubscribeFunc(stuckType, func(Event) {
		close(entered)
		<-release
	})
	bus.Publish(stuckType, NewEvent(stuckType, struct{}{}))
	testutil.RequireReceive(t, entered, 5*time.Second, "handler never started")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- bus.CloseContext(ctx) }()

	err := testutil.RequireReceive(t, done, 5*time.Second,
		"CloseContext did not return while a handler was blocked")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, string(stuckType))

	releaseHandler()
}

func TestCloseContextClosesBusWhenNoHandlerIsBlocked(t *testing.T) {
	t.Parallel()

	bus := NewEventBus(nil, nil)
	bus.SubscribeFunc("test.close_context_idle", func(Event) {})
	require.NoError(t, bus.CloseContext(context.Background()))
	require.True(t, bus.closed)
}
