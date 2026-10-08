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
	"testing"

	"github.com/stretchr/testify/require"
)

type failingSubscriber struct{ err error }

func (s failingSubscriber) Deliver(Event) error { return s.err }

func (failingSubscriber) Close() {}

// A subscriber's delivery error reaches the publish loop wrapped with the
// delivery step, and the publish loop's errors.Is match on
// errChannelSubscriberClosed still sees through the wrapping.
func TestDeliverWithTimeoutWrapsSubscriberErrorKeepingSentinel(t *testing.T) {
	t.Parallel()

	bus := NewEventBus(nil, nil)
	defer bus.Stop()

	err := bus.deliverWithTimeout(
		failingSubscriber{err: errChannelSubscriberClosed},
		NewEvent("test", "payload"),
	)

	require.ErrorContains(t, err, "deliver to subscriber")
	require.ErrorIs(t, err, errChannelSubscriberClosed)

	require.NoError(
		t,
		bus.deliverWithTimeout(
			failingSubscriber{},
			NewEvent("test", "payload"),
		),
	)
}
