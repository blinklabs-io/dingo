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

package event_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

func TestEpochNonceReadyEventType(t *testing.T) {
	assert.Equal(
		t,
		event.EventType("epoch.nonce_ready"),
		event.EpochNonceReadyEventType,
	)
}

func TestEpochNonceReadyEventPublishSubscribe(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.EpochNonceReadyEvent{
		CurrentEpoch: 1238,
		ReadyEpoch:   1239,
		CutoffSlot:   107015040,
	}

	_, subCh := eb.Subscribe(event.EpochNonceReadyEventType)

	eb.Publish(
		event.EpochNonceReadyEventType,
		event.NewEvent(event.EpochNonceReadyEventType, testEvent),
	)

	select {
	case evt, ok := <-subCh:
		require.True(t, ok, "event channel closed unexpectedly")
		require.Equal(t, event.EpochNonceReadyEventType, evt.Type)

		readyEvent, ok := evt.Data.(event.EpochNonceReadyEvent)
		require.True(t, ok, "event data was not EpochNonceReadyEvent")

		assert.Equal(t, testEvent.CurrentEpoch, readyEvent.CurrentEpoch)
		assert.Equal(t, testEvent.ReadyEpoch, readyEvent.ReadyEpoch)
		assert.Equal(t, testEvent.CutoffSlot, readyEvent.CutoffSlot)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for epoch nonce ready event")
	}
}
