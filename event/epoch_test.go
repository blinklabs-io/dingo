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

	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEpochTransitionEventType(t *testing.T) {
	assert.Equal(
		t,
		event.EventType("epoch.transition"),
		event.EpochTransitionEventType,
	)
}

func TestEpochTransitionEventPublishSubscribe(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.EpochTransitionEvent{
		PreviousEpoch:   100,
		NewEpoch:        101,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02, 0x03, 0x04},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	_, subCh := eb.Subscribe(event.EpochTransitionEventType)

	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, testEvent),
	)

	select {
	case evt, ok := <-subCh:
		require.True(t, ok, "event channel closed unexpectedly")
		require.Equal(t, event.EpochTransitionEventType, evt.Type)

		epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
		require.True(t, ok, "event data was not EpochTransitionEvent")

		assert.Equal(t, uint64(100), epochEvent.PreviousEpoch)
		assert.Equal(t, uint64(101), epochEvent.NewEpoch)
		assert.Equal(t, uint64(432000), epochEvent.BoundarySlot)
		assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, epochEvent.EpochNonce)
		assert.Equal(t, uint(8), epochEvent.ProtocolVersion)
		assert.Equal(t, uint64(431999), epochEvent.SnapshotSlot)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for epoch transition event")
	}
}

func TestEpochTransitionEventSubscribeFunc(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    21600,
		EpochNonce:      []byte{0xab, 0xcd},
		ProtocolVersion: 6,
		SnapshotSlot:    21599,
	}

	receivedCh := make(chan event.EpochTransitionEvent, 1)

	eb.SubscribeFunc(event.EpochTransitionEventType, func(evt event.Event) {
		if epochEvent, ok := evt.Data.(event.EpochTransitionEvent); ok {
			receivedCh <- epochEvent
		}
	})

	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, testEvent),
	)

	select {
	case received := <-receivedCh:
		assert.Equal(t, testEvent.PreviousEpoch, received.PreviousEpoch)
		assert.Equal(t, testEvent.NewEpoch, received.NewEpoch)
		assert.Equal(t, testEvent.BoundarySlot, received.BoundarySlot)
		assert.Equal(t, testEvent.EpochNonce, received.EpochNonce)
		assert.Equal(t, testEvent.ProtocolVersion, received.ProtocolVersion)
		assert.Equal(t, testEvent.SnapshotSlot, received.SnapshotSlot)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for epoch transition event via SubscribeFunc")
	}
}

func TestEpochTransitionEventMultipleSubscribers(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.EpochTransitionEvent{
		PreviousEpoch:   50,
		NewEpoch:        51,
		BoundarySlot:    1080000,
		EpochNonce:      []byte{0xff},
		ProtocolVersion: 7,
		SnapshotSlot:    1079999,
	}

	_, sub1Ch := eb.Subscribe(event.EpochTransitionEventType)
	_, sub2Ch := eb.Subscribe(event.EpochTransitionEventType)

	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, testEvent),
	)

	var gotSub1, gotSub2 bool
	timeout := time.After(1 * time.Second)

	for !gotSub1 || !gotSub2 {
		select {
		case evt, ok := <-sub1Ch:
			require.True(t, ok, "sub1 channel closed unexpectedly")
			epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
			require.True(t, ok, "sub1 event data was not EpochTransitionEvent")
			assert.Equal(t, testEvent.NewEpoch, epochEvent.NewEpoch)
			gotSub1 = true
		case evt, ok := <-sub2Ch:
			require.True(t, ok, "sub2 channel closed unexpectedly")
			epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
			require.True(t, ok, "sub2 event data was not EpochTransitionEvent")
			assert.Equal(t, testEvent.NewEpoch, epochEvent.NewEpoch)
			gotSub2 = true
		case <-timeout:
			t.Fatal("timeout waiting for events from multiple subscribers")
		}
	}
}

func TestEpochTransitionEventZeroValues(t *testing.T) {
	// Test that zero-value event works correctly
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        0,
		BoundarySlot:    0,
		EpochNonce:      nil,
		ProtocolVersion: 0,
		SnapshotSlot:    0,
	}

	_, subCh := eb.Subscribe(event.EpochTransitionEventType)

	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, testEvent),
	)

	select {
	case evt, ok := <-subCh:
		require.True(t, ok, "event channel closed unexpectedly")
		epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
		require.True(t, ok, "event data was not EpochTransitionEvent")
		assert.Equal(t, uint64(0), epochEvent.PreviousEpoch)
		assert.Equal(t, uint64(0), epochEvent.NewEpoch)
		assert.Equal(t, uint64(0), epochEvent.BoundarySlot)
		assert.Nil(t, epochEvent.EpochNonce)
		assert.Equal(t, uint(0), epochEvent.ProtocolVersion)
		assert.Equal(t, uint64(0), epochEvent.SnapshotSlot)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for zero-value epoch transition event")
	}
}
