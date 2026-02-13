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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package event_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

func TestHardForkEventType(t *testing.T) {
	assert.Equal(
		t,
		event.EventType("hardfork.transition"),
		event.HardForkEventType,
	)
}

func TestHardForkEventFields(t *testing.T) {
	evt := event.HardForkEvent{
		Slot:            432000,
		EpochNo:         100,
		FromEra:         5,
		ToEra:           6,
		OldMajorVersion: 8,
		OldMinorVersion: 0,
		NewMajorVersion: 9,
		NewMinorVersion: 0,
	}

	assert.Equal(t, uint64(432000), evt.Slot)
	assert.Equal(t, uint64(100), evt.EpochNo)
	assert.Equal(t, uint(5), evt.FromEra)
	assert.Equal(t, uint(6), evt.ToEra)
	assert.Equal(t, uint(8), evt.OldMajorVersion)
	assert.Equal(t, uint(0), evt.OldMinorVersion)
	assert.Equal(t, uint(9), evt.NewMajorVersion)
	assert.Equal(t, uint(0), evt.NewMinorVersion)
}

func TestHardForkEventPublishSubscribe(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.HardForkEvent{
		Slot:            432000,
		EpochNo:         100,
		FromEra:         5,
		ToEra:           6,
		OldMajorVersion: 8,
		OldMinorVersion: 0,
		NewMajorVersion: 9,
		NewMinorVersion: 0,
	}

	_, subCh := eb.Subscribe(event.HardForkEventType)

	eb.Publish(
		event.HardForkEventType,
		event.NewEvent(
			event.HardForkEventType,
			testEvent,
		),
	)

	select {
	case evt, ok := <-subCh:
		require.True(
			t,
			ok,
			"event channel closed unexpectedly",
		)
		require.Equal(
			t,
			event.HardForkEventType,
			evt.Type,
		)

		hfEvent, ok := evt.Data.(event.HardForkEvent)
		require.True(
			t,
			ok,
			"event data was not HardForkEvent",
		)

		assert.Equal(t, uint64(432000), hfEvent.Slot)
		assert.Equal(t, uint64(100), hfEvent.EpochNo)
		assert.Equal(t, uint(5), hfEvent.FromEra)
		assert.Equal(t, uint(6), hfEvent.ToEra)
		assert.Equal(t, uint(8), hfEvent.OldMajorVersion)
		assert.Equal(t, uint(0), hfEvent.OldMinorVersion)
		assert.Equal(t, uint(9), hfEvent.NewMajorVersion)
		assert.Equal(t, uint(0), hfEvent.NewMinorVersion)
	case <-time.After(1 * time.Second):
		t.Fatal(
			"timeout waiting for hard fork event",
		)
	}
}

func TestHardForkEventSubscribeFunc(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.HardForkEvent{
		Slot:            86400,
		EpochNo:         10,
		FromEra:         4,
		ToEra:           5,
		OldMajorVersion: 6,
		OldMinorVersion: 0,
		NewMajorVersion: 7,
		NewMinorVersion: 0,
	}

	receivedCh := make(chan event.HardForkEvent, 1)

	eb.SubscribeFunc(
		event.HardForkEventType,
		func(evt event.Event) {
			if hfEvent, ok := evt.Data.(event.HardForkEvent); ok {
				receivedCh <- hfEvent
			}
		},
	)

	eb.Publish(
		event.HardForkEventType,
		event.NewEvent(
			event.HardForkEventType,
			testEvent,
		),
	)

	select {
	case received := <-receivedCh:
		assert.Equal(
			t,
			testEvent.Slot,
			received.Slot,
		)
		assert.Equal(
			t,
			testEvent.EpochNo,
			received.EpochNo,
		)
		assert.Equal(
			t,
			testEvent.FromEra,
			received.FromEra,
		)
		assert.Equal(
			t,
			testEvent.ToEra,
			received.ToEra,
		)
		assert.Equal(
			t,
			testEvent.OldMajorVersion,
			received.OldMajorVersion,
		)
		assert.Equal(
			t,
			testEvent.OldMinorVersion,
			received.OldMinorVersion,
		)
		assert.Equal(
			t,
			testEvent.NewMajorVersion,
			received.NewMajorVersion,
		)
		assert.Equal(
			t,
			testEvent.NewMinorVersion,
			received.NewMinorVersion,
		)
	case <-time.After(1 * time.Second):
		t.Fatal(
			"timeout waiting for hard fork event " +
				"via SubscribeFunc",
		)
	}
}

func TestHardForkEventZeroValues(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.HardForkEvent{}

	_, subCh := eb.Subscribe(event.HardForkEventType)

	eb.Publish(
		event.HardForkEventType,
		event.NewEvent(
			event.HardForkEventType,
			testEvent,
		),
	)

	select {
	case evt, ok := <-subCh:
		require.True(
			t,
			ok,
			"event channel closed unexpectedly",
		)
		hfEvent, ok := evt.Data.(event.HardForkEvent)
		require.True(
			t,
			ok,
			"event data was not HardForkEvent",
		)
		assert.Equal(t, uint64(0), hfEvent.Slot)
		assert.Equal(t, uint64(0), hfEvent.EpochNo)
		assert.Equal(t, uint(0), hfEvent.FromEra)
		assert.Equal(t, uint(0), hfEvent.ToEra)
		assert.Equal(t, uint(0), hfEvent.OldMajorVersion)
		assert.Equal(t, uint(0), hfEvent.OldMinorVersion)
		assert.Equal(t, uint(0), hfEvent.NewMajorVersion)
		assert.Equal(t, uint(0), hfEvent.NewMinorVersion)
	case <-time.After(1 * time.Second):
		t.Fatal(
			"timeout waiting for zero-value " +
				"hard fork event",
		)
	}
}
