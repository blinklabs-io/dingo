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

func TestBlockForgedEventType(t *testing.T) {
	assert.Equal(
		t,
		event.EventType("block.forged"),
		event.BlockForgedEventType,
	)
}

func TestBlockForgedEventPublishSubscribe(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	now := time.Now()
	testEvent := event.BlockForgedEvent{
		Slot:        12345,
		BlockNumber: 100,
		BlockHash:   []byte{0xab, 0xcd, 0xef},
		TxCount:     5,
		BlockSize:   1024,
		Timestamp:   now,
	}

	_, subCh := eb.Subscribe(event.BlockForgedEventType)

	eb.Publish(
		event.BlockForgedEventType,
		event.NewEvent(event.BlockForgedEventType, testEvent),
	)

	select {
	case evt, ok := <-subCh:
		require.True(t, ok, "event channel closed unexpectedly")
		require.Equal(t, event.BlockForgedEventType, evt.Type)

		blockEvent, ok := evt.Data.(event.BlockForgedEvent)
		require.True(t, ok, "event data was not BlockForgedEvent")

		assert.Equal(t, uint64(12345), blockEvent.Slot)
		assert.Equal(t, uint64(100), blockEvent.BlockNumber)
		assert.Equal(t, []byte{0xab, 0xcd, 0xef}, blockEvent.BlockHash)
		assert.Equal(t, uint(5), blockEvent.TxCount)
		assert.Equal(t, uint(1024), blockEvent.BlockSize)
		assert.Equal(t, now, blockEvent.Timestamp)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for block forged event")
	}
}

func TestBlockForgedEventSubscribeFunc(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	now := time.Now()
	testEvent := event.BlockForgedEvent{
		Slot:        67890,
		BlockNumber: 200,
		BlockHash:   []byte{0x01, 0x02},
		TxCount:     10,
		BlockSize:   2048,
		Timestamp:   now,
	}

	receivedCh := make(chan event.BlockForgedEvent, 1)

	eb.SubscribeFunc(event.BlockForgedEventType, func(evt event.Event) {
		if blockEvent, ok := evt.Data.(event.BlockForgedEvent); ok {
			receivedCh <- blockEvent
		}
	})

	eb.Publish(
		event.BlockForgedEventType,
		event.NewEvent(event.BlockForgedEventType, testEvent),
	)

	select {
	case received := <-receivedCh:
		assert.Equal(t, testEvent.Slot, received.Slot)
		assert.Equal(t, testEvent.BlockNumber, received.BlockNumber)
		assert.Equal(t, testEvent.BlockHash, received.BlockHash)
		assert.Equal(t, testEvent.TxCount, received.TxCount)
		assert.Equal(t, testEvent.BlockSize, received.BlockSize)
		assert.Equal(t, testEvent.Timestamp, received.Timestamp)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for block forged event via SubscribeFunc")
	}
}

func TestBlockForgedEventMultipleSubscribers(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.BlockForgedEvent{
		Slot:        11111,
		BlockNumber: 50,
		BlockHash:   []byte{0xff},
		TxCount:     3,
		BlockSize:   512,
		Timestamp:   time.Now(),
	}

	_, sub1Ch := eb.Subscribe(event.BlockForgedEventType)
	_, sub2Ch := eb.Subscribe(event.BlockForgedEventType)

	eb.Publish(
		event.BlockForgedEventType,
		event.NewEvent(event.BlockForgedEventType, testEvent),
	)

	var gotSub1, gotSub2 bool
	timeout := time.After(1 * time.Second)

	for !gotSub1 || !gotSub2 {
		select {
		case evt, ok := <-sub1Ch:
			require.True(t, ok, "sub1 channel closed unexpectedly")
			blockEvent, ok := evt.Data.(event.BlockForgedEvent)
			require.True(t, ok, "sub1 event data was not BlockForgedEvent")
			assert.Equal(t, testEvent.Slot, blockEvent.Slot)
			gotSub1 = true
		case evt, ok := <-sub2Ch:
			require.True(t, ok, "sub2 channel closed unexpectedly")
			blockEvent, ok := evt.Data.(event.BlockForgedEvent)
			require.True(t, ok, "sub2 event data was not BlockForgedEvent")
			assert.Equal(t, testEvent.Slot, blockEvent.Slot)
			gotSub2 = true
		case <-timeout:
			t.Fatal("timeout waiting for events from multiple subscribers")
		}
	}
}

func TestBlockForgedEventZeroValues(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	testEvent := event.BlockForgedEvent{
		Slot:        0,
		BlockNumber: 0,
		BlockHash:   nil,
		TxCount:     0,
		BlockSize:   0,
		Timestamp:   time.Time{},
	}

	_, subCh := eb.Subscribe(event.BlockForgedEventType)

	eb.Publish(
		event.BlockForgedEventType,
		event.NewEvent(event.BlockForgedEventType, testEvent),
	)

	select {
	case evt, ok := <-subCh:
		require.True(t, ok, "event channel closed unexpectedly")
		blockEvent, ok := evt.Data.(event.BlockForgedEvent)
		require.True(t, ok, "event data was not BlockForgedEvent")
		assert.Equal(t, uint64(0), blockEvent.Slot)
		assert.Equal(t, uint64(0), blockEvent.BlockNumber)
		assert.Nil(t, blockEvent.BlockHash)
		assert.Equal(t, uint(0), blockEvent.TxCount)
		assert.Equal(t, uint(0), blockEvent.BlockSize)
		assert.True(t, blockEvent.Timestamp.IsZero())
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for zero-value block forged event")
	}
}
