// Copyright 2024 Blink Labs Software
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

func TestEventBusSingleSubscriber(t *testing.T) {
	var testEvtData int = 999
	var testEvtType event.EventType = "test.event"
	eb := event.NewEventBus(nil, nil)
	_, subCh := eb.Subscribe(testEvtType)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, testEvtData))
	select {
	case evt, ok := <-subCh:
		if !ok {
			t.Fatalf("event channel closed unexpectedly")
		}
		switch v := evt.Data.(type) {
		case int:
			if v != testEvtData {
				t.Fatalf("did not get expected event")
			}
		default:
			t.Fatalf("event data was not of expected type, expected int, got %T", evt.Data)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for event")
	}
}

func TestEventBusMultipleSubscribers(t *testing.T) {
	var testEvtData int = 999
	var testEvtType event.EventType = "test.event"
	eb := event.NewEventBus(nil, nil)
	_, sub1Ch := eb.Subscribe(testEvtType)
	_, sub2Ch := eb.Subscribe(testEvtType)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, testEvtData))
	var gotVal1, gotVal2 bool
	for {
		if gotVal1 && gotVal2 {
			break
		}
		select {
		case evt, ok := <-sub1Ch:
			if !ok {
				t.Fatalf("event channel closed unexpectedly")
			}
			if gotVal1 {
				t.Fatalf("received unexpected event")
			}
			switch v := evt.Data.(type) {
			case int:
				if v != testEvtData {
					t.Fatalf("did not get expected event")
				}
			default:
				t.Fatalf("event data was not of expected type, expected int, got %T", evt.Data)
			}
			gotVal1 = true
		case evt, ok := <-sub2Ch:
			if !ok {
				t.Fatalf("event channel closed unexpectedly")
			}
			if gotVal2 {
				t.Fatalf("received unexpected event")
			}
			switch v := evt.Data.(type) {
			case int:
				if v != testEvtData {
					t.Fatalf("did not get expected event")
				}
			default:
				t.Fatalf("event data was not of expected type, expected int, got %T", evt.Data)
			}
			gotVal2 = true
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for event")
		}
	}
}

func TestEventBusUnsubscribe(t *testing.T) {
	var testEvtData int = 999
	var testEvtType event.EventType = "test.event"
	eb := event.NewEventBus(nil, nil)
	subId, subCh := eb.Subscribe(testEvtType)
	eb.Unsubscribe(testEvtType, subId)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, testEvtData))
	select {
	case _, ok := <-subCh:
		if !ok {
			// Expected: Unsubscribe closes the subscriber channel
			return
		}
		t.Fatalf("received unexpected event")
	case <-time.After(1 * time.Second):
		t.Fatalf("subscriber channel was not closed after Unsubscribe")
	}
}

func TestEventBusStop(t *testing.T) {
	var testEvtType event.EventType = "test.event"
	eb := event.NewEventBus(nil, nil)

	// Subscribe regular subscriber
	_, subCh1 := eb.Subscribe(testEvtType)

	// Subscribe function subscriber
	doneCh := make(chan bool, 1)
	eb.SubscribeFunc(testEvtType, func(evt event.Event) {
		doneCh <- true
	})

	// Publish an event before Stop
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "before"))
	select {
	case <-doneCh:
		// Good, event was received
	case <-time.After(100 * time.Millisecond):
		t.Fatal("SubscribeFunc did not receive event before Stop")
	}

	// Call Stop
	eb.Stop()

	// Drain any buffered events and verify channel eventually closes
	channelClosed := false
	timeout := time.After(100 * time.Millisecond)
	for !channelClosed {
		select {
		case _, ok := <-subCh1:
			if !ok {
				channelClosed = true
			}
		case <-timeout:
			t.Fatal("regular subscriber channel was not closed within timeout")
		}
	}

	// Verify SubscribeFunc goroutine exits (by trying to publish, which should not reach the handler)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "after"))
	select {
	case <-doneCh:
		t.Fatal("SubscribeFunc should not have received event after Stop")
	case <-time.After(100 * time.Millisecond):
		// Good, no event received
	}

	// Verify we can still subscribe after Stop
	_, subCh3 := eb.Subscribe(testEvtType)

	// Publish to the new subscriber
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "new"))
	select {
	case _, ok := <-subCh3:
		if !ok {
			t.Fatal("new subscriber should receive event")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("new subscriber did not receive event")
	}

	// Clean up with second Stop
	eb.Stop()
	select {
	case _, ok := <-subCh3:
		if ok {
			t.Fatal("new subscriber channel should be closed after second Stop")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("new subscriber channel was not closed after second Stop")
	}
}

func TestSubscribeFuncPanicRecovery(t *testing.T) {
	var testEvtType event.EventType = "test.panic"
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	var received atomic.Int32

	// Register a handler that panics on the first event, then succeeds
	eb.SubscribeFunc(testEvtType, func(evt event.Event) {
		count := received.Add(1)
		if count == 1 {
			panic("intentional test panic")
		}
	})

	// First event triggers the panic -- the goroutine must survive
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "panic"))

	// Second event should still be delivered to the same handler
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "after-panic"))

	// Wait for the handler to process both events
	require.Eventually(t, func() bool {
		return received.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"handler should continue processing events after a panic",
	)
}
