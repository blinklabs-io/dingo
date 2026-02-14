package event

import (
	"fmt"
	"testing"
	"time"
)

// mockSubscriber returns an error on Deliver to simulate a failing remote client.
type mockSubscriber struct {
	closed bool
}

func (m *mockSubscriber) Deliver(evt Event) error {
	return fmt.Errorf("deliver failed")
}

func (m *mockSubscriber) Close() {
	m.closed = true
}

func TestDeliverFailureUnregisters(t *testing.T) {
	// Create a bus without metrics
	eb := NewEventBus(nil, nil)
	// Register mock subscriber
	sub := &mockSubscriber{}
	subId := eb.RegisterSubscriber("test.fail", sub)
	if subId == 0 {
		t.Fatalf("expected non-zero sub id")
	}
	// Publish event should cause deliver failure and unregister
	eb.Publish("test.fail", NewEvent("test.fail", "x"))
	// After publish, subscriber map for event type should not contain subId
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if subs, ok := eb.subscribers["test.fail"]; ok {
		if _, exists := subs[subId]; exists {
			t.Fatalf("expected subscriber to be removed after deliver failure")
		}
	}
	if !sub.closed {
		t.Fatalf(
			"expected subscriber Close() to be called after deliver failure",
		)
	}
}

// TestChannelSubscriberDeliverNonBlocking verifies that channelSubscriber.Deliver
// does not block when the channel buffer is full. This is the core fix for the
// MEM-06 goroutine leak: previously, Deliver used a blocking send which caused
// goroutines spawned by publishWithTimeout to leak.
func TestChannelSubscriberDeliverNonBlocking(t *testing.T) {
	const bufferSize = 5
	sub := newChannelSubscriber(bufferSize, nil)

	// Fill the buffer completely
	for i := range bufferSize {
		err := sub.Deliver(NewEvent("test", i))
		if err != nil {
			t.Fatalf("unexpected error on buffered deliver: %v", err)
		}
	}

	// Deliver to the full buffer should return immediately without blocking.
	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "overflow"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error on non-blocking deliver: %v", err)
		}
		// Good: Deliver returned without blocking
	case <-time.After(1 * time.Second):
		t.Fatal("Deliver blocked on full channel buffer; expected non-blocking drop")
	}

	// Verify the original buffered events are still present
	for range bufferSize {
		select {
		case <-sub.ch:
			// Expected
		default:
			t.Fatal("expected buffered event not found")
		}
	}

	// Verify no extra event was inserted (the overflow should have been dropped)
	select {
	case evt := <-sub.ch:
		t.Fatalf("unexpected extra event in channel: %v", evt)
	default:
		// Good: buffer only contains the original events
	}
}

// TestChannelSubscriberDeliverAfterClose verifies that Deliver to a closed
// subscriber returns nil (not a panic) and does not block.
func TestChannelSubscriberDeliverAfterClose(t *testing.T) {
	sub := newChannelSubscriber(5, nil)
	sub.Close()

	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "after-close"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Deliver after Close should return nil, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Deliver blocked after Close")
	}
}
