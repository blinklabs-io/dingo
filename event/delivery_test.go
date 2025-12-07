package event

import (
	"fmt"
	"testing"
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
		t.Fatalf("expected subscriber Close() to be called after deliver failure")
	}
}
