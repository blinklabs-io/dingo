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

// TestChannelSubscriberDeliverWaitsForCapacity verifies that
// channelSubscriber.Deliver waits for buffer capacity instead of dropping the
// event. Regression test for blinklabs-io/dingo#2932: the non-blocking send
// this replaces silently discarded events under sustained load.
func TestChannelSubscriberDeliverWaitsForCapacity(t *testing.T) {
	const bufferSize = 5
	sub := newChannelSubscriber("test", bufferSize, nil)

	// Fill the buffer completely
	for i := range bufferSize {
		err := sub.Deliver(NewEvent("test", i))
		if err != nil {
			t.Fatalf("unexpected error on buffered deliver: %v", err)
		}
	}

	// Deliver to the full buffer must wait rather than drop.
	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "overflow"))
	}()

	select {
	case <-done:
		t.Fatal("Deliver returned while the buffer was full; event was dropped")
	case <-time.After(50 * time.Millisecond):
		// Expected: Deliver is waiting for capacity.
	}

	// Draining one slot releases the waiting Deliver.
	first := <-sub.ch

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error after capacity freed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver did not complete after buffer capacity was freed")
	}

	// Every event is accounted for: the drained one, the rest of the
	// original batch, and the event that had to wait.
	got := []any{first.Data}
	for range bufferSize {
		select {
		case evt := <-sub.ch:
			got = append(got, evt.Data)
		default:
			t.Fatalf("expected %d events, only got %d", bufferSize+1, len(got))
		}
	}
	want := []any{0, 1, 2, 3, 4, "overflow"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d: expected %v, got %v", i, want[i], got[i])
		}
	}
}

// TestChannelSubscriberDeliverAfterClose verifies that Deliver to a closed
// subscriber returns nil (not a panic) and does not block.
func TestChannelSubscriberDeliverAfterClose(t *testing.T) {
	sub := newChannelSubscriber("test", 5, nil)
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
