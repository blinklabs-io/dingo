package event

import (
	"testing"
	"time"
)

func TestAsyncWorkerDropsQueuedEventAfterStop(t *testing.T) {
	const attempts = 128
	const testEvtType EventType = "test.async.stop"

	for attempt := range attempts {
		eb := &EventBus{
			subscribers:         make(map[EventType]map[EventSubscriberId]Subscriber),
			subscriberSnapshots: make(map[EventType][]subscriberEntry),
			asyncQueue:          make(chan asyncEvent, 1),
			stopCh:              make(chan struct{}),
		}
		sub := newChannelSubscriber(1, nil)
		eb.subscriberSnapshots[testEvtType] = []subscriberEntry{
			{
				id:         1,
				sub:        sub,
				channelSub: sub,
				kind:       "in-memory",
			},
		}
		eb.asyncQueue <- asyncEvent{
			eventType: testEvtType,
			event:     NewEvent(testEvtType, attempt),
		}
		close(eb.stopCh)

		eb.asyncWg.Add(1)
		go eb.asyncWorker()

		workerDone := make(chan struct{})
		go func() {
			eb.asyncWg.Wait()
			close(workerDone)
		}()

		select {
		case <-workerDone:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for async worker shutdown")
		}

		select {
		case evt := <-sub.ch:
			t.Fatalf(
				"queued async event was delivered after stop on attempt %d: %v",
				attempt,
				evt,
			)
		default:
		}
	}
}
