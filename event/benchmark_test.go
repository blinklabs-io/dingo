package event_test

import (
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/event"
)

const publishBatchSize = 128

func BenchmarkPublishSubscribers(b *testing.B) {
	testType := event.EventType("bench.publish")
	testEvent := event.NewEvent(testType, 1)
	for _, subscribers := range []int{1, 10, 100, 500} {
		b.Run(strconv.Itoa(subscribers), func(b *testing.B) {
			eb := event.NewEventBus(nil, nil)
			b.Cleanup(eb.Close)
			subChans := make([]<-chan event.Event, 0, subscribers)
			for range subscribers {
				_, subCh := eb.Subscribe(testType)
				subChans = append(subChans, subCh)
			}
			b.ReportAllocs()
			b.ResetTimer()
			remaining := b.N
			for remaining > 0 {
				batchSize := min(remaining, publishBatchSize)
				for range batchSize {
					eb.Publish(testType, testEvent)
				}
				b.StopTimer()
				// Drain every published event so later iterations keep measuring
				// subscriber fan-out instead of the dropped-event fast path.
				for _, subCh := range subChans {
					for range batchSize {
						<-subCh
					}
				}
				remaining -= batchSize
				if remaining > 0 {
					b.StartTimer()
				}
			}
		})
	}
}
