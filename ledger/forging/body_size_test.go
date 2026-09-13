package forging

import (
	"fmt"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func TestSegmentedBodySizeSparseMetadata(t *testing.T) {
	for _, count := range []int{0, 23, 24, 25, 255, 256, 257} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var size segmentedBodySize
			transactions := make([]MempoolTransaction, count)
			for index := range transactions {
				transaction := bodyBudgetTransaction(
					t, eraConway, index, index == count-1,
				)
				transactions[index] = transaction
				var parts []cbor.RawMessage
				_, err := cbor.Decode(transaction.Cbor, &parts)
				require.NoError(t, err)
				var metadata cbor.RawMessage
				if index == count-1 {
					metadata = parts[3]
				}
				size = size.withTransaction(parts[0], parts[1], metadata)
			}
			require.Equal(t, bodyBudgetEncodedSize(t, eraConway, transactions),
				size.size(eraConway))
		})
	}
}

func TestSegmentedBodySizeCandidateDoesNotAllocate(t *testing.T) {
	body := cbor.RawMessage{0xa0}
	witnesses := cbor.RawMessage{0xa0}
	metadata := cbor.RawMessage{0xa0}
	var size segmentedBodySize
	allocations := testing.AllocsPerRun(100, func() {
		size = segmentedBodySize{}
		for range 1024 {
			size = size.withTransaction(body, witnesses, metadata)
		}
	})
	require.Equal(t, float64(0), allocations)
	require.Equal(t, 1024, size.txCount)
}

func BenchmarkSegmentedBodySize(b *testing.B) {
	for _, count := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("incremental/%d", count), func(b *testing.B) {
			body := cbor.RawMessage{0xa0}
			witnesses := cbor.RawMessage{0xa0}
			metadata := cbor.RawMessage{0xa0}
			b.ReportAllocs()
			for b.Loop() {
				var size segmentedBodySize
				for range count {
					size = size.withTransaction(body, witnesses, metadata)
					if size.size(eraConway) == 0 {
						b.Fatal("empty candidate size")
					}
				}
			}
		})
		b.Run(fmt.Sprintf("reencode/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				bodies := []cbor.RawMessage{}
				witnesses := []cbor.RawMessage{}
				metadata := map[uint]cbor.RawMessage{}
				for index := range count {
					bodies = append(bodies, cbor.RawMessage{0xa0})
					witnesses = append(witnesses, cbor.RawMessage{0xa0})
					metadata[uint(index)] = cbor.RawMessage{0xa0}
					for _, component := range []any{bodies, witnesses, metadata} {
						if _, err := cbor.Encode(component); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}
