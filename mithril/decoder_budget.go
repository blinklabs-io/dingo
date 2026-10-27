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

package mithril

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"
)

const (
	// decoderMemoryBudgetBytes bounds the zstd history buffers that decoders
	// running at once may hold. A hostile frame can declare any window up to
	// the decoder limit, and the decoder allocates that history before tar
	// framing or digests are checked, so the number of decoders alive at once
	// has to follow memory rather than the download worker count.
	decoderMemoryBudgetBytes int64 = 2 << 30

	// decoderOverheadBytes covers what a stream decoder holds beyond its
	// history buffer: the history's extra block of slack and the per-stream
	// block buffers.
	decoderOverheadBytes = 8 << 20
)

// decoderMemoryBudget is a weighted reservation shared by every zstd decoder
// of one bootstrap. A nil budget reserves nothing.
type decoderMemoryBudget struct {
	sem      *semaphore.Weighted
	capacity int64
}

func newDecoderMemoryBudget() *decoderMemoryBudget {
	return &decoderMemoryBudget{
		sem:      semaphore.NewWeighted(decoderMemoryBudgetBytes),
		capacity: decoderMemoryBudgetBytes,
	}
}

// decoderWeight is the most memory one stream decoder can reach under the
// given limits. In a streaming decode klauspost/compress allocates a history
// of the frame's declared window, and WithDecoderMaxMemory caps that window
// as well, so the smaller limit bounds the history.
func decoderWeight(maxWindow, maxMemory uint64) int64 {
	window := min(maxWindow, maxMemory, uint64(decoderMemoryBudgetBytes))
	return int64(window) + decoderOverheadBytes // #nosec G115 -- clamped above
}

// reserve blocks until a decoder with the given limits fits in the budget and
// returns the function that gives the reservation back. A decoder larger than
// the whole budget is charged the whole budget and so runs alone.
func (b *decoderMemoryBudget) reserve(
	ctx context.Context,
	maxWindow, maxMemory uint64,
) (func(), error) {
	if b == nil {
		return func() {}, nil
	}
	weight := min(decoderWeight(maxWindow, maxMemory), b.capacity)
	if err := b.sem.Acquire(ctx, weight); err != nil {
		return nil, fmt.Errorf("reserving zstd decoder memory: %w", err)
	}
	return func() { b.sem.Release(weight) }, nil
}
