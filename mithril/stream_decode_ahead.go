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
	"sync"
)

// chunkSource is the part of ChunkFeed that decodeAhead reads.
type chunkSource interface {
	Next(ctx context.Context) (VerifiedChunk, error)
}

// decodeAhead takes chunks from a feed on its own goroutine, so the chunks
// after the batch being applied decode while it applies. The run receives
// them, and every error, in exactly the order Next returned them.
//
// Decoded chunks waiting for the run are bounded by maxChunks and, when
// maxBlocks is positive, by maxBlocks blocks: the goroutine starts the next
// Next only while both are below their bound. One batch's worth of each is
// the intended setting, so at most one batch plus one chunk is decoded ahead
// of the run. Chunks taken ahead also hold feed window slots until released,
// which the run's window flush already accounts for: it counts only the
// chunks it has received, and it receives every queued chunk before waiting
// on one the window holds back.
type decodeAhead struct {
	source    chunkSource
	taken     func(num uint64)
	maxChunks int
	maxBlocks int
	cancel    context.CancelFunc
	done      chan struct{}

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []decodedChunk
	blocks int // blocks in queue
	// err is the error that ended the goroutine, once it has been queued.
	err     error
	stopped bool
}

type decodedChunk struct {
	chunk VerifiedChunk
	err   error
}

// startDecodeAhead starts taking chunks from source under ctx. The caller
// must call stop. taken, when set, is called with each chunk's number once
// Next has returned it.
func startDecodeAhead(
	ctx context.Context,
	source chunkSource,
	maxChunks, maxBlocks int,
	taken func(num uint64),
) *decodeAhead {
	nextCtx, cancel := context.WithCancel(ctx)
	d := &decodeAhead{
		source:    source,
		taken:     taken,
		maxChunks: max(maxChunks, 1),
		maxBlocks: max(maxBlocks, 0),
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	d.cond = sync.NewCond(&d.mu)
	go d.run(nextCtx)
	return d
}

// full reports whether the queue is at a bound. Callers hold d.mu.
func (d *decodeAhead) full() bool {
	return len(d.queue) >= d.maxChunks ||
		(d.maxBlocks > 0 && d.blocks >= d.maxBlocks)
}

func (d *decodeAhead) run(ctx context.Context) {
	defer close(d.done)
	for {
		d.mu.Lock()
		for !d.stopped && d.full() {
			d.cond.Wait()
		}
		if d.stopped {
			d.mu.Unlock()
			return
		}
		d.mu.Unlock()

		// ctx is the run's own, so a cancelled run fails here exactly as it
		// failed the run's own Next; stop also cancels it, which is what
		// frees a Next blocked on the feed's window.
		chunk, err := d.source.Next(ctx)
		if err == nil && d.taken != nil {
			d.taken(chunk.Num)
		}

		d.mu.Lock()
		if d.stopped {
			d.mu.Unlock()
			return
		}
		d.queue = append(d.queue, decodedChunk{chunk: chunk, err: err})
		d.blocks += len(chunk.Blocks)
		if err != nil {
			d.err = err
		}
		d.cond.Broadcast()
		d.mu.Unlock()
		if err != nil {
			return
		}
	}
}

// next returns the next chunk or error in Next's order, blocking until the
// goroutine has one. It must not be called after stop. Every exit of the
// goroutine short of stop queues the error that caused it, so next never
// waits on a goroutine that has gone.
func (d *decodeAhead) next() (VerifiedChunk, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.queue) == 0 {
		if d.err != nil {
			return VerifiedChunk{}, d.err
		}
		d.cond.Wait()
	}
	item := d.queue[0]
	// Cleared so the backing array does not keep delivered blocks alive.
	d.queue[0] = decodedChunk{}
	d.queue = d.queue[1:]
	d.blocks -= len(item.chunk.Blocks)
	d.cond.Broadcast()
	return item.chunk, item.err
}

// stop ends the goroutine and waits for it. A decode already started runs to
// the end of its chunk first, as it would have on the run's own goroutine;
// whatever was taken ahead is dropped, and the feed's Close removes its
// files. It is safe to call more than once.
func (d *decodeAhead) stop() {
	d.mu.Lock()
	d.stopped = true
	d.queue = nil
	d.cond.Broadcast()
	d.mu.Unlock()
	d.cancel()
	<-d.done
}
