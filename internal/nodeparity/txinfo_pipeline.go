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

package nodeparity

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
)

// txInfoWindowRounds is how many rounds of concurrent /tx_info requests the
// reconstruction holds unapplied before it stops reading the chain. Results
// are applied in chain order, so one stalled request holds back every result
// after it; the window lets the other requests run on past it meanwhile. On
// mainnet a request that stalls past the client's header timeout on every
// attempt, then waits and is fetched again, costs about 11 times an ordinary
// one. At 1/100 scale (TestRunFromGenesisTxInfoThroughput), 8 rounds came
// within a quarter of the time the requests themselves take and 16 gained
// nothing more; each round holds concurrency times the batch size of
// results in memory.
const txInfoWindowRounds = 8

type txInfoFetchFunc func(
	context.Context, []string,
) ([]koiosparity.KoiosTxInfoItem, error)

// txInfoPipeline fetches /tx_info for the UTxO reconstruction's transaction
// hashes in chunks of batch hashes, with at most concurrency requests in
// flight, while the chain is read further, and applies the results to the
// reconstruction strictly in chain order: a UTxO created in one chunk may be
// spent in a later one, and spending it first would silently do nothing.
//
// A chunk is fetched whole every time, so a result is only ever a complete
// chunk. One that fails with koiosparity.ErrTxInfoMissing, or with any
// failure that is not koiosparity.ErrKoiosPermanent when retryTransient is
// set, is fetched again after each wait in waits; it gives up its request slot
// while it waits, and is first in line for the next free one. Without
// retryTransient other failures are not fetched again: the shared Koios
// client has already retried them, and a caller racing Dingo's rollback
// window cannot wait longer.
//
// Fetches run under the pipeline's context rather than a chainsync session's:
// a session that ends while the caller waits leaves every chunk in place and
// fetching, and the next session applies them. Only reset and close cancel
// them.
//
// Not safe for concurrent use: every method but the fetches themselves runs
// on the chainsync callback that owns the reconstruction.
type txInfoPipeline struct {
	fetch          txInfoFetchFunc
	batch          int
	window         int
	slots          *orderedSlots
	waits          []time.Duration
	retryTransient bool
	wait           func(context.Context, time.Duration) error
	logf           func(format string, args ...any)

	ctx       context.Context
	genCtx    context.Context
	cancelGen context.CancelFunc
	running   sync.WaitGroup
	seq       uint64
	partial   []string
	queue     []*txInfoChunk

	waitCount   int
	waitElapsed time.Duration
}

type txInfoChunk struct {
	seq    uint64
	hashes []string
	done   chan struct{}
	items  []koiosparity.KoiosTxInfoItem
	err    error
}

func newTxInfoPipeline(
	ctx context.Context,
	fetch txInfoFetchFunc,
	batch, concurrency, window int,
	waits []time.Duration,
	retryTransient bool,
	wait func(context.Context, time.Duration) error,
	logf func(format string, args ...any),
) *txInfoPipeline {
	p := &txInfoPipeline{
		fetch:          fetch,
		batch:          max(batch, 1),
		window:         max(window, 1),
		slots:          newOrderedSlots(max(concurrency, 1)),
		waits:          waits,
		retryTransient: retryTransient,
		wait:           wait,
		logf:           logf,
		ctx:            ctx,
	}
	p.genCtx, p.cancelGen = context.WithCancel(ctx)
	return p
}

// add queues hashes, in chain order after every hash already queued, and
// applies to utxoRefs every chunk whose result has arrived. It waits only
// while window chunks are held unapplied.
//
// failed reports a chunk that failed for good: the reconstruction is missing
// an unknown set of changes, and the caller must reset the pipeline and
// re-baseline. An error means sessionCtx ended while add waited; hashes stay
// queued.
func (p *txInfoPipeline) add(
	sessionCtx context.Context, utxoRefs UTxOSet, hashes ...string,
) (failed bool, err error) {
	p.partial = append(p.partial, hashes...)
	for len(p.partial) >= p.batch {
		p.start(slices.Clone(p.partial[:p.batch])) //nolint:contextcheck // fetches outlive the session
		p.partial = append(p.partial[:0], p.partial[p.batch:]...)
	}
	if failed, err := p.apply(utxoRefs); failed || err != nil {
		return failed, err
	}
	if len(p.queue) < p.window {
		return false, nil
	}
	return p.drain(sessionCtx, utxoRefs, p.window-1)
}

// flush applies every queued hash to utxoRefs, waiting for any result still
// to arrive. failed and err are as for add.
func (p *txInfoPipeline) flush(
	sessionCtx context.Context, utxoRefs UTxOSet,
) (failed bool, err error) {
	if len(p.partial) > 0 {
		p.start(slices.Clone(p.partial)) //nolint:contextcheck // fetches outlive the session
		p.partial = p.partial[:0]
	}
	return p.drain(sessionCtx, utxoRefs, 0)
}

// drain applies chunks in order until at most keep are left unapplied.
func (p *txInfoPipeline) drain(
	sessionCtx context.Context, utxoRefs UTxOSet, keep int,
) (failed bool, err error) {
	if len(p.queue) <= keep {
		return false, nil
	}
	start := time.Now()
	defer func() {
		p.waitCount++
		p.waitElapsed += time.Since(start)
	}()
	for len(p.queue) > keep {
		select {
		case <-p.queue[0].done:
		case <-sessionCtx.Done():
			return false, sessionCtx.Err()
		}
		if failed, err := p.apply(utxoRefs); failed || err != nil {
			return failed, err
		}
	}
	return false, nil
}

// apply applies every chunk at the head of the queue whose result has
// arrived, stopping at the first that has not.
func (p *txInfoPipeline) apply(utxoRefs UTxOSet) (failed bool, err error) {
	for len(p.queue) > 0 {
		c := p.queue[0]
		select {
		case <-c.done:
		default:
			return false, nil
		}
		if c.err != nil {
			// A fetch the pipeline's own context ended did not fail at
			// Koios: the run is ending.
			if ctxErr := p.genCtx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			p.logf(
				"nodeparity: koios tx_info fetch failed for %d pending tx(es): %v",
				len(c.hashes), c.err,
			)
			return true, nil
		}
		p.queue = p.queue[1:]
		UTxOChanges(utxoRefs, c.items)
	}
	return false, nil
}

// reset discards every queued hash and cancels every fetch, for a
// reconstruction the caller has replaced.
func (p *txInfoPipeline) reset() {
	p.cancelGen()
	p.genCtx, p.cancelGen = context.WithCancel(p.ctx)
	p.queue = nil
	p.partial = p.partial[:0]
}

// close cancels every fetch and waits for them to return.
func (p *txInfoPipeline) close() {
	p.cancelGen()
	p.running.Wait()
}

// takeWaits returns how many times, and for how long in all, add and flush
// waited for results since the last call.
func (p *txInfoPipeline) takeWaits() (int, time.Duration) {
	n, d := p.waitCount, p.waitElapsed
	p.waitCount, p.waitElapsed = 0, 0
	return n, d
}

func (p *txInfoPipeline) start(hashes []string) {
	p.seq++
	c := &txInfoChunk{seq: p.seq, hashes: hashes, done: make(chan struct{})}
	p.queue = append(p.queue, c)
	ctx := p.genCtx
	p.running.Go(func() { p.run(ctx, c) })
}

func (p *txInfoPipeline) retryable(err error) bool {
	return errors.Is(err, koiosparity.ErrTxInfoMissing) ||
		(p.retryTransient && !errors.Is(err, koiosparity.ErrKoiosPermanent))
}

func (p *txInfoPipeline) run(ctx context.Context, c *txInfoChunk) {
	defer close(c.done)
	for attempt := 0; ; attempt++ {
		if err := p.slots.acquire(ctx, c.seq); err != nil {
			c.err = err
			return
		}
		c.items, c.err = p.fetch(ctx, c.hashes)
		p.slots.release()
		if c.err == nil {
			return
		}
		if err := ctx.Err(); err != nil {
			c.items, c.err = nil, err
			return
		}
		if attempt >= len(p.waits) || !p.retryable(c.err) {
			return
		}
		p.logf(
			"nodeparity: koios has not answered every pending tx yet; "+
				"fetching 1 chunk(s) again in %s: %v",
			p.waits[attempt], c.err,
		)
		if err := p.wait(ctx, p.waits[attempt]); err != nil {
			c.items, c.err = nil, err
			return
		}
	}
}

// orderedSlots is a counting semaphore that, when a slot frees, hands it to
// the waiter with the lowest sequence number rather than the longest-waiting
// one. Chunks are numbered in chain order, so a chunk fetched again after a
// wait goes ahead of every later chunk: the results behind it cannot be
// applied until it is.
type orderedSlots struct {
	mu      sync.Mutex
	free    int
	waiting []*slotWaiter
}

type slotWaiter struct {
	seq     uint64
	granted chan struct{}
}

func newOrderedSlots(n int) *orderedSlots {
	return &orderedSlots{free: n}
}

func (s *orderedSlots) acquire(ctx context.Context, seq uint64) error {
	s.mu.Lock()
	if s.free > 0 && len(s.waiting) == 0 {
		s.free--
		s.mu.Unlock()
		return nil
	}
	w := &slotWaiter{seq: seq, granted: make(chan struct{})}
	i, _ := slices.BinarySearchFunc(
		s.waiting, seq, func(w *slotWaiter, seq uint64) int {
			switch {
			case w.seq < seq:
				return -1
			case w.seq > seq:
				return 1
			}
			return 0
		},
	)
	s.waiting = slices.Insert(s.waiting, i, w)
	s.mu.Unlock()
	select {
	case <-w.granted:
		return nil
	case <-ctx.Done():
	}
	s.mu.Lock()
	if i := slices.Index(s.waiting, w); i >= 0 {
		s.waiting = slices.Delete(s.waiting, i, i+1)
		s.mu.Unlock()
		return ctx.Err()
	}
	s.mu.Unlock()
	// Granted as ctx ended: pass the slot on.
	s.release()
	return ctx.Err()
}

func (s *orderedSlots) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiting) == 0 {
		s.free++
		return
	}
	w := s.waiting[0]
	s.waiting = s.waiting[1:]
	close(w.granted)
}
