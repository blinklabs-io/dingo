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
	"fmt"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// watcherMinBackoff and watcherMaxBackoff bound the reconnect delay when a
// watcher's ChainSync session drops, matching
// internal/test/devnet/observer.go's own reconnect pattern.
const (
	watcherMinBackoff = 250 * time.Millisecond
	watcherMaxBackoff = 5 * time.Second
)

// BlockEvent is sent on a Watcher's Events channel every time the watched
// node's chain advances or rolls back. It carries no data -- it only means
// "something changed, go look" -- a caller that wants to know what changed
// re-reads the tip itself (e.g. via ReadTip on its own connection).
type BlockEvent struct{}

// newBlockEventSignal returns a channel that holds at most one pending
// BlockEvent, and a notify function that posts to it without ever
// blocking. A burst of several notify calls while a value is already
// pending collapses into that one pending value: a caller only needs to
// know "something changed," not how many times, so extra notifies during a
// burst (or while the caller is busy handling the previous one) are
// dropped rather than queued or blocked on.
func newBlockEventSignal() (<-chan BlockEvent, func()) {
	events := make(chan BlockEvent, 1)
	notify := func() {
		select {
		case events <- BlockEvent{}:
		default:
		}
	}
	return events, notify
}

// nextBackoff computes the reconnect delay after a Watcher's session ends.
// A session that got as far as established (following the chain, then
// dropping) resets to the minimum delay, since that looks like a node
// restart rather than a node that will not talk to us at all; anything
// else doubles the previous delay, capped at watcherMaxBackoff.
func nextBackoff(current time.Duration, established bool) time.Duration {
	if established {
		return watcherMinBackoff
	}
	return min(2*current, watcherMaxBackoff)
}

// Watcher follows one node's chain over a persistent ChainSync session and
// sends a BlockEvent on Events every time its tip changes, so a caller (see
// cmd/node-parity's watch command) can trigger a Check the moment a new
// block appears instead of polling on a fixed clock. It reconnects on its
// own if the session drops, matching
// internal/test/devnet/observer.go's pattern, so a node restart does not
// require the caller to do anything.
type Watcher struct {
	Events <-chan BlockEvent
	cancel context.CancelFunc
	done   chan struct{}
}

// Close stops the watcher and waits for its background goroutine to exit.
// Safe to call once.
func (w *Watcher) Close() {
	w.cancel()
	<-w.done
}

// WatchBlocks starts following addr's chain in the background and returns
// immediately; the caller reads BlockEvent values off the returned
// Watcher's Events channel as they arrive. Events are coalesced (the
// channel is buffered to 1, and a full channel is left alone rather than
// blocked on): a burst of several blocks arriving in quick succession is
// delivered as a single pending signal, since a caller only cares that
// something changed, not how many times.
func WatchBlocks(
	ctx context.Context,
	addr string,
	magic uint32,
	logf func(format string, args ...any),
) *Watcher {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	events, notify := newBlockEventSignal()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		followBlocks(runCtx, addr, magic, notify, logf)
	}()
	return &Watcher{Events: events, cancel: cancel, done: done}
}

// followBlocks keeps one node's ChainSync session alive for the life of
// runCtx, calling notify on every RollForward/RollBackward, and
// reconnecting with a bounded backoff if the session drops.
func followBlocks(
	runCtx context.Context,
	addr string,
	magic uint32,
	notify func(),
	logf func(format string, args ...any),
) {
	backoff := watcherMinBackoff
	for runCtx.Err() == nil {
		established, err := watchSession(runCtx, addr, magic, notify)
		if runCtx.Err() != nil {
			return
		}
		logf(
			"nodeparity: watcher %s: session ended (%v); reconnecting in %s",
			addr, err, backoff,
		)
		select {
		case <-runCtx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff, established)
	}
}

// watchSession runs one ChainSync connection from dial to teardown. It
// reports whether the session got as far as following the chain, and the
// reason it ended; a cancelled context returns a nil error.
func watchSession(
	ctx context.Context,
	addr string,
	magic uint32,
	notify func(),
) (established bool, err error) {
	proto := protoFromAddr(addr)
	conn, connErr := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithChainSyncConfig(chainsync.NewConfig(
			chainsync.WithRollForwardFunc(
				func(
					_ chainsync.CallbackContext, _ uint, _ any, _ chainsync.Tip,
				) error {
					notify()
					return nil
				},
			),
			chainsync.WithRollBackwardFunc(
				func(
					_ chainsync.CallbackContext, _ pcommon.Point, _ chainsync.Tip,
				) error {
					notify()
					return nil
				},
			),
		)),
	)
	if connErr != nil {
		return false, fmt.Errorf("ouroboros.New: %w", connErr)
	}
	defer conn.Close() //nolint:errcheck

	if dialErr := conn.DialTimeout(proto, addr, dialTimeout); dialErr != nil {
		return false, fmt.Errorf("dial %s %s: %w", proto, addr, dialErr)
	}

	cs := conn.ChainSync()
	if cs == nil || cs.Client == nil {
		return false, errors.New("ChainSync client unavailable")
	}
	tip, tipErr := cs.Client.GetCurrentTip()
	if tipErr != nil {
		return false, fmt.Errorf("get current tip: %w", tipErr)
	}
	if syncErr := cs.Client.Sync([]pcommon.Point{tip.Point}); syncErr != nil {
		return false, fmt.Errorf("start chainsync: %w", syncErr)
	}

	select {
	case <-ctx.Done():
		return true, nil
	case sessionErr, ok := <-conn.ErrorChan():
		if !ok {
			return true, errors.New("connection closed")
		}
		return true, sessionErr
	}
}
