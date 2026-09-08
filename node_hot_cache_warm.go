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

package dingo

import (
	"context"
	"errors"
	"time"
)

// warmHotUtxoCacheInBackground kicks off a one-shot background pass that
// resolves every currently-live UTxO's CBOR into the hot UTxO cache
// (database.Database.WarmHotUtxoCache), so a freshly started or freshly
// Mithril-bootstrapped node is not left cold on its first whole-UTxO-set
// query (GetUTxOWhole) -- see blinklabs-io/dingo#4082.
//
// The goroutine holds n.db read transactions for as long as it runs, so it
// runs on its own context, derived from n.ctx but independently
// cancellable: a live database restore/truncate
// (node_lifecycle.go's quiesceForLiveLifecycleOp) closes n.db out from
// under it WITHOUT ever cancelling n.ctx, so relying on n.ctx alone would
// leave this pass running (and reading) straight through that teardown.
// stopHotCacheWarmup cancels this pass on its own and waits for it to exit;
// waitHotCacheWarmup only waits (for the normal shutdown path, where n.ctx
// cancellation elsewhere already asks it to stop). Both confirm the
// goroutine has actually exited via hotCacheWarmupDone before the caller
// proceeds to close storage.
//
// Ordinary chain-sync going forward keeps the cache warm on its own (see
// database/transaction.go's SetTransactionWithOpts, which warms produced
// outputs and evicts spent inputs as blocks are applied); this pass only
// matters for whatever was already live before this process started, which
// chain-sync's own write-path warming had no chance to see. It is also
// re-run after a live database restore/truncate rebuilds n.db and
// n.ledgerState (node_lifecycle.go), for the same reason a fresh process
// start needs it: the restored/truncated live set is new, and chain-sync's
// write-path warming has had no chance to see any of it yet either.
func (n *Node) warmHotUtxoCacheInBackground() {
	workers := n.config.cacheHotUtxoWarmupWorkers
	ctx, cancel := context.WithCancel(n.ctx)
	done := make(chan struct{})
	n.hotCacheWarmupMu.Lock()
	n.hotCacheWarmupCancel = cancel
	n.hotCacheWarmupDone = done
	n.hotCacheWarmupMu.Unlock()
	go func() {
		defer close(done)
		defer cancel()
		start := time.Now()
		n.config.logger.Info(
			"warming hot UTxO cache for the live set in the background",
			"component", "node",
		)
		warmed, err := n.db.WarmHotUtxoCache(ctx, workers)
		elapsed := time.Since(start)
		switch {
		case err != nil && errors.Is(err, context.Canceled):
			n.config.logger.Info(
				"hot UTxO cache warmup stopped",
				"component", "node",
				"warmed", warmed,
				"elapsed", elapsed,
			)
		case err != nil:
			n.config.logger.Error(
				"hot UTxO cache warmup did not finish cleanly",
				"component", "node",
				"warmed", warmed,
				"elapsed", elapsed,
				"error", err,
			)
		default:
			n.config.logger.Info(
				"hot UTxO cache warmup complete",
				"component", "node",
				"warmed", warmed,
				"elapsed", elapsed,
			)
		}
	}()
}

// waitHotCacheWarmup blocks until a running warmHotUtxoCacheInBackground
// goroutine has exited, or returns immediately if none was ever started.
// The caller must cancel n.ctx (or otherwise ensure the pass will stop)
// before calling this, the same contract waitChainSelectedNoneWorker
// documents for its own worker. Used by the normal shutdown path
// (node_shutdown.go), where n.ctx cancellation already asks the pass to
// stop; a live restore/truncate never cancels n.ctx, so it must use
// stopHotCacheWarmup instead.
func (n *Node) waitHotCacheWarmup() {
	n.hotCacheWarmupMu.Lock()
	done := n.hotCacheWarmupDone
	n.hotCacheWarmupMu.Unlock()
	if done != nil {
		<-done
	}
}

// stopHotCacheWarmup cancels a running warmHotUtxoCacheInBackground pass on
// its own (independent of n.ctx, which a live database restore/truncate
// never cancels) and waits for it to exit, or returns immediately if none
// was ever started or the previous one already finished. Matches the
// namedStop shape quiesceComponentStops uses so it can be bounded by
// stopWithDeadline like every other component that path stops.
func (n *Node) stopHotCacheWarmup() error {
	n.hotCacheWarmupMu.Lock()
	cancel := n.hotCacheWarmupCancel
	done := n.hotCacheWarmupDone
	n.hotCacheWarmupMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}
