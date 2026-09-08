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
// This is deliberately fire-and-forget rather than tracked in Run's `started`
// cleanup stack: it does nothing that needs undoing on shutdown (it only
// reads and populates an in-memory cache), and it already respects n.ctx's
// cancellation via database.Database.ResolveLiveUtxoRefsConcurrent's ctx
// checks between dispatched jobs, so a shutdown during warmup lets it wind
// down on its own without blocking graceful shutdown on it.
//
// Ordinary chain-sync going forward keeps the cache warm on its own (see
// database/transaction.go's SetTransactionWithOpts, which warms produced
// outputs and evicts spent inputs as blocks are applied); this pass only
// matters for whatever was already live before this process started, which
// chain-sync's own write-path warming had no chance to see.
func (n *Node) warmHotUtxoCacheInBackground() {
	workers := n.config.cacheHotUtxoWarmupWorkers
	go func() {
		start := time.Now()
		n.config.logger.Info(
			"warming hot UTxO cache for the live set in the background",
			"component", "node",
		)
		warmed, err := n.db.WarmHotUtxoCache(n.ctx, workers)
		elapsed := time.Since(start)
		switch {
		case err != nil && errors.Is(err, context.Canceled):
			n.config.logger.Info(
				"hot UTxO cache warmup stopped by shutdown",
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
