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

package database

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// WarmHotUtxoCacheDefaultWorkers bounds concurrency for a background
// WarmHotUtxoCache pass by default. This is deliberately lower than
// ledger/queries_utxowhole.go's utxoWholeResolveWorkers (16): that pool
// answers one blocking NtC query racing gouroboros' 120s mux read timeout,
// while this pass runs in the background at startup and competes with real
// chain-sync/validation I/O for the same disk over a much longer window, so
// it should stay a lower-priority consumer of that shared resource.
const WarmHotUtxoCacheDefaultWorkers = 8

// ResolveLiveUtxoRefsConcurrent iterates every live UTxO ref (database.
// IterateLiveUtxoRefs) and resolves its CBOR bytes across a bounded worker
// pool of independent read transactions, invoking fn once for each
// successfully resolved ref. fn is always called from a single goroutine
// (the caller need not synchronize it).
//
// This is the shared concurrency shape behind both GetUTxOWhole
// (ledger.queryShelleyUtxoWhole) and WarmHotUtxoCache: resolving through
// TieredCborCache.ResolveUtxoCbor always populates the hot cache as a side
// effect (see cbor_cache.go), so a caller that only wants that side effect
// can pass a no-op fn -- see WarmHotUtxoCache below.
//
// A ref that cannot be resolved (types.ErrBlobKeyNotFound) is silently
// skipped, matching queryShelleyUtxoWhole's existing tolerance for a
// stale/racing live-set snapshot. ctx is checked between dispatched jobs so
// a long pass can be cancelled promptly; a job already dispatched to a
// worker still runs to completion.
func (d *Database) ResolveLiveUtxoRefsConcurrent(
	ctx context.Context,
	workers int,
	fn func(ref UtxoRef, cbor []byte) error,
) error {
	if workers <= 0 {
		workers = WarmHotUtxoCacheDefaultWorkers
	}

	var live []UtxoRef
	if err := d.IterateLiveUtxoRefs(nil, func(u *models.Utxo) error {
		var ref UtxoRef
		copy(ref.TxId[:], u.TxId)
		ref.OutputIdx = u.OutputIdx
		live = append(live, ref)
		return nil
	}); err != nil {
		return fmt.Errorf("iterate live utxo refs: %w", err)
	}
	if len(live) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	type resolved struct {
		ref  UtxoRef
		cbor []byte
		err  error
	}
	jobs := make(chan UtxoRef)
	results := make(chan resolved)
	workerCount := min(workers, len(live))
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			// Each worker owns its own transaction -- a *Txn is not
			// shared-safe across goroutines the way the underlying blob
			// store's own reads are (see utxoWholeResolveWorkers' doc
			// comment in ledger/queries_utxowhole.go).
			txn := d.Transaction(false)
			defer txn.Release()
			for ref := range jobs {
				cborBytes, err := d.cborCache.ResolveUtxoCbor(
					ref.TxId[:],
					ref.OutputIdx,
					txn,
				)
				if err != nil {
					if errors.Is(err, types.ErrBlobKeyNotFound) {
						continue
					}
					select {
					case results <- resolved{err: fmt.Errorf(
						"resolve utxo cbor %x#%d: %w",
						ref.TxId[:8], ref.OutputIdx, err,
					)}:
					case <-ctx.Done():
					}
					continue
				}
				select {
				case results <- resolved{ref: ref, cbor: cborBytes}:
				case <-ctx.Done():
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for _, ref := range live {
			select {
			case jobs <- ref:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		if err := fn(r.ref, r.cbor); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return firstErr
}

// WarmHotUtxoCache resolves every currently-live UTxO's CBOR bytes once,
// discarding the result and keeping only TieredCborCache's hot-cache
// population side effect. It returns the number of refs it resolved (not
// necessarily the number now hot -- a low hot-cache capacity can still evict
// under concurrent LRU/LFU pressure from other resolves).
//
// Intended to run once, in the background, after node startup (see node.go's
// post-ledgerState.Start hook) so a freshly started or freshly
// Mithril-bootstrapped node is not left cold on its first GetUTxOWhole query.
// Ordinary chain-sync going forward keeps the cache warm on its own (see
// SetTransactionWithOpts's warmHotUtxoCache/evictHotUtxoCache calls in
// transaction.go); this pass only matters for whatever was already live
// before this process's write-path warming had a chance to see it. See
// blinklabs-io/dingo#4082.
func (d *Database) WarmHotUtxoCache(ctx context.Context, workers int) (int, error) {
	if d.cborCache == nil {
		return 0, types.ErrBlobStoreUnavailable
	}
	warmed := 0
	err := d.ResolveLiveUtxoRefsConcurrent(
		ctx,
		workers,
		func(_ UtxoRef, _ []byte) error {
			warmed++
			return nil
		},
	)
	return warmed, err
}
