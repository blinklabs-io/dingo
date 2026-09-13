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

package ledger

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// utxoWholeResolveWorkers bounds how many goroutines
// queryShelleyUtxoWhole uses to resolve live UTxOs' CBOR concurrently. Each
// worker opens its own read transaction (TieredCborCache's own caches
// synchronize themselves, but a *Txn is not shared-safe across goroutines
// the way the underlying blob store's reads are), so this is
// disk-I/O-bound parallelism, not CPU-bound -- a plain constant rather
// than a runtime.NumCPU()-scaled value, since the bottleneck this
// parallelizes (badger reads for cold blocks scattered across the whole
// chain) isn't a CPU resource.
//
// 16 was chosen from a controlled sweep (4/8/12/16/24 workers, two
// interleaved trials each) against a real Preview node's 3.17M live
// UTxOs: 16 workers had the lowest mean resolve time (~113.4s vs 8
// workers' ~146.2s) and won every one of its 8 head-to-head trials
// against 4, 8, 12, and 24 workers. A CPU profile at 16 workers put
// ~23% of cumulative samples in badger's own levelHandler.get ->
// table.Iterator.seek -> table.Table.block path (locating and decoding
// an SSTable block for a cold key) and another ~18% in GC mark work --
// neither is lock contention, which is why more workers stop helping
// past a point (24 workers, like an earlier 32-worker trial, measured
// worse than 16): badger's own per-lookup block-index seek cost
// dominates over any further parallelism this pool can extract. See
// blinklabs-io/dingo#4082 for the full sweep data.
//
// That sweep predates each worker holding a blob-only transaction instead
// of a full one (blinklabs-io/dingo#1900 review): at the metadata read
// pool's default size (DatabaseWorkers = 5), every trial from 8 workers up
// was actually saturating at 5 concurrent metadata connections regardless
// of this constant, so the 16-vs-8 gap above may partly reflect that
// contention rather than pure disk-I/O parallelism. Worth re-sweeping now
// that the worker transaction no longer takes a metadata connection at
// all on the common path.
const utxoWholeResolveWorkers = 16

// queryShelleyUtxoWhole answers GetUTxOWhole: every live UTxO in the
// current ledger state.
//
// cardano-cli's own client-side guidance is that a whole-UTxO dump
// ("query utxo --whole-utxo") is only practical against a small network;
// Dingo does not impose an additional limit here, but callers on a
// mainnet-scale chain should expect this to be slow and to return a large
// reply. This exists primarily to support LocalStateQuery-based tooling
// (e.g. the devnet cross-node ledger-state comparison,
// blinklabs-io/dingo#1900) rather than as a query aimed at a busy chain.
//
// Resolves every live UTxO's CBOR across a worker pool rather than
// IterateLiveUtxos' inline per-row loadCbor: on a chain whose live UTxOs
// are scattered across its whole history (the common case -- most were
// created long before the current tip, and typically only one live UTxO
// survives per originating block once the rest of that block's outputs
// are spent), almost every row is both a tiered-cache miss and a distinct
// block, so resolving them one at a time serializes millions of
// independent blob-store reads with no data dependency between them.
//
// Measured against a real Preview node (3.17M live UTxOs): the
// row-at-a-time path took roughly 5 minutes and exceeded gouroboros' NtC
// mux read timeout (120s) before completing; see blinklabs-io/dingo#1900's
// node-parity tool, which is what surfaced this. An 8-worker pool measured
// ~127s -- real, but still over the ceiling. Two further tuning attempts
// (grouping lookups by originating block via the batch resolve API;
// avoiding hot-cache writes for entries this one-shot query never
// rereads) did not produce a reliable further improvement. A later,
// controlled worker-count sweep (see utxoWholeResolveWorkers' doc
// comment) resolved that inconsistency: 16 workers is reliably faster
// than 8 (~113s mean resolve time, best single trial ~124s total), but
// every trial from 4 to 24 workers still finished over 120s -- the
// remaining cost is badger's own per-lookup block-index seek, which a
// worker pool cannot parallelize away. This is not yet a complete fix;
// see the linked issue for the full investigation and a recommended next
// step (streaming the reply instead of fully materializing it).
func (ls *LedgerState) queryShelleyUtxoWhole() (any, error) {
	// A bare UtxoRef, not a ref-plus-UtxoId pair: UtxoId is trivially
	// reconstructed from a ref (see resolveRow below), so storing it here
	// too would retain the same 32-byte hash and index twice per entry --
	// 80 bytes/entry instead of UtxoRef's 36, about 140MB of pure
	// duplication at the 3.17M live UTxOs measured against a real Preview
	// node (chrisguiney review). Holding the full live set in memory at
	// all (rather than a bounded window) is a further, larger memory cost
	// this doc comment already tracks as future work -- see the linked
	// issue's "streaming the reply" note -- deliberately not attempted
	// here: IterateLiveUtxoRefs' enumeration transaction would have to
	// stay open for the whole resolve phase instead of the current brief
	// enumeration pass, holding one more connection from the same scarce
	// metadata pool this PR's worker-side blob-only-txn fix exists to
	// stop starving (wolf31o2 review).
	var live []database.UtxoRef
	err := ls.db.IterateLiveUtxoRefs(nil, func(u *models.Utxo) error {
		var ref database.UtxoRef
		copy(ref.TxId[:], u.TxId)
		ref.OutputIdx = u.OutputIdx
		live = append(live, ref)
		return nil
	})
	if err != nil {
		return nil, err
	}

	type resolved struct {
		id    olocalstatequery.UtxoId
		txOut ledger.TransactionOutput
		err   error
	}
	// resolveRow resolves and decodes one row, recovering any panic from
	// either step into an ordinary error result instead of letting it
	// escape the worker goroutine below. A panic here cannot be recovered
	// by the goroutine that spawned this worker -- Go does not propagate a
	// panic across a wg.Go boundary the way Txn.Do's own recover contains
	// one on the caller's own goroutine -- so without this, a single
	// malformed row would terminate the whole node process instead of
	// failing the query with an ordinary error the way the previous
	// sequential implementation did (its resolve loop ran inside
	// IterateLiveUtxos' Txn.Do, whose recover converts a panic to
	// ErrTxnPanic). Precedent: callRewardPrecompute
	// (ledger/reward_calculation.go) (chrisguiney review).
	//
	// The recovered error wraps database.ErrTxnPanic via NewTxnPanicError,
	// not a bare fmt.Errorf, so it matches the same sentinel the
	// sequential path's Txn.Do recovery would have produced: a caller
	// using errors.Is(err, database.ErrTxnPanic) to distinguish "the
	// worker's transaction machinery panicked" from an ordinary resolve
	// failure must see the same sentinel regardless of which
	// implementation answered the query (cubic review).
	resolveRow := func(txn *database.Txn, ref database.UtxoRef) (r resolved) {
		defer func() {
			if rec := recover(); rec != nil {
				r = resolved{err: database.NewTxnPanicError(
					fmt.Sprintf(
						"resolve utxo cbor %x#%d",
						ref.TxId[:8], ref.OutputIdx,
					),
					rec,
				)}
			}
		}()
		id := olocalstatequery.UtxoId{
			Hash: ledger.NewBlake2b256(ref.TxId[:]),
			Idx:  int(ref.OutputIdx),
		}
		// WithRecovery, not the tiered cache's bare ResolveUtxoCbor: a
		// missing blob is not necessarily gone for good -- IterateLiveUtxos'
		// inline loadCbor reconstructs it from the producing block when
		// possible, and this reply must not silently regress to omitting a
		// row that path would have recovered. Even once recovery itself
		// confirms the CBOR is unrecoverable (ErrUtxoCborUnavailable), this
		// is still a live row GetUTxOWhole's contract can't omit -- main
		// fails the whole query on that sentinel rather than silently
		// returning a short set, since #1900's cross-node comparison would
		// otherwise read a dropped row as a ledger divergence rather than a
		// storage fault.
		cborBytes, err := ls.db.ResolveUtxoCborWithRecovery(
			ref.TxId[:],
			ref.OutputIdx,
			txn,
		)
		if err != nil {
			return resolved{err: fmt.Errorf(
				"resolve utxo cbor %x#%d: %w",
				ref.TxId[:8], ref.OutputIdx, err,
			)}
		}
		txOut, err := decodeUtxoWholeCborFunc(ref, cborBytes)
		return resolved{id: id, txOut: txOut, err: err}
	}

	jobs := make(chan database.UtxoRef)
	results := make(chan resolved)
	// done is closed the moment the first resolve failure arrives on
	// results, telling the feeder goroutine below to stop sending new
	// jobs. Without it, a node with one unrecoverable row still paid the
	// full resolve cost (and full peak memory) for every other row before
	// returning the error it already had at the first one -- the previous
	// sequential implementation aborted its whole traversal on the first
	// failure, via the error it returned from IterateLiveUtxos' callback
	// (chrisguiney review).
	done := make(chan struct{})
	workerCount := min(utxoWholeResolveWorkers, len(live))
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			// Each worker owns its own transaction -- see
			// utxoWholeResolveWorkers' doc comment for why one *Txn can't
			// be shared across these goroutines. Blob-only: the resolve
			// hot path (TieredCborCache.ResolveUtxoCbor) never touches
			// metadata, only ResolveUtxoCborWithRecovery's rare recovery
			// fallback does, and it opens its own metadata-capable
			// transaction on demand for that branch. A full
			// Database.Transaction(false) here would hold a metadata read
			// connection from the shared pool (sized by DatabaseWorkers,
			// 5 by default) for this whole worker's lifetime, well past
			// utxoWholeResolveWorkers workers deep -- starving every other
			// concurrent metadata reader in the node for the resolve
			// phase's entire duration (blinklabs-io/dingo#1900 review).
			txn := ls.db.BlobTxn(false)
			defer txn.Release()
			for ref := range jobs {
				results <- resolveRow(txn, ref)
			}
		})
	}
	go func() {
		defer close(jobs)
		for _, ref := range live {
			// Checked separately, and first, from the send below: once
			// done is closed, a worker simultaneously ready to receive on
			// jobs makes both cases of a single select ready together, and
			// select picks uniformly at random between ready cases rather
			// than preferring done -- so the abort was not actually
			// guaranteed to stop feeding promptly, only increasingly
			// likely to over repeated iterations (cubic review). A
			// non-blocking check up front gives done priority; the second
			// select still catches the remaining narrow window where done
			// closes between this check and the send.
			select {
			case <-done:
				return
			default:
			}
			select {
			case jobs <- ref:
			case <-done:
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	ret := make(map[olocalstatequery.UtxoId]ledger.TransactionOutput, len(live))
	// firstErr is the first error to arrive on results, which is
	// scheduling-dependent, not the first row in iteration order -- unlike
	// the previous sequential implementation, which always failed on the
	// same row for the same data. A deliberate choice, not an overlooked
	// one: serializing worker completion order to make this deterministic
	// would give up the parallelism this pool exists for, and every
	// resolve failure is still reported (just possibly naming a different
	// one of several unresolvable rows across runs), so nothing is lost
	// except which specific ref is named first (blinklabs-io/dingo#1900
	// review).
	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
				close(done)
				if utxoWholeAbortObservedFunc != nil {
					utxoWholeAbortObservedFunc()
				}
			}
			continue
		}
		ret[r.id] = r.txOut
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return []any{ret}, nil
}

// decodeUtxoWholeCborFunc is decodeUtxoWholeCbor by default. resolveRow
// calls it through this package-level variable rather than the function
// directly so a test can substitute a stub -- e.g. one that panics, to
// prove resolveRow's recover actually contains a panic from this step
// without depending on a specific CBOR byte sequence that happens to
// panic the real decoder, which isn't itself a documented, stable
// behavior to write a test against. Not t.Parallel-safe for a test that
// swaps it.
var decodeUtxoWholeCborFunc = decodeUtxoWholeCbor

// utxoWholeAbortObservedFunc, when non-nil, is called the instant
// queryShelleyUtxoWhole's first resolve failure closes done. nil (its
// production default) skips the call entirely. This lets a test observe
// exactly when the abort signal fires without depending on a fixed number
// of already-dispatched jobs or an arbitrary timing margin -- the feeder's
// own done-select race (see its comment above) already means how many jobs
// are in flight at that instant varies run to run, so a test asserting a
// specific count would be asserting on an implementation detail this
// function deliberately doesn't guarantee. Not t.Parallel-safe for a test
// that swaps it, same as decodeUtxoWholeCborFunc above.
var utxoWholeAbortObservedFunc func()

// decodeUtxoWholeCbor decodes one resolved UTxO's CBOR into
// GetUTxOWhole's reply shape. Split out from queryShelleyUtxoWhole so it
// can be tested directly against hand-built inputs.
func decodeUtxoWholeCbor(
	ref database.UtxoRef,
	cbor []byte,
) (ledger.TransactionOutput, error) {
	txOut, err := ledger.NewTransactionOutputFromCbor(cbor)
	if err != nil {
		// hex.EncodeToString rather than a %x-formatted slice of TxId: a
		// malformed row's TxId could be shorter than a fixed-length
		// slice bound, which would panic while building this very error
		// message.
		return nil, fmt.Errorf(
			"decode utxo %s#%d: %w",
			hex.EncodeToString(ref.TxId[:]),
			ref.OutputIdx,
			err,
		)
	}
	return txOut, nil
}
