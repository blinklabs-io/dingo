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
	"errors"
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
	type liveUtxo struct {
		id  olocalstatequery.UtxoId
		ref database.UtxoRef
	}
	var live []liveUtxo
	err := ls.db.IterateLiveUtxoRefs(nil, func(u *models.Utxo) error {
		var ref database.UtxoRef
		copy(ref.TxId[:], u.TxId)
		ref.OutputIdx = u.OutputIdx
		live = append(live, liveUtxo{
			id: olocalstatequery.UtxoId{
				Hash: ledger.NewBlake2b256(u.TxId),
				Idx:  int(u.OutputIdx),
			},
			ref: ref,
		})
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
	jobs := make(chan liveUtxo)
	results := make(chan resolved)
	workerCount := min(utxoWholeResolveWorkers, len(live))
	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			// Each worker owns its own transaction -- see
			// utxoWholeResolveWorkers' doc comment for why one *Txn can't
			// be shared across these goroutines.
			txn := ls.db.Transaction(false)
			defer txn.Release()
			for u := range jobs {
				// WithRecovery, not the tiered cache's bare ResolveUtxoCbor:
				// a missing blob is not necessarily gone for good --
				// IterateLiveUtxos' inline loadCbor reconstructs it from
				// the producing block when possible, and this reply must
				// not silently regress to omitting a row that path would
				// have recovered.
				cborBytes, err := ls.db.ResolveUtxoCborWithRecovery(
					u.ref.TxId[:],
					u.ref.OutputIdx,
					txn,
				)
				if err != nil {
					if errors.Is(err, database.ErrUtxoCborUnavailable) {
						// Recovery itself confirmed this ref's CBOR cannot
						// be reconstructed (e.g. the producing block is
						// gone) -- only now is dropping it rather than
						// failing the whole reply appropriate.
						continue
					}
					results <- resolved{err: fmt.Errorf(
						"resolve utxo cbor %x#%d: %w",
						u.ref.TxId[:8], u.ref.OutputIdx, err,
					)}
					continue
				}
				txOut, err := decodeUtxoWholeCbor(u.ref, cborBytes)
				results <- resolved{id: u.id, txOut: txOut, err: err}
			}
		})
	}
	go func() {
		for _, u := range live {
			jobs <- u
		}
		close(jobs)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	ret := make(map[olocalstatequery.UtxoId]ledger.TransactionOutput, len(live))
	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
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
