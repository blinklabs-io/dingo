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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosEndorserBlockReferencer is implemented by a block header that references
// a Leios endorser block via its header extension (the Dijkstra
// [eb_hash, eb_size] pair).
type leiosEndorserBlockReferencer interface {
	LeiosEndorserBlockRef() (lcommon.Blake2b256, uint64, bool)
}

// applyEndorserBlock decodes a Leios endorser block's standalone transactions
// and applies them to the ledger ahead of the ranking block that references it,
// so the endorser-resident outputs the ranking block's transactions spend are
// present in the UTxO set.
//
// Endorser-block transactions are not part of any chain block, so — mirroring
// the genesis path (buildGenesisBlockCbor / SetGenesisCbor) — their CBOR is
// persisted as a standalone blob keyed by the endorser block's (slot, hash) and
// referenced by DOFF offsets, after which resolution works through the normal
// TieredCborCache cold-extract path. Crucially, the transactions' ledger
// effects (metadata rows, spent inputs, produced UTxOs) are recorded under the
// RANKING block's point (rbPoint), not the endorser block's: a rollback of the
// ranking block must remove them, and the ranking block is what admits the
// endorser block to the chain.
//
// It returns the number of transactions applied. Decode/build failures happen
// before storage is mutated and callers may treat them as best-effort. Once
// the endorser blob or transaction rows start writing, any error is wrapped in
// leiosEndorserBlockStorageError so callers can abort the outer transaction
// instead of committing a partial endorser-block application.
func (ls *LedgerState) applyEndorserBlock(
	txn *database.Txn,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	ebSlot uint64,
	ebHashBytes []byte,
	rawTxs []cbor.RawMessage,
) (int, error) {
	if len(rawTxs) == 0 {
		return 0, nil
	}
	if len(ebHashBytes) != lcommon.Blake2b256Size {
		return 0, fmt.Errorf(
			"endorser block hash must be %d bytes, got %d",
			lcommon.Blake2b256Size,
			len(ebHashBytes),
		)
	}
	var ebHash [lcommon.Blake2b256Size]byte
	copy(ebHash[:], ebHashBytes)

	// Decode each standalone endorser transaction, capturing its body CBOR
	// (the first array element) for the transaction-offset entry.
	txs := make([]lcommon.Transaction, 0, len(rawTxs))
	bodyCbors := make([][]byte, 0, len(rawTxs))
	for i, raw := range rawTxs {
		// leios-fetch carries each endorser transaction CBOR-in-CBOR: the
		// tx_list entry is a CBOR byte string wrapping the transaction's own
		// CBOR (LeiosTx = encodeBytes(txCbor)). Unwrap it to the inner
		// transaction bytes before decoding. (A non-byte-string entry — major
		// type != 2 — is already the bare transaction.)
		txCbor := []byte(raw)
		if len(txCbor) > 0 && txCbor[0]>>5 == 2 {
			var inner []byte
			if _, err := cbor.Decode(txCbor, &inner); err != nil {
				return 0, fmt.Errorf("unwrap endorser tx %d: %w", i, err)
			}
			txCbor = inner
		}
		var elems []cbor.RawMessage
		if _, err := cbor.Decode(txCbor, &elems); err != nil {
			return 0, fmt.Errorf("decode endorser tx %d envelope: %w", i, err)
		}
		if len(elems) < 2 {
			return 0, fmt.Errorf(
				"endorser tx %d has %d elements, want >= 2",
				i,
				len(elems),
			)
		}
		// An endorser block referenced by a Dijkstra ranking block is
		// Dijkstra-era, so decode its transactions as Dijkstra directly.
		// DetermineTransactionType is heuristic and cannot reliably identify a
		// bare standalone transaction without block/era context (it returns
		// "unknown transaction type" for these), so it must not be used here.
		tx, err := ledger.NewTransactionFromCbor(ledger.TxTypeDijkstra, txCbor)
		if err != nil {
			return 0, fmt.Errorf("decode endorser tx %d: %w", i, err)
		}
		txs = append(txs, tx)
		bodyCbors = append(bodyCbors, []byte(elems[0]))
	}

	// Deduplicate against the ledger. The Leios prototype re-includes
	// unconfirmed mempool transactions in successive endorser blocks, so a
	// transaction here may already have been applied by an earlier endorser
	// block in this batch (visible through the open txn's read-your-writes) or
	// by a committed block. Re-applying it would double-spend its inputs and
	// wedge the ledger in a permanent "UTxO already spent" retry loop
	// (issue #2699), so drop any transaction already recorded before building
	// the blob and delta.
	if len(txs) > 0 {
		hashes := make([][]byte, len(txs))
		for i, tx := range txs {
			hashes[i] = tx.Hash().Bytes()
		}
		existing, err := ls.db.GetExistingTransactionHashes(hashes, txn)
		if err != nil {
			return 0, fmt.Errorf("dedup endorser transactions: %w", err)
		}
		if len(existing) > 0 {
			skip := make(map[string]struct{}, len(existing))
			for _, h := range existing {
				skip[string(h)] = struct{}{}
			}
			keptTxs := txs[:0]
			keptBodies := bodyCbors[:0]
			for i, tx := range txs {
				if _, dup := skip[string(tx.Hash().Bytes())]; dup {
					continue
				}
				keptTxs = append(keptTxs, tx)
				keptBodies = append(keptBodies, bodyCbors[i])
			}
			txs = keptTxs
			bodyCbors = keptBodies
		}
	}
	if len(txs) == 0 {
		// Every transaction was already applied by an earlier endorser block
		// (or a committed block); nothing new to store or apply.
		return 0, nil
	}

	// Build the endorser-block blob and its offsets, then persist the blob
	// under (ebSlot, ebHash) so cold-extract can resolve the DOFF refs.
	blob, offsets, err := buildEndorserBlockBlob(txs, bodyCbors, ebSlot, ebHash)
	if err != nil {
		return 0, fmt.Errorf("build endorser block blob: %w", err)
	}
	if err := ls.db.SetGenesisCbor(ebSlot, ebHash[:], blob, txn); err != nil {
		return 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("store endorser block blob: %w", err),
		}
	}

	// Apply the endorser transactions as a delta recorded under the ranking
	// block's point (so a rollback removes them), with offsets pointing into
	// the endorser-block blob.
	delta := NewLedgerDelta(rbPoint, uint(dijkstra.EraIdDijkstra), rbBlockNumber)
	defer delta.Release()
	delta.Offsets = offsets
	for i, tx := range txs {
		delta.addTransaction(tx, i)
	}
	if err := delta.apply(ls, txn); err != nil {
		return 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("apply endorser block transactions: %w", err),
		}
	}
	return len(txs), nil
}

type leiosEndorserBlockStorageError struct {
	err error
}

func (e *leiosEndorserBlockStorageError) Error() string {
	return e.err.Error()
}

func (e *leiosEndorserBlockStorageError) Unwrap() error {
	return e.err
}

// buildEndorserBlockBlob lays out a standalone CBOR blob holding, for each
// endorser transaction, its body CBOR followed by each produced output's CBOR,
// recording the byte ranges as DOFF offsets keyed by (ebSlot, ebHash). The blob
// is not a chain block — cold-extract only slices it by offset/length — so a
// flat concatenation with precise offsets is sufficient.
func buildEndorserBlockBlob(
	txs []lcommon.Transaction,
	bodyCbors [][]byte,
	ebSlot uint64,
	ebHash [lcommon.Blake2b256Size]byte,
) ([]byte, *database.BlockIngestionResult, error) {
	var buf bytes.Buffer
	result := &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset, len(txs)),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	writeRange := func(b []byte) (uint32, uint32, error) {
		off := buf.Len()
		if off > math.MaxUint32 || len(b) > math.MaxUint32 {
			return 0, 0, errors.New("endorser block blob offset out of uint32 range")
		}
		buf.Write(b)
		//nolint:gosec // bounds checked above
		return uint32(off), uint32(len(b)), nil
	}
	for i, tx := range txs {
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		off, length, err := writeRange(bodyCbors[i])
		if err != nil {
			return nil, nil, err
		}
		result.TxOffsets[txHash] = database.CborOffset{
			BlockSlot:  ebSlot,
			BlockHash:  ebHash,
			ByteOffset: off,
			ByteLength: length,
		}
		for _, utxo := range tx.Produced() {
			outCbor := utxo.Output.Cbor()
			if len(outCbor) == 0 {
				enc, err := cbor.Encode(utxo.Output)
				if err != nil {
					return nil, nil, fmt.Errorf("encode endorser output: %w", err)
				}
				outCbor = enc
			}
			off, length, err := writeRange(outCbor)
			if err != nil {
				return nil, nil, err
			}
			result.UtxoOffsets[database.UtxoRef{
				TxId:      txHash,
				OutputIdx: utxo.Id.Index(),
			}] = database.CborOffset{
				BlockSlot:  ebSlot,
				BlockHash:  ebHash,
				ByteOffset: off,
				ByteLength: length,
			}
		}
	}
	return buf.Bytes(), result, nil
}

// ensureReferencedEndorserBlocks gates delivery of a batch of blocks to
// ledgerProcessBlock on the availability of the Leios endorser blocks they
// reference. The prototype produces an endorser block and the ranking block
// that endorses it in the same slot and diffuses them together, so the ranking
// block routinely reaches the ledger a few milliseconds ahead of its endorser
// block; without this gate applyEndorserBlock always misses the cache and the
// endorser-resident outputs are never added before the ranking block spends
// them.
//
// The wait window is EndorserBlockWaitSlots (the pipeline timing's
// CertifyByDeadlineSlots, the bound for when a referenced endorser block is
// actually available to fetch) converted to wall-clock via the Shelley slot
// length, not a hardcoded duration. Callers invoke this before opening the
// block-processing DB transaction, so the wait never holds a transaction open.
//
// Referenced endorser blocks that are not cached are handled by where the
// ranking block sits relative to the live head:
//
//   - Near the head (within the wait window): the relay co-produces and
//     diffuses the endorser block with its ranking block, so it is already
//     being pushed; wait for the in-flight leios-notify/leios-fetch to cache
//     it, with an active by-point fetch as a fallback if the window elapses.
//   - Historical backlog (well below the head, e.g. during a from-scratch
//     catch-up): the relay does not diffuse these, but it does serve any
//     endorser block by point on demand, so actively fetch them -- in parallel
//     across the available relay connections -- and apply the endorser-resident
//     outputs instead of leaving the UTxO set incomplete and trusting the
//     chain. This is what lets a from-scratch sync build a full UTxO set.
func (ls *LedgerState) ensureReferencedEndorserBlocks(
	ctx context.Context,
	blocks []ledger.Block,
) {
	if ls.config.EndorserBlockProvider == nil ||
		ls.config.EndorserBlockWaitSlots == 0 {
		return
	}
	slotLen := ls.shelleySlotLength()
	if slotLen <= 0 {
		// Without a known slot length the slot-denominated diffusion window
		// cannot be converted to wall-clock; skip the wait rather than guess.
		return
	}
	//nolint:gosec // EndorserBlockWaitSlots is a small protocol window
	timeout := time.Duration(ls.config.EndorserBlockWaitSlots) * slotLen
	// Cache re-check cadence (polling granularity, not a protocol parameter):
	// a fraction of a slot so arrival is noticed promptly, floored at 1ms so
	// the ticker interval is always positive.
	poll := max(slotLen/10, time.Millisecond)
	// wallSlot is the current wall-clock slot (the live head). A block more than
	// the wait window below it is historical backlog.
	wallSlot, wallErr := ls.CurrentSlot()
	var backfill []leiosEbRef
	var tipWait []leiosEbRef
	for _, blk := range blocks {
		ref, ok := blk.Header().(leiosEndorserBlockReferencer)
		if !ok {
			continue
		}
		ebHash, _, ok := ref.LeiosEndorserBlockRef()
		if !ok {
			continue
		}
		if _, _, cached := ls.config.EndorserBlockProvider(
			ebHash.Bytes(),
		); cached {
			continue
		}
		r := leiosEbRef{slot: blk.SlotNumber(), hash: ebHash}
		if wallErr == nil && wallSlot > blk.SlotNumber() &&
			wallSlot-blk.SlotNumber() > ls.config.EndorserBlockWaitSlots {
			backfill = append(backfill, r)
		} else {
			tipWait = append(tipWait, r)
		}
	}
	// Historical backlog: ensure a by-point fetch is in flight for each
	// referenced endorser block (prefetchBatchEndorserBlocks has usually already
	// started them for the whole read batch), then wait for it to land in the
	// cache. The fetches run concurrently in the background pool; the waits below
	// are mostly cache hits, so this does not serialize catch-up on fetch latency
	// the way a per-chunk barrier did.
	if len(backfill) > 0 && ls.leiosBackfill != nil {
		for _, r := range backfill {
			ls.leiosBackfill.spawn(r)
		}
		for _, r := range backfill {
			if _, _, ok := ls.config.EndorserBlockProvider(r.hash.Bytes()); ok {
				continue
			}
			ls.leiosBackfill.awaitFetch(ctx, r, poll)
		}
	}
	for _, r := range tipWait {
		if _, _, ok := ls.config.EndorserBlockProvider(r.hash.Bytes()); ok {
			continue
		}
		ls.waitForEndorserBlock(ctx, r.slot, r.hash, timeout, poll)
		if ls.config.EndorserBlockFetcher == nil {
			continue
		}
		if _, _, ok := ls.config.EndorserBlockProvider(r.hash.Bytes()); !ok {
			if err := ls.config.EndorserBlockFetcher(
				r.slot,
				r.hash.Bytes(),
			); err != nil {
				ls.config.Logger.Debug(
					"endorser block tip fetch fallback failed",
					"component", "ledger",
					"slot", r.slot,
					"eb_hash", r.hash.String(),
					"error", err,
				)
			}
		}
	}
}

// leiosEbRef pairs a ranking block's slot with the hash of the endorser block
// it references. The endorser block shares the ranking block's slot.
type leiosEbRef struct {
	slot uint64
	hash lcommon.Blake2b256
}

// leiosBackfillConcurrency bounds how many historical endorser blocks are
// fetched at once. The per-connection fetch guard serializes work on any one
// connection, so effective parallelism is capped by the relay connection count
// anyway; this is just an upper bound so a busy chunk cannot spawn an unbounded
// number of fetch goroutines. It is kept modest deliberately: the prototype
// relay serves endorser blocks reliably when requests are paced (one chunk's
// worth at a time, with the block-application gap between chunks) but returns
// empty manifests when hammered, so the backfill must not flood it.
const leiosBackfillConcurrency = 8

// leiosBackfiller fetches historical Leios endorser blocks by point, paced one
// block-application chunk at a time, so a from-scratch sync builds a complete
// UTxO set. It dedups in-flight fetches by endorser-block hash and bounds their
// concurrency. The prototype relay serves any endorser block by point on
// demand, so availability is not the constraint; pacing is.
type leiosBackfiller struct {
	fetch    EndorserBlockFetcherFunc
	provider EndorserBlockProviderFunc
	logger   *slog.Logger
	sem      chan struct{}
	inflight sync.Map
}

// newLeiosBackfiller returns a backfiller, or nil when no endorser-block fetcher
// is configured (in which case backfill is disabled and the ledger falls back
// to the interim trust path for unresolved endorser-resident inputs).
func newLeiosBackfiller(cfg LedgerStateConfig) *leiosBackfiller {
	if cfg.EndorserBlockFetcher == nil || cfg.EndorserBlockProvider == nil {
		return nil
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &leiosBackfiller{
		fetch:    cfg.EndorserBlockFetcher,
		provider: cfg.EndorserBlockProvider,
		logger:   logger,
		sem:      make(chan struct{}, leiosBackfillConcurrency),
	}
}

// spawn starts a background by-point fetch of the endorser block referenced by
// r unless it is already cached or a fetch is already in flight. It returns
// immediately. Deduping by hash means the read-batch prefetch and the per-chunk
// gate never fetch the same endorser block twice.
func (b *leiosBackfiller) spawn(r leiosEbRef) {
	key := string(r.hash.Bytes())
	if _, loaded := b.inflight.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	if _, _, ok := b.provider(r.hash.Bytes()); ok {
		b.inflight.Delete(key)
		return
	}
	go func() {
		b.sem <- struct{}{}
		defer func() {
			<-b.sem
			b.inflight.Delete(key)
		}()
		if _, _, ok := b.provider(r.hash.Bytes()); ok {
			return
		}
		if err := b.fetch(r.slot, r.hash.Bytes()); err != nil {
			b.logger.Debug(
				"leios endorser block backfill failed",
				"component", "ledger",
				"slot", r.slot,
				"eb_hash", r.hash.String(),
				"error", err,
			)
		}
	}()
}

// waitForEndorserBlock polls the EndorserBlockProvider until the endorser block
// identified by ebHash is fetched and cached complete, ctx is cancelled, or the
// diffusion-window timeout elapses. The concurrent leios-notify/leios-fetch
// handlers keep making progress while this blocks, so the in-flight fetch
// completes during the wait.
func (ls *LedgerState) waitForEndorserBlock(
	ctx context.Context,
	rbSlot uint64,
	ebHash lcommon.Blake2b256,
	timeout, poll time.Duration,
) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if _, _, ok := ls.config.EndorserBlockProvider(
			ebHash.Bytes(),
		); ok {
			return
		}
		select {
		case <-waitCtx.Done():
			ls.config.Logger.Info(
				"endorser block not fetched within diffusion window; proceeding without it",
				"component", "ledger",
				"slot", rbSlot,
				"eb_hash", ebHash.String(),
			)
			return
		case <-ticker.C:
		}
	}
}

// leiosBackfillMaxWait bounds how long block processing waits for a historical
// endorser block to be backfilled before proceeding without it (leaving the
// interim trust path to cover the unresolved inputs). The relay serves
// historical endorser blocks on demand, so this is only a backstop against a
// genuinely unavailable one; it is far longer than the tip diffusion window
// because a from-scratch backfill is throughput-bound, not diffusion-bound.
const leiosBackfillMaxWait = 2 * time.Minute

// awaitFetch waits for the in-flight by-point fetch of the endorser block
// referenced by r to finish (it has already been spawned). The spawned fetch
// (FetchEndorserBlockByPoint) tries every connected peer in turn before
// failing, so by the time it clears its in-flight marker it has tried all
// peers. If it cached the block, the referencing ranking block can apply it; if
// it finished without caching (every peer's response was flaky/incomplete, e.g.
// a single connection that cannot serve a large endorser block's tail), do NOT
// keep retrying the same peers — skip and let the interim Dijkstra validation
// trust path cover the unresolved inputs, so catch-up advances. With multiple
// healthy peers the all-peers attempt assembles the complete block; retrying is
// only useful when there are other peers to try, which the all-peers loop
// already exhausted. leiosBackfillMaxWait is a backstop against a fetch that
// neither caches nor clears (the fetch itself is bounded by the leios-fetch
// timeout, so this is rarely reached).
func (b *leiosBackfiller) awaitFetch(
	ctx context.Context,
	r leiosEbRef,
	poll time.Duration,
) {
	waitCtx, cancel := context.WithTimeout(ctx, leiosBackfillMaxWait)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	key := string(r.hash.Bytes())
	for {
		if _, _, ok := b.provider(r.hash.Bytes()); ok {
			return // cached: the referencing block can apply it
		}
		if _, inFlight := b.inflight.Load(key); !inFlight {
			return // the all-peers fetch finished without caching: skip fast
		}
		select {
		case <-waitCtx.Done():
			return
		case <-ticker.C:
		}
	}
}
