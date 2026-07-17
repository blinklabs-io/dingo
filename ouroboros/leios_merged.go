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

package ouroboros

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/protocol"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	leiosEndorserBlockCacheMaxEntries = 1024
	leiosEndorserBlockCacheTTL        = 10 * time.Minute
	// defaultLeiosClosureWaitTimeout bounds how long the NtC chainsync
	// server waits for a certifying ranking block's endorser block
	// transaction closure to be cached before falling back to serving the
	// raw block. CertRB activity is sparse, so a short wait covers the
	// common near-tip race between the async leiosnotify/leiosfetch client
	// path and NtC delivery.
	defaultLeiosClosureWaitTimeout = 3 * time.Second
)

func effectiveLeiosClosureWaitTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return defaultLeiosClosureWaitTimeout
	}
	return timeout
}

type leiosEndorserBlockData struct {
	point      ocommon.Point
	blockRaw   []byte
	txsRaw     []cbor.RawMessage
	txCount    int
	cacheKeys  []string
	insertedAt time.Time
}

func leiosBlockKey(hash []byte) string {
	return string(hash)
}

func cloneRawMessages(in []cbor.RawMessage) []cbor.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make([]cbor.RawMessage, len(in))
	for i := range in {
		out[i] = slices.Clone(in[i])
	}
	return out
}

func (o *Ouroboros) storeLeiosEndorserBlock(
	point ocommon.Point,
	blockRaw []byte,
	txsRaw []cbor.RawMessage,
) error {
	if len(blockRaw) == 0 {
		return errors.New("leios endorser block cache: empty block")
	}
	if len(point.Hash) == 0 {
		return errors.New("leios endorser block cache: empty point hash")
	}
	// Verify the served bytes hash to the requested point BEFORE decoding.
	// A peer that returns an empty, truncated, or otherwise wrong manifest
	// (the prototype relay returns empty manifests for large endorser blocks
	// when hammered; see leiosBackfiller) must be diagnosed as a fetch/serving
	// problem ("point hash mismatch") rather than misreported as a decode
	// invariant violation ("must contain at least one transaction reference").
	// The hash covers the full manifest and does not require decoding, so
	// checking it first is strictly safe and turns a wrong response into a
	// retryable fetch error instead of a terminal-looking decode failure.
	blockHash := lcommon.Blake2b256Hash(blockRaw)
	if !slices.Equal(blockHash.Bytes(), point.Hash) {
		return errors.New("leios endorser block cache: point hash mismatch")
	}
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(blockRaw)
	if err != nil {
		return fmt.Errorf("decode leios endorser block: %w", err)
	}
	cacheKeys := []string{leiosBlockKey(point.Hash)}
	data := &leiosEndorserBlockData{
		point:      point,
		blockRaw:   slices.Clone(blockRaw),
		txsRaw:     cloneRawMessages(txsRaw),
		txCount:    len(block.TransactionReferences),
		cacheKeys:  cacheKeys,
		insertedAt: time.Now(),
	}
	o.leiosMu.Lock()
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	if existing := o.leiosEndorserBlocks[cacheKeys[0]]; existing != nil &&
		existing.point.Slot != point.Slot {
		o.leiosMu.Unlock()
		return fmt.Errorf(
			"leios endorser block cache: point slot mismatch for hash: cached %d, got %d",
			existing.point.Slot,
			point.Slot,
		)
	}
	for _, key := range cacheKeys {
		o.leiosEndorserBlocks[key] = data
	}
	// Wake any NtC serving path waiting on this closure once it is complete.
	if data.completeTxCache() && len(data.txsRaw) > 0 {
		for _, key := range cacheKeys {
			o.signalLeiosClosureWaitersLocked(key)
		}
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	o.leiosMu.Unlock()
	// Queue manifest and (when complete) txs for asynchronous persistence to
	// the blob store so they can be served to downstream peers after the
	// in-memory cache expires. Best-effort and off the hot path: the write
	// happens on a background writer, not under the leios-fetch guard, so it
	// does not serialize against block application during catch-up.
	o.enqueueLeiosPersist(point, blockRaw, data)
	// Trigger local vote emission for the stored block, outside the
	// cache lock
	if o.LeiosVotes != nil {
		o.LeiosVotes.HandleEndorserBlock(point.Slot, blockHash)
	}
	// Register the block into the Leios pipeline for stage/timing
	// tracking and EB equivocation detection
	if o.LeiosPipeline != nil {
		o.LeiosPipeline.ObserveEndorserBlock(point.Slot, blockHash)
	}
	return nil
}

// leiosDatabase returns the underlying Database when the LedgerState is wired
// up, or nil when running without a database (unit tests, etc.).
func (o *Ouroboros) leiosDatabase() *database.Database {
	if o.LedgerState == nil {
		return nil
	}
	return o.LedgerState.Database()
}

func (data *leiosEndorserBlockData) completeTxCache() bool {
	return data != nil && len(data.txsRaw) == data.txCount
}

func (data *leiosEndorserBlockData) expired(now time.Time) bool {
	return data != nil &&
		data.insertedAt.Before(now.Add(-leiosEndorserBlockCacheTTL))
}

func (o *Ouroboros) pruneLeiosEndorserBlockCacheLocked(now time.Time) {
	if len(o.leiosEndorserBlocks) == 0 {
		return
	}
	uniqueBlocks := make(
		map[*leiosEndorserBlockData]struct{},
		len(o.leiosEndorserBlocks),
	)
	for key, data := range o.leiosEndorserBlocks {
		if data == nil {
			delete(o.leiosEndorserBlocks, key)
			continue
		}
		uniqueBlocks[data] = struct{}{}
	}
	for data := range uniqueBlocks {
		if data.expired(now) {
			o.deleteLeiosEndorserBlockDataLocked(data)
			delete(uniqueBlocks, data)
		}
	}
	if len(uniqueBlocks) <= leiosEndorserBlockCacheMaxEntries {
		return
	}
	blocks := make([]*leiosEndorserBlockData, 0, len(uniqueBlocks))
	for data := range uniqueBlocks {
		blocks = append(blocks, data)
	}
	slices.SortFunc(blocks, func(a, b *leiosEndorserBlockData) int {
		return a.insertedAt.Compare(b.insertedAt)
	})
	for _, data := range blocks[:len(blocks)-leiosEndorserBlockCacheMaxEntries] {
		o.deleteLeiosEndorserBlockDataLocked(data)
	}
}

func (o *Ouroboros) deleteLeiosEndorserBlockDataLocked(
	data *leiosEndorserBlockData,
) {
	if len(data.cacheKeys) > 0 {
		for _, key := range data.cacheKeys {
			if o.leiosEndorserBlocks[key] == data {
				delete(o.leiosEndorserBlocks, key)
			}
		}
		return
	}
	for key, cached := range o.leiosEndorserBlocks {
		if cached == data {
			delete(o.leiosEndorserBlocks, key)
		}
	}
}

func (o *Ouroboros) lookupLeiosEndorserBlock(
	hash []byte,
) (*leiosEndorserBlockData, bool) {
	key := leiosBlockKey(hash)
	now := time.Now()
	o.leiosMu.RLock()
	data, ok := o.leiosEndorserBlocks[key]
	if !ok || data == nil {
		o.leiosMu.RUnlock()
		// Memory cache miss: try the persistent blob store so we can serve
		// historical EBs whose in-memory TTL has elapsed.
		return o.loadLeiosEBFromDB(hash)
	}
	if !data.expired(now) {
		o.leiosMu.RUnlock()
		return data, true
	}
	o.leiosMu.RUnlock()

	o.leiosMu.Lock()
	data, ok = o.leiosEndorserBlocks[key]
	if !ok || data == nil {
		o.leiosMu.Unlock()
		return o.loadLeiosEBFromDB(hash)
	}
	if data.expired(now) {
		o.deleteLeiosEndorserBlockDataLocked(data)
		o.leiosMu.Unlock()
		return o.loadLeiosEBFromDB(hash)
	}
	o.leiosMu.Unlock()
	return data, true
}

// loadLeiosEBFromDB loads a Leios endorser block's manifest (and txs, if
// stored) from the persistent blob store and caches the result in memory.
// Returns (nil, false) when the blob store has no manifest for this hash.
func (o *Ouroboros) loadLeiosEBFromDB(hash []byte) (*leiosEndorserBlockData, bool) {
	db := o.leiosDatabase()
	if db == nil {
		return nil, false
	}
	slot, manifestRaw, err := db.GetLeiosEBManifest(hash)
	if err != nil {
		// ErrBlobKeyNotFound is the normal "not stored" path; anything else
		// is worth surfacing at Debug for diagnostics.
		if !errors.Is(err, types.ErrBlobKeyNotFound) {
			o.config.Logger.Debug(
				"failed to load leios EB manifest from blob store",
				"component", "network",
				"error", err,
			)
		}
		return nil, false
	}
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	if err != nil {
		o.config.Logger.Debug(
			"failed to decode leios EB manifest loaded from blob store",
			"component", "network",
			"error", err,
		)
		return nil, false
	}
	// Load txs if they were persisted (best-effort; may not be present for
	// EBs that completed before tx persistence was added).
	txsRaw, err := db.GetLeiosEBTxs(hash)
	if err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
		o.config.Logger.Debug(
			"failed to load leios EB txs from blob store",
			"component", "network",
			"error", err,
		)
		return nil, false
	}

	cacheKeys := []string{leiosBlockKey(hash)}
	data := &leiosEndorserBlockData{
		point:      ocommon.Point{Slot: slot, Hash: slices.Clone(hash)},
		blockRaw:   slices.Clone(manifestRaw),
		txsRaw:     cloneRawMessages(txsRaw),
		txCount:    len(block.TransactionReferences),
		cacheKeys:  cacheKeys,
		insertedAt: time.Now(),
	}
	// Populate the in-memory cache so subsequent lookups skip the DB.
	o.leiosMu.Lock()
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	// Only cache if no fresher entry has appeared while we were loading.
	if existing := o.leiosEndorserBlocks[cacheKeys[0]]; existing == nil || existing.expired(time.Now()) {
		o.pruneLeiosEndorserBlockCacheLocked(time.Now())
		for _, key := range cacheKeys {
			o.leiosEndorserBlocks[key] = data
		}
	} else {
		data = existing
	}
	o.leiosMu.Unlock()
	return data, true
}

func leiosTxsFromBitmap(
	txs []cbor.RawMessage,
	bitmaps map[uint16]uint64,
) []cbor.RawMessage {
	if len(txs) == 0 || len(bitmaps) == 0 {
		return nil
	}
	ret := make([]cbor.RawMessage, 0, len(txs))
	for idx, tx := range txs {
		bucket := idx / 64
		if bucket > math.MaxUint16 {
			break
		}
		mask := bitmaps[uint16(bucket)] // #nosec G115 -- checked above
		// MSB-first bitmap (see leiosWindowNeededMask): the tx at window
		// offset o is bit 63-o.
		if mask&(1<<uint(63-(idx%64))) == 0 {
			continue
		}
		ret = append(ret, slices.Clone(tx))
	}
	return ret
}

func validateLeiosTxBitmap(count int, bitmaps map[uint16]uint64) error {
	for bucket, mask := range bitmaps {
		if mask == 0 {
			continue
		}
		baseIdx := int(bucket) * 64
		for bit := range 64 {
			if mask&(1<<uint(bit)) == 0 {
				continue
			}
			// MSB-first bitmap (see leiosWindowNeededMask): bit b denotes
			// window offset 63-b.
			idx := baseIdx + (63 - bit)
			if idx >= count {
				return fmt.Errorf(
					"leios tx bitmap references tx index %d beyond %d cached txs",
					idx,
					count,
				)
			}
		}
	}
	return nil
}

// EndorserBlockTxsByHash returns the slot and the complete set of standalone
// transaction CBORs of the cached endorser block with the given hash, for the
// ledger to apply when the referencing Dijkstra ranking block is processed. ok
// is false when the endorser block is not cached or its transactions are
// incomplete. It satisfies ledger.EndorserBlockProviderFunc.
func (o *Ouroboros) EndorserBlockTxsByHash(
	ebHash []byte,
) (uint64, []cbor.RawMessage, bool) {
	data, ok := o.lookupLeiosEndorserBlock(ebHash)
	if !ok || !data.completeTxCache() {
		return 0, nil, false
	}
	return data.point.Slot, cloneRawMessages(data.txsRaw), true
}

// leiosAnnouncementFromBlockCbor returns the endorser block announced by the
// given ranking block's header, if any. It works for both the Conway-tagged
// (5-component) and Dijkstra (2-component) Musashi block shapes because the
// header is element 0 in both, and the Leios-extended header decodes uniformly
// via DijkstraBlockHeader.
func leiosAnnouncementFromBlockCbor(
	blockCbor []byte,
) (lcommon.Blake2b256, bool) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &top); err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, false
	}
	var header gdijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(top[0], &header); err != nil {
		return lcommon.Blake2b256{}, false
	}
	ebHash, _, ok := header.LeiosAnnouncement()
	if !ok {
		return lcommon.Blake2b256{}, false
	}
	return ebHash, true
}

// certifiedEndorserBlockHash returns the hash of the endorser block a
// certifying ranking block (CertRB) inlines over node-to-client, or ok=false
// when the block is not a CertRB or its endorser block cannot be resolved.
//
// As of prototype-2026w27 the endorser block a CertRB certifies is not named in
// the CertRB itself: the CertRB carries a leios_certificate and empty
// transaction segments, and the endorser block is the one announced by the
// immediately-preceding block on the chain (the prototype's prevAnn mechanism;
// see ouroboros-consensus MiniProtocol/ChainSync/Server.hs). We reproduce that
// by resolving the parent via the header prev-hash and reading its
// leios_announcement. Announcing and plain ranking blocks carry their own
// transactions and are not CertRBs (ok=false here).
func (o *Ouroboros) certifiedEndorserBlockHash(
	blockCbor []byte,
) (lcommon.Blake2b256, bool) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &top); err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, false
	}
	var header gdijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(top[0], &header); err != nil {
		return lcommon.Blake2b256{}, false
	}
	if cert, present := header.LeiosCertified(); !present || !cert {
		return lcommon.Blake2b256{}, false
	}
	if o.LedgerState == nil {
		return lcommon.Blake2b256{}, false
	}
	prevHash := header.PrevHash()
	parent, err := o.LedgerState.BlockByHash(prevHash.Bytes())
	if err != nil {
		return lcommon.Blake2b256{}, false
	}
	return leiosAnnouncementFromBlockCbor(parent.Cbor)
}

// resolveCertifiedEndorserTxs returns the endorser-block transactions that a
// certifying ranking block (CertRB) inlines over node-to-client, or ok=false
// when the block is not a CertRB or its endorser block is not fully available.
func (o *Ouroboros) resolveCertifiedEndorserTxs(
	blockCbor []byte,
) ([]cbor.RawMessage, bool) {
	ebHash, ok := o.certifiedEndorserBlockHash(blockCbor)
	if !ok {
		return nil, false
	}
	data, found := o.lookupLeiosEndorserBlock(ebHash.Bytes())
	if !found || !data.completeTxCache() {
		return nil, false
	}
	return cloneRawMessages(data.txsRaw), true
}

// leiosClosureCompleteLocked reports whether a complete, non-empty transaction
// closure is cached in memory for the given cache key. The caller must hold
// leiosMu.
func (o *Ouroboros) leiosClosureCompleteLocked(key string) bool {
	data, ok := o.leiosEndorserBlocks[key]
	return ok && data.completeTxCache() && len(data.txsRaw) > 0
}

// signalLeiosClosureWaitersLocked wakes and clears every waiter registered for
// the given cache key. The caller must hold leiosMu.
func (o *Ouroboros) signalLeiosClosureWaitersLocked(key string) {
	waiters := o.leiosClosureWaiters[key]
	if len(waiters) == 0 {
		return
	}
	for _, ch := range waiters {
		close(ch)
	}
	delete(o.leiosClosureWaiters, key)
}

// removeLeiosClosureWaiter deregisters a waiter channel, e.g. after its context
// is cancelled. It does not close the channel.
func (o *Ouroboros) removeLeiosClosureWaiter(key string, ch chan struct{}) {
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	waiters := o.leiosClosureWaiters[key]
	for i, w := range waiters {
		if w == ch {
			o.leiosClosureWaiters[key] = slices.Delete(waiters, i, i+1)
			break
		}
	}
	if len(o.leiosClosureWaiters[key]) == 0 {
		delete(o.leiosClosureWaiters, key)
	}
}

// waitForLeiosEndorserClosure blocks until a complete transaction closure for
// ebHash is cached in memory or ctx is done. It returns true once the closure
// is available.
func (o *Ouroboros) waitForLeiosEndorserClosure(
	ctx context.Context,
	ebHash []byte,
) bool {
	key := leiosBlockKey(ebHash)
	o.leiosMu.Lock()
	if o.leiosClosureCompleteLocked(key) {
		o.leiosMu.Unlock()
		return true
	}
	ch := make(chan struct{})
	o.leiosClosureWaiters[key] = append(o.leiosClosureWaiters[key], ch)
	o.leiosMu.Unlock()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		o.removeLeiosClosureWaiter(key, ch)
		// The store path may have completed the closure between ctx
		// cancellation and deregistration; re-check to avoid a lost wakeup.
		o.leiosMu.RLock()
		defer o.leiosMu.RUnlock()
		return o.leiosClosureCompleteLocked(key)
	}
}

// awaitMergedLeiosRankingBlock waits (bounded by ctx) for a certifying ranking
// block's endorser closure and returns the merged CBOR once available. It
// returns ok=false if the closure does not arrive before ctx is done.
func (o *Ouroboros) awaitMergedLeiosRankingBlock(
	ctx context.Context,
	blockCbor []byte,
	ebHash lcommon.Blake2b256,
) ([]byte, bool) {
	if !o.waitForLeiosEndorserClosure(ctx, ebHash.Bytes()) {
		return nil, false
	}
	merged, ok, err := o.mergedLeiosRankingBlockCbor(blockCbor)
	if err != nil || !ok {
		return nil, false
	}
	return merged, true
}

// spliceEndorserTxsIntoDijkstraBlock returns rankingBlockCbor with the endorser
// block's transactions inlined into the ranking block's (empty) transaction
// segment, matching the node-to-client "merged" block the prototype serves for
// a certifying ranking block. The Dijkstra block is [header, block_body] with
// block_body = [invalid_transactions, transactions, leios_certificate,
// peras_certificate]; only the transactions element (index 1) is replaced. The
// header, certificate, peras, and invalid-transactions elements are preserved
// verbatim so the served block's hash (a hash of the header) is unchanged; the
// header's block_body_hash intentionally no longer matches, which is acceptable
// over node-to-client because local clients do not re-verify the body hash.
//
// It returns an error (and the caller serves the raw block) when the block is
// not a fillable CertRB shape: the top level must have two elements, the body
// four, and the existing transactions segment must be empty. ebTxsRaw must be
// complete Dijkstra transactions ([transaction_body, transaction_witness_set,
// auxiliary_data/nil]) in endorser-block order.
func spliceEndorserTxsIntoDijkstraBlock(
	rankingBlockCbor []byte,
	ebTxsRaw []cbor.RawMessage,
) ([]byte, error) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(rankingBlockCbor, &top); err != nil {
		return nil, fmt.Errorf("decode dijkstra block: %w", err)
	}
	if len(top) != 2 {
		return nil, fmt.Errorf(
			"dijkstra block has %d top-level elements, expected 2",
			len(top),
		)
	}
	var body []cbor.RawMessage
	if _, err := cbor.Decode(top[1], &body); err != nil {
		return nil, fmt.Errorf("decode dijkstra block body: %w", err)
	}
	if len(body) != 4 {
		return nil, fmt.Errorf(
			"dijkstra block body has %d elements, expected 4",
			len(body),
		)
	}
	var existingTxs []cbor.RawMessage
	if _, err := cbor.Decode(body[1], &existingTxs); err != nil {
		return nil, fmt.Errorf("decode dijkstra transactions: %w", err)
	}
	if len(existingTxs) != 0 {
		return nil, fmt.Errorf(
			"ranking block already has %d transactions; not a fillable CertRB",
			len(existingTxs),
		)
	}
	newTxs, err := cbor.Encode(ebTxsRaw)
	if err != nil {
		return nil, fmt.Errorf("encode endorser transactions: %w", err)
	}
	newBody, err := cbor.Encode([]cbor.RawMessage{
		body[0], cbor.RawMessage(newTxs), body[2], body[3],
	})
	if err != nil {
		return nil, fmt.Errorf("encode merged block body: %w", err)
	}
	merged, err := cbor.Encode([]cbor.RawMessage{
		top[0], cbor.RawMessage(newBody),
	})
	if err != nil {
		return nil, fmt.Errorf("encode merged block: %w", err)
	}
	return merged, nil
}

// mergedLeiosRankingBlockCbor returns the node-to-client representation of a
// ranking block. For a certifying ranking block it inlines the certified
// endorser block's transactions (ok=true); every other block is returned
// unchanged (ok=false). An error is returned only when a CertRB was identified
// but its bytes could not be spliced, in which case the caller serves the raw
// block.
func (o *Ouroboros) mergedLeiosRankingBlockCbor(
	blockCbor []byte,
) ([]byte, bool, error) {
	ebTxsRaw, ok := o.resolveCertifiedEndorserTxs(blockCbor)
	if !ok {
		return blockCbor, false, nil
	}
	merged, err := spliceEndorserTxsIntoDijkstraBlock(blockCbor, ebTxsRaw)
	if err != nil {
		return blockCbor, false, err
	}
	return merged, true, nil
}

func (o *Ouroboros) chainsyncServerBlockCbor(
	ctx ochainsync.CallbackContext,
	block models.Block,
) []byte {
	if !o.config.EnableLeios ||
		block.Type != uint(gdijkstra.BlockTypeDijkstra) ||
		ctx.Server == nil {
		return block.Cbor
	}
	p := ctx.Server.ProtocolInstance()
	if p == nil || p.Mode() != protocol.ProtocolModeNodeToClient {
		return block.Cbor
	}
	merged, ok, err := o.mergedLeiosRankingBlockCbor(block.Cbor)
	if err != nil {
		o.config.Logger.Warn(
			"failed to build merged Leios block for NtC chainsync",
			"error", err,
			"slot", block.Slot,
		)
		return block.Cbor
	}
	if ok {
		o.recordLeiosCertRbServe("merged")
		o.config.Logger.Debug(
			"serving merged Leios block over NtC chainsync",
			"slot", block.Slot,
			"hash", hex.EncodeToString(block.Hash),
		)
		return merged
	}
	// The block was not merged. If it is a certifying ranking block whose
	// endorser closure is not cached yet, wait a bounded window for the async
	// leiosnotify/leiosfetch client path to populate it before falling back to
	// serving the raw (empty-transaction) block. Non-CertRB blocks are served
	// as-is.
	ebHash, certified := o.certifiedEndorserBlockHash(block.Cbor)
	if !certified {
		return block.Cbor
	}
	return o.serveLeiosCertRbWithWait(block, ebHash)
}

// serveLeiosCertRbWithWait waits a bounded window for a certifying ranking
// block's endorser closure to be cached, serving the merged block if it
// arrives. On timeout it falls back to serving the raw block (with empty
// transaction segments) and records the incomplete serve.
func (o *Ouroboros) serveLeiosCertRbWithWait(
	block models.Block,
	ebHash lcommon.Blake2b256,
) []byte {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		o.config.LeiosClosureWaitTimeout,
	)
	defer cancel()
	start := time.Now()
	merged, ok := o.awaitMergedLeiosRankingBlock(ctx, block.Cbor, ebHash)
	waited := time.Since(start)
	if ok {
		o.recordLeiosCertRbServe("merged_after_wait")
		o.recordLeiosCertRbWait("resolved", waited)
		o.config.Logger.Debug(
			"serving merged Leios block over NtC chainsync after closure wait",
			"slot", block.Slot,
			"hash", hex.EncodeToString(block.Hash),
			"eb_hash", ebHash.String(),
			"waited", waited,
		)
		return merged
	}
	o.recordLeiosCertRbServe("raw_unresolved")
	o.recordLeiosCertRbWait("timeout", waited)
	o.config.Logger.Warn(
		"serving CertRB with unresolved endorser closure over NtC chainsync; client may record a block with no transactions",
		"slot", block.Slot,
		"hash", hex.EncodeToString(block.Hash),
		"eb_hash", ebHash.String(),
		"waited", waited,
	)
	return block.Cbor
}
