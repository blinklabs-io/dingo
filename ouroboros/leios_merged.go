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
)

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
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(blockRaw)
	if err != nil {
		return fmt.Errorf("decode leios endorser block: %w", err)
	}
	if len(point.Hash) == 0 {
		return errors.New("leios endorser block cache: empty point hash")
	}
	blockHash := lcommon.Blake2b256Hash(blockRaw)
	if !slices.Equal(blockHash.Bytes(), point.Hash) {
		return errors.New("leios endorser block cache: point hash mismatch")
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
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	o.leiosMu.Unlock()
	// Persist manifest and (when complete) txs to the blob store so they
	// can be served to downstream peers after the in-memory cache expires.
	// Best-effort: a storage failure does not affect in-memory serving.
	o.persistLeiosEBToDB(point, blockRaw, data)
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

// persistLeiosEBToDB writes the endorser-block manifest (always) and, when the
// tx cache is complete, its raw transaction bodies to the blob store. Both
// writes are best-effort: a failure is logged at Debug and does not affect the
// in-memory serving path.
func (o *Ouroboros) persistLeiosEBToDB(
	point ocommon.Point,
	blockRaw []byte,
	data *leiosEndorserBlockData,
) {
	db := o.leiosDatabase()
	if db == nil {
		return
	}
	if err := db.SetLeiosEBManifest(point.Slot, point.Hash, blockRaw); err != nil {
		o.config.Logger.Debug(
			"failed to persist leios EB manifest to blob store",
			"component", "network",
			"slot", point.Slot,
			"error", err,
		)
	}
	if data == nil || !data.completeTxCache() || data.txCount == 0 {
		return
	}
	if err := db.SetLeiosEBTxs(point.Hash, data.txsRaw); err != nil {
		o.config.Logger.Debug(
			"failed to persist leios EB txs to blob store",
			"component", "network",
			"slot", point.Slot,
			"error", err,
		)
	}
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
	defer o.leiosMu.Unlock()
	data, ok = o.leiosEndorserBlocks[key]
	if !ok || data == nil {
		return nil, false
	}
	if data.expired(now) {
		o.deleteLeiosEndorserBlockDataLocked(data)
		return nil, false
	}
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
		if err != types.ErrBlobKeyNotFound {
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
	txsRaw, _ := db.GetLeiosEBTxs(hash)

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

// leiosEndorserTxCountForRankingBlock resolves the endorser block referenced
// by a Dijkstra ranking block and returns its cached transaction count. The
// ranking block references its endorser block through the Leios header
// extension ([eb_hash, eb_size]) — NOT the block-level leios_cert, which is an
// empty placeholder in the current Dijkstra CDDL. ok is true only when the
// endorser block has been fetched and its full transaction set is cached.
func (o *Ouroboros) leiosEndorserTxCountForRankingBlock(
	blockCbor []byte,
) (lcommon.Blake2b256, int, bool) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &top); err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, 0, false
	}
	var header gdijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(top[0], &header); err != nil {
		return lcommon.Blake2b256{}, 0, false
	}
	ebHash, _, ok := header.LeiosEndorserBlockRef()
	if !ok {
		return lcommon.Blake2b256{}, 0, false
	}
	data, found := o.lookupLeiosEndorserBlock(ebHash.Bytes())
	if !found || !data.completeTxCache() {
		return ebHash, 0, false
	}
	return ebHash, len(data.txsRaw), true
}

// The error return is always nil today but is part of this seam's contract for
// future NtC representation work, and the caller already handles it.
//
//nolint:unparam // error reserved for future NtC representation work
func (o *Ouroboros) mergedLeiosRankingBlockCbor(
	blockCbor []byte,
) ([]byte, bool, error) {
	// A Dijkstra ranking block references its endorser block via the Leios
	// header extension ([eb_hash, eb_size]); the block-level leios_cert is an
	// empty placeholder in the current Dijkstra CDDL and carries no reference.
	// When the referenced endorser block (and its transactions) has been
	// fetched, its transactions are the endorser-resident transactions whose
	// outputs the ranking-block transactions spend.
	//
	// Applying those transactions to the ledger cannot be done by splicing them
	// into the ranking-block CBOR: the header's block_body_hash covers only the
	// ranking block's own body, so a spliced block would fail body-hash
	// verification. Endorser-block transactions are applied by ledgerProcessBlock
	// as a ledger-internal side delta when the referencing ranking block is
	// processed. This NtC path therefore serves the original ranking-block CBOR
	// and only logs whether a complete cached endorser block is available.
	if ebHash, txCount, ok := o.leiosEndorserTxCountForRankingBlock(
		blockCbor,
	); ok {
		o.config.Logger.Debug(
			"ranking block has a fetched endorser block available for application",
			"component", "network",
			"endorser_block_hash", ebHash.String(),
			"endorser_tx_count", txCount,
		)
	}
	return blockCbor, false, nil
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
	if !ok {
		return block.Cbor
	}
	o.config.Logger.Debug(
		"serving merged Leios block over NtC chainsync",
		"slot", block.Slot,
		"hash", hex.EncodeToString(block.Hash),
	)
	return merged
}
