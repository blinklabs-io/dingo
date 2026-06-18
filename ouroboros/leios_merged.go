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

	"github.com/blinklabs-io/dingo/database/models"
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

var errLeiosEndorserBlockNotCached = errors.New("leios endorser block not cached")

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
		return nil, false
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
		if mask&(1<<uint(idx%64)) == 0 {
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
		for offset := range 64 {
			if mask&(1<<uint(offset)) == 0 {
				continue
			}
			idx := baseIdx + offset
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

func (o *Ouroboros) mergedLeiosRankingBlockCbor(
	blockCbor []byte,
) ([]byte, bool, error) {
	// Dijkstra carries nullable Leios/Peras certificate slots, but the current
	// Leios certificate placeholder is an empty list and does not contain an
	// endorser-block hash to drive the old merge path.
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
