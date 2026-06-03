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
	gleios "github.com/blinklabs-io/gouroboros/ledger/leios"
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
	refs       []gleios.TxReference
	txsRaw     []cbor.RawMessage
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
	block, err := gleios.NewLeiosEndorserBlockFromCbor(blockRaw)
	if err != nil {
		return fmt.Errorf("decode leios endorser block: %w", err)
	}
	var refs []gleios.TxReference
	if block.Body != nil {
		refs = slices.Clone(block.Body.TxReferences)
	}
	blockHash := block.Hash()
	cacheKeys := []string{leiosBlockKey(blockHash.Bytes())}
	if len(point.Hash) > 0 {
		pointKey := leiosBlockKey(point.Hash)
		if pointKey != cacheKeys[0] {
			cacheKeys = append(cacheKeys, pointKey)
		}
	}
	data := &leiosEndorserBlockData{
		point:      point,
		blockRaw:   slices.Clone(blockRaw),
		refs:       refs,
		txsRaw:     cloneRawMessages(txsRaw),
		cacheKeys:  cacheKeys,
		insertedAt: time.Now(),
	}
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	for _, key := range cacheKeys {
		o.leiosEndorserBlocks[key] = data
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	return nil
}

func (data *leiosEndorserBlockData) completeTxCache() bool {
	return data != nil && len(data.txsRaw) == len(data.refs)
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

func leiosAllTxBitmap(count int) (map[uint16]uint64, error) {
	if count <= 0 {
		return nil, nil
	}
	if count > (math.MaxUint16+1)*64 {
		return nil, fmt.Errorf("too many leios txs for bitmap: %d", count)
	}
	ret := make(map[uint16]uint64, (count+63)/64)
	for idx := range count {
		bucket := uint16(idx / 64) // #nosec G115 -- bounded above
		ret[bucket] |= 1 << uint(idx%64)
	}
	return ret, nil
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

func cborRawIsNull(raw cbor.RawMessage) bool {
	return len(raw) == 1 && raw[0] == 0xf6
}

func decodeRawArray(raw cbor.RawMessage, name string) ([]cbor.RawMessage, error) {
	var ret []cbor.RawMessage
	if _, err := cbor.Decode(raw, &ret); err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	return ret, nil
}

func decodeUintArray(raw cbor.RawMessage, name string) ([]uint64, error) {
	items, err := decodeRawArray(raw, name)
	if err != nil {
		return nil, err
	}
	ret := make([]uint64, 0, len(items))
	for _, item := range items {
		var idx uint64
		if _, err := cbor.Decode(item, &idx); err != nil {
			return nil, fmt.Errorf("decode %s item: %w", name, err)
		}
		ret = append(ret, idx)
	}
	return ret, nil
}

func encodeRaw(value any, name string) (cbor.RawMessage, error) {
	encoded, err := cbor.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", name, err)
	}
	return cbor.RawMessage(encoded), nil
}

func mergeLeiosTransactionArrays(
	blockCbor []byte,
	txsRaw []cbor.RawMessage,
) ([]byte, error) {
	blockItems, err := decodeRawArray(blockCbor, "leios ranking block")
	if err != nil {
		return nil, err
	}
	if len(blockItems) < 5 {
		return nil, fmt.Errorf(
			"leios ranking block: expected at least 5 fields, got %d",
			len(blockItems),
		)
	}
	bodies, err := decodeRawArray(blockItems[1], "transaction bodies")
	if err != nil {
		return nil, err
	}
	witnesses, err := decodeRawArray(blockItems[2], "transaction witness sets")
	if err != nil {
		return nil, err
	}
	if len(bodies) != len(witnesses) {
		return nil, fmt.Errorf(
			"leios ranking block: body/witness count mismatch: %d != %d",
			len(bodies),
			len(witnesses),
		)
	}
	metadata := map[uint64]cbor.RawMessage{}
	if !cborRawIsNull(blockItems[3]) {
		if _, err := cbor.Decode(blockItems[3], &metadata); err != nil {
			return nil, fmt.Errorf("decode transaction metadata set: %w", err)
		}
	}
	invalid, err := decodeUintArray(blockItems[4], "invalid transactions")
	if err != nil {
		return nil, err
	}
	baseTxCount := len(bodies)
	for idx, txRaw := range txsRaw {
		txItems, err := decodeRawArray(txRaw, "leios endorser transaction")
		if err != nil {
			return nil, err
		}
		if len(txItems) != 4 {
			return nil, fmt.Errorf(
				"leios endorser transaction: expected 4 fields, got %d",
				len(txItems),
			)
		}
		bodies = append(bodies, txItems[0])
		witnesses = append(witnesses, txItems[1])
		var isValid bool
		if _, err := cbor.Decode(txItems[2], &isValid); err != nil {
			return nil, fmt.Errorf("decode leios tx validity flag: %w", err)
		}
		mergedTxIndex := uint64(baseTxCount + idx) // #nosec G115 -- slice length fits uint64
		if !isValid {
			invalid = append(invalid, mergedTxIndex)
		}
		if !cborRawIsNull(txItems[3]) {
			metadata[mergedTxIndex] = slices.Clone(txItems[3])
		}
	}
	blockItems[1], err = encodeRaw(bodies, "merged transaction bodies")
	if err != nil {
		return nil, err
	}
	blockItems[2], err = encodeRaw(witnesses, "merged transaction witness sets")
	if err != nil {
		return nil, err
	}
	blockItems[3], err = encodeRaw(metadata, "merged transaction metadata set")
	if err != nil {
		return nil, err
	}
	blockItems[4], err = encodeRaw(invalid, "merged invalid transactions")
	if err != nil {
		return nil, err
	}
	merged, err := cbor.Encode(blockItems)
	if err != nil {
		return nil, fmt.Errorf("encode merged leios block: %w", err)
	}
	return merged, nil
}

func (o *Ouroboros) mergedLeiosRankingBlockCbor(
	blockCbor []byte,
) ([]byte, bool, error) {
	blockItems, err := decodeRawArray(blockCbor, "leios ranking block")
	if err != nil {
		return nil, false, err
	}
	if len(blockItems) < 6 || cborRawIsNull(blockItems[5]) {
		return blockCbor, false, nil
	}
	var cert lcommon.LeiosEbCertificate
	if _, err := cbor.Decode(blockItems[5], &cert); err != nil {
		return nil, false, fmt.Errorf("decode leios EB certificate: %w", err)
	}
	data, ok := o.lookupLeiosEndorserBlock(cert.EndorserBlockHash.Bytes())
	if !ok || !data.completeTxCache() || len(data.txsRaw) == 0 {
		return blockCbor, false, nil
	}
	merged, err := mergeLeiosTransactionArrays(blockCbor, data.txsRaw)
	if err != nil {
		return nil, false, err
	}
	return merged, true, nil
}

func (o *Ouroboros) chainsyncServerBlockCbor(
	ctx ochainsync.CallbackContext,
	block models.Block,
) []byte {
	if !o.config.EnableLeios ||
		block.Type != uint(gleios.BlockTypeLeiosRanking) ||
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
