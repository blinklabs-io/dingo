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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
)

var _ types.PayloadResolver = (*Database)(nil)

// ResolvePayload returns the bytes the encoded CborOffset names, read from
// the block stores through the block cache. A block the chain no longer holds
// is read from its retained rolled-back body, so a row above a rollback's
// fork point stays readable until the IndexerDB writer has removed it.
func (d *Database) ResolvePayload(encoded []byte) ([]byte, error) {
	if types.IsExpiredPayload(encoded) {
		expired, err := types.DecodeExpiredPayload(encoded)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(
			"payload was not kept when its block expired: %w", expired,
		)
	}
	if types.IsEndorserOffset(encoded) {
		return d.resolveEndorserPayload(encoded)
	}
	if types.IsLiveOutputOffset(encoded) {
		return d.resolveLiveOutputPayload(encoded)
	}
	offset, err := DecodeCborOffset(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode payload offset: %w", err)
	}
	c := d.cborCache
	if cached, ok := c.blockLRU.Get(offset.BlockSlot, offset.BlockHash); ok {
		c.metrics.IncBlockLRUHit()
		if out := cached.Extract(offset.ByteOffset, offset.ByteLength); out != nil {
			return out, nil
		}
	}
	c.metrics.IncBlockLRUMiss()
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	block, err := c.getBlockCbor(
		store, txn, offset.BlockSlot, offset.BlockHash[:],
	)
	if errors.Is(err, types.ErrBlobKeyNotFound) {
		block, err = d.retainedRolledBackBody(
			store, txn, offset.BlockSlot, offset.BlockHash[:],
		)
	}
	if err != nil {
		if live, liveErr := d.livePayloadFallback(
			offset, err,
		); liveErr == nil {
			return live, nil
		}
		return nil, fmt.Errorf(
			"read block %d for payload: %w", offset.BlockSlot, err,
		)
	}
	c.metrics.IncColdExtraction()
	cached := newCachedBlock(block)
	c.blockLRU.Put(offset.BlockSlot, offset.BlockHash, cached)
	out := cached.Extract(offset.ByteOffset, offset.ByteLength)
	if out == nil {
		return nil, fmt.Errorf(
			"payload offset out of bounds: offset=%d, length=%d, block_size=%d",
			offset.ByteOffset, offset.ByteLength, len(block),
		)
	}
	return out, nil
}

// LeiosEndorserBacking returns the bytes an endorser-block offset indexes: the
// CBOR encoding of the endorser block's raw transaction list. The writer
// locates payloads in it and the resolver reads them back from it.
func LeiosEndorserBacking(txs []cbor.RawMessage) ([]byte, error) {
	backing, err := cbor.Encode(txs)
	if err != nil {
		return nil, fmt.Errorf("encode endorser block transactions: %w", err)
	}
	return backing, nil
}

// resolveEndorserPayload reads a payload of a certified closure's transaction
// from the endorser block's transaction list in the LeiosDB, which keeps a
// certified endorser block for good.
func (d *Database) resolveEndorserPayload(encoded []byte) ([]byte, error) {
	offset, err := types.DecodeEndorserOffset(encoded)
	if err != nil {
		return nil, err
	}
	txs, err := d.GetLeiosEBTxs(offset.BlockHash[:], offset.BlockSlot)
	if err != nil {
		return nil, fmt.Errorf(
			"read endorser block %d for payload: %w", offset.BlockSlot, err,
		)
	}
	backing, err := LeiosEndorserBacking(txs)
	if err != nil {
		return nil, err
	}
	end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
	if end > uint64(len(backing)) {
		return nil, fmt.Errorf(
			"endorser payload offset out of bounds: offset=%d, length=%d, size=%d",
			offset.ByteOffset, offset.ByteLength, len(backing),
		)
	}
	return append([]byte(nil), backing[offset.ByteOffset:end]...), nil
}

// resolveLiveOutputPayload reads the bytes a live-output offset names from the
// output in the ledger's live UTxO set. An output that is no longer live took
// its payload out of the active data, so the read reports expired history.
func (d *Database) resolveLiveOutputPayload(encoded []byte) ([]byte, error) {
	offset, err := types.DecodeLiveOutputOffset(encoded)
	if err != nil {
		return nil, err
	}
	cbor, ok, err := d.liveOutput(offset.TxHash[:], offset.OutputIndex)
	if err != nil {
		return nil, err
	}
	end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
	if !ok || end > uint64(len(cbor)) {
		return nil, fmt.Errorf(
			"the live output %x#%d that held the payload is spent: %w",
			offset.TxHash, offset.OutputIndex, types.ErrHistoryExpired,
		)
	}
	return append([]byte(nil), cbor[offset.ByteOffset:end]...), nil
}
