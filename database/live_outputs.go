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

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

// SetLiveOutputs installs the read-only lookup a read falls back to when an
// offset names a block whose history has expired: an active output is read
// from the ledger's live UTxO set, which holds its full CBOR, so expiry only
// ever removes inactive history. A nil src removes the fallback. The lookup is
// for payload resolution only; it supplies no facts to the writer.
func (d *Database) SetLiveOutputs(src types.LiveOutputReader) {
	if src == nil {
		d.liveOutputs.Store(nil)
		return
	}
	d.liveOutputs.Store(&src)
}

// liveOutput returns the CBOR of the live output at (txID, index) and whether
// it is live. Without a lookup no output is live.
func (d *Database) liveOutput(
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	src := d.liveOutputs.Load()
	if src == nil {
		return nil, false, nil
	}
	return (*src).LiveUtxoCbor(nil, txID, index)
}

// liveUtxoFallback resolves an output whose block has expired from the live
// set. It returns err unchanged when the failure is not an expiry or the
// output is not live, so a spent output still reports ErrHistoryExpired.
func (d *Database) liveUtxoFallback(
	txID []byte,
	index uint32,
	err error,
) ([]byte, error) {
	if !errors.Is(err, types.ErrHistoryExpired) {
		return nil, err
	}
	cbor, ok, liveErr := d.liveOutput(txID, index)
	if liveErr != nil {
		return nil, errors.Join(err, liveErr)
	}
	if !ok {
		return nil, err
	}
	return cbor, nil
}

// livePayloadFallback resolves the bytes at offset in an expired block from a
// live output of that block that contains them: an inline datum or reference
// script lies inside the output that carries it. It returns err unchanged when
// no live output of the block covers the range.
func (d *Database) livePayloadFallback(
	offset *CborOffset,
	err error,
) ([]byte, error) {
	if !errors.Is(err, types.ErrHistoryExpired) ||
		d.liveOutputs.Load() == nil || d.metadata == nil {
		return nil, err
	}
	ctx := context.Background()
	mdTxn := d.MetadataTxn(ctx, false)
	refs, listErr := d.utxoStore().GetUtxosBySlot(
		offset.BlockSlot, mdTxn.Metadata(),
	)
	mdTxn.Release()
	if listErr != nil {
		return nil, errors.Join(err, listErr)
	}
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return nil, err
	}
	blobTxn := store.NewTransaction(false)
	defer blobTxn.Rollback() //nolint:errcheck
	for _, ref := range refs {
		data, getErr := store.GetUtxo(blobTxn, ref.Hash, ref.Idx)
		if getErr != nil || !IsUtxoOffsetStorage(data) {
			continue
		}
		out, decErr := DecodeUtxoOffset(data)
		if decErr != nil || out.BlockHash != offset.BlockHash ||
			offset.ByteOffset < out.ByteOffset ||
			uint64(offset.ByteOffset)+uint64(offset.ByteLength) >
				uint64(out.ByteOffset)+uint64(out.ByteLength) {
			continue
		}
		cbor, ok, liveErr := d.liveOutput(ref.Hash, ref.Idx)
		if liveErr != nil {
			return nil, errors.Join(err, liveErr)
		}
		if !ok || uint32(len(cbor)) != out.ByteLength { // #nosec G115 -- output size
			continue
		}
		start := offset.ByteOffset - out.ByteOffset
		return append([]byte(nil), cbor[start:start+offset.ByteLength]...), nil
	}
	return nil, err
}

// BlockHoldsActivePayloads reports whether the metadata store still names
// bytes of the block at (slot, hash) from a payload column in use, so history
// expiry keeps the block. A database without such a store holds none.
func (d *Database) BlockHoldsActivePayloads(
	ctx context.Context,
	slot uint64,
	hash []byte,
	currentEpoch uint64,
) (bool, error) {
	checker, ok := d.metadata.(metadata.ActivePayloadChecker)
	if !ok {
		return false, nil
	}
	return checker.BlockHoldsActivePayloads(ctx, slot, hash, currentEpoch)
}
