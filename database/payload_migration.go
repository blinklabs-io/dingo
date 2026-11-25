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
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// MigratePayloadOffsets replaces the payload columns of an API store written
// before they held offsets with the CBOR offsets of their bytes in the blocks
// that carried them, in place. It resumes where an interrupted run stopped.
// A datum or script row whose block is not canonical or lacks the bytes is
// re-pointed to a canonical carrier, or deleted when no canonical transaction
// carries it. Any other row fails the migration, leaving what it converted
// converted, when its block cannot be read for a reason other than expiry or
// does not hold the payload. A payload of an expired block is named inside a
// live output when one holds it, and otherwise kept as an expired-payload
// marker.
func (d *Database) MigratePayloadOffsets(ctx context.Context) error {
	if err := d.requireStore("MigratePayloadOffsets"); err != nil {
		return err
	}
	migrator, ok := d.metadata.(metadata.PayloadColumnMigrator)
	if !ok {
		return errors.New("the metadata store cannot migrate payload columns")
	}
	return migrator.MigratePayloadColumns(
		ctx, &slotPayloadSource{db: d},
	)
}

// slotPayloadSource reads the canonical block at a slot, keeping the last
// one: the rows of a block are adjacent.
type slotPayloadSource struct {
	db *Database
	// decode, when set, replaces the ledger's block decoder.
	decode  func(blockType uint, cbor []byte) ([]lcommon.Transaction, error)
	slot    uint64
	locator types.PayloadLocator
	loaded  bool
}

func (s *slotPayloadSource) LocatorAtSlot(
	ctx context.Context,
	slot uint64,
) (types.PayloadLocator, error) {
	if s.loaded && s.slot == slot {
		return s.locator, nil
	}
	var locator types.PayloadLocator
	txn := s.db.BlobTxn(false)
	err := txn.Do(func(txn *Txn) error {
		point, _, err := CanonicalBlockIdentityAtSlotTxn(ctx, txn, slot)
		if err != nil {
			return fmt.Errorf("find the canonical block at slot %d: %w", slot, err)
		}
		block, err := BlockByPointTxn(txn, point)
		if err != nil {
			return fmt.Errorf("read the block at slot %d: %w", slot, err)
		}
		if len(block.Cbor) == 0 {
			return fmt.Errorf("the block at slot %d has no body", slot)
		}
		locator, err = types.NewBlockPayloadLocator(
			block.Slot, block.Hash, block.Cbor,
		)
		return err
	})
	if expired, ok := errors.AsType[*types.HistoryExpiredError](err); ok {
		// The block is gone, but an active output of it may still hold the
		// payload.
		return &liveBlockLocator{db: s.db, ctx: ctx, expired: expired}, nil
	}
	if err != nil {
		return nil, err
	}
	s.slot, s.locator, s.loaded = slot, locator, true
	return locator, nil
}

var _ metadata.PayloadCarrierScanner = (*slotPayloadSource)(nil)

// carriedContent returns the keys of the scripts a transaction carries in its
// auxiliary data and of the datums and reference scripts its sub-transaction
// outputs carry: content the store indexes no table row for.
func carriedContent(tx lcommon.Transaction) []metadata.ContentKey {
	var keys []metadata.ContentKey
	script := func(s lcommon.Script) {
		keys = append(keys, metadata.ContentKey{
			Table: "script", Hash: string(s.Hash().Bytes()),
		})
	}
	if aux := tx.AuxiliaryData(); aux != nil {
		if scripts, err := aux.NativeScripts(); err == nil {
			for _, s := range scripts {
				script(s)
			}
		}
		if scripts, err := aux.PlutusV1Scripts(); err == nil {
			for _, s := range scripts {
				script(s)
			}
		}
		if scripts, err := aux.PlutusV2Scripts(); err == nil {
			for _, s := range scripts {
				script(s)
			}
		}
		if scripts, err := aux.PlutusV3Scripts(); err == nil {
			for _, s := range scripts {
				script(s)
			}
		}
		if scripts, err := aux.PlutusV4Scripts(); err == nil {
			for _, s := range scripts {
				script(s)
			}
		}
	}
	for _, output := range lcommon.SubTransactionOutputsFromTransaction(tx) {
		if output == nil {
			continue
		}
		if ref := output.ScriptRef(); ref != nil {
			script(ref)
		}
		if datum := output.Datum(); datum != nil && len(datum.Cbor()) > 0 {
			keys = append(keys, metadata.ContentKey{
				Table: "datum",
				Hash:  string(lcommon.Blake2b256Hash(datum.Cbor()).Bytes()),
			})
		}
	}
	return keys
}

// FindCarriers implements metadata.PayloadCarrierScanner. It reads the
// canonical blocks once in ascending order and stops when every wanted key has
// a carrier. The scan runs only for the orphaned rows no indexed table
// accounts for, so a store without any pays nothing.
func (s *slotPayloadSource) FindCarriers(
	ctx context.Context,
	wanted map[metadata.ContentKey]struct{},
) (map[metadata.ContentKey]uint64, *types.HistoryExpiredError, error) {
	found := make(map[metadata.ContentKey]uint64, len(wanted))
	if len(wanted) == 0 {
		return found, nil, nil
	}
	iter, err := s.db.CanonicalBlocksFromIndex(ctx, 0, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()
	var unreadable *types.HistoryExpiredError
	for len(found) < len(wanted) {
		block, err := iter.NextRaw()
		if expired, ok := errors.AsType[*types.HistoryExpiredError](err); ok {
			if unreadable == nil {
				unreadable = expired
			}
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if block == nil {
			break
		}
		txs, err := s.transactions(block.BlockType, block.Cbor)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"decode the block at slot %d: %w", block.Slot, err,
			)
		}
		for _, tx := range txs {
			for _, key := range carriedContent(tx) {
				if _, ok := wanted[key]; !ok {
					continue
				}
				if _, ok := found[key]; !ok {
					found[key] = block.Slot
				}
			}
		}
	}
	if len(found) == len(wanted) {
		unreadable = nil
	}
	return found, unreadable, nil
}

func (s *slotPayloadSource) transactions(
	blockType uint,
	cbor []byte,
) ([]lcommon.Transaction, error) {
	if s.decode != nil {
		return s.decode(blockType, cbor)
	}
	block, err := ledger.NewBlockFromCbor(blockType, cbor)
	if err != nil {
		return nil, err
	}
	return block.Transactions(), nil
}

// liveBlockLocator locates a payload of an expired block inside the live
// outputs of that block, so a payload of active data stays reachable. An output
// whose entry is still an offset into the block is read from the ledger's live
// set, when one is installed, and the payload is named by its position in the
// block. An output whose entry was materialized as raw CBOR when the block
// expired has lost its position in the block, so the payload is named by its
// position inside that output (types.LiveOutputOffset), which the live set
// reads back. A payload no live output holds is inactive history, reported as
// the block's expiry.
type liveBlockLocator struct {
	db      *Database
	ctx     context.Context
	expired *types.HistoryExpiredError
}

func (l *liveBlockLocator) LocatePayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("locate payload: empty payload")
	}
	d := l.db
	// One read transaction serves the rows and the entries: an IndexerDB
	// keeps its entries in its metadata store, and an entry read outside the
	// held transaction would wait for a second connection the pool may not
	// have.
	txn := d.Transaction(l.ctx, false)
	defer txn.Release()
	store := txn.BlobStore()
	if store == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	refs, err := d.utxoStore().GetUtxosBySlot(l.expired.Slot, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("list the outputs of the expired block: %w", err)
	}
	for _, ref := range refs {
		data, err := store.GetUtxo(txn.Blob(), ref.Hash, ref.Idx)
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read the entry of an expired block's output: %w", err)
		}
		if !IsUtxoOffsetStorage(data) {
			// Raw CBOR: the output is live while the metadata store holds it
			// unspent.
			if live, err := d.utxoStore().GetUtxo(
				ref.Hash, ref.Idx, txn.Metadata(),
			); err != nil || live == nil {
				continue
			}
			if at := bytes.Index(data, payload); at >= 0 {
				return (&types.LiveOutputOffset{
					TxHash:      [32]byte(ref.Hash),
					OutputIndex: ref.Idx,
					ByteOffset:  uint32(at),           // #nosec G115 -- inside the output
					ByteLength:  uint32(len(payload)), // #nosec G115 -- inside the output
				}).Encode(), nil
			}
			continue
		}
		out, err := DecodeUtxoOffset(data)
		if err != nil || !bytes.Equal(out.BlockHash[:], l.expired.Hash) {
			continue
		}
		cbor, ok, err := d.liveOutput(ref.Hash, ref.Idx)
		if err != nil {
			return nil, err
		}
		if !ok || uint32(len(cbor)) != out.ByteLength { // #nosec G115 -- output size
			continue
		}
		if at := bytes.Index(cbor, payload); at >= 0 {
			return (&CborOffset{
				BlockSlot:  l.expired.Slot,
				BlockHash:  out.BlockHash,
				ByteOffset: out.ByteOffset + uint32(at), // #nosec G115 -- inside the output
				ByteLength: uint32(len(payload)),        // #nosec G115 -- inside the output
			}).Encode(), nil
		}
	}
	return nil, l.expired
}
