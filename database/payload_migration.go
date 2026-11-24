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
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// MigratePayloadOffsets replaces the payload columns of an API store written
// before they held offsets with the CBOR offsets of their bytes in the blocks
// that carried them, in place. It resumes where an interrupted run stopped,
// and fails, leaving what it converted converted, when a block the rows name
// cannot be read or does not hold the payload.
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
