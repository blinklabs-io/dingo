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
	db      *Database
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
