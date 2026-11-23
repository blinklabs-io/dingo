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

package mithril

import (
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// deleteBlobBlocksAboveSlotExcept removes blocks above slot unless their
// hashes are preserved for replay. The ledger state those blocks were applied
// to is the live UTxO set's, which the import replaces, so nothing else is
// removed with them.
//
// The blocks are deleted outright rather than kept for a rollback
// notification (database.BlockRetireTxn). The import replaces the ledger
// state these blocks were applied to, so no subscriber is owed their undo,
// and its completion removes any pending rollback record
// (Database.ClearImportedChainState). A record read before then tolerates a
// body that is gone.
func deleteBlobBlocksAboveSlotExcept(
	ctx context.Context,
	db *database.Database,
	slot uint64,
	preserved map[string]struct{},
) error {
	ctx = context.WithoutCancel(ctx)
	if slot == ^uint64(0) {
		return nil
	}
	iter := db.BlocksInRangeContext(ctx, slot+1, ^uint64(0))
	var stale []ocommon.Point
	for {
		next, err := iter.NextRaw()
		if err != nil {
			iter.Close()
			return fmt.Errorf(
				"iterating stale volatile blocks above slot %d: %w",
				slot, err,
			)
		}
		if next == nil {
			break
		}
		if _, keep := preserved[string(next.Hash)]; keep {
			continue
		}
		stale = append(stale, ocommon.Point{
			Slot: next.Slot,
			Hash: append([]byte(nil), next.Hash...),
		})
	}
	iter.Close()
	if len(stale) == 0 && len(preserved) == 0 {
		return nil
	}
	txn := db.BlockTransaction(ctx)
	return txn.Do(func(txn *database.Txn) error {
		for _, p := range stale {
			block, err := database.BlockByPointTxn(txn, p)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf(
					"lookup stale volatile block at slot %d: %w",
					p.Slot, err,
				)
			}
			if err := database.BlockDeleteTxn(txn, block); err != nil {
				return fmt.Errorf(
					"delete stale volatile block at slot %d: %w",
					p.Slot, err,
				)
			}
		}
		return nil
	})
}
