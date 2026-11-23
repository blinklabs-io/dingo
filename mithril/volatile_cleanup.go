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
// hashes are preserved for replay. It drops derived metadata above slot and
// restores UTxOs spent by removed blocks.
//
// Every cut is the raw slot: the rows of the removed blocks sit at slots
// where the rows a ledger-state import wrote can sit too (imported UTxOs at
// the state slot, reconcile tombstones at the state slot, a hydrated
// producer's slot), and no slot rule separates the two. A caller must
// therefore run it with slot at or above the state slot.
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
		// Drop the metadata indexed for the removed blocks, or the chain
		// that replaces them would inherit their UTxO consumption, duplicate
		// transaction records and orphan governance proposals and votes.
		if err := db.UtxosDeleteRolledback(ctx, slot, txn); err != nil {
			return fmt.Errorf(
				"delete rolled-back UTxOs above slot %d: %w",
				slot, err,
			)
		}
		if err := db.TransactionsDeleteRolledback(ctx, slot, txn); err != nil {
			return fmt.Errorf(
				"delete rolled-back transactions above slot %d: %w",
				slot, err,
			)
		}
		if err := db.UtxosUnspend(ctx, slot, txn); err != nil {
			return fmt.Errorf(
				"restore UTxOs spent above slot %d: %w",
				slot, err,
			)
		}
		if err := db.DeleteGovernanceVotesAfterSlot(
			ctx,
			slot, txn,
		); err != nil {
			return fmt.Errorf(
				"delete rolled-back governance votes above slot %d: %w",
				slot, err,
			)
		}
		if err := db.DeleteGovernanceProposalsAfterSlot(
			ctx,
			slot, txn,
		); err != nil {
			return fmt.Errorf(
				"delete rolled-back governance proposals above slot %d: %w",
				slot, err,
			)
		}
		return nil
	})
}
