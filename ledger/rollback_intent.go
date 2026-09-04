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

package ledger

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// durableRollbackIntentSyncKey is an outbox for the rollback undo notification
// that is being coordinated with chain and ledger-state truncation. The block
// bodies are part of the record because chain rollback removes them from the
// block store before metadata truncation commits.
const durableRollbackIntentSyncKey = "ledger.rollback.pending"

type durableRollbackIntent struct {
	Slot   *uint64        `json:"slot"`
	Hash   *string        `json:"hash"`
	Blocks []models.Block `json:"blocks"`
}

func persistRollbackIntent(
	db *database.Database,
	point ocommon.Point,
	blocks []models.Block,
) error {
	if db == nil || db.Metadata() == nil {
		return nil
	}
	hash := hex.EncodeToString(point.Hash)
	data, err := json.Marshal(durableRollbackIntent{ //nolint:musttag // persisted envelope fields are tagged.
		Slot:   &point.Slot,
		Hash:   &hash,
		Blocks: blocks,
	})
	if err != nil {
		return fmt.Errorf("encode rollback intent: %w", err)
	}
	if err := db.SetSyncState(durableRollbackIntentSyncKey, string(data), nil); err != nil {
		return fmt.Errorf("persist rollback intent: %w", err)
	}
	return nil
}

func clearRollbackIntent(db *database.Database) error {
	if db == nil || db.Metadata() == nil {
		return nil
	}
	return db.DeleteSyncState(durableRollbackIntentSyncKey, nil)
}

func (ls *LedgerState) ensureRollbackIntent(point ocommon.Point) error {
	existing, _, pending, err := loadRollbackIntent(ls.db)
	if err != nil {
		return err
	}
	if pending {
		if existing.Slot != point.Slot ||
			!bytes.Equal(existing.Hash, point.Hash) {
			return fmt.Errorf(
				"rollback intent point %d does not match requested point %d",
				existing.Slot,
				point.Slot,
			)
		}
		return nil
	}
	blocks, err := ls.readBlocksAboveSlot(point.Slot)
	if err != nil {
		return fmt.Errorf("read rollback undo blocks: %w", err)
	}
	return persistRollbackIntent(ls.db, point, blocks)
}

func loadRollbackIntent(
	db *database.Database,
) (ocommon.Point, []models.Block, bool, error) {
	if db == nil || db.Metadata() == nil {
		return ocommon.Point{}, nil, false, nil
	}
	raw, err := db.GetSyncState(durableRollbackIntentSyncKey, nil)
	if err != nil {
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"load rollback intent: %w", err,
		)
	}
	if raw == "" {
		return ocommon.Point{}, nil, false, nil
	}
	var intent durableRollbackIntent
	if err := json.Unmarshal([]byte(raw), &intent); err != nil { //nolint:musttag // persisted envelope fields are tagged.
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"decode rollback intent: %w", err,
		)
	}
	if intent.Slot == nil || intent.Hash == nil {
		return ocommon.Point{}, nil, false, errors.New("rollback intent is missing slot or hash")
	}
	if (*intent.Slot == 0) != (*intent.Hash == "") {
		return ocommon.Point{}, nil, false, errors.New("rollback intent has invalid origin point")
	}
	hash, err := hex.DecodeString(*intent.Hash)
	if err != nil {
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"decode rollback intent hash: %w", err,
		)
	}
	if *intent.Slot > 0 && len(hash) == 0 {
		return ocommon.Point{}, nil, false, errors.New("rollback intent has empty non-origin hash")
	}
	return ocommon.Point{Slot: *intent.Slot, Hash: hash}, intent.Blocks, true, nil
}

// recoverRollbackIntent finishes a rollback whose durable undo outbox was
// written but whose chain or metadata mutation did not complete. The outbox
// owns the block payload because the primary-chain rewind may already have
// deleted the corresponding block rows.
func (ls *LedgerState) recoverRollbackIntent() error {
	point, blocks, pending, err := loadRollbackIntent(ls.db)
	if err != nil || !pending {
		return err
	}
	ls.config.Logger.Warn(
		"recovering interrupted ledger rollback",
		"component", "ledger",
		"slot", point.Slot,
	)

	ls.transactionEventMutex.Lock()
	defer ls.transactionEventMutex.Unlock()
	if point.Slot > 0 {
		contains, err := ls.primaryChainContainsPoint(point)
		if err != nil {
			return fmt.Errorf("validate rollback intent point: %w", err)
		}
		if !contains {
			return fmt.Errorf("rollback intent point is not on the primary chain")
		}
	}
	if ls.config.ChainManager != nil {
		if err := ls.config.ChainManager.RewindPrimaryChainToPoint(point); err != nil {
			return fmt.Errorf("recover primary chain rollback: %w", err)
		}
	}

	ls.RLock()
	current := ls.currentTip.Point
	ls.RUnlock()
	if current.Slot < point.Slot {
		return fmt.Errorf(
			"ledger tip %d is behind rollback intent point %d",
			current.Slot,
			point.Slot,
		)
	}
	if current.Slot != point.Slot || !bytes.Equal(current.Hash, point.Hash) {
		ls.emitRollbackTransactionEvents(blocks)
	}
	if err := ls.rollback(point); err != nil {
		return fmt.Errorf("recover rollback intent: %w", err)
	}
	return nil
}
