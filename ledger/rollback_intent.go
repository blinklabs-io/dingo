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
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const durableRollbackIntentSyncKey = "ledger.rollback.pending"

type durableRollbackIntent struct {
	Slot *uint64 `json:"slot"`
	Hash *string `json:"hash"`
}

func persistRollbackIntent(db *database.Database, point ocommon.Point) error {
	if db == nil || db.Metadata() == nil {
		return nil
	}
	hash := hex.EncodeToString(point.Hash)
	data, err := json.Marshal(durableRollbackIntent{
		Slot: &point.Slot,
		Hash: &hash,
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

func loadRollbackIntent(db *database.Database) (ocommon.Point, bool, error) {
	if db == nil || db.Metadata() == nil {
		return ocommon.Point{}, false, nil
	}
	raw, err := db.GetSyncState(durableRollbackIntentSyncKey, nil)
	if err != nil {
		return ocommon.Point{}, false, fmt.Errorf("load rollback intent: %w", err)
	}
	if raw == "" {
		return ocommon.Point{}, false, nil
	}
	var intent durableRollbackIntent
	if err := json.Unmarshal([]byte(raw), &intent); err != nil {
		return ocommon.Point{}, false, fmt.Errorf("decode rollback intent: %w", err)
	}
	if intent.Slot == nil || intent.Hash == nil {
		return ocommon.Point{}, false, fmt.Errorf("rollback intent is missing slot or hash")
	}
	if (*intent.Slot == 0) != (*intent.Hash == "") {
		return ocommon.Point{}, false, fmt.Errorf("rollback intent has invalid origin point")
	}
	hash, err := hex.DecodeString(*intent.Hash)
	if err != nil {
		return ocommon.Point{}, false, fmt.Errorf("decode rollback intent hash: %w", err)
	}
	return ocommon.Point{Slot: *intent.Slot, Hash: hash}, true, nil
}

func (ls *LedgerState) recoverRollbackIntent() error {
	point, pending, err := loadRollbackIntent(ls.db)
	if err != nil || !pending {
		return err
	}
	ls.config.Logger.Warn("recovering interrupted ledger rollback", "component", "ledger", "slot", point.Slot)
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
	ls.emitRollbackTransactionEvents(ls.blocksAboveSlot(point.Slot))
	if err := ls.rollback(point); err != nil {
		return fmt.Errorf("recover rollback intent: %w", err)
	}
	return clearRollbackIntent(ls.db)
}
