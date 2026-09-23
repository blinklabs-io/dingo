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
	"context"
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

const (
	durableRollbackIntentVersion  = 1
	maxRollbackIntentBlocks       = 4096
	maxRollbackIntentPayloadBytes = 64 << 20
)

var (
	errInvalidRollbackIntent  = errors.New("invalid rollback intent")
	errRollbackIntentConflict = errors.New("rollback intent conflict")
	errRollbackIntentTooLarge = errors.New("rollback intent exceeds durable limit")
)

type durableRollbackIntent struct {
	Version int                    `json:"format_version"`
	Slot    *uint64                `json:"slot"`
	Hash    *string                `json:"hash"`
	Blocks  []durableRollbackBlock `json:"blocks"`
}

type durableRollbackBlock struct {
	Hash     []byte `json:"hash"`
	PrevHash []byte `json:"prev_hash"`
	Cbor     []byte `json:"cbor"`
	ID       uint64 `json:"id"`
	Slot     uint64 `json:"slot"`
	Number   uint64 `json:"number"`
	Type     uint   `json:"type"`
}

func durableBlocks(blocks []models.Block) ([]durableRollbackBlock, error) {
	if len(blocks) > maxRollbackIntentBlocks {
		return nil, fmt.Errorf(
			"%w: %d blocks exceeds %d",
			errRollbackIntentTooLarge,
			len(blocks),
			maxRollbackIntentBlocks,
		)
	}
	total := 0
	ret := make([]durableRollbackBlock, 0, len(blocks))
	for _, block := range blocks {
		total += len(block.Hash) + len(block.PrevHash) + len(block.Cbor)
		if total > maxRollbackIntentPayloadBytes {
			return nil, fmt.Errorf(
				"%w: block bytes exceed %d",
				errRollbackIntentTooLarge,
				maxRollbackIntentPayloadBytes,
			)
		}
		ret = append(ret, durableRollbackBlock{
			Hash: block.Hash, PrevHash: block.PrevHash, Cbor: block.Cbor,
			ID: block.ID, Slot: block.Slot, Number: block.Number,
			Type: block.Type,
		})
	}
	return ret, nil
}

func modelBlocks(blocks []durableRollbackBlock) []models.Block {
	ret := make([]models.Block, 0, len(blocks))
	for _, block := range blocks {
		ret = append(ret, models.Block{
			Hash: block.Hash, PrevHash: block.PrevHash, Cbor: block.Cbor,
			ID: block.ID, Slot: block.Slot, Number: block.Number,
			Type: block.Type,
		})
	}
	return ret
}

func persistRollbackIntent(
	db *database.Database,
	point ocommon.Point,
	blocks []models.Block,
) error {
	if db == nil || db.Metadata() == nil {
		return nil
	}
	persistedBlocks, err := durableBlocks(blocks)
	if err != nil {
		return err
	}
	hash := hex.EncodeToString(point.Hash)
	data, err := json.Marshal(durableRollbackIntent{
		Version: durableRollbackIntentVersion,
		Slot:    &point.Slot,
		Hash:    &hash,
		Blocks:  persistedBlocks,
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

// ensureRollbackIntent records the undo payload for a rollback to point.
//
// A nil rollbackBlocks means "resolve the payload from the chain", which is
// only correct before the bodies above point are deleted. Callers that rewind
// the primary chain first -- reconcilePrimaryChainTipWithLedgerTip's two
// branches instead pass their captured applied blocks here and retain the
// record through undo delivery. Recovery rewinds that reject a block before
// its apply event was published have no transaction undo to deliver.
//
// A pending record for a different point is completed or merged, never
// discarded: at or above the pending slot the caller recovers the record and
// retries, and below it the two payloads are merged, because the older
// record's bodies no longer exist on the chain to be re-read.
func (ls *LedgerState) ensureRollbackIntent(
	point ocommon.Point,
	rollbackBlocks []models.Block,
) error {
	existing, existingBlocks, pending, err := loadRollbackIntent(ls.db)
	if err != nil {
		return err
	}
	if pending {
		if existing.Slot != point.Slot ||
			!bytes.Equal(existing.Hash, point.Hash) {
			if point.Slot >= existing.Slot {
				return fmt.Errorf(
					"%w: intent at slot %d must complete before rollback to slot %d",
					errRollbackIntentConflict,
					existing.Slot,
					point.Slot,
				)
			}
			if rollbackBlocks == nil {
				rollbackBlocks, err = ls.readBlocksAboveSlot(point.Slot)
				if err != nil {
					return fmt.Errorf("read rollback undo blocks: %w", err)
				}
			}
			rollbackBlocks = mergeRollbackBlocks(
				existingBlocks,
				rollbackBlocks,
			)
		} else {
			return nil
		}
	}
	if rollbackBlocks == nil {
		rollbackBlocks, err = ls.readBlocksAboveSlot(point.Slot)
		if err != nil {
			return fmt.Errorf("read rollback undo blocks: %w", err)
		}
	}
	if len(rollbackBlocks) == 0 {
		return nil
	}
	return persistRollbackIntent(ls.db, point, rollbackBlocks)
}

func (ls *LedgerState) prepareRollbackIntent(
	point ocommon.Point,
	rollbackBlocks []models.Block,
) error {
	if err := ls.ensureRollbackIntent(point, rollbackBlocks); err != nil {
		if !errors.Is(err, errRollbackIntentConflict) {
			return err
		}
		if recoverErr := ls.recoverRollbackIntentLocked(); recoverErr != nil {
			return fmt.Errorf("complete previous rollback intent: %w", recoverErr)
		}
		if retryErr := ls.ensureRollbackIntent(point, rollbackBlocks); retryErr != nil {
			return retryErr
		}
	}
	return nil
}

func mergeRollbackBlocks(
	existing []models.Block,
	additional []models.Block,
) []models.Block {
	ret := append([]models.Block(nil), existing...)
	seen := make(map[string]struct{}, len(ret))
	for _, block := range ret {
		seen[string(block.Hash)] = struct{}{}
	}
	for _, block := range additional {
		if _, ok := seen[string(block.Hash)]; ok {
			continue
		}
		seen[string(block.Hash)] = struct{}{}
		ret = append(ret, block)
	}
	return ret
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
	if err := json.Unmarshal([]byte(raw), &intent); err != nil {
		detail := err.Error()
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"%w: decode: %s", errInvalidRollbackIntent, detail,
		)
	}
	if intent.Version != durableRollbackIntentVersion {
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"%w: unsupported format version %d",
			errInvalidRollbackIntent,
			intent.Version,
		)
	}
	if intent.Slot == nil || intent.Hash == nil {
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"%w: missing slot or hash",
			errInvalidRollbackIntent,
		)
	}
	if (*intent.Slot == 0) != (*intent.Hash == "") {
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"%w: invalid origin point",
			errInvalidRollbackIntent,
		)
	}
	hash, err := hex.DecodeString(*intent.Hash)
	if err != nil {
		detail := err.Error()
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"%w: decode hash: %s", errInvalidRollbackIntent, detail,
		)
	}
	if *intent.Slot > 0 && len(hash) == 0 {
		return ocommon.Point{}, nil, false, fmt.Errorf(
			"%w: empty non-origin hash",
			errInvalidRollbackIntent,
		)
	}
	return ocommon.Point{Slot: *intent.Slot, Hash: hash},
		modelBlocks(intent.Blocks), true, nil
}

// recoverRollbackIntent finishes a rollback whose durable undo outbox was
// written but whose chain or metadata mutation did not complete. The outbox
// owns the block payload because the primary-chain rewind may already have
// deleted the corresponding block rows.
func (ls *LedgerState) recoverRollbackIntent() error {
	// consumedUtxoPruneMutex before transactionEventMutex, matching
	// rollbackChainAndStateDeferred and reconcilePrimaryChainTipWithLedgerTip.
	// recoverRollbackIntentLocked truncates through rollbackWithBlocks, so it
	// must not run concurrently with the consumed-UTxO sweep; the in-rollback
	// callers already hold both and reach the Locked variant directly.
	return ls.withConsumedUtxoPruneBoundary(func() error {
		ls.transactionEventMutex.Lock()
		defer ls.transactionEventMutex.Unlock()
		return ls.recoverRollbackIntentLocked()
	})
}

func (ls *LedgerState) recoverRollbackIntentLocked() error {
	point, blocks, pending, err := loadRollbackIntent(ls.db)
	if errors.Is(err, errInvalidRollbackIntent) {
		ls.config.Logger.Error(
			"discarding unreadable rollback intent",
			"component", "ledger",
			"error", err,
		)
		return clearRollbackIntent(ls.db)
	}
	if err != nil || !pending {
		return err
	}
	ls.config.Logger.Warn(
		"recovering interrupted ledger rollback",
		"component", "ledger",
		"slot", point.Slot,
	)

	ls.RLock()
	current := ls.currentTip.Point
	ls.RUnlock()
	if current.Slot < point.Slot {
		// No metadata rollback is possible below the applied tip, but the
		// captured bodies are the outbox's only copy: the chain truncation
		// that preceded this record already deleted them. Deliver the undo
		// before retiring the record, as the off-primary-chain branch below
		// does, so the at-least-once contract holds on every exit.
		ls.config.Logger.Warn(
			"replaying rollback undo whose intent leads the applied ledger tip",
			"component", "ledger", "ledger_tip_slot", current.Slot,
			"intent_slot", point.Slot,
		)
		ls.emitRollbackTransactionEvents(blocks)
		return ls.finishRollbackIntent()
	}
	if point.Slot > 0 {
		contains, err := ls.primaryChainContainsPoint(point)
		if err != nil {
			return fmt.Errorf("validate rollback intent point: %w", err)
		}
		if !contains {
			ls.config.Logger.Warn(
				"replaying rollback undo whose point left the primary chain",
				"component", "ledger",
				"intent_slot", point.Slot,
			)
			ls.emitRollbackTransactionEvents(blocks)
			return ls.finishRollbackIntent()
		}
	}
	if ls.config.ChainManager != nil {
		if err := ls.config.ChainManager.RewindPrimaryChainToPoint(point); err != nil {
			return fmt.Errorf("recover primary chain rollback: %w", err)
		}
	}

	ls.emitRollbackTransactionEvents(blocks)
	if err := ls.rollbackWithBlocks(point, blocks, false); err != nil {
		return fmt.Errorf("recover rollback intent: %w", err)
	}
	return nil
}

func (ls *LedgerState) finishRollbackIntent() error {
	_, blocks, pending, err := loadRollbackIntent(ls.db)
	if err != nil || !pending {
		return err
	}
	if len(blocks) > 0 && ls.config.EventBus != nil {
		ctx := ls.publishCtx
		if ctx == nil {
			ctx = context.Background()
		}
		if !ls.config.EventBus.FlushOrderedContext(ctx, TransactionEventType) {
			ls.config.Logger.Warn(
				"retaining rollback intent until ordered undo delivery completes",
				"component", "ledger",
			)
			return nil
		}
	}
	return clearRollbackIntent(ls.db)
}

func (ls *LedgerState) finishRollbackIntentForPoint(
	point ocommon.Point,
) error {
	pendingPoint, _, pending, err := loadRollbackIntent(ls.db)
	if err != nil || !pending {
		return err
	}
	if !pointMatches(pendingPoint, point) {
		return nil
	}
	return ls.finishRollbackIntent()
}
