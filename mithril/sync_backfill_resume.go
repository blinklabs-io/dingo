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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/node"
)

// resumeMithrilBackfill finishes an API-mode sync whose sync_status is
// "backfill". That status is written only after every block is in the blob
// store and the ledger state is imported, and the extracted snapshot may
// already be deleted, so the backfill resumes from its checkpoint without
// contacting the aggregator.
func resumeMithrilBackfill(
	ctx context.Context,
	cfg SyncConfig,
	db *database.Database,
	nodeCfg *cardano.CardanoNodeConfig,
	explicitPin string,
	network string,
	logger *slog.Logger,
) (SyncResult, error) {
	pin, hasPin, err := getPinnedArtifact(db)
	if err != nil {
		return SyncResult{}, err
	}
	if err := validateExplicitArtifactPin(explicitPin, pin, hasPin); err != nil {
		return SyncResult{}, err
	}
	if hasPin {
		if err := pin.validateForRun(cfg.Backend, network); err != nil {
			return SyncResult{}, err
		}
	}
	ledgerStateSlot, ledgerStateHash, err := mithrilLedgerAnchor(db)
	if err != nil {
		return SyncResult{}, err
	}
	logger.Info(
		"resuming Mithril API backfill from its checkpoint",
		"component", "mithril",
		"ledger_slot", ledgerStateSlot,
	)

	cfg.emit(SyncProgress{Phase: PhaseBackfill, Active: true})
	if err := node.RunPlannerStats(db, logger); err != nil {
		return SyncResult{}, fmt.Errorf(
			"running planner statistics before backfill: %w", err,
		)
	}
	cleanupBulkLoadPragmas := node.WithBulkLoadPragmas(db, logger)
	pragmasRestored := false
	defer func() {
		if pragmasRestored {
			return
		}
		if err := cleanupBulkLoadPragmas(); err != nil {
			logger.Error(
				"failed to restore normal metadata settings",
				"component", "mithril",
				"error", err,
			)
		}
	}()

	bf := node.NewBackfill(db, nodeCfg, logger)
	// See the bootstrap path in Sync: Mithril sync and the delegator-inactivity
	// gate are mutually exclusive, and only a complete fresh ledger-state
	// import may use running-total finalization.
	bf.SetDelegatorInactivityEnabled(false)
	bf.SetUseRunningTotalsFinalization(false)
	bf.SetEndSlot(ledgerStateSlot)
	if err := bf.SetBatchSize(cfg.BackfillBatchSize); err != nil {
		return SyncResult{}, fmt.Errorf(
			"invalid backfill batch size %d in SetBatchSize: %w",
			cfg.BackfillBatchSize,
			err,
		)
	}
	bf.SetProgressFunc(func(p node.BackfillProgress) {
		cfg.emit(SyncProgress{
			Phase:          PhaseBackfill,
			Active:         true,
			CurrentSlot:    p.Slot,
			TipSlot:        p.TipSlot,
			Percent:        p.ProgressPercent,
			BytesPerSecond: p.BlocksPerSecond,
			Count:          p.ProcessedBlocks,
		})
	})
	if err := bf.Run(ctx); err != nil {
		return SyncResult{}, fmt.Errorf("backfill: %w", err)
	}
	cfg.emit(SyncProgress{Phase: PhaseBackfill, Active: false})

	cfg.emit(SyncProgress{Phase: PhaseIndexRebuild, Active: true})
	if err := node.RepairCriticalDeferredIndexes(ctx, db, logger); err != nil {
		return SyncResult{}, err
	}
	cfg.emit(SyncProgress{Phase: PhaseIndexRebuild, Active: false})
	if err := cleanupBulkLoadPragmas(); err != nil {
		return SyncResult{}, fmt.Errorf(
			"restoring normal metadata settings: %w", err,
		)
	}
	pragmasRestored = true
	if err := node.FinalizeMithrilBackfillPlannerStats(
		ctx, db, logger, false,
	); err != nil {
		return SyncResult{}, err
	}
	if err := updateMithrilReadyState(
		ctx, db, logger, nil, ledgerStateSlot, ledgerStateHash, "", true,
	); err != nil {
		return SyncResult{}, err
	}
	if hasPin {
		if err := setImmutableImportMarker(
			db, pin.ImmutableFileNumber,
		); err != nil {
			logger.Warn(
				"failed to record Mithril immutable-import marker",
				"component", "mithril",
				"error", err,
			)
		}
	}
	cfg.emit(SyncProgress{Phase: PhaseComplete, Active: true})
	logger.Info(
		"Mithril API backfill complete",
		"component", "mithril",
		"ledger_slot", ledgerStateSlot,
	)
	return SyncResult{LedgerSlot: ledgerStateSlot}, nil
}

// mithrilLedgerAnchor reads the stable ledger point recorded when the sync
// reached the backfill stage.
func mithrilLedgerAnchor(db *database.Database) (uint64, []byte, error) {
	slotText, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("reading Mithril ledger slot: %w", err)
	}
	if slotText == "" {
		return 0, nil, errors.New(
			"backfill sync status has no recorded Mithril ledger slot",
		)
	}
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing Mithril ledger slot: %w", err)
	}
	hashText, err := db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("reading Mithril ledger hash: %w", err)
	}
	hash, err := hex.DecodeString(hashText)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing Mithril ledger hash: %w", err)
	}
	return slot, hash, nil
}
