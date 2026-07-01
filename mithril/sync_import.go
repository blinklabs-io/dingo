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
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledgerstate"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	mithrilLedgerSlotSyncKey = "mithril_ledger_slot"
	mithrilLedgerHashSyncKey = "mithril_ledger_hash"
)

// epochLengthFromConfig returns an EpochLengthFunc that resolves
// era parameters from the Cardano node config.
func epochLengthFromConfig(
	nodeCfg *cardano.CardanoNodeConfig,
) ledgerstate.EpochLengthFunc {
	if nodeCfg == nil {
		return nil
	}
	return func(eraId uint) (uint, uint, error) {
		eraDesc := eras.GetEraById(eraId)
		if eraDesc == nil || eraDesc.EpochLengthFunc == nil {
			return 0, 0, fmt.Errorf(
				"unknown era %d", eraId,
			)
		}
		return eraDesc.EpochLengthFunc(nodeCfg)
	}
}

func ensureMithrilBackfillCheckpoint(db *database.Database) error {
	cp, err := db.Metadata().GetBackfillCheckpoint(
		node.BackfillPhase, nil,
	)
	if err != nil {
		return fmt.Errorf("reading backfill checkpoint: %w", err)
	}

	if cp != nil && !cp.Completed {
		return nil
	}

	now := time.Now()
	if cp == nil {
		cp = &models.BackfillCheckpoint{
			Phase:     node.BackfillPhase,
			LastSlot:  0,
			StartedAt: now,
			UpdatedAt: now,
			Completed: false,
		}
	} else {
		cp.Completed = false
		cp.UpdatedAt = now
	}

	if err := db.Metadata().SetBackfillCheckpoint(cp, nil); err != nil {
		return fmt.Errorf("setting backfill checkpoint: %w", err)
	}
	return nil
}

func updateMithrilReadyState(
	db *database.Database,
	logger *slog.Logger,
	loadResult *node.LoadBlobsResult,
	ledgerStateSlot uint64,
	ledgerStateHash []byte,
	syncStatus string,
	clearSyncState bool,
) error {
	recentBlocks, err := database.BlocksRecent(db, 1)
	if err != nil {
		return fmt.Errorf("reading final chain tip: %w", err)
	}
	if len(recentBlocks) > 0 {
		chainTip := recentBlocks[0]
		if err := db.SetTip(
			ochainsync.Tip{
				Point: ocommon.Point{
					Slot: chainTip.Slot,
					Hash: chainTip.Hash,
				},
				BlockNumber: chainTip.Number,
			},
			nil,
		); err != nil {
			return fmt.Errorf("updating metadata tip: %w", err)
		}
		var blocksCopied int
		if loadResult != nil {
			blocksCopied = loadResult.BlocksCopied
		}
		logger.Info(
			"metadata tip set",
			"component", "mithril",
			"slot", chainTip.Slot,
			"blocks_loaded", blocksCopied,
		)
	}

	// Record the trust boundary as the chain tip AFTER gap closure,
	// not the Mithril snapshot slot. Gap blocks between the snapshot
	// slot and chain tip were imported via SetGapBlockTransaction
	// (no UTxO tracking), so chainsync replay must skip them too.
	// Fall back to the snapshot slot when no gap blocks were fetched.
	trustBoundarySlot := ledgerStateSlot
	trustBoundaryHash := ledgerStateHash
	if len(recentBlocks) > 0 {
		trustBoundarySlot = recentBlocks[0].Slot
		trustBoundaryHash = recentBlocks[0].Hash
	}

	txn := db.MetadataTxn(true)
	if err := txn.Do(func(txn *database.Txn) error {
		if clearSyncState {
			if err := db.ClearSyncState(txn); err != nil {
				return fmt.Errorf("cleaning up sync state: %w", err)
			}
		} else if syncStatus != "" {
			if err := db.SetSyncState(
				"sync_status", syncStatus, txn,
			); err != nil {
				return fmt.Errorf(
					"recording sync status: %w", err,
				)
			}
		}
		if err := db.SetSyncState(
			mithrilLedgerSlotSyncKey,
			strconv.FormatUint(trustBoundarySlot, 10),
			txn,
		); err != nil {
			return fmt.Errorf(
				"recording mithril ledger slot: %w", err,
			)
		}
		if len(trustBoundaryHash) > 0 {
			if err := db.SetSyncState(
				mithrilLedgerHashSyncKey,
				hex.EncodeToString(trustBoundaryHash),
				txn,
			); err != nil {
				return fmt.Errorf(
					"recording mithril ledger hash: %w",
					err,
				)
			}
		} else if err := db.DeleteSyncState(
			mithrilLedgerHashSyncKey,
			txn,
		); err != nil {
			return fmt.Errorf(
				"clearing mithril ledger hash: %w",
				err,
			)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// importLedgerState finds, parses, and imports the ledger state
// from the extracted Mithril snapshot. It searches both the main
// extract directory and the ancillary directory for the state file.
func importLedgerState(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	nodeCfg *cardano.CardanoNodeConfig,
	result *BootstrapResult,
	onLedger func(ledgerstate.ImportProgress),
) (ledgerStateSlot uint64, ledgerStateHash []byte, err error) {
	// Search for ledger state: prefer ancillary dir, fall back to
	// main extract dir.
	searchDirs := []string{}
	if result.AncillaryDir != "" {
		searchDirs = append(searchDirs, result.AncillaryDir)
	}
	searchDirs = append(searchDirs, result.ExtractDir)

	var lstatePath string
	var searchDir string
	for _, dir := range searchDirs {
		path, findErr := ledgerstate.FindLedgerStateFile(dir)
		if findErr == nil {
			lstatePath = path
			searchDir = dir
			break
		}
		logger.Debug(
			"ledger state not found in directory",
			"component", "mithril",
			"dir", dir,
			"error", findErr,
		)
	}

	if lstatePath == "" {
		logger.Warn(
			"no ledger state file found in snapshot, "+
				"skipping ledger state import",
			"component", "mithril",
		)
		return 0, nil, nil
	}

	logger.Info(
		"found ledger state file",
		"component", "mithril",
		"path", lstatePath,
	)

	// Parse the snapshot
	state, err := ledgerstate.ParseSnapshot(lstatePath)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing ledger state: %w", err)
	}

	// Check for UTxO-HD tvar file (UTxOs stored separately)
	tvarPath := ledgerstate.FindUTxOTableFile(searchDir)
	if tvarPath != "" {
		state.UTxOTablePath = tvarPath
		logger.Info(
			"found UTxO table file (UTxO-HD format)",
			"component", "mithril",
			"path", tvarPath,
		)
	}

	if state.Tip == nil {
		return 0, nil, errors.New(
			"parsed ledger state has no tip (Origin snapshot)",
		)
	}

	nonceHex := "neutral"
	if len(state.EpochNonce) > 0 {
		nonceHex = hex.EncodeToString(state.EpochNonce)
	}
	logger.Info(
		"parsed ledger state",
		"component", "mithril",
		"era", ledgerstate.EraName(state.EraIndex),
		"epoch", state.Epoch,
		"slot", state.Tip.Slot,
		"era_bound_slot", state.EraBoundSlot,
		"era_bound_epoch", state.EraBoundEpoch,
		"epoch_nonce", nonceHex,
	)

	// Build import key for resume tracking
	importKey := ""
	if result.Snapshot != nil && result.Snapshot.Digest != "" {
		digest := result.Snapshot.Digest
		if len(digest) > 16 {
			digest = digest[:16]
		}
		importKey = fmt.Sprintf(
			"%s:%d",
			digest,
			state.Tip.Slot,
		)
	}

	// Import the ledger state
	if err := ledgerstate.ImportLedgerState(
		ctx,
		ledgerstate.ImportConfig{
			Database:  db,
			State:     state,
			Logger:    logger,
			ImportKey: importKey,
			EpochLength: epochLengthFromConfig(
				nodeCfg,
			),
			OnProgress: func(p ledgerstate.ImportProgress) {
				if onLedger != nil {
					onLedger(p)
				}
				attrs := []any{
					"component", "mithril",
					"stage", p.Stage,
				}
				msg := p.Description
				var pct float64
				switch {
				case p.Percent > 0:
					pct = p.Percent
				case p.Total > 0:
					pct = float64(p.Current) /
						float64(p.Total) * 100
				}
				if pct > 0 {
					attrs = append(
						attrs,
						"progress",
						fmt.Sprintf("%.1f%%", pct),
					)
				}
				if p.Total > 0 {
					attrs = append(
						attrs,
						"current", p.Current,
						"total", p.Total,
					)
				} else if p.Current > 0 {
					attrs = append(
						attrs, "current", p.Current,
					)
				}
				logger.Info(msg, attrs...)
			},
		},
	); err != nil {
		return 0, nil, fmt.Errorf("importing ledger state: %w", err)
	}
	return state.Tip.Slot, state.Tip.BlockHash, nil
}
