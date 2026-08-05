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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledgerstate"
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
	ledgerTip, err := db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("reading imported ledger tip: %w", err)
	}
	if ledgerTip.Point.Slot != ledgerStateSlot ||
		!bytes.Equal(ledgerTip.Point.Hash, ledgerStateHash) {
		return fmt.Errorf(
			"imported ledger tip %d.%x does not match stable Mithril anchor %d.%x",
			ledgerTip.Point.Slot,
			ledgerTip.Point.Hash,
			ledgerStateSlot,
			ledgerStateHash,
		)
	}
	var blocksCopied int
	if loadResult != nil {
		blocksCopied = loadResult.BlocksCopied
	}
	logger.Info(
		"metadata tip retained at stable Mithril anchor",
		"component", "mithril",
		"slot", ledgerTip.Point.Slot,
		"blocks_loaded", blocksCopied,
	)

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
			strconv.FormatUint(ledgerStateSlot, 10),
			txn,
		); err != nil {
			return fmt.Errorf(
				"recording mithril ledger slot: %w", err,
			)
		}
		if len(ledgerStateHash) > 0 {
			if err := db.SetSyncState(
				mithrilLedgerHashSyncKey,
				hex.EncodeToString(ledgerStateHash),
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
	reconcile bool,
	maxTrustedSlot uint64,
	onLedger func(ledgerstate.ImportProgress),
) (ledgerStateSlot uint64, ledgerStateHash []byte, err error) {
	// Search for ledger state: prefer the ancillary tree, fall back to the
	// main extraction tree.
	//
	// Both are searched through the handle the bootstrap vetted them with, and
	// the state file is opened through the same handle rather than by the name
	// the search produced. That is what makes the manifest verification mean
	// something downstream: the bytes imported come from the tree that was
	// verified, not from whatever holds its name once the import starts.
	type searchTree struct {
		name string
		root *os.Root
	}
	searchTrees := []searchTree{}
	if result.AncillaryRoot != nil {
		searchTrees = append(
			searchTrees,
			searchTree{result.AncillaryDir, result.AncillaryRoot},
		)
	}
	if result.ExtractRoot != nil {
		searchTrees = append(
			searchTrees,
			searchTree{result.ExtractDir, result.ExtractRoot},
		)
	}
	if len(searchTrees) == 0 {
		return 0, nil, errors.New(
			"bootstrap result carries no verified directory handle to " +
				"import ledger state from",
		)
	}

	var (
		stateRel  string
		stateRoot *os.Root
		stateDir  string
	)
	for _, tree := range searchTrees {
		rel, findErr := ledgerstate.FindLedgerStateAtOrBefore(
			tree.root,
			maxTrustedSlot,
		)
		if findErr == nil {
			stateRel, stateRoot, stateDir = rel, tree.root, tree.name
			break
		}
		logger.Debug(
			"ledger state not found in directory",
			"component", "mithril",
			"dir", tree.name,
			"error", findErr,
		)
	}

	if stateRel == "" {
		return 0, nil, fmt.Errorf(
			"no ledger state at or before certified ImmutableDB tip slot %d; "+
				"refusing to trust a volatile ancillary ledger state",
			maxTrustedSlot,
		)
	}
	lstatePath := filepath.Join(stateDir, filepath.FromSlash(stateRel))

	logger.Info(
		"found ledger state file",
		"component", "mithril",
		"path", lstatePath,
		"max_trusted_slot", maxTrustedSlot,
	)

	// Parse the snapshot, read through the handle the search used.
	stateFile, err := ledgerstate.RootOpen(stateRoot, stateRel)
	if err != nil {
		return 0, nil, fmt.Errorf("opening ledger state: %w", err)
	}
	state, err := ledgerstate.ParseSnapshotFile(stateFile)
	_ = stateFile.Close()
	if err != nil {
		return 0, nil, fmt.Errorf("parsing ledger state: %w", err)
	}

	// Check for UTxO-HD tvar file (UTxOs stored separately). Held open for the
	// whole import, since that is what the UTxO stream is read from.
	tvarRel := ledgerstate.FindUTxOTableForState(stateRoot, stateRel)
	if tvarRel != "" {
		tvarFile, openErr := ledgerstate.RootOpen(stateRoot, tvarRel)
		if openErr != nil {
			return 0, nil, fmt.Errorf(
				"opening UTxO table: %w", openErr,
			)
		}
		defer func() { _ = tvarFile.Close() }()
		state.UTxOTablePath = filepath.Join(
			stateDir, filepath.FromSlash(tvarRel),
		)
		state.UTxOTableFile = tvarFile
		logger.Info(
			"found UTxO table file (UTxO-HD format)",
			"component", "mithril",
			"path", state.UTxOTablePath,
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

	// Build import key for resume tracking. A catch-up reconcile must run the
	// full import pass (so its snapshot key set is complete), so resume is
	// disabled by leaving the import key empty.
	importKey := ""
	if !reconcile && result.Snapshot != nil && result.Snapshot.Digest != "" {
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
			Reconcile: reconcile,
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
