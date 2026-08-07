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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
	// discovery hands back the state and UTxO table already open rather than
	// names for them. That is what makes the manifest verification mean
	// something downstream: the bytes imported come from the files that were
	// verified, and no name is resolved between checking and reading.
	type searchTree struct {
		name string
		root *os.Root
		// digests is the signed ancillary manifest's digest map, present
		// exactly when verified is set. Discovery hands back open files, and
		// these are what the selected ones are re-checked against before
		// anything is parsed.
		digests map[string]string
		// verified reports that this tree's contents were checked against the
		// signed ancillary manifest. Nothing is looked at after one of these.
		verified bool
	}
	searchTrees := []searchTree{}
	if result.AncillaryRoot != nil {
		// A tree claiming a signature but carrying no digest map cannot have
		// its selected files re-checked, so the claim would stand on a check
		// that ran earlier and closed every file it looked at. Refused rather
		// than downgraded to an unverified read of a tree the flag says is
		// verified.
		if result.AncillaryVerified && len(result.AncillaryDigests) == 0 {
			return 0, nil, errors.New(
				"ancillary tree is marked verified but carries no signed " +
					"manifest digests to check its files against",
			)
		}
		searchTrees = append(searchTrees, searchTree{
			result.AncillaryDir,
			result.AncillaryRoot,
			result.AncillaryDigests,
			result.AncillaryVerified,
		})
	}
	if result.ExtractRoot != nil {
		searchTrees = append(searchTrees, searchTree{
			result.ExtractDir, result.ExtractRoot, nil, false,
		})
	}
	if len(searchTrees) == 0 {
		return 0, nil, errors.New(
			"bootstrap result carries no verified directory handle to " +
				"import ledger state from",
		)
	}

	var (
		snapshot *ledgerstate.SnapshotFiles
		stateDir string
		signedBy map[string]string
	)
	for _, tree := range searchTrees {
		files, findErr := ledgerstate.OpenSnapshotAtOrBefore(
			tree.root,
			maxTrustedSlot,
		)
		if findErr == nil {
			snapshot, stateDir, signedBy = files, tree.name, tree.digests
			break
		}
		// Only a tree with no usable ledger state moves on to the next. One
		// holding something unusable — a symlink, a substitution, a state that
		// exists but will not open — fails the import, because falling through
		// would let a planted ancillary tree choose the extraction directory
		// as the source instead.
		if !errors.Is(findErr, ledgerstate.ErrNoUsableLedgerState) {
			return 0, nil, fmt.Errorf(
				"inspecting ledger state in %s: %w", tree.name, findErr,
			)
		}
		// Nor is anything looked at after a tree whose contents a signature
		// covers. Emptying it is destruction rather than planting, but the
		// effect would be the same: the import would move from the tree the
		// ancillary key signed to one nothing vouches for, and whoever emptied
		// the first would have chosen the second.
		//
		// An unverified tree yielding nothing is different — there is no
		// downgrade to make, and the fallback is how the v1 layout works, its
		// ledger state living in the main archive rather than the ancillary
		// one. It also covers the ancillary tree holding only states newer
		// than the certified tip, which is ordinary and not adversarial.
		if tree.verified {
			return 0, nil, fmt.Errorf(
				"verified ancillary data in %s has no usable ledger state; "+
					"refusing to import one from elsewhere: %w",
				tree.name, findErr,
			)
		}
		logger.Debug(
			"ledger state not found in directory",
			"component", "mithril",
			"dir", tree.name,
			"error", findErr,
		)
	}

	if snapshot == nil {
		return 0, nil, fmt.Errorf(
			"no ledger state at or before certified ImmutableDB tip slot %d; "+
				"refusing to trust a volatile ancillary ledger state",
			maxTrustedSlot,
		)
	}
	// Held open for the whole import: the UTxO stream is read from the table
	// handle, and closing early would put a name back in its place.
	defer snapshot.Close()
	// The manifest check hashed these files and closed them; discovery opened
	// them again. Between those two the tree is the same tree — the handle
	// says so — but a file inside it need not be the same file, and what gets
	// parsed below would then not be what the ancillary key signed. So the
	// signature is re-established here, on the descriptors the import reads
	// through, before any of their bytes reach a parser.
	if signedBy != nil {
		if err := verifySignedSnapshot(snapshot, signedBy); err != nil {
			return 0, nil, fmt.Errorf(
				"verifying ledger state in %s: %w", stateDir, err,
			)
		}
	}
	lstatePath := filepath.Join(
		stateDir, filepath.FromSlash(snapshot.StatePath),
	)

	logger.Info(
		"found ledger state file",
		"component", "mithril",
		"path", lstatePath,
		"max_trusted_slot", maxTrustedSlot,
	)

	// Parsed from the file discovery opened. There is no name here to reopen:
	// lstatePath exists only for the messages above and below.
	state, err := ledgerstate.ParseSnapshotFile(snapshot.State)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing ledger state: %w", err)
	}

	// UTxO-HD keeps the UTxO set in a table beside the state; discovery opened
	// it from the same directory handle, so the two belong to one snapshot.
	if snapshot.Table != nil {
		state.UTxOTablePath = filepath.Join(
			stateDir, filepath.FromSlash(snapshot.TablePath),
		)
		state.UTxOTableFile = snapshot.Table
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

// errAncillaryDigestMismatch reports a file selected for import whose bytes are
// not the bytes the signed ancillary manifest covered — either because they
// changed after the manifest was checked, or because nothing in the manifest
// covers that file at all.
//
// The second is not the lesser case. verifyAncillaryManifest already refuses a
// tree holding a file the manifest does not list, but that is a statement about
// the tree as it was then; a file planted afterwards has to be refused where it
// is used, or it is refused only by a check that has already run.
var errAncillaryDigestMismatch = errors.New(
	"ancillary file is not the file the manifest signature covers",
)

// verifySignedSnapshot re-establishes the ancillary signature over the files
// snapshot discovery selected and opened.
//
// It hashes the open descriptors rather than the paths beside them. Those paths
// exist for log lines: resolving one here would reintroduce exactly the gap
// this closes, since the file it named at manifest time and the file it names
// now are not required to be the same file. Each descriptor is left rewound, so
// the parser reads the bytes that were just hashed.
func verifySignedSnapshot(
	files *ledgerstate.SnapshotFiles,
	digests map[string]string,
) error {
	for _, entry := range []struct {
		name string
		file *os.File
	}{
		{files.StatePath, files.State},
		{files.TablePath, files.Table},
	} {
		// A legacy snapshot embeds its UTxO set in the state file and has no
		// table.
		if entry.file == nil {
			continue
		}
		expected, ok := digests[entry.name]
		if !ok {
			return fmt.Errorf(
				"%w: %s is not covered by the manifest",
				errAncillaryDigestMismatch, entry.name,
			)
		}
		sum, err := sha256OpenFile(entry.file)
		if err != nil {
			return fmt.Errorf("hashing %s: %w", entry.name, err)
		}
		if sum != expected {
			return fmt.Errorf(
				"%w: %s computed %s, signed %s",
				errAncillaryDigestMismatch, entry.name, sum, expected,
			)
		}
	}
	return nil
}

// sha256OpenFile hashes an open file from its start and rewinds it.
func sha256OpenFile(f *os.File) (string, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
