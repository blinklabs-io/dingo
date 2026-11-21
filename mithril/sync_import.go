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
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/blinklabs-io/dingo/config/cardano"
	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledgerstate"
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

func updateMithrilReadyState(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	loadResult *node.LoadBlobsResult,
	ledgerStateSlot uint64,
	ledgerStateHash []byte,
) error {
	// The clear below deletes every key outside the keep-list, which omits
	// the drain markers.
	if err := db.RefuseLedgerStateRewrite(ctx); err != nil {
		return err
	}
	tipTxn := db.MetadataTxn(ctx, false)
	ledgerTip, err := db.GetTip(tipTxn)
	tipTxn.Release()
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

	txn := db.MetadataTxn(ctx, true)
	if err := txn.Do(func(txn *database.Txn) error {
		// A read-clear-restore cycle can overwrite a newer fence on
		// backends that allow concurrent writers. Leave persistent rows
		// untouched so a forger's advancing slot never enters that cycle.
		keys, err := db.ListSyncStateKeysByPrefix("", txn)
		if err != nil {
			return fmt.Errorf("listing sync state: %w", err)
		}
		for _, key := range keys {
			if key == deferred.SyncStateKey ||
				key == metadata.PlannerStatsBackfillSyncKey ||
				key == nodeconsensus.DatabaseIdentitySyncKey ||
				key == database.UtxoSetOnlySyncKey ||
				key == database.DrainedImportSyncKey ||
				key == database.HistoryExpiryCursorSyncKey ||
				strings.HasPrefix(key, forging.ForgeFenceSyncKeyPrefix) {
				continue
			}
			if err := db.DeleteSyncState(key, txn); err != nil {
				return fmt.Errorf("cleaning up sync state %q: %w", key, err)
			}
		}
		return db.SetImportedLedgerStateBoundary(
			ocommon.NewPoint(ledgerStateSlot, ledgerStateHash), txn,
		)
	}); err != nil {
		return err
	}
	return nil
}

// selectLedgerStateSnapshot searches result's ancillary tree for the newest
// ledger state at or before immutableTipSlot, the slot of the last block the
// import puts in the ImmutableDB.
//
// It is searched through the handle the bootstrap vetted it with, and
// discovery hands back the state and UTxO table already open rather than
// names for them. That is what makes the manifest verification mean something
// downstream: the bytes imported come from the files that were verified, and
// no name is resolved between checking and reading.
//
// A state later than immutableTipSlot is passed over for an older one, and
// when the tree holds no older one the error is ErrLedgerStateTooRecent. The
// ancillary signature does not change that: a state is loaded only where the
// blocks behind it are in the ImmutableDB.
func selectLedgerStateSnapshot(
	logger *slog.Logger,
	result *BootstrapResult,
	immutableTipSlot uint64,
) (
	snapshot *ledgerstate.SnapshotFiles,
	stateDir string,
	signedBy map[string]string,
	err error,
) {
	if result.AncillaryRoot == nil {
		if !result.CertificateVerified {
			return nil, "", nil, errors.New(
				"bootstrap produced no ancillary ledger state to import",
			)
		}
		return nil, "", nil, errors.New(
			"bootstrap result carries no verified directory handle to " +
				"import ledger state from",
		)
	}
	// A tree claiming a signature but carrying no digest map cannot have
	// its selected files re-checked, so the claim would stand on a check
	// that ran earlier and closed every file it looked at. Refused rather
	// than downgraded to an unverified read of a tree the flag says is
	// verified.
	if result.AncillaryVerified && len(result.AncillaryDigests) == 0 {
		return nil, "", nil, errors.New(
			"ancillary tree is marked verified but carries no signed " +
				"manifest digests to check its files against",
		)
	}
	files, findErr := ledgerstate.OpenSnapshotAtOrBefore(
		result.AncillaryRoot,
		immutableTipSlot,
	)
	if findErr == nil {
		return files, result.AncillaryDir, result.AncillaryDigests, nil
	}
	// A tree holding something unusable (a symlink, a substitution, a state
	// that exists but will not open) fails the import rather than being
	// treated as empty.
	if !errors.Is(findErr, ledgerstate.ErrNoUsableLedgerState) {
		return nil, "", nil, fmt.Errorf(
			"inspecting ledger state in %s: %w", result.AncillaryDir, findErr,
		)
	}
	newest, newestSlot, newestErr := ledgerstate.OpenNewestSnapshot(
		result.AncillaryRoot,
	)
	if newestErr != nil {
		if !errors.Is(newestErr, ledgerstate.ErrNoUsableLedgerState) {
			return nil, "", nil, fmt.Errorf(
				"inspecting ledger state in %s: %w",
				result.AncillaryDir, newestErr,
			)
		}
		return nil, "", nil, fmt.Errorf(
			"no ledger state at or before ImmutableDB tip slot %d in %s: %w",
			immutableTipSlot, result.AncillaryDir, findErr,
		)
	}
	newest.Close()
	logger.Warn(
		"discarding ancillary ledger state later than the ImmutableDB tip",
		"component", "mithril",
		"dir", result.AncillaryDir,
		"slot", newestSlot,
		"immutable_tip_slot", immutableTipSlot,
	)
	return nil, "", nil, fmt.Errorf(
		"%w: state slot %d, ImmutableDB tip slot %d: %w",
		ErrLedgerStateTooRecent, newestSlot, immutableTipSlot, findErr,
	)
}

// importLedgerState finds, parses, and imports the ledger state
// from the extracted Mithril snapshot. It searches the ancillary directory
// for the state file. Each beforeImport hook runs on the selected state, after
// it is verified and before the import writes anything; a hook error stops the
// import.
func importLedgerState(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	nodeCfg *cardano.CardanoNodeConfig,
	result *BootstrapResult,
	reconcile bool,
	immutableTipSlot uint64,
	utxoSet ledgerstate.UtxoSetLoader,
	onLedger func(ledgerstate.ImportProgress),
	beforeImport ...func(*preparedLedgerStateImport) error,
) (
	ledgerStateSlot uint64,
	ledgerStateHash []byte,
	err error,
) {
	prepared, err := prepareLedgerStateImport(
		logger, result, immutableTipSlot,
	)
	if err != nil {
		return 0, nil, err
	}
	defer prepared.Close()
	for _, hook := range beforeImport {
		if err := hook(prepared); err != nil {
			return 0, nil, err
		}
	}
	return importPreparedLedgerState(
		ctx, db, logger, nodeCfg, result, prepared, reconcile, utxoSet,
		onLedger,
	)
}

type preparedLedgerStateImport struct {
	snapshot *ledgerstate.SnapshotFiles
	state    *ledgerstate.RawLedgerState
	stateDir string
}

// point is the block the prepared ledger state was taken at.
func (p *preparedLedgerStateImport) point() ocommon.Point {
	return ocommon.NewPoint(p.state.Tip.Slot, p.state.Tip.BlockHash)
}

func (p *preparedLedgerStateImport) Close() {
	if p != nil {
		p.snapshot.Close()
	}
}

func prepareLedgerStateImport(
	logger *slog.Logger,
	result *BootstrapResult,
	immutableTipSlot uint64,
) (_ *preparedLedgerStateImport, err error) {
	snapshot, stateDir, signedBy, err := selectLedgerStateSnapshot(
		logger,
		result,
		immutableTipSlot,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			snapshot.Close()
		}
	}()
	lstatePath := filepath.Join(
		stateDir, filepath.FromSlash(snapshot.StatePath),
	)

	logger.Info(
		"found ledger state file",
		"component", "mithril",
		"path", lstatePath,
		"immutable_tip_slot", immutableTipSlot,
	)

	// Read once, from the file discovery opened. There is no name here to
	// reopen: lstatePath exists only for the messages above and below.
	//
	// The bytes are then both what the signature is checked against and what
	// the parser is given. Hashing the descriptor and handing the parser the
	// same descriptor would not be that: the parser re-reads, and a write
	// through the file between the two reads is visible to the second — an
	// in-place write reaches a descriptor already open on the file, which is
	// the one substitution the handle and the descriptor cannot rule out.
	// One buffer leaves nothing to change.
	stateBytes, err := ledgerstate.ReadSnapshotFile(snapshot.State)
	if err != nil {
		return nil, fmt.Errorf("reading ledger state: %w", err)
	}
	if signedBy != nil {
		if err := verifySignedState(
			snapshot.StatePath, stateBytes, signedBy,
		); err != nil {
			return nil, fmt.Errorf(
				"verifying ledger state in %s: %w", stateDir, err,
			)
		}
	}
	state, err := ledgerstate.ParseSnapshotBytes(stateBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing ledger state: %w", err)
	}

	// UTxO-HD keeps the UTxO set in a table beside the state; discovery opened
	// it from the same directory handle, so the two belong to one snapshot.
	if snapshot.Table != nil {
		if err := attachSignedTable(
			state, snapshot, stateDir, signedBy,
		); err != nil {
			return nil, fmt.Errorf(
				"verifying ledger state in %s: %w", stateDir, err,
			)
		}
		logger.Info(
			"found UTxO table file (UTxO-HD format)",
			"component", "mithril",
			"path", state.UTxOTablePath,
		)
	}

	if state.Tip == nil {
		return nil, errors.New(
			"parsed ledger state has no tip (Origin snapshot)",
		)
	}
	// The directory a state sits in names its slot; the state itself is what
	// says where it was taken.
	if state.Tip.Slot > immutableTipSlot {
		return nil, fmt.Errorf(
			"%w: state slot %d, ImmutableDB tip slot %d",
			ErrLedgerStateTooRecent, state.Tip.Slot, immutableTipSlot,
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
	return &preparedLedgerStateImport{
		snapshot: snapshot,
		state:    state,
		stateDir: stateDir,
	}, nil
}

func importPreparedLedgerState(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	nodeCfg *cardano.CardanoNodeConfig,
	result *BootstrapResult,
	prepared *preparedLedgerStateImport,
	reconcile bool,
	utxoSet ledgerstate.UtxoSetLoader,
	onLedger func(ledgerstate.ImportProgress),
) (
	ledgerStateSlot uint64,
	ledgerStateHash []byte,
	err error,
) {
	// The prepared object keeps the selected files open from verification
	// through import, so the UTxO table and state are the checked files.
	state := prepared.state

	// Build import key for resume tracking. A catch-up reconcile must run the
	// full import pass (so its snapshot key set is complete), so resume is
	// disabled by leaving the import key empty.
	importKey := importCheckpointKey(reconcile, result, state.Tip.Slot)

	// Import the ledger state
	if err := ledgerstate.ImportLedgerState(
		ctx,
		ledgerstate.ImportConfig{
			Database:  db,
			State:     state,
			Logger:    logger,
			ImportKey: importKey,
			Reconcile: reconcile,
			UtxoSet:   utxoSet,
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

// importCheckpointKey is the checkpoint key an import of the state at slot
// resumes from, empty when the import does not resume.
func importCheckpointKey(
	reconcile bool,
	result *BootstrapResult,
	slot uint64,
) string {
	if reconcile || result.Snapshot == nil || result.Snapshot.Digest == "" {
		return ""
	}
	return ledgerImportKey(result.Snapshot.Digest, slot)
}

// ledgerImportKey is the ledger-state import checkpoint key for an
// artifact's state at slot: "{digest prefix}:{slot}".
func ledgerImportKey(digest string, slot uint64) string {
	if len(digest) > 16 {
		digest = digest[:16]
	}
	return fmt.Sprintf("%s:%d", digest, slot)
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

// verifySignedState re-establishes the ancillary signature over the ledger
// state the import is about to parse.
//
// It takes the bytes rather than the descriptor, and the caller passes those
// same bytes to the parser. Hashing a descriptor and rewinding it looks
// equivalent and is not: the parser then reads the file a second time, and an
// in-place write between the two reads is visible through a descriptor already
// open on it. That is the substitution neither the directory handle nor the
// descriptor rules out, and one buffer is what removes it.
//
// The path is used only for the message. Resolving it would reintroduce the
// gap being closed, since the file it named when the manifest was checked and
// the file it names now are not required to be the same file.
func verifySignedState(
	statePath string,
	data []byte,
	digests map[string]string,
) error {
	expected, ok := digests[statePath]
	if !ok {
		return fmt.Errorf(
			"%w: %s is not covered by the manifest",
			errAncillaryDigestMismatch, statePath,
		)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != expected {
		return fmt.Errorf(
			"%w: %s computed %s, signed %s",
			errAncillaryDigestMismatch, statePath, got, expected,
		)
	}
	return nil
}

// attachSignedTable points the import at the UTxO-HD table discovery opened,
// and at the digest the manifest holds for it.
//
// The table is mapped rather than read — it is gigabytes — so the state's
// read-once-and-hash-the-buffer does not transfer. The digest travels down
// instead and is checked against the mapping the decoder walks, which keeps
// the check and the parse on one set of bytes. Carrying it is therefore not
// bookkeeping: an absent digest is how an unsigned table is decoded, so a
// signed one that failed to arrive would be decoded unchecked.
func attachSignedTable(
	state *ledgerstate.RawLedgerState,
	snapshot *ledgerstate.SnapshotFiles,
	stateDir string,
	digests map[string]string,
) error {
	state.UTxOTablePath = filepath.Join(
		stateDir, filepath.FromSlash(snapshot.TablePath),
	)
	state.UTxOTableFile = snapshot.Table
	if digests == nil {
		return nil
	}
	digest, err := signedTableDigest(digests, snapshot.TablePath)
	if err != nil {
		return err
	}
	state.UTxOTableDigest = digest
	return nil
}

// signedTableDigest returns the digest the manifest holds for a UTxO-HD table.
//
// A table the manifest does not cover is refused rather than passed on with no
// digest. An empty digest is how an unsigned tree is decoded — any tree
// nothing vouched for — so letting one through here would turn "nobody signed
// this file" into "nothing needs checking", which is the direction that must
// never be reachable by removing an entry.
func signedTableDigest(
	digests map[string]string,
	tablePath string,
) (string, error) {
	digest, ok := digests[tablePath]
	if !ok {
		return "", fmt.Errorf(
			"%w: %s is not covered by the manifest",
			errAncillaryDigestMismatch, tablePath,
		)
	}
	return digest, nil
}
