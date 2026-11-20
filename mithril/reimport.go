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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledgerstate"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// defaultReimportArtifacts is how many of the aggregator's newest artifacts a
// re-import looks through for a ledger state on the local chain.
const defaultReimportArtifacts = 8

// ErrNoReimportState reports that none of the aggregator's artifacts holds a
// ledger state at or below the local tip that lies on the local chain.
var ErrNoReimportState = errors.New(
	"no Mithril artifact holds a ledger state at or below the local tip on the local chain",
)

// ReimportConfig configures ReimportLedgerState.
type ReimportConfig struct {
	// Bootstrap carries the network, aggregator, trust keys and download
	// limits. The certificate chain is always verified, and no immutable file
	// is downloaded: the chain's blocks are the ones already stored.
	Bootstrap BootstrapConfig
	DB        *database.Database
	// CardanoNodeConfig supplies era epoch lengths to the ledger import.
	CardanoNodeConfig *cardano.CardanoNodeConfig
	// UtxoSet receives the ledger state; the database drained families into
	// it, so the import must be able to load them back.
	UtxoSet ledgerstate.FamilyLoader
	// Commit, when set, runs once the import has loaded UtxoSet and before the
	// run is marked finished. It makes the loaded set the node's live set: the
	// import loaded a copy of it, so a run that fails earlier leaves the live
	// set as it was.
	Commit func(ctx context.Context, point ocommon.Point) error
	// LocalTip is the newest block the node stores. The imported ledger state
	// is at or below it: it is the import's maxTrustedSlot, the highest slot
	// the import may anchor the Mithril trust boundary at.
	LocalTip ocommon.Point
	// OnChain reports whether a point is a block of the local primary chain.
	OnChain func(ctx context.Context, point ocommon.Point) (bool, error)
	// EpochOf returns the epoch of a slot. With it, an artifact whose beacon
	// epoch is more than one past LocalTip's epoch is skipped before anything
	// is downloaded: its ledger state, taken at most one epoch before its
	// beacon, is above the local tip. Nil downloads every artifact tried.
	EpochOf func(slot uint64) (uint64, error)
	// MaxArtifacts bounds how many artifacts are tried, newest first; zero
	// selects defaultReimportArtifacts.
	MaxArtifacts int
	Logger       *slog.Logger

	bootstrap func(context.Context, BootstrapConfig) (*BootstrapResult, error)
	list      func(
		context.Context, BootstrapConfig, string,
	) ([]CardanoDatabaseSnapshotListItem, error)
}

var errNotOnLocalChain = errors.New(
	"the ledger state's tip is not a block of the local chain",
)

var errBlocksNotStored = errors.New(
	"blocks between the ledger state and the Mithril trust boundary are no longer stored",
)

// ReimportLedgerState loads the ledger state of the newest Mithril artifact
// that holds one at or below cfg.LocalTip, on the local chain, into the live
// UTxO set and the metadata store, and returns its point. Only the signed
// ledger state is downloaded: the ledger replays the blocks it already stores
// from the returned point.
//
// The run is marked in progress before it writes, so a start that finds it
// unfinished runs it again; a run that stops part way is resumed the same way
// a catch-up is, from a full pass over the same snapshot.
func ReimportLedgerState(
	ctx context.Context,
	cfg ReimportConfig,
) (ocommon.Point, error) {
	if cfg.DB == nil || cfg.UtxoSet == nil || cfg.OnChain == nil {
		return ocommon.Point{}, errors.New(
			"re-import needs a database, a live UTxO set and a chain check",
		)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	bootstrap := cfg.bootstrap
	if bootstrap == nil {
		bootstrap = Bootstrap
	}
	list := cfg.list
	if list == nil {
		list = listArtifacts
	}
	bcfg, url, err := prepareFeedConfig(cfg.Bootstrap)
	if err != nil {
		return ocommon.Point{}, err
	}
	artifacts, err := list(ctx, bcfg, url)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf("listing Mithril artifacts: %w", err)
	}
	limit := cfg.MaxArtifacts
	if limit <= 0 {
		limit = defaultReimportArtifacts
	}
	if err := cfg.DB.DeleteSyncState(
		database.LedgerReimportSwappedSyncKey, nil,
	); err != nil {
		return ocommon.Point{}, fmt.Errorf("clearing the re-import swap mark: %w", err)
	}
	if err := cfg.DB.SetSyncState(
		database.LedgerReimportSyncKey, "1", nil,
	); err != nil {
		return ocommon.Point{}, fmt.Errorf("marking the re-import in progress: %w", err)
	}
	if err := cfg.DB.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	); err != nil {
		return ocommon.Point{}, fmt.Errorf("marking the re-import in progress: %w", err)
	}
	var skipped []error
	var localEpoch uint64
	haveEpoch := false
	if cfg.EpochOf != nil {
		if e, err := cfg.EpochOf(cfg.LocalTip.Slot); err == nil {
			localEpoch, haveEpoch = e, true
		}
	}
	for i, artifact := range artifacts {
		if i >= limit {
			break
		}
		if haveEpoch && artifact.Beacon.Epoch > localEpoch+1 {
			logger.Info(
				"mithril: artifact is above the local tip; not downloading it",
				"component", "mithril",
				"artifact", artifact.Hash,
				"beacon_epoch", artifact.Beacon.Epoch,
				"local_tip_epoch", localEpoch,
			)
			skipped = append(skipped, fmt.Errorf(
				"artifact %s (epoch %d) is above the local tip's epoch %d",
				artifact.Hash, artifact.Beacon.Epoch, localEpoch,
			))
			continue
		}
		point, err := reimportFromArtifact(
			ctx, cfg, bcfg, bootstrap, logger, artifact,
		)
		if errors.Is(err, errNotOnLocalChain) ||
			errors.Is(err, errBlocksNotStored) ||
			errors.Is(err, ledgerstate.ErrNoUsableLedgerState) {
			logger.Info(
				"mithril: artifact has no usable ledger state for the local chain",
				"component", "mithril",
				"artifact", artifact.Hash, "error", err,
			)
			skipped = append(skipped, err)
			continue
		}
		if err != nil {
			return ocommon.Point{}, err
		}
		if cfg.Commit != nil {
			if err := cfg.Commit(ctx, point); err != nil {
				return ocommon.Point{}, fmt.Errorf(
					"making the imported live UTxO set current: %w", err,
				)
			}
		}
		if err := cfg.DB.SetSyncState(
			database.LedgerReimportSwappedSyncKey, database.LedgerReimportPointMark(point), nil,
		); err != nil {
			return ocommon.Point{}, fmt.Errorf("recording the swapped live UTxO set: %w", err)
		}
		if err := finishReimport(ctx, cfg.DB, point); err != nil {
			return ocommon.Point{}, err
		}
		return point, nil
	}
	// Nothing was written: the marker only held the place of a run that did.
	if err := clearReimportMarks(cfg.DB); err != nil {
		return ocommon.Point{}, err
	}
	return ocommon.Point{}, fmt.Errorf(
		"%w (tried %d artifacts below slot %d): %w",
		ErrNoReimportState, min(len(artifacts), limit),
		cfg.LocalTip.Slot, errors.Join(skipped...),
	)
}

func listArtifacts(
	ctx context.Context,
	cfg BootstrapConfig,
	url string,
) ([]CardanoDatabaseSnapshotListItem, error) {
	return newMithrilClient(url, cfg.AllowInsecureHTTP).
		ListCardanoDatabaseSnapshots(ctx)
}

// reimportFromArtifact downloads one artifact's signed ledger state and imports
// it when its tip is on the local chain.
func reimportFromArtifact(
	ctx context.Context,
	cfg ReimportConfig,
	bcfg BootstrapConfig,
	bootstrap func(context.Context, BootstrapConfig) (*BootstrapResult, error),
	logger *slog.Logger,
	artifact CardanoDatabaseSnapshotListItem,
) (ocommon.Point, error) {
	bcfg.PinnedDigest = artifact.Hash
	// Every immutable file is "already present": only the digest list and
	// the ancillary archive are fetched.
	bcfg.StartImmutable = artifact.Beacon.ImmutableFileNumber + 1
	logger.Info(
		"mithril: downloading a signed ledger state to re-import",
		"component", "mithril",
		"artifact", artifact.Hash,
		"immutable_file_number", artifact.Beacon.ImmutableFileNumber,
		"local_tip_slot", cfg.LocalTip.Slot,
	)
	result, err := bootstrap(ctx, bcfg)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf(
			"downloading Mithril artifact %s: %w", artifact.Hash, err,
		)
	}
	defer result.Cleanup(logger)
	slot, hash, err := importLedgerState(
		ctx, cfg.DB, logger, cfg.CardanoNodeConfig, result,
		true, cfg.LocalTip.Slot, cfg.UtxoSet,
		func(ledgerstate.ImportProgress) {},
		func(p *preparedLedgerStateImport) error {
			point := p.point()
			if err := refuseStateAboveImmutableTip(
				cfg.DB, cfg.LocalTip, point,
			); err != nil {
				return err
			}
			onChain, err := cfg.OnChain(ctx, point)
			if err != nil {
				return fmt.Errorf("check the ledger state's tip on the chain: %w", err)
			}
			if !onChain {
				return fmt.Errorf("%w: slot %d", errNotOnLocalChain, point.Slot)
			}
			return blocksStoredToBoundary(ctx, cfg.DB, point)
		},
	)
	if err != nil {
		return ocommon.Point{}, err
	}
	return ocommon.Point{Slot: slot, Hash: hash}, nil
}

// blocksStoredToBoundary checks, before anything is written, that every block
// from point up to an existing Mithril trust boundary is still stored. The
// import keeps a boundary later than the state it loads, so the ledger replays
// those blocks from the state, and history expiry may have pruned them: a state
// whose replay cannot run is not worth loading. A point at or past the
// boundary, or a database with none recorded, has nothing to replay below it.
func blocksStoredToBoundary(
	ctx context.Context,
	db *database.Database,
	point ocommon.Point,
) error {
	slot, hash, err := mithrilLedgerAnchor(db)
	if err != nil {
		slotText, gerr := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
		if gerr == nil && slotText == "" {
			return nil
		}
		return err
	}
	if slot <= point.Slot {
		return nil
	}
	boundary, err := database.BlockByHash(ctx, db, hash)
	if errors.Is(err, models.ErrBlockNotFound) {
		return fmt.Errorf(
			"%w: the block at the trust boundary, slot %d, is not stored",
			errBlocksNotStored, slot,
		)
	}
	if err != nil {
		return fmt.Errorf("reading the block at the trust boundary: %w", err)
	}
	_, stored, err := localChainBlockHashesAfterPoint(ctx, db, boundary, point)
	if err != nil {
		return err
	}
	if !stored {
		return fmt.Errorf(
			"%w: the chain from the ledger state at slot %d to the boundary at slot %d is not stored in full",
			errBlocksNotStored, point.Slot, slot,
		)
	}
	return nil
}

// FinishReimport completes a re-import whose loaded state is already the live
// set, at point: it records the Mithril trust boundary and clears the marks.
func FinishReimport(
	ctx context.Context,
	db *database.Database,
	point ocommon.Point,
) error {
	return finishReimport(ctx, db, point)
}

// finishReimport records the imported point as the Mithril trust boundary,
// unless an older import set a later one, and clears the in-progress mark.
func finishReimport(
	ctx context.Context,
	db *database.Database,
	point ocommon.Point,
) error {
	tipTxn := db.MetadataTxn(ctx, false)
	tip, err := db.GetTip(tipTxn)
	tipTxn.Release()
	if err != nil {
		return fmt.Errorf("reading the imported ledger tip: %w", err)
	}
	if tip.Point.Slot != point.Slot {
		return fmt.Errorf(
			"imported ledger tip %d does not match the ledger state %d",
			tip.Point.Slot, point.Slot,
		)
	}
	prev, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	if err != nil {
		return err
	}
	if prevSlot, perr := strconv.ParseUint(prev, 10, 64); perr != nil ||
		prevSlot < point.Slot {
		if err := setMithrilLedgerAnchor(db, point); err != nil {
			return err
		}
	}
	return clearReimportMarks(db)
}

// ReimportInterrupted reports whether db holds a ledger-state re-import the
// node itself started and did not finish. The node runs it again at start, so
// no sync command is needed to finish it.
func ReimportInterrupted(db *database.Database) (bool, error) {
	status, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return false, fmt.Errorf("reading sync_status: %w", err)
	}
	if status != syncStatusInProgress {
		return false, nil
	}
	active, err := db.GetSyncState(database.LedgerReimportSyncKey, nil)
	if err != nil {
		return false, fmt.Errorf("reading the re-import mark: %w", err)
	}
	return active != "", nil
}

func clearReimportMarks(db *database.Database) error {
	for _, key := range []string{
		"sync_status",
		database.LedgerReimportSyncKey,
		database.LedgerReimportSwappedSyncKey,
	} {
		if err := db.DeleteSyncState(key, nil); err != nil {
			return fmt.Errorf("clearing the re-import mark: %w", err)
		}
	}
	return nil
}

func setMithrilLedgerAnchor(db *database.Database, point ocommon.Point) error {
	if err := db.SetSyncState(
		mithrilLedgerSlotSyncKey, strconv.FormatUint(point.Slot, 10), nil,
	); err != nil {
		return fmt.Errorf("recording the Mithril ledger slot: %w", err)
	}
	return db.SetSyncState(mithrilLedgerHashSyncKey, hex.EncodeToString(point.Hash), nil)
}

// mithrilLedgerAnchor reads the stable ledger point a Mithril import recorded.
func mithrilLedgerAnchor(db *database.Database) (uint64, []byte, error) {
	slotText, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("reading Mithril ledger slot: %w", err)
	}
	if slotText == "" {
		return 0, nil, errors.New(
			"the database has no recorded Mithril ledger slot",
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
