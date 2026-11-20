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

// Package nodestorage composes the storage a ledger runs on: the live UTxO
// set beside a database, its reconcile with the ledger, the ledger-state
// drain, and the ImmutableDB configuration. The node and `dingo load` both
// build on it, so a database either of them wrote is one the other opens.
package nodestorage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledgerstate"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// UtxoSetConfig configures OpenUtxoSet.
type UtxoSetConfig struct {
	// DataDir is the database's data directory; empty keeps the set in a
	// temporary directory.
	DataDir           string
	Logger            *slog.Logger
	Registerer        prometheus.Registerer
	CardanoNodeConfig *cardano.CardanoNodeConfig
	// RetainRewardRecords is set on the split layout; see
	// utxoset.Config.RetainRewardRecords.
	RetainRewardRecords bool
	// CheckpointKeep is how many checkpoints the set retains.
	CheckpointKeep int
}

// OpenUtxoSet opens the live UTxO set of the database in cfg.DataDir, which
// holds the UTxO set and the ledger state. With no data directory the set
// lives in a temporary directory, returned for the caller to remove once the
// set is closed.
func OpenUtxoSet(cfg UtxoSetConfig) (*utxoset.Set, string, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	// The Shelley window, 3k/f, is longer than Byron's 2k, so it covers
	// every era's rollbacks.
	history, err := ledger.ShelleyStabilityWindow(
		cfg.CardanoNodeConfig.ShelleyGenesis(),
	)
	if err != nil {
		logger.Warn(
			"no stability window for the live UTxO set; keeping its whole rollback journal",
			"error",
			err,
		)
		history = 0
	}
	dataDir := cfg.DataDir
	var tempDir string
	if dataDir == "" {
		tempDir, err = os.MkdirTemp("", "dingo-storage-")
		if err != nil {
			return nil, "", fmt.Errorf(
				"create temporary storage directory: %w", err,
			)
		}
		dataDir = tempDir
	}
	set, err := utxoset.Open(utxoset.Config{
		Dir:                 utxoset.DirFor(dataDir),
		Logger:              logger,
		Registerer:          cfg.Registerer,
		HistorySlots:        history,
		Only:                true,
		RetainRewardRecords: cfg.RetainRewardRecords,
		Authoritative:       true,
		// The set is the only copy of the ledger state, so it needs a
		// checkpoint to recover from.
		CheckpointDir:  utxoset.CheckpointDirFor(dataDir),
		CheckpointKeep: cfg.CheckpointKeep,
	})
	if err != nil {
		if tempDir != "" {
			_ = os.RemoveAll(tempDir)
		}
		return nil, "", fmt.Errorf("open live UTxO set: %w", err)
	}
	return set, tempDir, nil
}

// Reconcile brings a database and its live UTxO set to the state a ledger
// starts from.
type Reconcile struct {
	DB *database.Database
	// Set is the open live UTxO set; nil fails any step that needs it.
	Set *utxoset.Set
	// DataDir is the data directory whose volume a migration must fit in.
	DataDir string
	Logger  *slog.Logger
	// FinishImport completes a pending import of ledger state into the set;
	// nil uses FinishDrainedImport.
	FinishImport func(context.Context) error
	// Publish receives the drained families when they are read and each
	// time the drain latches one, so readers stop falling back to rows that
	// were deleted.
	Publish func(nodesettings.FamilyMask)
	// Resume rolls the ledger back to the set's tip; see utxoset.ResumeFunc.
	Resume utxoset.ResumeFunc
	// Reconciled, when set, runs once the set equals the ledger and before
	// the drain.
	Reconciled func(context.Context) error
	// Guard, when non-nil, is armed for every drained family.
	Guard statedrain.Guard
	// CardanoNodeConfig names the zero-coin Byron genesis outputs Run
	// removes from the metadata store and the live UTxO set; nil removes
	// none.
	CardanoNodeConfig *cardano.CardanoNodeConfig
	// RewindBlocks is the security parameter: how many blocks below its tip
	// a database that holds its ledger state in the metadata store is
	// reverted before the state moves into the set. Zero moves it at the tip,
	// which leaves a set that cannot roll back below it.
	RewindBlocks uint64
	// DelegatorInactivityEnabled and DelegatorInactivity are the CIP-0163
	// settings the rewind recomputes account expirations with.
	DelegatorInactivityEnabled bool
	DelegatorInactivity        uint64
}

// Run finishes a pending import, moves a metadata UTxO set into the live UTxO
// set, marks the database as holding its UTxO set only there, makes the set
// equal to the ledger, and drains the graduated ledger-state families from
// the metadata store. It runs after database recovery and before the ledger
// writes its genesis or applies a block, so a new database never holds those
// families in SQL.
func (r Reconcile) Run(ctx context.Context) error {
	zero, err := ledger.ZeroValueByronGenesisOutputs(r.CardanoNodeConfig)
	if err != nil {
		return fmt.Errorf("derive zero-value Byron genesis outputs: %w", err)
	}
	// Before anything copies the metadata store's outputs into the live UTxO
	// set.
	if err := ledger.RepairZeroValueByronGenesis(
		ctx, r.DB, zero, r.Logger, nil,
	); err != nil {
		return err
	}
	finish := r.FinishImport
	if finish == nil {
		finish = func(ctx context.Context) error {
			return FinishDrainedImport(ctx, r.DB, r.Set, r.Logger)
		}
	}
	if err := finish(ctx); err != nil {
		return err
	}
	if err := r.rewindMetadataLedger(ctx); err != nil {
		return err
	}
	if err := MigrateUtxoRows(
		ctx, r.DB, r.Set, r.DataDir, r.Logger,
	); err != nil {
		return err
	}
	if err := CheckUtxoSetOnly(r.DB); err != nil {
		return err
	}
	// Published before the reconcile, which can roll the SQL state back, so
	// a family whose drain was interrupted is already served from the set.
	mask, err := r.DB.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	if r.Publish != nil {
		r.Publish(mask)
	}
	if r.Set != nil {
		if err := r.Set.Reconcile(
			ctx, dbadapter.New(r.DB), r.Resume,
		); err != nil {
			return fmt.Errorf("reconcile live UTxO set: %w", err)
		}
	}
	// After the set is reconciled, which can restore a checkpoint older than
	// the repair, and before anything reads it.
	if err := r.removeLiveSetZeroValueByronGenesis(zero); err != nil {
		return err
	}
	if r.Reconciled != nil {
		if err := r.Reconciled(ctx); err != nil {
			return err
		}
	}
	return DrainLedgerState(ctx, r.DB, r.Set, r.Logger, r.Guard, r.Publish)
}

// removeLiveSetZeroValueByronGenesis removes the zero-coin Byron genesis
// outputs from the live UTxO set, where a node that seeded them before it
// dropped them holds them. It looks at the few outputs by key on every start
// rather than keeping a mark: the set can be rebuilt or restored from a
// checkpoint that predates the removal, and a mark would then outlive the
// outputs it vouched for. An output the set does not hold, because it was
// spent, is not touched, and neither is one with coin.
func (r Reconcile) removeLiveSetZeroValueByronGenesis(
	zero ledger.ZeroValueByronOutputs,
) error {
	if r.Set == nil || len(zero.Refs) == 0 {
		return nil
	}
	removed, err := r.Set.RemoveZeroCoinOutputs(zero.Refs)
	if err != nil {
		return fmt.Errorf(
			"remove zero-value Byron genesis outputs from the live UTxO set: %w",
			err,
		)
	}
	if removed > 0 && r.Logger != nil {
		r.Logger.Info(
			"removed zero-value Byron genesis outputs from the live UTxO set",
			"component", "node",
			"outputs", removed,
		)
	}
	return nil
}

// FinishDrainedImport completes an import of ledger state into the live UTxO
// set that a crash left after its SQL rows were written and before the set
// loaded them. An import that is still writing, which leaves sync_status set,
// is refused: only the import that began it can finish it.
func FinishDrainedImport(
	ctx context.Context,
	db *database.Database,
	set *utxoset.Set,
	logger *slog.Logger,
) error {
	_, pending, err := db.DrainedImportPending(ctx)
	if err != nil || !pending {
		return err
	}
	status, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return fmt.Errorf("read sync status: %w", err)
	}
	if status != "" {
		return database.ErrDrainedImportPending
	}
	if set == nil {
		return errors.New(
			"an import of ledger state into the live UTxO set is pending and the set is not open",
		)
	}
	if logger != nil {
		logger.Info(
			"finishing an import of ledger state into the live UTxO set",
			"component", "node",
		)
	}
	return ledgerstate.FinishDrainedImport(ctx, db, set)
}

// SecurityParam returns the Shelley genesis security parameter, the value of
// Reconcile.RewindBlocks, or zero when it is unknown.
func SecurityParam(cfg *cardano.CardanoNodeConfig) uint64 {
	if cfg == nil {
		return 0
	}
	sg := cfg.ShelleyGenesis()
	if sg == nil || sg.SecurityParam <= 0 {
		return 0
	}
	return uint64(sg.SecurityParam) //nolint:gosec // checked positive above
}

// MigrationRewindSyncKey is the sync_state key that records that a database
// holding its ledger state in the metadata store was reverted below its tip
// for the move into the live UTxO set. It is written in the transaction that
// reverts the store and removed once the move has finished.
const MigrationRewindSyncKey = "ledger.utxo_set_migration.rewound"

// rewindMetadataLedger reverts a database that still holds its UTxO set and
// ledger state in the metadata store to RewindBlocks blocks below its tip,
// once, before the move into the live UTxO set. The set's rollback journal
// starts at the point the state is moved at, and a node can be asked to roll
// back RewindBlocks blocks below its tip, so a move at the tip would leave a
// set that cannot follow such a rollback. The metadata store can: its rows
// carry the slots they were added and deleted at. The blocks stay, and the
// ledger applies them again through the set, which journals each.
func (r Reconcile) rewindMetadataLedger(ctx context.Context) error {
	marked, err := r.DB.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	if err != nil {
		return fmt.Errorf("read UTxO set mode: %w", err)
	}
	rewound, err := r.DB.GetSyncState(MigrationRewindSyncKey, nil)
	if err != nil {
		return fmt.Errorf("read migration rewind mark: %w", err)
	}
	if marked != "" {
		if rewound == "" {
			return nil
		}
		// The move has finished, so nothing reads the mark again.
		if err := r.DB.DeleteSyncState(
			MigrationRewindSyncKey, nil,
		); err != nil {
			return fmt.Errorf("clear migration rewind mark: %w", err)
		}
		return nil
	}
	if rewound != "" || r.RewindBlocks == 0 {
		return nil
	}
	// A move an earlier start began has built or verified the set at the
	// tip it found; reverting the store now would only make them differ.
	progress, err := r.DB.GetSyncState(utxoset.MigrationSyncKey, nil)
	if err != nil {
		return fmt.Errorf("read UTxO set migration progress: %w", err)
	}
	drained, err := r.DB.EffectiveDrainedFamilies(ctx)
	if err != nil {
		return err
	}
	if progress != "" || drained != 0 {
		return nil
	}
	tip, err := r.DB.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read tip: %w", err)
	}
	if isOrigin(tip.Point) {
		return nil
	}
	target, reverted, err := lifecycle.RewindMetadata(
		ctx, r.DB, r.RewindBlocks,
		r.DelegatorInactivityEnabled, r.DelegatorInactivity,
		func(txn *database.Txn) error {
			return r.DB.SetSyncState(MigrationRewindSyncKey, "1", txn)
		},
	)
	if err != nil {
		return fmt.Errorf(
			"revert the metadata store below its tip for the move into the live UTxO set: %w",
			err,
		)
	}
	if reverted && r.Logger != nil {
		r.Logger.Info(
			"reverted the metadata store below its tip; the ledger applies the blocks after it again through the live UTxO set",
			"component", "node",
			"tip_slot", tip.Point.Slot,
			"slot", target.Slot,
			"block", target.Number,
		)
	}
	return nil
}

// MigrateUtxoRows moves a database that holds its UTxO set in the metadata
// store into the live UTxO set, or finishes a move an earlier start began. It
// runs before CheckUtxoSetOnly, which finds the database marked once the move
// has finished.
func MigrateUtxoRows(
	ctx context.Context,
	db *database.Database,
	set *utxoset.Set,
	dataDir string,
	logger *slog.Logger,
) error {
	marked, err := db.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	if err != nil {
		return fmt.Errorf("read UTxO set mode: %w", err)
	}
	progress, err := db.GetSyncState(utxoset.MigrationSyncKey, nil)
	if err != nil {
		return fmt.Errorf("read UTxO set migration progress: %w", err)
	}
	if progress == "" {
		if marked != "" {
			return nil
		}
		tip, err := db.GetTip(nil)
		if err != nil {
			return fmt.Errorf("read tip: %w", err)
		}
		if isOrigin(tip.Point) {
			return nil
		}
	}
	if set == nil {
		return errors.New(
			"the live UTxO set to move this database's UTxO set into is not open",
		)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger.Info(
		"moving the UTxO set from the metadata store into the live UTxO set",
		"component", "node",
	)
	return set.MigrateFromSQL(
		ctx,
		dbadapter.New(db),
		utxoset.MigrationEnv{
			Store: DrainStore{db},
			Mark: func(context.Context) error {
				return db.SetSyncState(database.UtxoSetOnlySyncKey, "1", nil)
			},
			Marked: func(context.Context) (bool, error) {
				v, err := db.GetSyncState(database.UtxoSetOnlySyncKey, nil)
				return v != "", err
			},
			FreeBytes: func() (uint64, error) {
				return utxoset.AvailableDiskBytes(dataDir)
			},
			Logger: logger,
		},
	)
}

// CheckUtxoSetOnly marks a database at origin as holding its UTxO set only in
// the live UTxO set. A database with a metadata UTxO set is marked by
// MigrateUtxoRows, which runs first, so one still unmarked past origin has a
// metadata UTxO set that was not moved.
func CheckUtxoSetOnly(db *database.Database) error {
	marked, err := db.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	if err != nil {
		return fmt.Errorf("read UTxO set mode: %w", err)
	}
	if marked != "" {
		return nil
	}
	tip, err := db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read tip: %w", err)
	}
	if !isOrigin(tip.Point) {
		return errors.New(
			"found a metadata UTxO set that was not moved into the live UTxO set",
		)
	}
	if err := db.SetSyncState(database.UtxoSetOnlySyncKey, "1", nil); err != nil {
		return fmt.Errorf("record UTxO set mode: %w", err)
	}
	return nil
}

func isOrigin(p ocommon.Point) bool {
	return p.Slot == 0 && len(p.Hash) == 0
}

// AnchorInterval is how often the live UTxO set's anchor is refreshed. The
// anchor is k blocks deep, so a few seconds of staleness cannot matter.
const AnchorInterval = 5 * time.Second

// ImmutablePointSource reports the primary-chain point k blocks behind the
// ledger's applied tip.
type ImmutablePointSource interface {
	ImmutablePoint(ctx context.Context) (ocommon.Point, bool, error)
}

// UpdateAnchor records the ledger's immutable point as the set's anchor, the
// point its rollback journal is never pruned past; until one is recorded the
// journal is not pruned at all. ImmutablePoint takes the ledger's lock and the
// chain's, which the ledger's commit path already holds while it applies the
// set, so the set never asks for it: a goroutine of the caller does, and only
// hands the set the result.
func UpdateAnchor(
	ctx context.Context,
	src ImmutablePointSource,
	set *utxoset.Set,
) {
	point, found, err := src.ImmutablePoint(ctx)
	if err != nil || !found {
		return
	}
	set.UpdateAnchor(point)
}
