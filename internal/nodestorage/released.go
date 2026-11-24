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

package nodestorage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
)

// Steps of a ReleasedMigration, in order, as its Hook sees them.
const (
	ReleasedStepOpened   = "opened"
	ReleasedStepRewound  = "rewound"
	ReleasedStepRewards  = "rewards"
	ReleasedStepOutputs  = "outputs"
	ReleasedStepLoaded   = "loaded"
	ReleasedStepVerified = "verified"
)

// ReleasedMigration moves the ledger state of a v0.94.2 database out of its
// metadata store into the LedgerDB, in place, once. It runs before the
// ledger's database is opened, which refuses a block store the migration has
// not finished.
//
// Every step keeps its progress where it is simplest to resume from: the
// metadata store's sync_state while that store is the authority, the set's
// own markers for what it holds. The block store's migration marker is
// written last, so a stop anywhere before it reruns the migration, and every
// step finds its own work done.
//
// The migration is one-way and changes the metadata store: opening it
// upgrades its schema past the last version v0.94.2 knows, so that release
// refuses it afterwards, and the rewind reverts it below its tip. Its rows
// are copied, never deleted, and nothing opens the store once the marker is
// written.
type ReleasedMigration struct {
	// Config is the ledger database's configuration; the migration opens the
	// v0.94.2 database with it under database.RoleReleased.
	Config *database.Config
	// Blob is the data directory's block store, already open. The migration
	// does not close it.
	Blob blob.BlobStore
	// OpenMetadata opens the v0.94.2 metadata store and returns the function
	// that closes it. It is called only for a block store that v0.94.2 opened.
	OpenMetadata func(context.Context) (metadata.MetadataStore, func() error, error)
	// UtxoSet opens the live UTxO set the state moves into; the migration
	// closes it before it returns. Its DataDir is the data directory whose
	// volume the set must fit in, and its CardanoNodeConfig names the
	// zero-coin Byron genesis outputs removed before the outputs are copied.
	UtxoSet UtxoSetConfig
	// Set, when non-nil, is the live UTxO set already open in
	// UtxoSet.DataDir; the migration uses it and leaves it open.
	Set    *utxoset.Set
	Logger *slog.Logger
	// RewindBlocks is the security parameter: how many blocks below its tip
	// the metadata store is reverted before its state is copied, so the
	// set's rollback journal covers every rollback the node can be asked for.
	RewindBlocks uint64
	// DelegatorInactivityEnabled and DelegatorInactivity are the CIP-0163
	// settings the rewind and the reward input repair use.
	DelegatorInactivityEnabled bool
	DelegatorInactivity        uint64
	// Hook, when non-nil, runs after each step; an error it returns stops the
	// migration there, as a crash would.
	Hook func(step string) error
}

// Run migrates the data directory when its block store says v0.94.2 opened
// it and the migration has not finished. It reports whether it migrated.
func (m ReleasedMigration) Run(ctx context.Context) (bool, error) {
	state, err := database.ReadLedgerStoreState(m.Blob)
	if err != nil {
		return false, err
	}
	if state != database.LedgerStoreReleased {
		return false, nil
	}
	if m.OpenMetadata == nil {
		return false, errors.New(
			"the database was written by v0.94.2 and no metadata store is " +
				"configured to move its ledger state from",
		)
	}
	if m.UtxoSet.DataDir == "" {
		return false, errors.New(
			"the database was written by v0.94.2 and has no data directory",
		)
	}
	logger := m.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger.Info(
		"moving the ledger state of a v0.94.2 database out of its metadata store",
		"component", "database",
	)
	set := m.Set
	closeSet := func() error { return nil }
	if set == nil {
		opened, _, err := OpenUtxoSet(m.UtxoSet)
		if err != nil {
			return false, err
		}
		set, closeSet = opened, opened.Close
	}
	store, closeStore, err := m.OpenMetadata(ctx)
	if err != nil {
		return false, errors.Join(
			fmt.Errorf("open the v0.94.2 metadata store: %w", err),
			closeSet(),
		)
	}
	err = m.migrate(ctx, store, set, logger)
	if err == nil {
		// The marker below says the set holds the state.
		err = set.Sync()
	}
	if closeErr := closeSet(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf(
			"close the live UTxO set: %w", closeErr,
		))
	}
	if closeErr := closeStore(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf(
			"close the v0.94.2 metadata store: %w", closeErr,
		))
	}
	if err != nil {
		return false, err
	}
	if err := database.MarkLedgerMigrated(m.Blob); err != nil {
		return false, err
	}
	logger.Info(
		"moved the ledger state out of the v0.94.2 metadata store",
		"component", "database",
	)
	return true, nil
}

func (m ReleasedMigration) step(name string) error {
	if m.Hook == nil {
		return nil
	}
	return m.Hook(name)
}

func (m ReleasedMigration) migrate(
	ctx context.Context,
	store metadata.MetadataStore,
	set *utxoset.Set,
	logger *slog.Logger,
) (err error) {
	// The source is a v0.94.2 database, so it opens with the ledger's own
	// configuration: its Alonzo protocol-parameter rows repair in place only
	// with the lovelace-per-word value that carries.
	if m.Config == nil {
		return errors.New("the v0.94.2 migration needs the ledger database's configuration")
	}
	cfg := *m.Config
	cfg.Role = database.RoleReleased
	cfg.Logger = logger
	// The ledger's database registers the collectors once it opens.
	cfg.PromRegistry = nil
	cfg.Immutable = database.ImmutableConfig{}
	// v0.94.2 keeps every block in the block store, so the source needs no
	// ImmutableDB, and the ledger's database keeps the one it opens.
	db, err := database.New(ctx, &cfg, database.Stores{
		Blob:     m.Blob,
		Metadata: store,
	})
	if db != nil {
		defer func() { err = errors.Join(err, db.Close()) }()
	}
	if err != nil {
		return fmt.Errorf("open the v0.94.2 database: %w", err)
	}
	if err := m.step(ReleasedStepOpened); err != nil {
		return err
	}
	return m.Move(ctx, db, set)
}

// Move runs the migration's steps over db, the v0.94.2 database open with its
// metadata store, into set. It writes no marker; Run does once it returns.
func (m ReleasedMigration) Move(
	ctx context.Context,
	db *database.Database,
	set *utxoset.Set,
) error {
	logger := m.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	zero, err := ledger.ZeroValueByronGenesisOutputs(m.UtxoSet.CardanoNodeConfig)
	if err != nil {
		return fmt.Errorf("derive zero-value Byron genesis outputs: %w", err)
	}
	// Before anything copies the metadata store's outputs into the set.
	if err := ledger.RepairZeroValueByronGenesis(
		ctx, db, zero, logger, nil,
	); err != nil {
		return err
	}
	if err := m.rewindMetadataLedger(ctx, db, logger); err != nil {
		return err
	}
	if err := m.step(ReleasedStepRewound); err != nil {
		return err
	}
	if err := m.resolveRewardInputs(ctx, db, logger); err != nil {
		return err
	}
	if err := m.step(ReleasedStepRewards); err != nil {
		return err
	}
	// The rows are copied, not removed: nothing reopens the store.
	if err := MigrateUtxoRows(
		ctx, db, set, m.UtxoSet.DataDir, logger,
	); err != nil {
		return err
	}
	if err := CheckUtxoSetOnly(db); err != nil {
		return err
	}
	if err := m.step(ReleasedStepOutputs); err != nil {
		return err
	}
	ldb := dbadapter.New(db)
	if err := set.ReconcileReleased(ctx, ldb); err != nil {
		return fmt.Errorf("load the ledger state into the live UTxO set: %w", err)
	}
	if err := removeLiveSetZeroValueByronGenesis(
		set, logger, zero,
	); err != nil {
		return err
	}
	if err := m.step(ReleasedStepLoaded); err != nil {
		return err
	}
	if err := verifyReleasedMove(ctx, db, set); err != nil {
		return err
	}
	return m.step(ReleasedStepVerified)
}

// verify checks that the set holds every ledger-state family and stands at
// the metadata store's tip, the point its state was copied at.
func verifyReleasedMove(
	ctx context.Context,
	db *database.Database,
	set *utxoset.Set,
) error {
	for _, family := range nodesettings.Families() {
		loaded, err := ledger.FamilyLoaded(family, set)
		if err != nil {
			return fmt.Errorf("read the set's %s markers: %w", family, err)
		}
		if !loaded {
			return fmt.Errorf(
				"the live UTxO set did not load the %s ledger state",
				family,
			)
		}
	}
	tip, err := db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read the v0.94.2 tip: %w", err)
	}
	setTip, ok, err := set.Tip()
	if err != nil {
		return fmt.Errorf("read the set's tip: %w", err)
	}
	if isOrigin(tip.Point) && !ok {
		return nil
	}
	if !ok || setTip.Slot != tip.Point.Slot ||
		string(setTip.Hash) != string(tip.Point.Hash) {
		return fmt.Errorf(
			"the live UTxO set is at slot %d and the v0.94.2 ledger at slot %d",
			setTip.Slot, tip.Point.Slot,
		)
	}
	return ctx.Err()
}
