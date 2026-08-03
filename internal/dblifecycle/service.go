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

// Package dblifecycle is the single entry point the CLI (and, in a later
// phase, a gRPC surface) calls to perform database snapshot, restore, and
// truncate operations, so both front ends run exactly the same code path.
//
// For now Service only supports the offline path: it opens its own
// *database.Database directly against the configured data directory, the
// same way the `load`/`mithril` CLI commands do. It must not be used
// concurrently with a running node (or another Service) pointed at the
// same data directory — a later phase adds a live-node path that quiesces
// a running node's storage layer first.
package dblifecycle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/version"
)

// LiveNode is satisfied by a running *dingo.Node (see node_lifecycle.go's
// Restore/Truncate methods there — this interface is defined here, not
// imported from the dingo package, specifically so dblifecycle does not
// need to import the root dingo package, which already imports
// dblifecycle for Manager; dingo.Node satisfies this interface
// structurally, with no additional wiring on its side.
type LiveNode interface {
	Snapshot(
		ctx context.Context,
		destDir string,
		name string,
		description string,
	) (lifecycle.Manifest, error)
	Restore(ctx context.Context, snapshotDir string) (lifecycle.Manifest, error)
	Truncate(ctx context.Context, target TruncateTarget) (blocksRemoved uint64, err error)
}

// Service performs database lifecycle operations against the data
// directory and plugins named in cfg. If bound to a LiveNode (SetLiveNode),
// Restore/Truncate delegate to it instead of operating offline — see
// SetLiveNode's doc comment for what that changes.
type Service struct {
	cfg                 *config.Config
	logger              *slog.Logger
	destinationRegistry *lifecycle.DestinationRegistry
	liveNode            LiveNode
}

// NewService creates a Service bound to cfg's database configuration
// (DatabasePath, Plugins.Storage, StorageMode, Network).
// destinationRegistry supplies the cloud destination schemes (s3, gcs)
// available for cfg.DatabaseLifecycle.SnapshotCloudDestination and any
// cloud snapshotDir passed to Restore — composition code owns
// constructing it; nil is valid when no cloud destination is ever used.
func NewService(
	cfg *config.Config,
	destinationRegistry *lifecycle.DestinationRegistry,
	logger *slog.Logger,
) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		cfg:                 cfg,
		destinationRegistry: destinationRegistry,
		logger:              logger,
	}
}

// SetLiveNode binds Service to a running node. Once set, Restore/Truncate
// quiesce and reinitialize that node's live storage in-process instead of
// opening the configured data directory offline — the data directory must
// then be the one the live node itself is already using, since the node
// (not this Service) owns opening/closing it. Snapshot still never
// quiesces (see database/lifecycle's package doc) but also delegates, so
// it reads from the node's own open database instead of opening a second,
// competing handle on the same data directory.
func (s *Service) SetLiveNode(n LiveNode) {
	s.liveNode = n
}

// openDatabase opens the configured database. Unlike the `load`/`mithril`
// bootstrap paths, it does not tolerate a commit-timestamp mismatch —
// lifecycle operations should fail loudly on a database that looks
// inconsistent rather than operate on it.
func (s *Service) openDatabase(
	ctx context.Context,
) (*internalplugins.DatabaseRuntime, error) {
	runtime, err := internalplugins.OpenDatabase(
		ctx,
		&database.Config{
			DataDir:     s.cfg.DatabasePath,
			Logger:      s.logger,
			StorageMode: s.cfg.StorageMode,
			Network:     s.cfg.Network,
		},
		internalplugins.StorageSelections{
			Blob:     s.cfg.Plugins.Storage.Blob,
			Metadata: s.cfg.Plugins.Storage.Metadata,
		},
		internalplugins.StorageDependencies{
			DataDir:        s.cfg.DatabasePath,
			StorageMode:    s.cfg.StorageMode,
			MaxConnections: s.cfg.DatabaseWorkers,
			Logger:         s.logger,
		},
	)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close(ctx)
		}
		return nil, fmt.Errorf("open database: %w", err)
	}
	if recoveryErr := runtime.RecoveryError(); recoveryErr != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf(
			"database is in a recoverable but inconsistent state: %w",
			recoveryErr,
		)
	}
	return runtime, nil
}

// Snapshot captures a point-in-time backup of the configured database
// into destDir, which must not already exist, or — if SetLiveNode was
// called — captures it from the bound running node's own already-open
// database instead of opening a second, competing handle on the same
// data directory. name/description are applied to the local manifest
// before any cloud mirroring, per lifecycle.SnapshotToCloud's doc comment
// — pass "" for either to leave a snapshot unlabeled.
func (s *Service) Snapshot(
	ctx context.Context,
	destDir string,
	name string,
	description string,
) (lifecycle.Manifest, error) {
	if s.liveNode != nil {
		return s.liveNode.Snapshot(ctx, destDir, name, description)
	}
	db, err := s.openDatabase(ctx)
	if err != nil {
		return lifecycle.Manifest{}, err
	}
	defer db.Close(ctx)
	return lifecycle.SnapshotToCloud(
		ctx,
		s.destinationRegistry,
		db.Database,
		destDir,
		lifecycle.TriggerManual,
		version.GetVersionString(),
		s.cfg.Plugins.Storage.Blob.Provider,
		s.cfg.Plugins.Storage.Metadata.Provider,
		s.cfg.DatabaseLifecycle.SnapshotCloudDestination,
		name,
		description,
	)
}

// Restore populates the configured database's data directory from the
// snapshot at snapshotDir (offline mode: the data directory must not
// already exist, or must be empty), or — if SetLiveNode was called —
// quiesces and restores the bound running node's own data directory
// in-process instead.
func (s *Service) Restore(
	ctx context.Context,
	snapshotDir string,
) (lifecycle.Manifest, error) {
	if s.liveNode != nil {
		return s.liveNode.Restore(ctx, snapshotDir)
	}
	// database.New defaults an empty StorageMode to "core" before it ever
	// opens a store, so s.cfg.StorageMode == "" means "core" too, not "no
	// storage mode configured" -- comparing the raw, possibly-still-empty
	// config value against the manifest's always-defaulted recorded value
	// would otherwise reject a perfectly compatible restore just because
	// the caller's config.Config was never explicitly defaulted.
	storageMode := s.cfg.StorageMode
	if storageMode == "" {
		storageMode = types.StorageModeCore
	}
	// A fresh, single-use plugin host built just for this restore, per
	// lifecycle.RestoreValidated's doc comment on why database/lifecycle
	// never builds one itself.
	host, err := internalplugins.NewHost()
	if err != nil {
		return lifecycle.Manifest{}, fmt.Errorf(
			"build storage plugin host: %w", err,
		)
	}
	defer host.Stop(context.WithoutCancel(ctx)) //nolint:errcheck
	manifest, err := lifecycle.RestoreValidated(
		ctx,
		host,
		s.destinationRegistry,
		snapshotDir,
		s.cfg.DatabasePath,
		func(m lifecycle.Manifest) error {
			if err := m.CheckCompatibility(
				s.cfg.Plugins.Storage.Blob.Provider,
				s.cfg.Plugins.Storage.Metadata.Provider,
				storageMode,
				s.cfg.Network,
			); err != nil {
				return fmt.Errorf(
					"snapshot is not compatible with the configured database: %w",
					err,
				)
			}
			return nil
		},
		lifecycle.RestoreStorageConfig{
			Blob:     s.cfg.Plugins.Storage.Blob.Config,
			Metadata: s.cfg.Plugins.Storage.Metadata.Config,
		},
	)
	if err != nil {
		return lifecycle.Manifest{}, err
	}
	return manifest, nil
}

// TruncateTarget identifies a truncate target by at least one of Slot,
// Hash, or BlockNumber. When more than one is set, ResolveTarget requires
// them to agree on the same block.
type TruncateTarget struct {
	Slot        *uint64
	Hash        []byte
	BlockNumber *uint64
}

// Truncate reverts the configured database to target: target becomes the
// new chain tip and everything after it is removed, per
// database/lifecycle.Truncate (offline mode), or — if SetLiveNode was
// called — quiesces and truncates the bound running node's own database
// in-process instead. Returns the number of blocks removed.
func (s *Service) Truncate(
	ctx context.Context,
	target TruncateTarget,
) (uint64, error) {
	if s.liveNode != nil {
		return s.liveNode.Truncate(ctx, target)
	}

	db, err := s.openDatabase(ctx)
	if err != nil {
		return 0, err
	}
	defer db.Close(ctx)

	if pending, pendingErr := lifecycle.GetPendingTruncate(db.Database); pendingErr != nil {
		return 0, pendingErr
	} else if pending != nil {
		return lifecycle.Truncate(
			ctx,
			db.Database,
			models.Block{},
			0,
			s.cfg.DelegatorInactivityEnabled,
			s.cfg.DelegatorInactivity,
		)
	}

	block, err := ResolveTarget(db.Database, target)
	if err != nil {
		return 0, err
	}
	return lifecycle.Truncate(
		ctx,
		db.Database,
		block,
		0,
		s.cfg.DelegatorInactivityEnabled,
		s.cfg.DelegatorInactivity,
	)
}

// ResolveTarget resolves target against db to a single canonical block. At
// least one of Slot, Hash, or BlockNumber must be set; when more than one
// is set, all supplied fields must agree on the same block. Exported so a
// live (in-process, already-open-node) truncate path can reuse the same
// resolution logic the offline Service.Truncate uses.
//
// Resolution itself always goes through exactly one lookup — hash if
// present (the most specific identifier), else block number, else slot —
// per Resolve*'s existing binary-search-style implementations. Any other
// field the caller also supplied is then checked against the resolved
// block's actual Hash/Slot/Number and rejected on a mismatch, mirroring
// database/lifecycle.Truncate's own onLineage.Slot/Hash cross-check
// (target.ID/Hash/Slot must all genuinely agree before a destructive
// truncate proceeds) rather than silently trusting an unverified
// combination or, at the other extreme, rejecting a caller-supplied,
// mutually-consistent combination outright (e.g. an operator passing both
// a slot and a hash it already resolved, for extra safety).
func ResolveTarget(
	db *database.Database,
	target TruncateTarget,
) (models.Block, error) {
	if target.Slot == nil && target.Hash == nil && target.BlockNumber == nil {
		return models.Block{}, errors.New(
			"at least one of slot, hash, or block number must be given",
		)
	}

	var (
		block models.Block
		err   error
	)
	switch {
	case target.Hash != nil:
		block, err = lifecycle.ResolveTargetByHash(db, target.Hash)
	case target.BlockNumber != nil:
		block, err = lifecycle.ResolveTargetByNumber(db, *target.BlockNumber)
	default:
		block, err = lifecycle.ResolveTargetBySlot(db, *target.Slot)
	}
	if err != nil {
		return models.Block{}, err
	}

	if target.Hash != nil && !bytes.Equal(block.Hash, target.Hash) {
		return models.Block{}, fmt.Errorf(
			"target hash %x does not match the resolved block (hash=%x)",
			target.Hash, block.Hash,
		)
	}
	if target.Slot != nil && block.Slot != *target.Slot {
		return models.Block{}, fmt.Errorf(
			"target slot %d does not match the resolved block (slot=%d)",
			*target.Slot, block.Slot,
		)
	}
	if target.BlockNumber != nil && block.Number != *target.BlockNumber {
		return models.Block{}, fmt.Errorf(
			"target block number %d does not match the resolved block (number=%d)",
			*target.BlockNumber, block.Number,
		)
	}
	return block, nil
}
