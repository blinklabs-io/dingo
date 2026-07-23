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
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/version"
)

// LiveNode is satisfied by a running *dingo.Node (see node_lifecycle.go's
// Restore/Truncate methods there — this interface is defined here, not
// imported from the dingo package, specifically so dblifecycle does not
// need to import the root dingo package, which already imports
// dblifecycle for Manager; dingo.Node satisfies this interface
// structurally, with no additional wiring on its side.
type LiveNode interface {
	Snapshot(ctx context.Context, destDir string) (lifecycle.Manifest, error)
	Restore(ctx context.Context, snapshotDir string) (lifecycle.Manifest, error)
	Truncate(ctx context.Context, target TruncateTarget) (blocksRemoved uint64, err error)
}

// Service performs database lifecycle operations against the data
// directory and plugins named in cfg. If bound to a LiveNode (SetLiveNode),
// Restore/Truncate delegate to it instead of operating offline — see
// SetLiveNode's doc comment for what that changes.
type Service struct {
	cfg      *config.Config
	logger   *slog.Logger
	liveNode LiveNode
}

// NewService creates a Service bound to cfg's database configuration
// (DatabasePath, BlobPlugin, MetadataPlugin, StorageMode, Network).
func NewService(cfg *config.Config, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{cfg: cfg, logger: logger}
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
func (s *Service) openDatabase() (*database.Database, error) {
	db, err := database.New(&database.Config{
		DataDir:        s.cfg.DatabasePath,
		Logger:         s.logger,
		BlobPlugin:     s.cfg.BlobPlugin,
		MetadataPlugin: s.cfg.MetadataPlugin,
		MaxConnections: s.cfg.DatabaseWorkers,
		StorageMode:    s.cfg.StorageMode,
		Network:        s.cfg.Network,
	})
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		return nil, fmt.Errorf("open database: %w", err)
	}
	return db, nil
}

// Snapshot captures a point-in-time backup of the configured database
// into destDir, which must not already exist, or — if SetLiveNode was
// called — captures it from the bound running node's own already-open
// database instead of opening a second, competing handle on the same
// data directory.
func (s *Service) Snapshot(
	ctx context.Context,
	destDir string,
) (lifecycle.Manifest, error) {
	if s.liveNode != nil {
		return s.liveNode.Snapshot(ctx, destDir)
	}
	db, err := s.openDatabase()
	if err != nil {
		return lifecycle.Manifest{}, err
	}
	defer db.Close()
	return lifecycle.SnapshotToCloud(
		ctx,
		db,
		destDir,
		lifecycle.TriggerManual,
		version.GetVersionString(),
		s.cfg.DatabaseLifecycle.SnapshotCloudDestination,
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
	return lifecycle.Restore(ctx, snapshotDir, s.cfg.DatabasePath)
}

// TruncateTarget identifies a truncate target by exactly one of Slot,
// Hash, or BlockNumber.
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

	db, err := s.openDatabase()
	if err != nil {
		return 0, err
	}
	defer db.Close()

	block, err := ResolveTarget(db, target)
	if err != nil {
		return 0, err
	}
	return lifecycle.Truncate(ctx, db, block, 0)
}

// ResolveTarget resolves target against db to a single canonical block,
// requiring exactly one of Slot, Hash, or BlockNumber to be set. Exported
// so a live (in-process, already-open-node) truncate path can reuse the
// same resolution logic the offline Service.Truncate uses.
func ResolveTarget(
	db *database.Database,
	target TruncateTarget,
) (models.Block, error) {
	set := 0
	if target.Slot != nil {
		set++
	}
	if target.Hash != nil {
		set++
	}
	if target.BlockNumber != nil {
		set++
	}
	if set != 1 {
		return models.Block{}, errors.New(
			"exactly one of slot, hash, or block number must be given",
		)
	}
	switch {
	case target.Hash != nil:
		return lifecycle.ResolveTargetByHash(db, target.Hash)
	case target.BlockNumber != nil:
		return lifecycle.ResolveTargetByNumber(db, *target.BlockNumber)
	default:
		return lifecycle.ResolveTargetBySlot(db, *target.Slot)
	}
}
