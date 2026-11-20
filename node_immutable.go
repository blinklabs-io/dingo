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

package dingo

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/blockpromote"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
)

// immutableEnabled reports whether block bodies move into the ImmutableDB.
// A node that expires history tombstones bodies in place in the blob store
// instead, and a chunk file cannot hold a tombstone, so the two never mix.
func (n *Node) immutableEnabled() bool {
	return !n.config.historyExpiry.Enabled
}

// refuseHistoryExpiryOverImmutable refuses history expiry on a database that
// holds an ImmutableDB state record: the bodies of its promoted blocks live in
// chunk files, which expiry cannot tombstone.
func refuseHistoryExpiryOverImmutable(
	cfg Config,
	db *database.Database,
) error {
	return nodestorage.RefuseHistoryExpiryOverImmutable(
		cfg.historyExpiry.Enabled, db,
	)
}

func (n *Node) immutableConfig() database.ImmutableConfig {
	return nodestorage.ImmutableConfig(
		n.config.historyExpiry.Enabled,
		n.config.cardanoNodeConfig,
		n.config.immutableDB.PromoteBatch,
	)
}

type immutablePointSource = nodestorage.ImmutablePointSource

// immutableCeiling is the highest block index that may leave the VolatileDB:
// the ledger's immutable point, the same point the live UTxO set is anchored
// at, so the chunk files never run ahead of the set's rollback bound.
func (n *Node) immutableCeiling(
	ctx context.Context,
) (uint64, bool, error) {
	if n.ledgerState == nil {
		return 0, false, nil
	}
	return immutableCeilingFor(ctx, n.db, n.ledgerState)
}

// immutableCeilingFor resolves the ceiling from the immutable point of src. The
// chain may be ahead of the ledger while it catches up, and a block the ledger
// has not applied can still be rolled back, so the chain tip is never
// consulted. A ledger without a primary chain or security parameter permits
// nothing.
func immutableCeilingFor(
	ctx context.Context,
	db *database.Database,
	src immutablePointSource,
) (uint64, bool, error) {
	point, found, err := src.ImmutablePoint(ctx)
	if err != nil {
		if errors.Is(err, ledger.ErrImmutablePointUnconfigured) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if !found || (point.Slot == 0 && len(point.Hash) == 0) {
		return 0, false, nil
	}
	txn := db.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	id, err := database.BlockIDByPointMetadataTxn(txn, point)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return id, id > 0, ctx.Err()
}

// immutableSpaceMargin is free space required beyond the block bodies a
// promotion step copies.
const immutableSpaceMargin = 64 << 20

// immutableSpaceCheck refuses a promotion step the data directory's volume
// cannot hold. A step copies block bodies into chunk files before the blob
// store reclaims them, so the bound is the blob store's size scaled by the
// share of its blocks the step promotes: the whole store when a database
// first moves its block bodies, a margin once the ImmutableDB keeps up.
func (n *Node) immutableSpaceCheck(
	ctx context.Context,
	ceiling uint64,
) error {
	// A node without a data directory keeps its blob store in memory and
	// its chunk files in a temporary directory; there is no volume to size.
	if n.config.dataDir == "" {
		return nil
	}
	var done uint64
	if st, ok := n.db.ImmutableState(); ok {
		done = st.TipIndex
	}
	if ceiling <= done {
		return nil
	}
	pending := ceiling - done
	need := uint64(immutableSpaceMargin)
	if pending > uint64(max(n.config.immutableDB.PromoteBatch, 1)) {
		blobBytes, err := dirBytes(ctx, filepath.Join(n.config.dataDir, "blob"))
		if err != nil {
			return fmt.Errorf("measure the blob store: %w", err)
		}
		// The share is computed in floating point: a block count times a
		// byte count overflows uint64 only on a chain far beyond any real one.
		need += uint64(float64(blobBytes) * float64(pending) / float64(ceiling))
		n.config.logger.Info(
			"moving block bodies into the ImmutableDB",
			"component", "node",
			"blocks_pending", pending,
			"space_needed_bytes", need,
		)
	}
	available := n.availableDiskBytes
	if available == nil {
		available = utxoset.AvailableDiskBytes
	}
	free, err := available(n.config.dataDir)
	if err != nil {
		return fmt.Errorf("check free disk space: %w", err)
	}
	if free < need {
		return fmt.Errorf(
			"not enough free disk space to move %d blocks into the "+
				"ImmutableDB: %d bytes needed and %d available in %s",
			pending, need, free, n.config.dataDir,
		)
	}
	return nil
}

// dirBytes sums the disk the files under dir occupy.
func dirBytes(ctx context.Context, dir string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(
		dir,
		func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if d.Type().IsRegular() {
				info, err := d.Info()
				if err != nil {
					return err
				}
				total += allocatedBytes(info)
			}
			return nil
		},
	)
	return total, err
}

func (n *Node) newBlockPromoter() *blockpromote.Promoter {
	freq := n.config.immutableDB.PromoteFrequency
	if freq <= 0 {
		freq = time.Minute
	}
	return blockpromote.New(blockpromote.Config{
		DB:         n.db,
		EventBus:   n.eventBus,
		Logger:     n.config.logger,
		Ceiling:    n.immutableCeiling,
		CheckSpace: n.immutableSpaceCheck,
		Frequency:  freq,
	})
}
