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
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/blockpromote"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// immutableGeometry derives the chunk geometry of nodeCfg's network; see
// cardano.CardanoNodeConfig.ImmutableChunkGeometry.
func immutableGeometry(
	nodeCfg *cardano.CardanoNodeConfig,
) (chunkSlots uint64, canContainEBB bool, err error) {
	return nodeCfg.ImmutableChunkGeometry()
}

// validateImmutableDB refuses configurations the ImmutableDB cannot serve.
func validateImmutableDB(cfg Config) error {
	if !cfg.immutableDB.Enabled {
		return nil
	}
	if provider := cfg.pluginSelections[plugin.CapabilityStorageBlob].Provider; provider != "" &&
		provider != "badger" && provider != "s3" && provider != "gcs" {
		return fmt.Errorf(
			"immutableDb.enabled requires the badger, s3 or gcs blob plugin, got %q",
			provider,
		)
	}
	if cfg.historyExpiry.Enabled {
		return errors.New(
			"immutableDb.enabled cannot be combined with historyExpiry.enabled: " +
				"history expiry tombstones block bodies in place, which the " +
				"chunk files do not support",
		)
	}
	return nil
}

// refuseHistoryExpiryOverImmutable refuses history expiry on a database that
// holds an ImmutableDB state record, whatever the configuration says now:
// expiry tombstones block bodies in place, and a promoted block's body lives
// in the chunk files.
func refuseHistoryExpiryOverImmutable(
	cfg Config,
	db *database.Database,
) error {
	if !cfg.historyExpiry.Enabled || db == nil {
		return nil
	}
	if _, ok := db.ImmutableState(); ok {
		return errors.New(
			"historyExpiry.enabled cannot be used on a database with an " +
				"ImmutableDB: block bodies below the immutable tip live in " +
				"chunk files",
		)
	}
	return nil
}

func (n *Node) immutableConfig() database.ImmutableConfig {
	cfg := database.ImmutableConfig{
		Enabled:      n.config.immutableDB.Enabled,
		PromoteBatch: n.config.immutableDB.PromoteBatch,
	}
	if !cfg.Enabled {
		return cfg
	}
	// A failure leaves the geometry zero, which database.New rejects with
	// the invalid-geometry error unless a recorded state supplies it.
	if slots, ebb, err := immutableGeometry(n.config.cardanoNodeConfig); err == nil {
		cfg.ChunkSlots, cfg.CanContainEBB = slots, ebb
	}
	return cfg
}

// immutablePointSource reports the primary-chain point k blocks behind the
// ledger's applied tip.
type immutablePointSource interface {
	ImmutablePoint(ctx context.Context) (ocommon.Point, bool, error)
}

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

func (n *Node) newBlockPromoter() *blockpromote.Promoter {
	freq := n.config.immutableDB.PromoteFrequency
	if freq <= 0 {
		freq = time.Minute
	}
	return blockpromote.New(blockpromote.Config{
		DB:        n.db,
		EventBus:  n.eventBus,
		Logger:    n.config.logger,
		Ceiling:   n.immutableCeiling,
		Frequency: freq,
	})
}
