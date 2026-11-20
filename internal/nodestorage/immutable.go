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
	"errors"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
)

// ImmutableConfig is the ImmutableDB configuration a database is opened with.
// A database that expires history tombstones block bodies in place in the
// blob store instead, and a chunk file cannot hold a tombstone, so history
// expiry turns the ImmutableDB off.
func ImmutableConfig(
	historyExpiry bool,
	nodeCfg *cardano.CardanoNodeConfig,
	promoteBatch int,
) database.ImmutableConfig {
	cfg := database.ImmutableConfig{
		Enabled:      !historyExpiry,
		PromoteBatch: promoteBatch,
	}
	if !cfg.Enabled {
		return cfg
	}
	// A failure leaves the geometry zero, which database.New rejects with
	// the invalid-geometry error unless a recorded state supplies it.
	if slots, ebb, err := nodeCfg.ImmutableChunkGeometry(); err == nil {
		cfg.ChunkSlots, cfg.CanContainEBB = slots, ebb
	}
	return cfg
}

// RefuseHistoryExpiryOverImmutable refuses history expiry on a database that
// holds an ImmutableDB state record: the bodies of its promoted blocks live in
// chunk files, which expiry cannot tombstone.
func RefuseHistoryExpiryOverImmutable(
	historyExpiry bool,
	db *database.Database,
) error {
	if !historyExpiry || db == nil {
		return nil
	}
	if _, ok := db.ImmutableState(); ok {
		return errors.New(
			"historyExpiry.enabled cannot be used on a database whose block " +
				"bodies were moved into the ImmutableDB chunk files: turn " +
				"historyExpiry off, or start from a new database with it on",
		)
	}
	return nil
}
