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

package indexer

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DatabaseSource is the ChainSource over a ledger database: its tip and its
// primary-chain block index.
type DatabaseSource struct {
	db *database.Database
}

// NewDatabaseSource reads the canonical chain from db.
func NewDatabaseSource(db *database.Database) *DatabaseSource {
	return &DatabaseSource{db: db}
}

// LedgerTip returns the database's durable tip.
func (s *DatabaseSource) LedgerTip(context.Context) (ochainsync.Tip, error) {
	return s.db.GetTip(nil)
}

// FromGenesis reports whether the chain starts at genesis rather than at a
// Mithril-imported ledger state.
func (s *DatabaseSource) FromGenesis(context.Context) (bool, error) {
	slot, err := s.db.MithrilTrustBoundarySlotStrict(nil)
	return slot == 0, err
}

// BlockByHash returns a stored block, canonical or not.
func (s *DatabaseSource) BlockByHash(
	ctx context.Context,
	hash []byte,
) (models.Block, error) {
	return database.BlockByHash(ctx, s.db, hash)
}

// IsCanonical reports whether the block is the primary-chain entry at its
// own index. A block the ledger no longer stores, as after a rollback that
// deleted it, is not canonical.
func (s *DatabaseSource) IsCanonical(
	ctx context.Context,
	hash []byte,
) (bool, error) {
	return database.IsCanonicalBlock(ctx, s.db, hash)
}

// BlocksAfter walks the primary-chain index from the entry after the given
// block. It stops at the limit or the first block above throughSlot, so a
// pass never reads the index to its end.
func (s *DatabaseSource) BlocksAfter(
	ctx context.Context,
	after indexerdb.Tip,
	throughSlot uint64,
	limit int,
) ([]models.Block, error) {
	start := database.BlockInitialIndex
	var previous *ocommon.Point
	if !after.IsZero() {
		blk, err := database.BlockByHash(ctx, s.db, after.Hash)
		if err != nil {
			return nil, fmt.Errorf("locate block to continue after: %w", err)
		}
		start = blk.ID + 1
		previous = &ocommon.Point{Slot: blk.Slot, Hash: blk.Hash}
	}
	it, err := s.db.CanonicalBlocksFromIndex(ctx, start, previous, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]models.Block, 0, limit)
	for len(out) < limit {
		res, err := it.NextRaw()
		if err != nil {
			return nil, err
		}
		if res == nil || res.Slot > throughSlot {
			break
		}
		out = append(out, models.Block{
			ID: res.Index, Slot: res.Slot, Hash: res.Hash,
			PrevHash: res.PrevHash, Cbor: res.Cbor,
			Number: res.Height, Type: res.BlockType,
		})
	}
	return out, nil
}
