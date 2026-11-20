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
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/node"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrLedgerStateTooRecent reports a ledger state whose point is later than
// the ImmutableDB tip, or is not a block the ImmutableDB holds. Such a state
// is discarded, as the reference node discards a ledger snapshot more recent
// than its ImmutableDB tip.
var ErrLedgerStateTooRecent = errors.New(
	"ledger state is more recent than the ImmutableDB tip",
)

// errAncillaryChunkOffChain reports a signed ancillary chunk whose blocks do
// not continue the stored chain.
var errAncillaryChunkOffChain = errors.New(
	"signed ancillary chunk does not continue the stored chain",
)

// readAncillaryChunk returns the blocks of the immutable file after
// certified, which the aggregator packs into the ancillary archive because
// its node was still appending to it when the snapshot was cut. The file's
// chunk, primary and secondary are read against their digests in the signed
// ancillary manifest; a file the manifest does not cover is never read.
//
// It returns no blocks when the tree was not verified against a signed
// manifest or the manifest does not cover all three files. The chunk is
// copied while the node writes to it, so its tail can stop inside a block:
// blocks are returned up to the first one that does not read back, as the
// reference node truncates an unfinished chunk when it opens it.
func readAncillaryChunk(
	logger *slog.Logger,
	result *BootstrapResult,
	certified uint64,
) ([]immutable.Block, error) {
	if result == nil || result.AncillaryRoot == nil ||
		!result.AncillaryVerified {
		return nil, nil
	}
	num := certified + 1
	digests := make(map[string]string, len(immutableFileExtensions))
	for _, ext := range immutableFileExtensions {
		name := fmt.Sprintf("%05d.%s", num, ext)
		digest, ok := result.AncillaryDigests["immutable/"+name]
		if !ok {
			return nil, nil
		}
		digests[name] = digest
	}
	root, err := result.AncillaryRoot.OpenRoot("immutable")
	if err != nil {
		return nil, fmt.Errorf("opening signed ancillary chunk: %w", err)
	}
	defer root.Close()
	imm, err := immutable.NewFromRootVerified(root, digests, "")
	if err != nil {
		return nil, fmt.Errorf("opening signed ancillary chunk: %w", err)
	}
	iter, err := imm.BlocksInChunk(num)
	if err != nil {
		return nil, fmt.Errorf("reading signed ancillary chunk: %w", err)
	}
	defer iter.Close()
	var blocks []immutable.Block
	for {
		block, err := iter.Next()
		if errors.Is(err, immutable.ErrDigestMismatch) {
			return nil, fmt.Errorf(
				"reading signed ancillary chunk %d: %w", num, err,
			)
		}
		if err != nil {
			logger.Warn(
				"signed ancillary chunk ends inside a block; keeping "+
					"the blocks before it",
				"component", "mithril",
				"chunk", num,
				"blocks", len(blocks),
				"error", err,
			)
			break
		}
		if block == nil {
			break
		}
		blocks = append(blocks, *block)
	}
	return blocks, nil
}

// ancillaryTip returns the point of the last block of blocks, or certified
// when there are none: the ImmutableDB tip an import of both reaches.
func ancillaryTip(
	certified ocommon.Point,
	blocks []immutable.Block,
) ocommon.Point {
	if len(blocks) == 0 {
		return certified
	}
	last := blocks[len(blocks)-1]
	return ocommon.NewPoint(last.Slot, last.Hash)
}

// landAncillaryChunk stores the signed ancillary chunk's blocks after the
// certified ones, exactly as the certified copy stores its own. Blocks the
// chain already holds are not stored again. It returns the number of blocks
// stored.
//
// When the database records an ImmutableDB tip, every block through the
// signed chunk's last one is then appended to the chunk files through the
// database's own chunk writer, certified bodies an import copied included, so
// the recorded tip is the signed chunk's last block before any ledger state is
// loaded. Left as bodies, they would sit above the recorded tip, where the
// chain accepts a rollback that the loaded ledger state cannot follow.
//
// The chain refuses a block that does not extend its tip, so a chunk that
// does not continue the certified chain stores nothing and is an error. The
// caller holds Database.LockImmutable.
func landAncillaryChunk(
	ctx context.Context,
	db *database.Database,
	blocks []immutable.Block,
) (int, error) {
	if len(blocks) == 0 {
		return 0, nil
	}
	cm, err := chain.NewManager(ctx, db, nil)
	if err != nil {
		return 0, fmt.Errorf("loading chain manager: %w", err)
	}
	c := cm.PrimaryChain()
	if c == nil {
		return 0, errors.New("primary chain not available")
	}
	tip := c.Tip().Point
	skip := 0
	for skip < len(blocks) && blocks[skip].Slot <= tip.Slot {
		skip++
	}
	stored, err := node.CopyImmutableBlocks(ctx, c, blocks[skip:])
	if err != nil {
		return stored, fmt.Errorf(
			"%w: %w", errAncillaryChunkOffChain, err,
		)
	}
	last := ancillaryTip(ocommon.Point{}, blocks)
	block, err := database.BlockByPoint(ctx, db, last)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return stored, fmt.Errorf(
				"%w: block %d.%x is not stored",
				errAncillaryChunkOffChain, last.Slot, last.Hash,
			)
		}
		return stored, fmt.Errorf("reading signed ancillary tip: %w", err)
	}
	recent, err := database.BlocksRecent(ctx, db, 1)
	if err != nil {
		return stored, fmt.Errorf("reading chain tip: %w", err)
	}
	if len(recent) == 0 {
		return stored, errAncillaryChunkOffChain
	}
	onChain, err := localChainDescendsFromPoint(ctx, db, recent[0], last)
	if err != nil {
		return stored, err
	}
	if !onChain {
		return stored, fmt.Errorf(
			"%w: chain tip %d.%x does not descend from block %d.%x",
			errAncillaryChunkOffChain,
			recent[0].Slot, recent[0].Hash, last.Slot, last.Hash,
		)
	}
	if st, ok := db.ImmutableState(); ok && st.HasTip &&
		st.TipIndex < block.ID {
		if _, err := db.PromoteThroughLocked(ctx, block.ID); err != nil {
			return stored, fmt.Errorf(
				"appending signed ancillary blocks to the ImmutableDB: %w",
				err,
			)
		}
	}
	return stored, nil
}

// immutableBoundSlot returns limit, or the slot of the tip the database's
// ImmutableDB records when that is lower: the latest slot a ledger state may
// be loaded at.
func immutableBoundSlot(db *database.Database, limit uint64) uint64 {
	if own, ok := db.ImmutablePoint(); ok && own.Slot < limit {
		return own.Slot
	}
	return limit
}

// requireStateInImmutableDB returns ErrLedgerStateTooRecent unless state is a
// stored block at or below immutableTip, the last block an import stored from
// the certified immutable files and the signed ancillary chunk, and at or
// below the tip the database's ImmutableDB records when it keeps one.
func requireStateInImmutableDB(
	ctx context.Context,
	db *database.Database,
	immutableTip ocommon.Point,
	state ocommon.Point,
) error {
	if bound := immutableBoundSlot(db, immutableTip.Slot); state.Slot > bound {
		return fmt.Errorf(
			"%w: state slot %d, ImmutableDB tip slot %d",
			ErrLedgerStateTooRecent, state.Slot, bound,
		)
	}
	if _, err := database.BlockByPoint(ctx, db, state); err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return fmt.Errorf(
				"%w: block %d.%x is not in the ImmutableDB",
				ErrLedgerStateTooRecent, state.Slot, state.Hash,
			)
		}
		return fmt.Errorf("reading ledger state block: %w", err)
	}
	return nil
}
