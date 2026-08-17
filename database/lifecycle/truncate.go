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

package lifecycle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrTruncateNotStarted marks a Truncate failure that occurred entirely
// during read-only validation — before DeleteBlocksAfter made any on-disk
// change — as opposed to a failure during or after it, where a batched
// bulk delete spanning more than one batch may have already partially
// committed. Callers deciding whether it's safe to resume normal service
// after a failed live truncate (rather than treat the data directory as
// possibly inconsistent) should check errors.Is against this.
var ErrTruncateNotStarted = errors.New(
	"truncate: not started, no data was modified",
)

const pendingTruncateSyncKey = "database_lifecycle_truncate_pending"

// PendingTruncate records enough information to resume a truncate whose
// batched blob deletion was interrupted before metadata was truncated.
// Checksum protects every field that controls the resumed delete range. The
// recorded blob tip may already have been deleted when a truncate resumes, so
// a lower current tip is valid partial progress; an equal or newer current tip
// must still agree with the marker.
type PendingTruncate struct {
	TargetID     uint64 `json:"targetId"`
	TargetSlot   uint64 `json:"targetSlot"`
	TargetHash   []byte `json:"targetHash"`
	TipID        uint64 `json:"tipId"`
	TipSlot      uint64 `json:"tipSlot"`
	TipHash      []byte `json:"tipHash"`
	MithrilFloor uint64 `json:"mithrilFloor"`
	Checksum     []byte `json:"checksum"`
}

func pendingTruncateChecksum(pending PendingTruncate) ([]byte, error) {
	pending.Checksum = nil
	value, err := json.Marshal(pending)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(value)
	return sum[:], nil
}

// latestIndexedBlobBlock returns the identity encoded by the highest "bi"
// index entry without resolving the referenced block object. A cloud delete
// is irreversible and removes several objects sequentially, so a failed
// attempt can legitimately leave the highest index pointing at an already
// deleted block object. Recovery must still be able to authenticate that
// index and retry its idempotent cleanup.
func latestIndexedBlobBlock(db *database.Database) (models.Block, error) {
	blob := db.Blob()
	if blob == nil {
		return models.Block{}, types.ErrBlobStoreUnavailable
	}
	txn := db.BlobTxn(false)
	var ret models.Block
	err := txn.Do(func(txn *database.Txn) error {
		blobTxn := txn.Blob()
		if blobTxn == nil {
			return types.ErrNilTxn
		}
		prefix := []byte(types.BlockBlobIndexKeyPrefix)
		it := blob.NewIterator(blobTxn, types.BlobIteratorOptions{
			Reverse: true,
			Prefix:  prefix,
		})
		if it == nil {
			return errors.New("blob iterator is nil")
		}
		defer it.Close()
		if err := it.Err(); err != nil {
			return fmt.Errorf("blob iterator: %w", err)
		}
		seekKey := append(
			append([]byte(nil), prefix...),
			0xff,
		)
		for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item == nil {
				continue
			}
			id, ok := blockIndexID(item.Key())
			if !ok {
				continue
			}
			blockKey, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("read index entry at %d: %w", id, err)
			}
			slot, hash, err := types.ParseBlockBlobKey(blockKey)
			if err != nil {
				return fmt.Errorf("parse index entry at %d: %w", id, err)
			}
			ret = models.Block{ID: id, Slot: slot, Hash: hash}
			return nil
		}
		if err := it.Err(); err != nil {
			return fmt.Errorf("blob iterator: %w", err)
		}
		return models.ErrBlockNotFound
	})
	return ret, err
}

// GetPendingTruncate reports a previously-started truncate that still needs
// completion. Its durable metadata marker prevents a partially committed blob
// deletion from going unnoticed on restart.
func GetPendingTruncate(db *database.Database) (*PendingTruncate, error) {
	value, err := db.GetSyncState(pendingTruncateSyncKey, nil)
	if err != nil {
		return nil, fmt.Errorf("read pending truncate marker: %w", err)
	}
	if value == "" {
		return nil, nil
	}
	var pending PendingTruncate
	if err := json.Unmarshal([]byte(value), &pending); err != nil {
		return nil, fmt.Errorf("decode pending truncate marker: %w", err)
	}
	// A genuine marker always names a real, already-resolved on-lineage
	// block: TargetID/TipID are sequentially assigned block IDs starting
	// at 1, and TargetHash is a real block hash. A partially corrupted
	// marker (e.g. a truncated or bit-flipped sync-state value) can still
	// decode as valid JSON with some fields simply absent -- those decode
	// to Go's zero values (0, nil) rather than a decode error, and would
	// otherwise silently pass the one check this used to have (TipID >=
	// TargetID, trivially true when TargetID is the zero value). Resuming
	// with TargetID=0 would make DeleteBlocksAfter/TruncateAfterSlot
	// delete from the very first block -- a full, unintended chain wipe
	// instead of the narrow range the marker was meant to record.
	if pending.TargetID == 0 || pending.TipID == 0 ||
		len(pending.TargetHash) == 0 || len(pending.TipHash) == 0 ||
		len(pending.Checksum) == 0 {
		return nil, errors.New(
			"invalid pending truncate marker: missing required field",
		)
	}
	expectedChecksum, err := pendingTruncateChecksum(pending)
	if err != nil {
		return nil, fmt.Errorf("checksum pending truncate marker: %w", err)
	}
	if !bytes.Equal(pending.Checksum, expectedChecksum) {
		return nil, errors.New(
			"invalid pending truncate marker: checksum mismatch",
		)
	}
	if pending.TipID < pending.TargetID {
		return nil, errors.New("invalid pending truncate marker")
	}
	// Beyond internally-consistent, non-zero fields, confirm the marker
	// still describes reality: a corrupted-but-non-zero TargetID/
	// TargetSlot/TargetHash combination, or a stale marker left over from
	// before some other operation altered the chain, must not be resumed
	// as if it still safely names the same block. Mirrors Truncate's own
	// on-lineage check for a fresh (non-resumed) target. Safe to check
	// against the blob store specifically for the TARGET (unlike TipID
	// below): target.ID is the exclusive lower bound of the delete range
	// (afterID, tipID], so it is never itself a deletion candidate and is
	// guaranteed to still be present regardless of how far a previous
	// attempt's batched delete got.
	onLineage, err := db.BlockByIndex(pending.TargetID, nil)
	if err != nil {
		return nil, fmt.Errorf(
			"pending truncate marker: verify target is on the current chain: %w",
			err,
		)
	}
	if onLineage.Slot != pending.TargetSlot ||
		!bytes.Equal(onLineage.Hash, pending.TargetHash) {
		return nil, fmt.Errorf(
			"invalid pending truncate marker: target at id=%d (slot=%d, "+
				"hash=%x) does not match the block on the current chain at "+
				"that id (slot=%d, hash=%x)",
			pending.TargetID,
			pending.TargetSlot,
			pending.TargetHash,
			onLineage.Slot,
			onLineage.Hash,
		)
	}

	// The old tip can disappear during a partially completed batched delete,
	// so a current blob tip below pending.TipID is expected recovery state.
	// It must never be above the authenticated upper bound, however: that
	// means blocks were appended after the marker was written. Resuming with
	// pending.TipID would delete only the old range while metadata truncation
	// removes state through the newer tip, orphaning those newer blobs.
	currentTip, err := latestIndexedBlobBlock(db)
	if err != nil {
		return nil, fmt.Errorf(
			"pending truncate marker: get current blob tip: %w",
			err,
		)
	}
	if currentTip.ID > pending.TipID {
		return nil, fmt.Errorf(
			"invalid pending truncate marker: recorded blob tip "+
				"(id=%d, slot=%d, hash=%x) does not match newer current "+
				"blob tip (id=%d, slot=%d, hash=%x)",
			pending.TipID,
			pending.TipSlot,
			pending.TipHash,
			currentTip.ID,
			currentTip.Slot,
			currentTip.Hash,
		)
	}
	if currentTip.ID == pending.TipID &&
		(currentTip.Slot != pending.TipSlot ||
			!bytes.Equal(currentTip.Hash, pending.TipHash)) {
		return nil, fmt.Errorf(
			"invalid pending truncate marker: recorded blob tip "+
				"(id=%d, slot=%d, hash=%x) does not match current blob "+
				"tip (id=%d, slot=%d, hash=%x)",
			pending.TipID,
			pending.TipSlot,
			pending.TipHash,
			currentTip.ID,
			currentTip.Slot,
			currentTip.Hash,
		)
	}
	return &pending, nil
}

func setPendingTruncate(
	db *database.Database,
	pending PendingTruncate,
) error {
	checksum, err := pendingTruncateChecksum(pending)
	if err != nil {
		return fmt.Errorf("checksum pending truncate marker: %w", err)
	}
	pending.Checksum = checksum
	value, err := json.Marshal(pending)
	if err != nil {
		return err
	}
	return db.SetSyncState(pendingTruncateSyncKey, string(value), nil)
}

// ResolveTargetByHash resolves a truncate target identified by block hash.
func ResolveTargetByHash(
	db *database.Database,
	hash []byte,
) (models.Block, error) {
	block, err := database.BlockByHash(db, hash)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by hash: %w", err,
		)
	}
	return block, nil
}

// ResolveTargetBySlot resolves a truncate target as the highest-slot
// block at or before the given slot, against whatever chain the local
// database currently has. Slots without a block of their own (the common
// case — cardano-node's ~20s average slot time means most slots are
// empty) resolve to their nearest ancestor, since an operator invoking a
// disaster-recovery truncate is very unlikely to know a block-populated
// slot exactly and should not have to.
func ResolveTargetBySlot(
	db *database.Database,
	slot uint64,
) (models.Block, error) {
	tip, err := db.GetTip(nil)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by slot: get tip: %w", err,
		)
	}
	tipBlock, err := database.BlockByPoint(db, tip.Point)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by slot: get tip block: %w", err,
		)
	}
	if slot >= tipBlock.Slot {
		return tipBlock, nil
	}
	// Binary search the internal-ID space (chronological order) for the
	// highest ID whose Slot does not exceed the target slot. The ID space
	// is not guaranteed contiguous — Mithril bootstrap/drain can leave
	// gaps of never-imported IDs (see BlockAtOrAfterIndex's doc comment)
	// — so a mid probe uses BlockAtOrAfterIndex, which seeks forward to
	// the next actually-indexed block instead of failing outright,
	// mirroring how Chain's forward iterator already recovers from the
	// same kind of gap (nextPersistentBlockAfterSparseIndex).
	lo, hi := uint64(1), tipBlock.ID
	var best *models.Block
	for lo <= hi {
		mid := lo + (hi-lo)/2
		block, err := db.BlockAtOrAfterIndex(mid, nil)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				// Nothing indexed at or after mid within the whole ID
				// space: the remaining window has no candidate.
				hi = mid - 1
				continue
			}
			return models.Block{}, fmt.Errorf(
				"resolve target by slot: look up block at or after index %d: %w",
				mid,
				err,
			)
		}
		if block.ID > hi {
			// The seek jumped past the current window: [mid, hi] holds no
			// indexed block, so any answer must be below mid.
			hi = mid - 1
			continue
		}
		if block.Slot <= slot {
			best = &block
			// block.ID, not mid+1: block.ID may be > mid if mid landed in
			// a gap, and re-probing anywhere in (mid, block.ID) would
			// just re-find this same block again.
			lo = block.ID + 1
		} else {
			hi = mid - 1
		}
	}
	if best == nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by slot: no block found at or before slot %d",
			slot,
		)
	}
	return *best, nil
}

// ResolveTargetByNumber resolves a truncate target identified by chain
// block number (height). Block numbers are not directly indexed in the
// blob store (only slot, hash, and internal sequential ID are), so this
// binary-searches the contiguous internal-ID space bounded by the current
// tip, comparing each candidate's Number field, mirroring the technique
// Chain.BlockBeforeSlot uses for slot-ordered lookups.
func ResolveTargetByNumber(
	db *database.Database,
	number uint64,
) (models.Block, error) {
	tip, err := db.GetTip(nil)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by number: get tip: %w", err,
		)
	}
	tipBlock, err := database.BlockByPoint(db, tip.Point)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by number: get tip block: %w", err,
		)
	}
	if number > tipBlock.Number {
		return models.Block{}, fmt.Errorf(
			"resolve target by number: block number %d is ahead of tip (%d)",
			number,
			tipBlock.Number,
		)
	}
	// See ResolveTargetBySlot's doc comment for why BlockAtOrAfterIndex,
	// not BlockByIndex, is used here: a mid probe landing on a gap in a
	// sparse (Mithril bootstrap/drain-imported) ID space must seek
	// forward to the next actually-indexed block instead of failing.
	lo, hi := uint64(1), tipBlock.ID
	for lo <= hi {
		mid := lo + (hi-lo)/2
		block, err := db.BlockAtOrAfterIndex(mid, nil)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				hi = mid - 1
				continue
			}
			return models.Block{}, fmt.Errorf(
				"resolve target by number: look up block at or after index %d: %w",
				mid,
				err,
			)
		}
		if block.ID > hi {
			hi = mid - 1
			continue
		}
		switch {
		case block.Number == number:
			return block, nil
		case block.Number < number:
			lo = block.ID + 1
		default:
			hi = mid - 1
		}
	}
	return models.Block{}, fmt.Errorf(
		"resolve target by number: no block found with number %d",
		number,
	)
}

// Truncate reverts the database to target: target becomes the new chain
// tip, every block with a strictly greater internal ID is removed from
// the blob store, and every metadata row (and blob-referenced UTxO/tx
// CBOR) added after target's slot is removed or restored to its
// pre-target state via database.TruncateAfterSlot.
//
// Unlike Chain.Rollback, this does not reject a target beyond the
// configured security parameter — that guard protects automatic rollback
// during normal sync; an operator explicitly invoking Truncate (e.g. for
// CIP-0135 disaster recovery from a long network partition) is the
// informed-consent replacement for it. It still refuses to truncate to a
// point before the Mithril trust boundary, if one is recorded: that
// boundary reflects what UTxO history is actually available locally, not
// a policy choice, and going below it would leave the database unable to
// validate the first block past the (now missing) boundary.
//
// This is an offline operation in the sense that it performs no chain-
// manager or in-memory ledger-state bookkeeping — it is safe to call
// against a database not concurrently owned by a live Chain/LedgerState
// (the offline CLI path, or the live path after quiescing the node).
//
// blocksRemoved is the number of blocks DeleteBlocksAfter actually found
// and deleted in (target.ID, tipBlock.ID] — not simply tipBlock.ID -
// target.ID, since that range is only an upper bound: a chain
// bootstrapped/drained from a Mithril snapshot can leave gaps of
// never-imported IDs in it (see DeleteBlocksAfter's own doc comment), and
// subtracting index values there would wildly overcount how many blocks
// actually existed to remove.
//
// DeleteBlocksAfter deletes blob-store blocks by ID range, while
// database.TruncateAfterSlot deletes metadata by slot cutoff; these agree
// for any normal chain (slots strictly increase with ID) except same-slot
// blocks — notably Byron epoch boundary blocks — where a later block
// sharing target's own slot would be removed from the blob store (ID >
// target.ID) but retained in metadata (slot not > target.Slot),
// diverging the two. Truncate refuses such a target outright (see the
// same-slot check above) rather than let that divergence happen.
func Truncate(
	ctx context.Context,
	db *database.Database,
	target models.Block,
	batchSize int,
	delegatorInactivityEnabled bool,
	delegatorInactivity uint64,
) (blocksRemoved uint64, err error) {
	if pending, err := GetPendingTruncate(db); err != nil {
		return 0, fmt.Errorf("truncate: %w", err)
	} else if pending != nil {
		return finishPendingTruncate(
			ctx,
			db,
			*pending,
			batchSize,
			delegatorInactivityEnabled,
			delegatorInactivity,
		)
	}

	metadataTip, err := db.GetTip(nil)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: get metadata tip: %w",
			ErrTruncateNotStarted,
			err,
		)
	}
	metadataTipBlock, err := database.BlockByPoint(db, metadataTip.Point)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: get metadata tip block: %w",
			ErrTruncateNotStarted,
			err,
		)
	}
	if target.ID > metadataTipBlock.ID {
		return 0, fmt.Errorf(
			"%w: target block (id=%d, slot=%d) is ahead of metadata tip (id=%d, slot=%d)",
			ErrTruncateNotStarted,
			target.ID,
			target.Slot,
			metadataTipBlock.ID,
			metadataTipBlock.Slot,
		)
	}

	recentBlocks, err := database.BlocksRecent(db, 1)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: get indexed blob tip: %w",
			ErrTruncateNotStarted,
			err,
		)
	}
	if len(recentBlocks) == 0 {
		return 0, fmt.Errorf(
			"%w: get indexed blob tip: %w",
			ErrTruncateNotStarted,
			models.ErrBlockNotFound,
		)
	}
	tipBlock := recentBlocks[0]

	// Confirm target is genuinely the block occupying its ID on the
	// current genesis-to-tip lineage, not just numerically within range.
	// ResolveTargetBySlot/ResolveTargetByNumber get this for free (they
	// binary-search the same contiguous ID space this reads), but
	// ResolveTargetByHash resolves purely through the hash index and has
	// no such structural guarantee -- a hash (and slot) lookup and an ID
	// lookup agreeing here is what's actually being relied on. This must
	// run before the target.ID == tipBlock.ID no-op check below: a caller
	// can pass a target whose ID happens to equal the current tip's but
	// whose Slot/Hash are stale or malformed, and without this check that
	// would report success without ever having verified target actually
	// names the current tip. DeleteBlocksAfter below deletes blob-store
	// blocks by ID range, while TruncateAfterSlot deletes metadata by
	// target.Slot as the cutoff; the two only describe the same rollback
	// when target's ID, Hash, AND Slot all genuinely match the same
	// on-lineage block -- checking Hash alone leaves Slot unverified, and
	// since TruncateAfterSlot trusts Slot directly (not ID), a target with
	// a valid ID/Hash pair but a forged or otherwise-wrong Slot would
	// still pass a hash-only check, then cut blob and metadata history at
	// two different points, silently diverging them instead of failing
	// closed the same way a hash mismatch does.
	onLineage, err := db.BlockByIndex(target.ID, nil)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: verify target is on the current chain: %w",
			ErrTruncateNotStarted,
			err,
		)
	}
	if onLineage.Slot != target.Slot ||
		!bytes.Equal(onLineage.Hash, target.Hash) {
		return 0, fmt.Errorf(
			"%w: target at id=%d (slot=%d, hash=%x) does not match the "+
				"block on the current chain at that id (slot=%d, hash=%x) "+
				"-- target is not an ancestor of the current tip",
			ErrTruncateNotStarted,
			target.ID,
			target.Slot,
			target.Hash,
			onLineage.Slot,
			onLineage.Hash,
		)
	}

	if target.ID == tipBlock.ID {
		return 0, nil
	}

	// Reject, rather than silently diverge, a target immediately followed
	// by a later block sharing its exact slot (Byron epoch-boundary blocks
	// are the only case this occurs in practice): DeleteBlocksAfter below
	// deletes blob-store blocks by ID range, so that later same-slot block
	// would be removed from the blob store, but database.TruncateAfterSlot
	// deletes metadata by slot cutoff and would keep its metadata rows,
	// since they share target's own slot -- leaving transactions,
	// certificates, UTxOs, and other metadata referencing a block no
	// longer present in the blob store. BlockAtOrAfterIndex (not
	// BlockByIndex) seeks past any never-imported ID gap to the next
	// actually-indexed block, matching ResolveTargetBySlot/ByNumber above;
	// a gap here means nothing was imported for the chronologically next
	// block either, so there is no same-slot metadata left to diverge.
	nextBlock, err := db.BlockAtOrAfterIndex(target.ID+1, nil)
	if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
		return 0, fmt.Errorf(
			"%w: check for a same-slot successor block: %w",
			ErrTruncateNotStarted,
			err,
		)
	}
	if err == nil && nextBlock.Slot == target.Slot {
		return 0, fmt.Errorf(
			"%w: target at id=%d (slot=%d) is immediately followed by "+
				"another block (id=%d) at the exact same slot -- truncating "+
				"here would remove that block from the blob store while "+
				"keeping its metadata, diverging the two; choose a "+
				"different target",
			ErrTruncateNotStarted,
			target.ID,
			target.Slot,
			nextBlock.ID,
		)
	}

	// MithrilTrustBoundarySlotStrict, not MithrilTrustBoundarySlot: this
	// check exists to refuse a truncate that would leave the database
	// unable to validate the first block past the boundary, so a failed
	// read must fail the truncate closed rather than being silently
	// treated as "no boundary recorded" and letting an unverifiable
	// truncate through.
	mithrilFloor, err := db.MithrilTrustBoundarySlotStrict(nil)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: could not verify Mithril trust boundary: %w",
			ErrTruncateNotStarted,
			err,
		)
	}
	if mithrilFloor > 0 && target.Slot < mithrilFloor {
		return 0, fmt.Errorf(
			"%w: target slot %d is before the Mithril trust boundary (%d); "+
				"the local database does not have complete history before that point",
			ErrTruncateNotStarted,
			target.Slot,
			mithrilFloor,
		)
	}

	// A context already cancelled before any mutation is attempted must be
	// reported the same way every other pre-mutation validation failure
	// above is: ErrTruncateNotStarted-wrapped. Without this check here,
	// nothing above ever consults ctx, so a pre-cancelled caller would
	// still get a durable pending marker recorded (setPendingTruncate
	// below) and then fail inside finishPendingTruncate's first
	// DeleteBlocksAfter call -- a path that deliberately does NOT wrap
	// ErrTruncateNotStarted, since it can genuinely follow a partial
	// delete. That would misreport "truncate may have partially run,
	// resume it" for a call that in fact never touched the database at
	// all, and leave an unnecessary durable marker forcing a resume of
	// what was really a no-op.
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrTruncateNotStarted, err)
	}

	pending := PendingTruncate{
		TargetID:     target.ID,
		TargetSlot:   target.Slot,
		TargetHash:   append([]byte(nil), target.Hash...),
		TipID:        tipBlock.ID,
		TipSlot:      tipBlock.Slot,
		TipHash:      append([]byte(nil), tipBlock.Hash...),
		MithrilFloor: mithrilFloor,
	}
	if err := setPendingTruncate(db, pending); err != nil {
		return 0, fmt.Errorf(
			"%w: record pending truncate: %w",
			ErrTruncateNotStarted,
			err,
		)
	}
	return finishPendingTruncate(
		ctx,
		db,
		pending,
		batchSize,
		delegatorInactivityEnabled,
		delegatorInactivity,
	)
}

func finishPendingTruncate(
	ctx context.Context,
	db *database.Database,
	pending PendingTruncate,
	batchSize int,
	delegatorInactivityEnabled bool,
	delegatorInactivity uint64,
) (uint64, error) {
	// Everything above this point is read-only validation; DeleteBlocksAfter
	// is where on-disk mutation actually begins (and, for a truncate
	// spanning more than one delete batch, may partially commit before an
	// error or context cancellation is noticed) — so an error from here on
	// is deliberately NOT wrapped in ErrTruncateNotStarted, unlike the
	// validation failures above.
	blocksDeleted, err := DeleteBlocksAfter(
		ctx,
		db,
		pending.TargetID,
		pending.TipID,
		batchSize,
	)
	if err != nil {
		return 0, fmt.Errorf("truncate: delete blocks after target: %w", err)
	}

	point := ocommon.Point{
		Slot: pending.TargetSlot,
		Hash: pending.TargetHash,
	}
	// CIP-0163: collect the reward-account credentials witnessed in the
	// truncated-away blocks (added_slot > target.Slot) before
	// TruncateAfterSlot's certificate/reward-withdrawal deletes remove
	// their rows, then recompute their expiration_epoch against the
	// surviving chain once TruncateAfterSlot has restored account state.
	// Both must run in the same write transaction as TruncateAfterSlot --
	// this is the exact same CIP-0163 bookkeeping
	// ledger.LedgerState.rollback applies for a normal (security-
	// parameter-bounded) rollback; skipping it here would leave a
	// CIP-0163-enabled network with incorrect stake/reward/DRep
	// calculations after any offline or live truncate, since rolled-away
	// activity could leave expiration_epoch renewed past what the
	// surviving chain actually witnessed.
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		var affectedRefs []models.StakeCredentialRef
		if delegatorInactivityEnabled {
			var affErr error
			affectedRefs, affErr = db.AccountsWitnessedAfterSlot(
				point.Slot,
				txn,
			)
			if affErr != nil {
				return fmt.Errorf(
					"collect truncated-away witnessed accounts: %w",
					affErr,
				)
			}
		}
		if _, _, err := db.TruncateAfterSlot(
			point,
			pending.MithrilFloor,
			txn,
		); err != nil {
			return fmt.Errorf("truncate metadata: %w", err)
		}
		if err := database.RecomputeAccountExpirationsAfterTruncate(
			db,
			txn,
			delegatorInactivityEnabled,
			delegatorInactivity,
			point.Slot,
			affectedRefs,
		); err != nil {
			return fmt.Errorf(
				"recompute account expirations after truncate: %w",
				err,
			)
		}
		if err := db.DeleteSyncState(pendingTruncateSyncKey, txn); err != nil {
			return fmt.Errorf("clear pending truncate marker: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("truncate: %w", err)
	}
	return blocksDeleted, nil
}
