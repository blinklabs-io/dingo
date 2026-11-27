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

package database

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// A rollback that has to tell subscribers which transactions it undid names
// the blocks it removes in a rollback intent, and the block store keeps their
// bodies until that intent clears: removing a block from the primary chain
// moves its body under types.RolledBackBlockKeyPrefix in the transaction that
// drops it from the chain, and ReleaseRolledBackBlocks deletes the moved
// bodies once no intent names them. A rolled-back body is therefore never
// reachable by point, by hash or by index, which is what the rest of the node
// takes chain membership to mean.

// RollbackIntentKey names the chain state record of a rollback whose undo
// notification has not been delivered to completion.
const RollbackIntentKey = "ledger.rollback.pending"

const (
	rollbackIntentVersion = 2

	// MaxRollbackIntentBlocks bounds the blocks one intent names.
	MaxRollbackIntentBlocks = 4096
	// maxRollbackIntentTxRanges bounds the endorser transaction ranges of
	// all blocks in one intent.
	maxRollbackIntentTxRanges = 1 << 16
	// maxRollbackIntentEndorserTxs bounds the endorser transactions those
	// ranges cover, which is what reading an intent expands them into.
	maxRollbackIntentEndorserTxs = 1 << 20
	// maxRollbackIntentEncodedBytes bounds a stored record before it is
	// decoded. A record at both limits above encodes to less than half of
	// it, and it is itself well under what one block store transaction
	// accepts.
	maxRollbackIntentEncodedBytes = 4 << 20
	// maxRollbackIntentHashBytes bounds each hash a record carries and each
	// hash a rolled-back block key is built from.
	maxRollbackIntentHashBytes = 64

	// rolledBackBlockReleaseBatch bounds the keys one release transaction
	// deletes.
	rolledBackBlockReleaseBatch = 1000
)

var (
	// ErrInvalidRollbackIntent reports a stored rollback intent that cannot
	// be read.
	ErrInvalidRollbackIntent = errors.New("invalid rollback intent")
	// ErrRollbackIntentTooLarge reports a rollback that names more than one
	// intent may hold.
	ErrRollbackIntentTooLarge = errors.New(
		"rollback intent exceeds durable limit",
	)
)

// RollbackIntent is a rollback to Point and the blocks it removes, newest
// first.
type RollbackIntent struct {
	Point  ocommon.Point
	Blocks []RollbackIntentBlock
}

// RollbackIntentBlock names one rolled-back block. Its body is read from the
// block store by Slot and Hash.
type RollbackIntentBlock struct {
	Hash     []byte
	PrevHash []byte
	// EndorserHash and EndorserSlot name the endorser block this ranking
	// block applied transactions from, and EndorserTxs which of them, in
	// application order. The transactions are read from the Leios store.
	EndorserHash []byte
	EndorserTxs  []RollbackIntentTxRange
	EndorserSlot uint64
	ID           uint64
	Slot         uint64
	Number       uint64
	Type         uint
}

// RollbackIntentTxRange is Count consecutive positions in an endorser block
// starting at Start.
type RollbackIntentTxRange struct {
	Start uint32
	Count uint32
}

type rollbackIntentRecord struct {
	Slot    *uint64                     `json:"slot"`
	Hash    *string                     `json:"hash"`
	Blocks  []rollbackIntentRecordBlock `json:"blocks"`
	Version int                         `json:"format_version"`
}

type rollbackIntentRecordBlock struct {
	Hash         []byte      `json:"hash"`
	PrevHash     []byte      `json:"prev_hash"`
	EndorserHash []byte      `json:"endorser_hash,omitempty"`
	EndorserTxs  [][2]uint32 `json:"endorser_txs,omitempty"`
	EndorserSlot uint64      `json:"endorser_slot,omitempty"`
	ID           uint64      `json:"id"`
	Slot         uint64      `json:"slot"`
	Number       uint64      `json:"number"`
	Type         uint        `json:"type"`
}

func checkRollbackIntentLimits(
	point ocommon.Point,
	blocks []rollbackIntentRecordBlock,
	limit error,
) error {
	if len(point.Hash) > maxRollbackIntentHashBytes {
		return fmt.Errorf(
			"%w: point hash is %d bytes", ErrInvalidRollbackIntent,
			len(point.Hash),
		)
	}
	if len(blocks) > MaxRollbackIntentBlocks {
		return fmt.Errorf(
			"%w: %d blocks exceeds %d",
			limit, len(blocks), MaxRollbackIntentBlocks,
		)
	}
	ranges := 0
	var txs uint64
	for _, block := range blocks {
		if len(block.Hash) > maxRollbackIntentHashBytes ||
			len(block.PrevHash) > maxRollbackIntentHashBytes ||
			len(block.EndorserHash) > maxRollbackIntentHashBytes {
			return fmt.Errorf(
				"%w: block hash exceeds %d bytes",
				ErrInvalidRollbackIntent, maxRollbackIntentHashBytes,
			)
		}
		ranges += len(block.EndorserTxs)
		if ranges > maxRollbackIntentTxRanges {
			return fmt.Errorf(
				"%w: endorser transaction ranges exceed %d",
				limit, maxRollbackIntentTxRanges,
			)
		}
		for _, r := range block.EndorserTxs {
			if uint64(r[0])+uint64(r[1]) > math.MaxUint32 {
				return fmt.Errorf(
					"%w: endorser transaction range %d+%d",
					ErrInvalidRollbackIntent, r[0], r[1],
				)
			}
			txs += uint64(r[1])
			if txs > maxRollbackIntentEndorserTxs {
				return fmt.Errorf(
					"%w: endorser transactions exceed %d",
					limit, maxRollbackIntentEndorserTxs,
				)
			}
		}
	}
	return nil
}

func encodeRollbackIntent(intent RollbackIntent) (string, error) {
	blocks := make([]rollbackIntentRecordBlock, 0, len(intent.Blocks))
	for _, block := range intent.Blocks {
		rec := rollbackIntentRecordBlock{
			Hash: block.Hash, PrevHash: block.PrevHash,
			EndorserHash: block.EndorserHash,
			EndorserSlot: block.EndorserSlot,
			ID:           block.ID, Slot: block.Slot, Number: block.Number,
			Type: block.Type,
		}
		for _, r := range block.EndorserTxs {
			rec.EndorserTxs = append(
				rec.EndorserTxs, [2]uint32{r.Start, r.Count},
			)
		}
		blocks = append(blocks, rec)
	}
	if err := checkRollbackIntentLimits(
		intent.Point, blocks, ErrRollbackIntentTooLarge,
	); err != nil {
		return "", err
	}
	hash := hex.EncodeToString(intent.Point.Hash)
	slot := intent.Point.Slot
	data, err := json.Marshal(rollbackIntentRecord{
		Version: rollbackIntentVersion,
		Slot:    &slot,
		Hash:    &hash,
		Blocks:  blocks,
	})
	if err != nil {
		return "", fmt.Errorf("encode rollback intent: %w", err)
	}
	if len(data) > maxRollbackIntentEncodedBytes {
		return "", fmt.Errorf(
			"%w: encoded size %d exceeds %d",
			ErrRollbackIntentTooLarge, len(data),
			maxRollbackIntentEncodedBytes,
		)
	}
	return string(data), nil
}

// rollbackIntentPoint validates and returns the point a record names.
func rollbackIntentPoint(slot *uint64, hash *string) (ocommon.Point, error) {
	if slot == nil || hash == nil {
		return ocommon.Point{}, fmt.Errorf(
			"%w: missing slot or hash", ErrInvalidRollbackIntent,
		)
	}
	if len(*hash) > 2*maxRollbackIntentHashBytes {
		return ocommon.Point{}, fmt.Errorf(
			"%w: point hash is too long", ErrInvalidRollbackIntent,
		)
	}
	decoded, err := hex.DecodeString(*hash)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf(
			"%w: decode hash: %s", ErrInvalidRollbackIntent, err.Error(),
		)
	}
	if *slot > 0 && len(decoded) == 0 {
		return ocommon.Point{}, fmt.Errorf(
			"%w: empty non-origin hash", ErrInvalidRollbackIntent,
		)
	}
	return ocommon.Point{Slot: *slot, Hash: decoded}, nil
}

func decodeRollbackIntent(raw string) (RollbackIntent, error) {
	if len(raw) > maxRollbackIntentEncodedBytes {
		return RollbackIntent{}, fmt.Errorf(
			"%w: encoded size %d exceeds %d",
			ErrInvalidRollbackIntent, len(raw),
			maxRollbackIntentEncodedBytes,
		)
	}
	var rec rollbackIntentRecord
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return RollbackIntent{}, fmt.Errorf(
			"%w: decode: %s", ErrInvalidRollbackIntent, err.Error(),
		)
	}
	if dec.More() {
		return RollbackIntent{}, fmt.Errorf(
			"%w: unexpected data after record", ErrInvalidRollbackIntent,
		)
	}
	if rec.Version != rollbackIntentVersion {
		return RollbackIntent{}, fmt.Errorf(
			"%w: unsupported format version %d",
			ErrInvalidRollbackIntent, rec.Version,
		)
	}
	point, err := rollbackIntentPoint(rec.Slot, rec.Hash)
	if err != nil {
		return RollbackIntent{}, err
	}
	if err := checkRollbackIntentLimits(
		point, rec.Blocks, ErrInvalidRollbackIntent,
	); err != nil {
		return RollbackIntent{}, err
	}
	intent := RollbackIntent{
		Point:  point,
		Blocks: make([]RollbackIntentBlock, 0, len(rec.Blocks)),
	}
	for _, block := range rec.Blocks {
		out := RollbackIntentBlock{
			Hash: block.Hash, PrevHash: block.PrevHash,
			EndorserHash: block.EndorserHash,
			EndorserSlot: block.EndorserSlot,
			ID:           block.ID, Slot: block.Slot, Number: block.Number,
			Type: block.Type,
		}
		for _, r := range block.EndorserTxs {
			out.EndorserTxs = append(
				out.EndorserTxs,
				RollbackIntentTxRange{Start: r[0], Count: r[1]},
			)
		}
		intent.Blocks = append(intent.Blocks, out)
	}
	return intent, nil
}

// SetRollbackIntent stores intent, replacing any stored one. It returns
// ErrRollbackIntentTooLarge when intent names more than a record may hold.
// An IndexerDB reads the ledger's block store and records nothing in it.
func (d *Database) SetRollbackIntent(intent RollbackIntent) error {
	if d.isIndexer() {
		return nil
	}
	value, err := encodeRollbackIntent(intent)
	if err != nil {
		return err
	}
	return d.SetChainState(RollbackIntentKey, value, nil)
}

// GetRollbackIntent returns the stored rollback intent and whether there is
// one. An unreadable record is reported as ErrInvalidRollbackIntent.
func (d *Database) GetRollbackIntent() (RollbackIntent, bool, error) {
	if d.isIndexer() {
		return RollbackIntent{}, false, nil
	}
	raw, err := d.GetChainState(RollbackIntentKey, nil)
	if err != nil {
		return RollbackIntent{}, false, err
	}
	if raw == "" {
		return RollbackIntent{}, false, nil
	}
	intent, err := decodeRollbackIntent(raw)
	if err != nil {
		return RollbackIntent{}, false, err
	}
	return intent, true, nil
}

// RetainRollbackIntentBlocks copies every body the stored rollback intent
// names that the chain still holds to its rolled-back key, one transaction
// per body, and returns how many it copied. A caller about to delete those
// blocks without retiring them calls it first.
func (d *Database) RetainRollbackIntentBlocks(
	ctx context.Context,
) (int, error) {
	intent, pending, err := d.GetRollbackIntent()
	if errors.Is(err, ErrInvalidRollbackIntent) {
		// The ledger discards an unreadable record without replaying it.
		return 0, nil
	}
	if err != nil || !pending {
		return 0, err
	}
	kept := 0
	for _, named := range intent.Blocks {
		if err := ctx.Err(); err != nil {
			return kept, err
		}
		block, err := BlockByPoint(
			ctx, d, ocommon.Point{Slot: named.Slot, Hash: named.Hash},
		)
		if err != nil || len(block.Cbor) == 0 {
			continue
		}
		if err := d.RetainRolledBackBlock(
			named.Slot, named.Hash, block.Cbor,
		); err != nil {
			return kept, err
		}
		kept++
	}
	return kept, nil
}

// ClearRollbackIntent removes the stored rollback intent. The bodies it named
// stay until ReleaseRolledBackBlocks.
func (d *Database) ClearRollbackIntent() error {
	if d.isIndexer() {
		return nil
	}
	return d.DeleteChainState(RollbackIntentKey, nil)
}

func rolledBackBlockKey(slot uint64, hash []byte) ([]byte, error) {
	if len(hash) == 0 || len(hash) > maxRollbackIntentHashBytes {
		return nil, fmt.Errorf(
			"rolled-back block hash is %d bytes", len(hash),
		)
	}
	key := make([]byte, 0, len(types.RolledBackBlockKeyPrefix)+8+len(hash))
	key = append(key, types.RolledBackBlockKeyPrefix...)
	key = binary.BigEndian.AppendUint64(key, slot)
	key = append(key, hash...)
	return key, nil
}

// BlockRetireTxn removes a block from the chain like BlockDeleteTxn and keeps
// its body as a rolled-back block, in the same transaction. A rollback uses
// it so the body outlives the chain truncation for as long as a rollback
// intent may still need it.
//
// The body is written without reading the key first: a read would make the
// transaction conflict with a release that deletes the same key.
func BlockRetireTxn(txn *Txn, block models.Block) error {
	if txn == nil || txn.Blob() == nil {
		return types.ErrNilTxn
	}
	store := txn.BlobStore()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	if len(block.Cbor) == 0 {
		return BlockDeleteTxn(txn, block)
	}
	key, err := rolledBackBlockKey(block.Slot, block.Hash)
	if err != nil {
		return err
	}
	// The body is staged ahead of the deletes, so a store that applies a
	// commit in order and stops part-way has kept it.
	if err := store.Set(txn.Blob(), key, block.Cbor); err != nil {
		return fmt.Errorf("keep rolled-back block: %w", err)
	}
	if txn.db != nil {
		// Once committed, so a release that scanned before the commit does
		// not leave the body marked as released.
		released := &txn.db.rolledBackBlocksReleased
		txn.AfterCommit(func() { released.Store(false) })
	}
	return BlockDeleteTxn(txn, block)
}

// RetainRolledBackBlock stores cbor as the rolled-back body of the block at
// slot and hash, for a caller that holds a body the chain has already
// dropped.
func (d *Database) RetainRolledBackBlock(
	slot uint64,
	hash []byte,
	cbor []byte,
) error {
	if d.isIndexer() || len(cbor) == 0 {
		return nil
	}
	key, err := rolledBackBlockKey(slot, hash)
	if err != nil {
		return err
	}
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	if err := store.Set(txn.Blob(), key, cbor); err != nil {
		return fmt.Errorf("keep rolled-back block: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("keep rolled-back block: commit: %w", err)
	}
	d.rolledBackBlocksReleased.Store(false)
	return nil
}

// RolledBackBlock returns the body of the block at slot and hash from the
// chain when the chain still holds it, and otherwise from the rolled-back
// bodies. found is false when neither holds it.
func (d *Database) RolledBackBlock(
	slot uint64,
	hash []byte,
) (cbor []byte, found bool, err error) {
	key, err := rolledBackBlockKey(slot, hash)
	if err != nil {
		return nil, false, nil //nolint:nilerr // no block has such a hash
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return nil, false, types.ErrBlobStoreUnavailable
	}
	block, err := BlockByPointTxn(txn, ocommon.Point{Slot: slot, Hash: hash})
	if err == nil && len(block.Cbor) > 0 {
		return block.Cbor, true, nil
	}
	value, err := store.Get(txn.Blob(), key)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read rolled-back block: %w", err)
	}
	return bytes.Clone(value), true, nil
}

// retainedRolledBackBody reads only the rolled-back body of the block at slot
// and hash, with ErrBlobKeyNotFound when none is kept.
func (d *Database) retainedRolledBackBody(
	store blob.BlobStore,
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, error) {
	key, err := rolledBackBlockKey(slot, hash)
	if err != nil {
		return nil, types.ErrBlobKeyNotFound
	}
	value, err := store.Get(txn, key)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(value), nil
}

// RolledBackBlockHold reports the block the IndexerDB's rows end at, and false
// when it holds none. The IndexerDB reads its rows' payloads from the blocks
// they were indexed in, so until its writer has rolled back past a block that
// a rollback removed from the chain, the block's retained body is the only
// copy a row can be read from.
type RolledBackBlockHold func() (tip ocommon.Point, ok bool, err error)

// SetRolledBackBlockHold makes ReleaseRolledBackBlocks keep the rolled-back
// bodies up to the block hold reports for as long as that block is itself a
// rolled-back body: the IndexerDB's rows then end on the abandoned branch.
// Once the writer has rolled back, the tip is a chain block, and every body
// is released. A nil hold keeps none.
func (d *Database) SetRolledBackBlockHold(hold RolledBackBlockHold) {
	d.rolledBackBlockHold.Store(&hold)
	d.rolledBackBlocksReleased.Store(false)
}

// ReleaseRolledBackBlocks deletes the rolled-back bodies, returning how many
// it deleted, except those a RolledBackBlockHold keeps. The caller has
// established that no rollback intent names them. It is safe to repeat and to
// interrupt: a body it did not reach is deleted by the next call, and so is a
// body that was held.
func (d *Database) ReleaseRolledBackBlocks(ctx context.Context) (int, error) {
	if d.isIndexer() {
		return 0, nil
	}
	// Set before the scan: a body kept while the scan runs clears the mark
	// again, so the next call looks for it.
	if d.rolledBackBlocksReleased.Swap(true) {
		return 0, nil
	}
	hold, holding, err := d.rolledBackBlockHoldSlot()
	if err != nil {
		d.rolledBackBlocksReleased.Store(false)
		return 0, fmt.Errorf("release rolled-back blocks: %w", err)
	}
	deleted := 0
	for {
		if err := ctx.Err(); err != nil {
			d.rolledBackBlocksReleased.Store(false)
			return deleted, err
		}
		var (
			keys [][]byte
			held bool
			err  error
		)
		if holding {
			keys, held, err = d.releasableRolledBackBlockKeys(
				rolledBackBlockReleaseBatch, hold,
			)
		} else {
			keys, err = d.rolledBackBlockKeys(rolledBackBlockReleaseBatch)
		}
		if err == nil && len(keys) > 0 {
			err = d.deleteBlobKeys(keys)
		}
		if err != nil {
			d.rolledBackBlocksReleased.Store(false)
			return deleted, fmt.Errorf("release rolled-back blocks: %w", err)
		}
		deleted += len(keys)
		if held {
			// A held body is released by a later call, once the hold moves.
			d.rolledBackBlocksReleased.Store(false)
		}
		if len(keys) < rolledBackBlockReleaseBatch {
			return deleted, nil
		}
	}
}

// rolledBackBlockHoldSlot returns the highest slot whose rolled-back bodies
// are held, and whether any are.
func (d *Database) rolledBackBlockHoldSlot() (uint64, bool, error) {
	fn := d.rolledBackBlockHold.Load()
	if fn == nil || *fn == nil {
		return 0, false, nil
	}
	tip, ok, err := (*fn)()
	if err != nil || !ok {
		return 0, false, err
	}
	key, err := rolledBackBlockKey(tip.Slot, tip.Hash)
	if err != nil {
		return 0, false, nil //nolint:nilerr // no block has such a hash
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return 0, false, types.ErrBlobStoreUnavailable
	}
	if _, err := store.Get(txn.Blob(), key); err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read rolled-back block: %w", err)
	}
	return tip.Slot, true, nil
}

// rolledBackBlockKeys returns up to limit rolled-back body keys.
func (d *Database) rolledBackBlockKeys(limit int) ([][]byte, error) {
	keys, _, err := d.scanRolledBackBlockKeys(limit, 0, false)
	return keys, err
}

// releasableRolledBackBlockKeys is rolledBackBlockKeys without the bodies at
// or below hold, and reports whether it passed over any.
func (d *Database) releasableRolledBackBlockKeys(
	limit int,
	hold uint64,
) ([][]byte, bool, error) {
	return d.scanRolledBackBlockKeys(limit, hold, true)
}

func (d *Database) scanRolledBackBlockKeys(
	limit int,
	hold uint64,
	holding bool,
) ([][]byte, bool, error) {
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return nil, false, types.ErrBlobStoreUnavailable
	}
	prefix := []byte(types.RolledBackBlockKeyPrefix)
	it := store.NewIterator(
		txn.Blob(), types.BlobIteratorOptions{Prefix: prefix},
	)
	if it == nil {
		return nil, false, types.ErrBlobStoreUnavailable
	}
	defer it.Close()
	var (
		keys [][]byte
		held bool
	)
	for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keys) < limit; it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if holding && len(key) >= len(prefix)+8 &&
			binary.BigEndian.Uint64(key[len(prefix):]) <= hold {
			held = true
			continue
		}
		keys = append(keys, bytes.Clone(key))
	}
	return keys, held, it.Err()
}

// deleteBlobKeys deletes keys in one transaction that reads nothing, so it
// cannot conflict with a writer of the same keys.
func (d *Database) deleteBlobKeys(keys [][]byte) error {
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	for _, key := range keys {
		err := store.Delete(txn.Blob(), key)
		if err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
			return err
		}
	}
	return txn.Commit()
}
