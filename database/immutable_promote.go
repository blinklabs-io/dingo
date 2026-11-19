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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// ImmutableStateKey holds the ImmutableDB state in the blob store. Its
	// presence is what installs the marker-resolving read wrapper, so a
	// reader never depends on configuration to find promoted blocks.
	ImmutableStateKey = "im_state"

	immutableStateVersion = 1
	// ImmutableDirName is the directory under the data directory that holds
	// the chunk files.
	ImmutableDirName = immplugin.DirName
)

// DefaultImmutablePromoteBatch is the number of blocks promoted per step when
// the configuration leaves it unset.
const DefaultImmutablePromoteBatch = 1000

// ErrRollbackBelowImmutableTip reports an attempt to remove or roll back a
// block that has been promoted to the ImmutableDB.
var ErrRollbackBelowImmutableTip = errors.New(
	"rollback below the immutable tip",
)

// ErrImmutableChunksBehind reports chunk files that end before the recorded
// immutable tip, which no automatic repair can recover.
var ErrImmutableChunksBehind = errors.New(
	"immutable chunk files end before the recorded immutable tip",
)

// ImmutableConfig configures the ImmutableDB of promoted block bodies.
type ImmutableConfig struct {
	// Enabled turns promotion on and creates the state record on first use.
	// An existing state record installs the read path regardless.
	Enabled bool
	// ChunkSlots and CanContainEBB are the chunk geometry; see
	// immutable.Geometry.
	ChunkSlots    uint64
	CanContainEBB bool
	// PromoteBatch bounds the blocks promoted per step; zero uses
	// DefaultImmutablePromoteBatch.
	PromoteBatch int
}

// ImmutableState is the persisted ImmutableDB record.
type ImmutableState struct {
	Geometry immutable.Geometry
	// HasTip is false until a block has been appended.
	HasTip bool
	// TipSlot and TipHash name the last block written to the chunk files.
	TipSlot uint64
	TipHash []byte
	// TipIndex is the highest block index covered; blocks at or below it are
	// immutable even where their body was already expired and nothing was
	// written for them.
	TipIndex uint64
}

func (s ImmutableState) encode() []byte {
	out := make([]byte, 0, 4+8+1+8+8+1+len(s.TipHash))
	out = binary.BigEndian.AppendUint32(out, immutableStateVersion)
	out = binary.BigEndian.AppendUint64(out, s.Geometry.ChunkSlots)
	var flags byte
	if s.Geometry.CanContainEBB {
		flags |= 1
	}
	if s.HasTip {
		flags |= 2
	}
	out = append(out, flags)
	out = binary.BigEndian.AppendUint64(out, s.TipSlot)
	out = binary.BigEndian.AppendUint64(out, s.TipIndex)
	out = append(
		out,
		byte(len(s.TipHash)),
	) // #nosec G115 -- hashes are 32 bytes
	return append(out, s.TipHash...)
}

func decodeImmutableState(data []byte) (ImmutableState, error) {
	const fixed = 4 + 8 + 1 + 8 + 8 + 1
	if len(data) < fixed {
		return ImmutableState{}, errors.New("immutable state is truncated")
	}
	if v := binary.BigEndian.Uint32(data); v != immutableStateVersion {
		return ImmutableState{}, fmt.Errorf(
			"unsupported immutable state version %d", v,
		)
	}
	n := int(data[fixed-1])
	if len(data) != fixed+n {
		return ImmutableState{}, errors.New("immutable state length mismatch")
	}
	flags := data[12]
	return ImmutableState{
		Geometry: immutable.Geometry{
			ChunkSlots:    binary.BigEndian.Uint64(data[4:]),
			CanContainEBB: flags&1 != 0,
		},
		HasTip:   flags&2 != 0,
		TipSlot:  binary.BigEndian.Uint64(data[13:]),
		TipIndex: binary.BigEndian.Uint64(data[21:]),
		TipHash:  bytes.Clone(data[fixed:]),
	}, nil
}

// immutableRuntime is the Database's ImmutableDB handle, present once a state
// record exists.
type immutableRuntime struct {
	dir string
	// svc is the ImmutableDB provider; it opens the chunk writer and reads
	// blocks back.
	svc immplugin.ImmutableDB
	// mu serializes promotion with operator truncation and every other
	// transition that rewrites the chunk files or the tip.
	mu    sync.Mutex
	store immplugin.Store
	// state mirrors the persisted record for lock-free floor checks.
	stateMu sync.RWMutex
	state   ImmutableState
	// unrecorded is set while a state created by this open has not been
	// persisted. Open records it only once the database has passed its
	// startup checks; every later state write carries the geometry anyway.
	unrecorded bool
	// fault, when set by a test, is called at each durability step of a
	// promotion batch and may fail it to simulate an interruption there.
	fault func(step string) error
}

// Promotion steps a test can interrupt; see immutableRuntime.fault.
const (
	promoteStepAppend = "append"
	promoteStepSync   = "sync"
	promoteStepCommit = "commit"
	promoteStepState  = "state"
)

// dropStore closes the chunk writer so the next use reopens it and reconciles
// it with the recorded tip. A step that fails after appending leaves the open
// writer ahead of that tip, and a writer that stays open would refuse every
// later append. The caller holds r.mu.
func (r *immutableRuntime) dropStore() {
	if r.store != nil {
		_ = r.store.Close()
		r.store = nil
	}
}

// resetImmutableStore re-reads the recorded state and drops the chunk writer.
// A failure can land after the state commit but before the in-memory copy
// advanced; reopening the writer against the stale copy would cut chunk bytes
// that committed markers already name. The caller holds rt.mu.
func (d *Database) resetImmutableStore() {
	rt := d.immutable
	if st, found, err := d.loadImmutableState(); err == nil && found {
		rt.setState(st)
	}
	rt.dropStore()
}

func (r *immutableRuntime) injectFault(step string) error {
	if r.fault == nil {
		return nil
	}
	return r.fault(step)
}

func (r *immutableRuntime) snapshot() ImmutableState {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return r.state
}

func (r *immutableRuntime) setState(s ImmutableState) {
	r.stateMu.Lock()
	r.state = s
	r.stateMu.Unlock()
}

// ImmutableDir returns the chunk directory, or "" when the database has no
// ImmutableDB.
func (d *Database) ImmutableDir() string {
	if d.immutable == nil {
		return ""
	}
	return d.immutable.dir
}

// PublishImmutableChunk makes a locally staged imported chunk durable through
// providers that store chunk files remotely. Local providers already own the
// files in Dir and need no extra step.
func (d *Database) PublishImmutableChunk(num uint64) error {
	if d.immutable == nil {
		return ErrImmutableNotEnabled
	}
	publisher, ok := d.immutable.svc.(immplugin.ChunkPublisher)
	if !ok {
		return nil
	}
	return publisher.PublishChunk(num)
}

// ImmutableChunksPublished reports whether the provider stores chunk files
// remotely, so an imported chunk needs PublishImmutableChunk.
func (d *Database) ImmutableChunksPublished() bool {
	if d.immutable == nil {
		return false
	}
	_, ok := d.immutable.svc.(immplugin.ChunkPublisher)
	return ok
}

// UnpublishImmutableChunk removes the remote copy of an imported chunk whose
// adoption failed before its markers committed. It refuses a chunk at or
// below the one the recorded tip ends in, whose objects committed markers may
// name. Local providers publish nothing and need no step.
func (d *Database) UnpublishImmutableChunk(num uint64) error {
	if d.immutable == nil {
		return ErrImmutableNotEnabled
	}
	unpublisher, ok := d.immutable.svc.(immplugin.ChunkUnpublisher)
	if !ok {
		return nil
	}
	st := d.immutable.snapshot()
	if st.HasTip && (!st.Geometry.Valid() ||
		num <= st.TipSlot/st.Geometry.ChunkSlots) {
		return fmt.Errorf(
			"refusing to unpublish ImmutableDB chunk %d: "+
				"the recorded tip at slot %d covers it",
			num, st.TipSlot,
		)
	}
	return unpublisher.UnpublishChunk(num)
}

// ReleaseImmutableChunk discards the local staging copy after a remotely
// stored chunk and its database markers are durable.
func (d *Database) ReleaseImmutableChunk(num uint64) error {
	if d.immutable == nil {
		return ErrImmutableNotEnabled
	}
	releaser, ok := d.immutable.svc.(immplugin.LocalChunkReleaser)
	if !ok {
		return nil
	}
	return releaser.ReleaseChunk(num)
}

// ImmutableState returns the persisted state, and false when the database
// has no ImmutableDB.
func (d *Database) ImmutableState() (ImmutableState, bool) {
	if d.immutable == nil {
		return ImmutableState{}, false
	}
	return d.immutable.snapshot(), true
}

// ImmutableFloorIndex returns the highest block index that is immutable. The
// bool is false when nothing has been promoted.
func (d *Database) ImmutableFloorIndex() (uint64, bool) {
	st, ok := d.ImmutableState()
	if !ok || (!st.HasTip && st.TipIndex == 0) {
		return 0, false
	}
	return st.TipIndex, true
}

// CheckImmutableFloor refuses removal of the block at index.
func (d *Database) CheckImmutableFloor(index uint64) error {
	if floor, ok := d.ImmutableFloorIndex(); ok && index <= floor {
		return fmt.Errorf(
			"%w: block index %d is at or below immutable tip index %d",
			ErrRollbackBelowImmutableTip, index, floor,
		)
	}
	return nil
}

// CheckImmutableRollback refuses a rollback whose target block index lies
// below the immutable tip. Rolling back to the tip itself keeps it.
func (d *Database) CheckImmutableRollback(targetIndex uint64) error {
	if floor, ok := d.ImmutableFloorIndex(); ok && targetIndex < floor {
		return fmt.Errorf(
			"%w: rollback target index %d is below immutable tip index %d",
			ErrRollbackBelowImmutableTip, targetIndex, floor,
		)
	}
	return nil
}

func (d *Database) loadImmutableState() (ImmutableState, bool, error) {
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return ImmutableState{}, false, types.ErrBlobStoreUnavailable
	}
	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	data, err := store.Get(txn, []byte(ImmutableStateKey))
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return ImmutableState{}, false, nil
		}
		return ImmutableState{}, false, err
	}
	st, err := decodeImmutableState(data)
	return st, err == nil, err
}

// recordedImmutableFloor reads the recorded tip index through txn, which sees
// the state a promotion commits together with its markers. The bool is false
// when nothing is immutable.
func recordedImmutableFloor(
	store blob.BlobStore,
	txn types.Txn,
) (uint64, bool, error) {
	data, err := rawBlobStore(store).Get(txn, []byte(ImmutableStateKey))
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	st, err := decodeImmutableState(data)
	if err != nil {
		return 0, false, err
	}
	if !st.HasTip && st.TipIndex == 0 {
		return 0, false, nil
	}
	return st.TipIndex, true, nil
}

func (d *Database) writeImmutableState(st ImmutableState) error {
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	txn := store.NewTransaction(true)
	if err := store.Set(txn, []byte(ImmutableStateKey), st.encode()); err != nil {
		_ = txn.Rollback()
		return err
	}
	return txn.Commit()
}

// initImmutable installs the marker-resolving read wrapper when the database
// holds an ImmutableDB state record, and creates the state when the
// configuration enables the feature; New records a created state once the
// open's startup checks pass. It runs before anything reads blocks.
func (d *Database) initImmutable() error {
	cfg := d.config.Immutable
	st, found, err := d.loadImmutableState()
	if err != nil {
		return fmt.Errorf("load immutable state: %w", err)
	}
	if !found && !cfg.Enabled {
		return nil
	}
	if d.config.DataDir == "" {
		return errors.New("the ImmutableDB requires a data directory")
	}
	if d.immutableDB == nil {
		return errors.New(
			"the ImmutableDB requires a storage.immutable provider",
		)
	}
	if !found {
		geom := immutable.Geometry{
			ChunkSlots:    cfg.ChunkSlots,
			CanContainEBB: cfg.CanContainEBB,
		}
		if !geom.Valid() {
			return fmt.Errorf("invalid ImmutableDB geometry %+v", geom)
		}
		st = ImmutableState{Geometry: geom}
	} else if cfg.Enabled && cfg.ChunkSlots != 0 &&
		(cfg.ChunkSlots != st.Geometry.ChunkSlots ||
			cfg.CanContainEBB != st.Geometry.CanContainEBB) {
		return fmt.Errorf(
			"ImmutableDB geometry %+v does not match the recorded %+v",
			immutable.Geometry{
				ChunkSlots:    cfg.ChunkSlots,
				CanContainEBB: cfg.CanContainEBB,
			},
			st.Geometry,
		)
	}
	installed := d.Blob()
	if installed == nil {
		return types.ErrBlobStoreUnavailable
	}
	base := rawBlobStore(installed)
	if old := d.immutable; old != nil {
		old.mu.Lock()
		old.dropStore()
		old.mu.Unlock()
	}
	rt := &immutableRuntime{
		dir:        d.immutableDB.Dir(),
		svc:        d.immutableDB,
		state:      st,
		unrecorded: !found,
	}
	d.immutable = rt
	d.SetBlobStore(&immutableBlobStore{
		BlobStore: base,
		read:      rt.svc.ReadBlockAt,
	})
	return nil
}

// recordImmutableState persists a state that initImmutable created. It runs
// after the open's startup checks, so a database that fails them, such as one
// opened with another network's configuration, is left without a geometry it
// never ran under.
func (d *Database) recordImmutableState() error {
	rt := d.immutable
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.unrecorded {
		return nil
	}
	// A state written since, by a promotion or adoption, already holds the
	// geometry and must not be replaced.
	if _, found, err := d.loadImmutableState(); err != nil {
		return fmt.Errorf("load immutable state: %w", err)
	} else if !found {
		if err := d.writeImmutableState(rt.snapshot()); err != nil {
			return fmt.Errorf("write immutable state: %w", err)
		}
	}
	rt.unrecorded = false
	return nil
}

// openImmutableStore opens the chunk writer and reconciles it with the
// recorded tip. The caller holds rt.mu.
func (d *Database) openImmutableStore() (immplugin.Store, error) {
	rt := d.immutable
	if rt.store != nil {
		return rt.store, nil
	}
	if _, pending, err := d.ImmutableAdoptPending(); err != nil {
		return nil, err
	} else if pending {
		return nil, ErrImmutableAdoptionPending
	}
	st := rt.snapshot()
	s, err := rt.svc.Open(st.Geometry)
	if err != nil {
		return nil, err
	}
	tip, hasTip := s.Tip()
	switch {
	case !st.HasTip && hasTip:
		// Chunks written before any marker committed.
		if err := s.TruncateAll(); err != nil {
			_ = s.Close()
			return nil, err
		}
	case st.HasTip && !hasTip:
		_ = s.Close()
		return nil, fmt.Errorf(
			"%w: chunk files are empty, recorded tip is slot %d; "+
				"restore the chunk files or truncate the database",
			ErrImmutableChunksBehind, st.TipSlot,
		)
	case st.HasTip && hasTip &&
		(tip.Slot != st.TipSlot || !bytes.Equal(tip.Hash, st.TipHash)):
		if tip.Slot < st.TipSlot {
			_ = s.Close()
			return nil, fmt.Errorf(
				"%w: chunk tip is slot %d, recorded tip is slot %d; "+
					"restore the chunk files or truncate the database",
				ErrImmutableChunksBehind, tip.Slot, st.TipSlot,
			)
		}
		if err := s.TruncateAfter(st.TipSlot, st.TipHash); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf(
				"reconcile immutable chunks with recorded tip: %w", err,
			)
		}
	}
	rt.store = s
	return s, nil
}

// ReconcileImmutable opens the chunk files and brings them in line with the
// recorded tip, so a torn promotion is repaired at startup rather than at the
// first promotion. It is a no-op without an ImmutableDB, and leaves the
// directory untouched while a chunk adoption is pending: those files are
// certified bytes the interrupted import resumes with.
func (d *Database) ReconcileImmutable() error {
	if d.immutable == nil {
		return nil
	}
	d.immutable.mu.Lock()
	defer d.immutable.mu.Unlock()
	_, err := d.openImmutableStore()
	if errors.Is(err, ErrImmutableAdoptionPending) {
		return nil
	}
	return err
}

// PromoteResult describes one PromoteThrough call.
type PromoteResult struct {
	// Promoted is the number of block bodies moved to chunk files.
	Promoted int
	// Tip is the immutable tip after the call.
	Tip ImmutableState
}

// PromoteThrough moves the bodies of blocks with index up to and including
// ceilingIndex into chunk files, replacing each bp value with a marker.
//
// The order is the durability contract: bodies are appended and synced to the
// chunk files first, and only then does one blob transaction swap the markers
// in and advance the recorded tip. A crash between the two leaves chunk bytes
// ahead of the recorded tip, which startup truncates; the badger bodies are
// still intact. It resumes from the recorded tip, so an interrupted run or a
// database that predates the feature simply continues.
func (d *Database) PromoteThrough(
	ctx context.Context,
	ceilingIndex uint64,
) (PromoteResult, error) {
	rt := d.immutable
	if rt == nil {
		return PromoteResult{}, errors.New("the ImmutableDB is not enabled")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	store, err := d.openImmutableStore()
	if err != nil {
		return PromoteResult{}, err
	}
	batch := d.config.Immutable.PromoteBatch
	if batch <= 0 {
		batch = DefaultImmutablePromoteBatch
	}
	var total int
	for {
		if err := ctx.Err(); err != nil {
			return PromoteResult{Promoted: total, Tip: rt.snapshot()}, err
		}
		st := rt.snapshot()
		next := st.TipIndex + 1
		if !st.HasTip && st.TipIndex == 0 {
			next = 1
		}
		if next > ceilingIndex {
			return PromoteResult{Promoted: total, Tip: st}, nil
		}
		n, err := d.promoteBatch(store, st, next, ceilingIndex, batch)
		if n > 0 {
			total += n
		}
		if err != nil {
			d.resetImmutableStore()
			return PromoteResult{Promoted: total, Tip: rt.snapshot()}, err
		}
		if n < 0 {
			return PromoteResult{Promoted: total, Tip: rt.snapshot()}, nil
		}
	}
}

type promotedBlock struct {
	key    []byte
	marker types.ImmutableMarker
	// bodySum identifies the body that was appended, so the commit can tell
	// whether the key still holds it.
	bodySum [sha256.Size]byte
}

// ErrPromotionRaced reports a block whose body changed or disappeared between
// being read for promotion and the commit that would replace it with a marker.
var ErrPromotionRaced = errors.New(
	"block changed while it was being promoted",
)

// promoteBatchLimit bounds batch by what one blob transaction can still
// stage, leaving the headroom a combined commit needs.
func (d *Database) promoteBatchLimit(batch int) int {
	probe := d.BlockBlobTxn()
	defer probe.Rollback() //nolint:errcheck
	want := batch + 1
	limit := stagedBlobDeleteLimit(
		probe.BlobStore(), probe.Blob(),
		types.BlockBlobKeySize+types.BlockImmutableMarkerSize, want,
	)
	return max(min(batch, limit-1), 1)
}

// promoteBatch promotes up to batch blocks starting at index next. It returns
// -1 (with no error) when no further block exists at or below ceiling.
func (d *Database) promoteBatch(
	store immplugin.Store,
	st ImmutableState,
	next, ceiling uint64,
	batch int,
) (int, error) {
	rt := d.immutable
	batch = d.promoteBatchLimit(batch)
	var (
		promoted  []promotedBlock
		lastIndex = st.TipIndex
		tipSlot   = st.TipSlot
		tipHash   = st.TipHash
		hasTip    = st.HasTip
		exhausted bool
	)
	read := d.BlobTxn(false)
	defer read.Rollback() //nolint:errcheck
	raw := rawBlobStore(read.BlobStore())
	if raw == nil {
		return 0, types.ErrBlobStoreUnavailable
	}
	for idx := next; len(promoted) < batch; idx++ {
		entry, err := blockIndexEntryAtOrAfterTxn(read, idx)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				exhausted = true
				break
			}
			return 0, err
		}
		if entry.id > ceiling {
			exhausted = true
			break
		}
		idx = entry.id
		point, err := BlockBlobKeyToPoint(entry.blockKey)
		if err != nil {
			return 0, err
		}
		body, md, err := raw.GetBlock(read.Blob(), point.Slot, point.Hash)
		if err != nil {
			if errors.Is(err, types.ErrHistoryExpired) {
				// No body survives to write; the index is still covered.
				lastIndex = idx
				continue
			}
			return 0, fmt.Errorf("read block %d for promotion: %w", idx, err)
		}
		if types.IsImmutableMarker(body) {
			lastIndex = idx
			continue
		}
		if err := rt.injectFault(promoteStepAppend); err != nil {
			return 0, err
		}
		loc, err := store.Append(immutable.WriteBlock{
			Hash: point.Hash,
			Cbor: body,
			Type: md.Type,
			Slot: point.Slot,
		})
		if err != nil {
			return 0, fmt.Errorf("append block %d to chunk files: %w", idx, err)
		}
		promoted = append(promoted, promotedBlock{
			key: entry.blockKey,
			marker: types.ImmutableMarker{
				Chunk: loc.Chunk, Offset: loc.Offset,
				Size: loc.Size, CRC: loc.CRC,
			},
			bodySum: sha256.Sum256(body),
		})
		lastIndex = idx
		tipSlot, tipHash, hasTip = point.Slot, point.Hash, true
	}
	if len(promoted) == 0 && lastIndex == st.TipIndex {
		return -1, nil
	}
	// Chunk bytes become durable before any marker exists.
	if err := rt.injectFault(promoteStepSync); err != nil {
		return 0, err
	}
	if err := store.Sync(); err != nil {
		return 0, err
	}
	newState := ImmutableState{
		Geometry: st.Geometry,
		HasTip:   hasTip,
		TipSlot:  tipSlot,
		TipHash:  bytes.Clone(tipHash),
		TipIndex: lastIndex,
	}
	write := d.BlockBlobTxn()
	committed := false
	defer func() {
		if !committed {
			_ = write.Rollback()
		}
	}()
	wstore := write.BlobStore()
	rawWrite := rawBlobStore(wstore)
	for _, p := range promoted {
		// The body was read under a different snapshot. A delete or rewrite
		// committed since would otherwise be undone by the marker.
		point, err := BlockBlobKeyToPoint(p.key)
		if err != nil {
			return 0, err
		}
		cur, _, err := rawWrite.GetBlock(write.Blob(), point.Slot, point.Hash)
		if err != nil {
			return 0, fmt.Errorf(
				"%w: slot %d: %w", ErrPromotionRaced, point.Slot, err,
			)
		}
		if sha256.Sum256(cur) != p.bodySum {
			return 0, fmt.Errorf(
				"%w: slot %d: body differs", ErrPromotionRaced, point.Slot,
			)
		}
		if err := wstore.Set(write.Blob(), p.key, p.marker.Encode()); err != nil {
			return 0, err
		}
	}
	if err := wstore.Set(
		write.Blob(), []byte(ImmutableStateKey), newState.encode(),
	); err != nil {
		return 0, err
	}
	if err := rt.injectFault(promoteStepCommit); err != nil {
		return 0, err
	}
	if err := write.Commit(); err != nil {
		return 0, err
	}
	committed = true
	if err := rt.injectFault(promoteStepState); err != nil {
		return 0, err
	}
	rt.setState(newState)
	if exhausted && len(promoted) == 0 {
		return -1, nil
	}
	return len(promoted), nil
}

// ReclaimBlobSpace compacts the blob store so the bodies that promotion
// replaced with markers stop occupying it. Replacing a value frees nothing
// until a compaction covers both versions, and the store would otherwise wait
// for one to happen on its own. A store with nothing to reclaim is a no-op.
func (d *Database) ReclaimBlobSpace() error {
	if r, ok := d.Blob().(blob.SpaceReclaimer); ok {
		return r.ReclaimSpace()
	}
	return nil
}

// ImmutablePoint returns the last block written to the chunk files.
func (d *Database) ImmutablePoint() (ocommon.Point, bool) {
	st, ok := d.ImmutableState()
	if !ok || !st.HasTip {
		return ocommon.Point{}, false
	}
	return ocommon.NewPoint(st.TipSlot, st.TipHash), true
}

// CheckImmutableSlot refuses removal of a blob at slot when that slot is at
// or below the last block written to the chunk files.
func (d *Database) CheckImmutableSlot(slot uint64) error {
	if st, ok := d.ImmutableState(); ok && st.HasTip && slot <= st.TipSlot {
		return fmt.Errorf(
			"%w: slot %d is at or below immutable tip slot %d",
			ErrRollbackBelowImmutableTip, slot, st.TipSlot,
		)
	}
	return nil
}

// LowerImmutableTip lowers the immutable tip to block index afterIndex and
// cuts the chunk files back to match, for an operator truncate that removes
// promoted blocks. It is idempotent, so a resumed truncate repeats it safely.
//
// The truncation point must be in the chunk files, or nothing changes. The
// recorded state is lowered before the chunk files are cut: a crash in
// between leaves chunk bytes ahead of the recorded tip, which startup
// reconciliation truncates, whereas the opposite order would leave the
// recorded tip ahead of the chunks, which it refuses.
func (d *Database) LowerImmutableTip(afterIndex uint64) error {
	rt := d.immutable
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st := rt.snapshot()
	if st.TipIndex <= afterIndex {
		return nil
	}
	store, err := d.openImmutableStore()
	if err != nil {
		return err
	}
	read := d.BlobTxn(false)
	defer read.Rollback() //nolint:errcheck
	raw := rawBlobStore(read.BlobStore())
	newState := ImmutableState{Geometry: st.Geometry, TipIndex: afterIndex}
	for idx := afterIndex; idx > 0; idx-- {
		point, err := d.BlockPointByIndex(idx, read)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				continue
			}
			return err
		}
		body, _, err := raw.GetBlock(read.Blob(), point.Slot, point.Hash)
		if err != nil {
			if errors.Is(err, types.ErrHistoryExpired) ||
				errors.Is(err, types.ErrBlobKeyNotFound) {
				continue
			}
			return err
		}
		if !types.IsImmutableMarker(body) {
			continue
		}
		newState.HasTip = true
		newState.TipSlot = point.Slot
		newState.TipHash = bytes.Clone(point.Hash)
		break
	}
	if newState.HasTip {
		// Truncation refuses a point the chunk files do not hold. Check that
		// before the lowered state is recorded, or every later open of the
		// store would retry a truncation that cannot succeed.
		blk, _, err := store.GetBlock(newState.TipSlot, newState.TipHash)
		if err != nil {
			return err
		}
		if blk == nil {
			return fmt.Errorf(
				"immutable store: point at slot %d with hash %x not found; "+
					"refusing to lower the immutable tip",
				newState.TipSlot, newState.TipHash,
			)
		}
	}
	if err := d.writeImmutableState(newState); err != nil {
		return err
	}
	rt.setState(newState)
	var truncErr error
	if newState.HasTip {
		truncErr = store.TruncateAfter(newState.TipSlot, newState.TipHash)
	} else {
		truncErr = store.TruncateAll()
	}
	if truncErr != nil {
		d.resetImmutableStore()
	}
	return truncErr
}
