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

// Package badgerutxo keeps the live UTxO set in Badger. Changes arrive
// complete, as outputs added and outputs removed, so the store keeps no
// history: a rollback arrives as the outputs it restores and removes.
//
// Keys:
//
//	u <txid:32> <index:4 BE>   output CBOR, one per live output
//	n <slot:8 BE> <hash>       block nonce, a checkpoint flag then the nonce
//	m tip                      the point of the last applied change
//	m count                    the number of live outputs
//	m nonce                    set once every nonce up to the tip is held
//	e, s, m epoch              epoch and pool stake snapshot rows (epoch.go)
//	f, m esummary              epoch summary rows (summary.go)
package badgerutxo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/internal/badgermem"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

const (
	refLen  = 36
	hashLen = 32
)

// formatVersion is the layout this code writes: outputs, the journal, and
// the running sums of the aggregator. Block nonces have their own marker,
// noncesKey, so a store from before them gains them without a rebuild.
const formatVersion = 2

// DirFor is the live UTxO set's directory under a node's data directory.
func DirFor(dataDir string) string {
	return filepath.Join(dataDir, "ledger", "utxo")
}

// Ref is a transaction ID followed by a big-endian output index.
type Ref [refLen]byte

// NewRef builds a Ref from a transaction ID and output index.
func NewRef(txID []byte, index uint32) Ref {
	var r Ref
	copy(r[:hashLen], txID)
	binary.BigEndian.PutUint32(r[hashLen:], index)
	return r
}

// Output is a produced output and its CBOR.
type Output struct {
	Ref  Ref
	Cbor []byte
}

// ApplyResult reports inconsistencies found while applying a change. A
// removed output that is not live, or an added output that already is,
// means this set and the ledger disagree.
type ApplyResult struct {
	Missing   []Ref
	Duplicate []Ref
}

// Store is a live UTxO set in a Badger database it owns.
type Store struct {
	db  *badger.DB
	agg Aggregator
	// snap is set on a store returned by Snapshot: every read goes through
	// its transaction and every write is refused.
	snap *snapshot
	// anchor is the immutable tip the node last reported, nil until it
	// reports one. PruneJournal keeps the journal floor at or below it.
	anchor atomic.Pointer[ocommon.Point]
	// gate keeps a read transaction from beginning while a chunked batch is
	// part written; see applyChunked.
	gate sync.RWMutex
	// budgetEntries, budgetBytes and afterChunk replace Badger's limits and
	// observe a chunked batch between transactions. Only tests set them.
	budgetEntries, budgetBytes int64
	afterChunk                 func(done int) error
	// afterRollbackStep observes a rollback between its transactions; see
	// Rollback. Only tests set it.
	afterRollbackStep func(done int) error
	// afterBootstrapFlush runs between a bootstrap's contents and its tip;
	// an error stops the bootstrap there. Only tests set it.
	afterBootstrapFlush func() error
	// loadsRefused is set by RefuseLoads.
	loadsRefused atomic.Bool
	// added holds, for each family, the index of rows below the export floor
	// that eachRowAddedIn serves. rowsRewritten counts the operations that can
	// put a row below the floor, each of which invalidates it.
	added         addedIndexes
	rowsRewritten atomic.Uint64
	// rangeRowsVisited counts the rows and index entries the range, prefix,
	// and added-row readers stepped over.
	rangeRowsVisited atomic.Uint64
	// rewardIndexVisited counts the index entries credit pages examined.
	rewardIndexVisited atomic.Uint64
	// memTableSize replaces Badger's memtable size when positive.
	memTableSize int64
	// blockCacheSize replaces Badger's block cache size when positive.
	blockCacheSize int64
}

// ErrLoadsRefused reports a family load refused by RefuseLoads.
var ErrLoadsRefused = errors.New(
	"badgerutxo: a row family cannot be reloaded from a source that has lost rows",
)

// RefuseLoads makes a Load* method fail with ErrLoadsRefused, changing
// nothing, while the rows it replaces are held. A load replaces a family's
// rows with the source's, which is wrong once the set is the ledger's only
// copy of its state: no metadata store holds the rows to load. A family that
// holds no row loses none.
func (s *Store) RefuseLoads() {
	s.loadsRefused.Store(true)
}

// LoadsRefused reports whether RefuseLoads was called. In-place upgrades of
// the set's rows use it to avoid querying a source that holds none.
func (s *Store) LoadsRefused() bool {
	return s.loadsRefused.Load()
}

// Open opens or creates the store in dir.
func Open(dir string, options ...Option) (*Store, error) {
	s := &Store{}
	for _, o := range options {
		o(s)
	}
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	if s.memTableSize > 0 {
		opts = opts.WithMemTableSize(s.memTableSize)
	}
	if s.blockCacheSize > 0 {
		opts = opts.WithBlockCacheSize(s.blockCacheSize)
	}
	db, _, err := badgermem.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badgerutxo: open %s: %w", dir, err)
	}
	s.db = db
	// An empty store is in the current format by definition; one written in
	// an older format keeps no mark and reports Stale until rebuilt.
	if err := db.Update(func(txn *badger.Txn) error {
		if _, ok, err := readTip(txn); err != nil || ok {
			return err
		}
		// A bootstrap that did not finish leaves no tip either, and its
		// contents are not an empty set in the current format.
		if err := checkBootstrapComplete(txn); err != nil {
			if errors.Is(err, ErrBootstrapIncomplete) {
				return nil
			}
			return err
		}
		return txn.Set(formatKey, []byte{formatVersion})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("badgerutxo: open %s: %w", dir, err)
	}
	if err := s.ensureRewardSeqIndex(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("badgerutxo: open %s: %w", dir, err)
	}
	if err := s.ensureExportFloor(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("badgerutxo: open %s: %w", dir, err)
	}
	return s, nil
}

// Stale reports whether the store was written in an older format, so its
// journal or sums cannot be trusted and it must be rebuilt.
func (s *Store) Stale() (bool, error) {
	var stale bool
	err := s.viewRaw(func(txn *badger.Txn) error {
		item, err := txn.Get(formatKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			stale = true
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			stale = len(v) != 1 || v[0] != formatVersion
			return nil
		})
	})
	return stale, err
}

// FormatVersion returns the layout version recorded in the store, whether one
// is recorded, and the version this code writes. A set that was bootstrapped
// or written in another layout needs its rows migrated before this code can
// read them in place.
func (s *Store) FormatVersion() (stored uint8, found bool, current uint8, err error) {
	current = formatVersion
	err = s.viewRaw(func(txn *badger.Txn) error {
		item, err := txn.Get(formatKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if len(v) == 1 {
				stored, found = v[0], true
			}
			return nil
		})
	})
	return stored, found, current, err
}

// DataBytes returns the disk the store's data files occupy.
func (s *Store) DataBytes() int64 {
	lsm, vlog := s.db.Size()
	return lsm + vlog
}

// Close closes the store. Closing a Snapshot releases the snapshot and
// leaves the database open.
func (s *Store) Close() error {
	if s.snap != nil {
		s.snap.release()
		return nil
	}
	return s.db.Close()
}

func utxoKey(r Ref) []byte {
	k := make([]byte, 1+refLen)
	k[0] = UTxOTag
	copy(k[1:], r[:])
	return k
}

func encodePoint(p ocommon.Point) []byte {
	v := make([]byte, 8+len(p.Hash))
	binary.BigEndian.PutUint64(v, p.Slot)
	copy(v[8:], p.Hash)
	return v
}

func decodePoint(v []byte) ocommon.Point {
	return ocommon.Point{
		Slot: binary.BigEndian.Uint64(v[:8]),
		Hash: append([]byte(nil), v[8:]...),
	}
}

func readCount(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get(countKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n uint64
	err = item.Value(func(v []byte) error {
		n = binary.BigEndian.Uint64(v)
		return nil
	})
	return n, err
}

func writeCount(txn *badger.Txn, n uint64) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, n)
	return txn.Set(countKey, v)
}

// Tip returns the point of the last applied change, or the bootstrap point.
// ok is false for an empty store.
func (s *Store) Tip() (ocommon.Point, bool, error) {
	var p ocommon.Point
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(tipKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		return item.Value(func(v []byte) error {
			p = decodePoint(v)
			return nil
		})
	})
	return p, ok, err
}

// Count returns the number of live outputs.
func (s *Store) Count() (uint64, error) {
	var n uint64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		n, err = readCount(txn)
		return err
	})
	return n, err
}

// Get returns the CBOR of the live output r. ok is false when r is not live.
func (s *Store) Get(r Ref) ([]byte, bool, error) {
	var out []byte
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(utxoKey(r))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		out, err = item.ValueCopy(nil)
		return err
	})
	return out, ok, err
}

// Bootstrap replaces the whole set with the outputs feed supplies, as of
// point.
func (s *Store) Bootstrap(
	point ocommon.Point,
	feed func(put func(Output) error) error,
) error {
	return s.bootstrap(point, feed, false, false)
}

func (s *Store) bootstrap(
	point ocommon.Point,
	feed func(put func(Output) error) error,
	catchUp bool,
	importing bool,
) error {
	// Clear the markers first, in one transaction: until the final commit
	// below the contents are partial, and a store with no tip and no format
	// marker is rebuilt rather than trusted.
	markers := bootstrapMarkers
	prefixes := bootstrapPrefixes
	if catchUp {
		markers, prefixes = catchUpMarkers, catchUpPrefixes
	}
	if err := s.updateRaw(func(txn *badger.Txn) error {
		if err := txn.Set(bootstrapKey, []byte{1}); err != nil {
			return err
		}
		// An import marks the set before anything of it changes; any other
		// bootstrap replaces what a stopped import left.
		if importing {
			err := txn.Set(importKey, importValue(point, false))
			if err != nil {
				return err
			}
		} else if err := txn.Delete(importKey); err != nil {
			return err
		}
		for _, key := range markers {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: clear markers for bootstrap: %w", err)
	}
	// The journal describes history the new contents do not share.
	// The tip is cleared, so no floor is raised: the contents the floors
	// describe are gone with the rows.
	defer s.rewriteRows()()
	if err := s.db.DropPrefix(prefixes...); err != nil {
		return fmt.Errorf("badgerutxo: drop for bootstrap: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	var n uint64
	sums := aggDeltas{}
	err := feed(func(o Output) error {
		n++
		s.addOutput(sums, o.Cbor, 1)
		return wb.Set(utxoKey(o.Ref), o.Cbor)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: bootstrap feed: %w", err)
	}
	for key, amount := range sums {
		if amount == 0 {
			continue
		}
		if err := wb.Set(
			aggKey([]byte(key)),
			binary.BigEndian.AppendUint64(nil, uint64(amount)), //nolint:gosec // a sum of outputs is never negative
		); err != nil {
			return fmt.Errorf("badgerutxo: bootstrap sums: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: bootstrap flush: %w", err)
	}
	if s.afterBootstrapFlush != nil {
		if err := s.afterBootstrapFlush(); err != nil {
			return err
		}
	}
	return s.updateRaw(func(txn *badger.Txn) error {
		if err := writeCount(txn, n); err != nil {
			return err
		}
		if err := txn.Delete(bootstrapKey); err != nil {
			return err
		}
		if err := txn.Set(formatKey, []byte{formatVersion}); err != nil {
			return err
		}
		if importing {
			if err := txn.Set(importKey, importValue(point, true)); err != nil {
				return err
			}
		}
		return txn.Set(tipKey, encodePoint(point))
	})
}

// Change is one change to the set: outputs it makes not live and outputs it
// makes live, applied in that order. Point is the block (or other point) the
// change belongs to; consecutive changes at one point share a journal record,
// so a rollback can stop between any two blocks of a batch.
type Change struct {
	Point   ocommon.Point
	Added   []Output
	Removed []Ref
}

// Apply makes added live and removed not live, then records point as the
// tip. It reports, but still applies past, removed outputs that are not live
// and added outputs that already are.
func (s *Store) Apply(
	point ocommon.Point,
	added []Output,
	removed []Ref,
) (ApplyResult, error) {
	return s.ApplyBatch(
		&point,
		[]Change{{Point: point, Added: added, Removed: removed}},
	)
}

// ApplyBatch applies changes in order in one transaction, so a reader or a
// crash sees all of them or none, and records tip when it is not nil. The
// transaction also writes a journal record per point the changes belong to,
// holding every touched output's state before that point's changes, which
// Rollback restores. A tip past the last change's point gets a record of its
// own. It reports, but still applies past, removed outputs that are not live
// and added outputs that already are.
func (s *Store) ApplyBatch(
	tip *ocommon.Point,
	changes []Change,
) (ApplyResult, error) {
	return s.ApplyLedgerBatch(tip, LedgerBatch{Changes: changes})
}

// LedgerBatch is one commit's changes to the store: UTxO changes, the
// nonces of the blocks it applied, and a nonce prune.
type LedgerBatch struct {
	Changes []Change
	Nonces  []Nonce
	// PruneNoncesBefore, when non-zero, deletes the non-checkpoint nonces
	// before that slot.
	PruneNoncesBefore uint64
	// DeleteNoncesAfter, when set, rolls the nonces, epochs and snapshots
	// back to that slot before anything else is written: the batch carries a
	// rollback to that slot.
	DeleteNoncesAfter *uint64
	// Epochs are epoch rows written, in order.
	Epochs []EpochRow
	// SnapshotOps are pool stake snapshot changes, in order.
	SnapshotOps []SnapshotOp
	// Summaries are epoch summary rows written, in order, after Epochs.
	Summaries []SummaryRow
	// EraSwitch, when set, records a mid-epoch era switch, after Epochs.
	EraSwitch *EraSwitch
	// NetworkStates and Donations are network state and donation rows
	// written, in order.
	NetworkStates []NetworkStateRow
	Donations     []DonationRow
	// PParams are protocol parameter rows written, in order; their Seq is
	// ignored.
	PParams []PParamsRow
	// Markers are protocol parameter sync marker writes, in order, after
	// PParams.
	Markers []MarkerWrite
	// Accounts are account writes, in order.
	Accounts []AccountPut
	// Rows are writes to the other journaled rows (DReps, DRep delegator
	// links, the dormancy counter), in order, after Accounts.
	Rows []RowPut
	// RewardRecords are the credits and withdrawals the account writes
	// applied.
	RewardRecords []RewardRecord
	// GovernanceIDs, when set, raises the next governance row IDs above the
	// highest proposal and vote ID the batch's rows carry.
	GovernanceIDs *GovernanceIDs
	// RewardBlocks are changes to the reward block rows, in order, after
	// Rows.
	RewardBlocks []RewardBlockOp
}

// ApplyLedgerBatch is ApplyBatch for a whole ledger commit: the nonces and
// the nonce prune are written in the same transaction as the UTxO changes.
// A rollback deletes the nonces above the point it restores, so nonces need
// no journal.
func (s *Store) ApplyLedgerBatch(
	tip *ocommon.Point,
	batch LedgerBatch,
) (ApplyResult, error) {
	changes := batch.Changes
	var res ApplyResult
	var pending bool
	if err := s.view(func(txn *badger.Txn) error {
		var err error
		_, pending, err = readPendingRollback(txn)
		return err
	}); err != nil {
		return ApplyResult{}, err
	}
	if pending {
		return ApplyResult{}, ErrRollbackPending
	}
	var pre *priorMark
	if batch.DeleteNoncesAfter != nil {
		var err error
		pre, err = s.beginBatchRollback(*batch.DeleteNoncesAfter)
		if err != nil {
			return ApplyResult{}, fmt.Errorf(
				"badgerutxo: roll back after slot %d: %w",
				*batch.DeleteNoncesAfter, err,
			)
		}
	}
	// A prefix wipe can hold more rows than one transaction takes, so the
	// rows go in transactions of their own first; the batch's own wipe then
	// finds the prefix empty. A crash between the two leaves the prefix
	// partly deleted, which the wipe, run again, finishes.
	for _, r := range batch.Rows {
		if !r.WipePrefix || len(r.Key) == 0 || r.Key[0] == AccountTag {
			continue
		}
		if err := s.deletePrefixRows(r.Key); err != nil {
			return ApplyResult{}, fmt.Errorf(
				"badgerutxo: wipe prefix %x: %w", r.Key, err,
			)
		}
	}
	err := s.applyChunked(tip, batch, &res, pre)
	if err != nil {
		slot := uint64(0)
		if tip != nil {
			slot = tip.Slot
		} else if len(changes) > 0 {
			slot = changes[len(changes)-1].Point.Slot
		}
		return ApplyResult{}, fmt.Errorf(
			"badgerutxo: apply %d changes at slot %d: %w",
			len(changes), slot, err,
		)
	}
	return res, nil
}

// chainState carries a chunked batch's journal chain from one transaction
// to the next: the point the previous chunk ended at, which is the tip
// before the next chunk's first record.
//
// advances is whether the batch moves the tip, which every record of it
// carries even in a chunk that leaves the tip alone, and baseline is the
// verdict recorded with the first record written.
type chainState struct {
	last     ocommon.Point
	has      bool
	advances bool
	baseline bool
}

// applyLedgerTxn writes one transaction of a batch. changes is the part of
// batch.Changes this transaction applies; the batch's other rows and tip are
// written only when the caller passes them.
func (s *Store) applyLedgerTxn(
	txn *badger.Txn,
	tip *ocommon.Point,
	batch LedgerBatch,
	changes []Change,
	chain *chainState,
	res *ApplyResult,
) error {
	if batch.DeleteNoncesAfter != nil {
		err := rollbackLedgerState(txn, *batch.DeleteNoncesAfter, nil)
		if err != nil {
			return err
		}
	}
	for _, e := range batch.Epochs {
		if err := putEpoch(txn, e); err != nil {
			return err
		}
	}
	for _, op := range batch.SnapshotOps {
		if err := applySnapshotOp(txn, op); err != nil {
			return err
		}
	}
	for _, r := range batch.Summaries {
		if err := putSummary(txn, r); err != nil {
			return err
		}
	}
	if batch.EraSwitch != nil {
		err := txn.Set(eraSwitchKey, batch.EraSwitch.encode())
		if err != nil {
			return err
		}
	}
	for _, r := range batch.NetworkStates {
		if err := putNetworkState(txn, r); err != nil {
			return err
		}
	}
	for _, r := range batch.Donations {
		if err := putDonation(txn, r); err != nil {
			return err
		}
	}
	if err := putPParams(txn, batch.PParams); err != nil {
		return err
	}
	if err := putMarkers(txn, batch.Markers); err != nil {
		return err
	}
	if err := putAccounts(txn, batch.Accounts); err != nil {
		return err
	}
	if err := putRows(txn, batch.Rows); err != nil {
		return err
	}
	if err := raiseGovernanceSeq(txn, batch.GovernanceIDs); err != nil {
		return err
	}
	if err := putRewardBlocks(txn, batch.RewardBlocks); err != nil {
		return err
	}
	if err := putRewardRecords(txn, batch.RewardRecords); err != nil {
		return err
	}
	if err := putNonces(txn, batch.Nonces); err != nil {
		return err
	}
	if batch.PruneNoncesBefore > 0 {
		if err := pruneNonces(txn, batch.PruneNoncesBefore); err != nil {
			return err
		}
	}
	count, err := readCount(txn)
	if err != nil {
		return err
	}
	var rec journalRecord
	if chain.has {
		rec.prevTip, rec.hadTip = chain.last, true
	} else if rec.prevTip, rec.hadTip, err = readTip(txn); err != nil {
		return err
	}
	// A batch with no tip written into a set with none is the baseline:
	// the state a rollback to the empty point restores. A set bootstrapped
	// before the first block records the empty point as its tip, and is as
	// much without one: the genesis outputs written next would otherwise be
	// undone by a read as of the empty point. The batch's chunks share the
	// verdict, so it is carried in chain once a record is written.
	baseline := chain.baseline
	if !chain.has {
		baseline = !chain.advances &&
			(!rec.hadTip || samePoint(rec.prevTip, ocommon.Point{}))
	}
	rec.advancesTip, rec.baseline = chain.advances, baseline
	open := false
	var segment ocommon.Point
	seen := make(map[Ref]struct{})
	// startRecord closes the open record and opens one for point, whose
	// tip before it is the previous record's point.
	startRecord := func(point ocommon.Point) error {
		if open {
			if err := appendJournal(txn, rec); err != nil {
				return err
			}
			rec = journalRecord{
				prevTip:     segment,
				hadTip:      true,
				advancesTip: chain.advances,
				baseline:    baseline,
			}
			seen = make(map[Ref]struct{})
		}
		open = true
		segment = point
		rec.point = point
		return nil
	}
	sums := aggDeltas{}
	// current returns r's value and whether it is live, and remembers
	// its state before the batch the first time the batch touches it.
	current := func(r Ref) ([]byte, bool, error) {
		item, err := txn.Get(utxoKey(r))
		if errors.Is(err, badger.ErrKeyNotFound) {
			if _, ok := seen[r]; !ok {
				seen[r] = struct{}{}
				rec.ops = append(rec.ops, journalOp{ref: r})
			}
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return nil, false, err
		}
		if _, ok := seen[r]; !ok {
			seen[r] = struct{}{}
			rec.ops = append(
				rec.ops,
				journalOp{ref: r, present: true, cbor: v},
			)
		}
		return v, true, nil
	}
	for _, c := range changes {
		if !open || !samePoint(c.Point, segment) {
			if err := startRecord(c.Point); err != nil {
				return err
			}
		}
		for _, r := range c.Removed {
			v, live, err := current(r)
			if err != nil {
				return err
			}
			if !live {
				res.Missing = append(res.Missing, r)
				continue
			}
			if err := txn.Delete(utxoKey(r)); err != nil {
				return err
			}
			s.addOutput(sums, v, -1)
			count--
		}
		for _, o := range c.Added {
			v, live, err := current(o.Ref)
			if err != nil {
				return err
			}
			if live {
				res.Duplicate = append(res.Duplicate, o.Ref)
				s.addOutput(sums, v, -1)
			} else {
				count++
			}
			if err := txn.Set(utxoKey(o.Ref), o.Cbor); err != nil {
				return err
			}
			s.addOutput(sums, o.Cbor, 1)
		}
	}
	if err := writeCount(txn, count); err != nil {
		return err
	}
	if err := applyAggDeltas(txn, sums); err != nil {
		return err
	}
	// A chunk that carried the journal chain up to the tip needs no record
	// of its own for it.
	tipChained := !open && chain.has && tip != nil &&
		samePoint(*tip, chain.last)
	if tip != nil && !tipChained && (!open || !samePoint(*tip, segment)) {
		if err := startRecord(*tip); err != nil {
			return err
		}
	}
	if open {
		if err := appendJournal(txn, rec); err != nil {
			return err
		}
		chain.last, chain.has, chain.baseline = segment, true, baseline
	}
	if tip == nil {
		return nil
	}
	return txn.Set(tipKey, encodePoint(*tip))
}

// Each calls fn for every live output, in key order, until fn returns an
// error.
func (s *Store) Each(fn func(Output) error) error {
	return s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         []byte{UTxOTag},
			PrefetchValues: true,
		})
		defer it.Close()
		for it.Seek([]byte{UTxOTag}); it.Valid(); it.Next() {
			var r Ref
			copy(r[:], it.Item().Key()[1:])
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(Output{Ref: r, Cbor: v}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Sample returns up to n live outputs starting from a random position in
// the key space.
func (s *Store) Sample(n int, rng *rand.Rand) ([]Output, error) {
	var out []Output
	start := make([]byte, 1+refLen)
	start[0] = UTxOTag
	for i := 1; i < len(start); i++ {
		start[i] = byte(rng.Uint32()) //nolint:gosec // truncation is the point
	}
	err := s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         []byte{UTxOTag},
			PrefetchValues: true,
		})
		defer it.Close()
		for pass, from := range [][]byte{start, {UTxOTag}} {
			for it.Seek(from); it.Valid() && len(out) < n; it.Next() {
				if pass == 1 && bytes.Compare(it.Item().Key(), start) >= 0 {
					break
				}
				var r Ref
				copy(r[:], it.Item().Key()[1:])
				v, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}
				out = append(out, Output{Ref: r, Cbor: v})
			}
			if len(out) >= n {
				break
			}
		}
		return nil
	})
	return out, err
}

func samePoint(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

// RemoveOutputs deletes the live outputs among refs whose CBOR removable
// accepts and returns how many it deleted. An output removable rejects stays.
// The count and the sums follow, in the same transaction as each delete.
//
// The removal is not a change of the chain, so it writes no journal record and
// leaves the tip alone: a record would sit newest in the journal and the next
// rollback, whatever its depth, would undo it first and make the outputs live
// again. A journal record that already holds an output keeps its prior state,
// which no rollback reaches for an output no block has touched. It refuses
// while a rollback is pending, like a batch.
func (s *Store) RemoveOutputs(
	refs []Ref,
	removable func(cbor []byte) bool,
) (int, error) {
	entries, _ := s.txnBudget()
	// A delete, a sum write at most and the count are written per output.
	chunk := int(max(entries/3, 1))
	removed := 0
	for start := 0; start < len(refs); start += chunk {
		part := refs[start:min(start+chunk, len(refs))]
		n := 0
		err := s.update(func(txn *badger.Txn) error {
			n = 0
			if _, pending, err := readPendingRollback(txn); err != nil {
				return err
			} else if pending {
				return ErrRollbackPending
			}
			count, err := readCount(txn)
			if err != nil {
				return err
			}
			sums := aggDeltas{}
			for _, r := range part {
				item, err := txn.Get(utxoKey(r))
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				if !removable(v) {
					continue
				}
				if err := txn.Delete(utxoKey(r)); err != nil {
					return err
				}
				s.addOutput(sums, v, -1)
				count--
				n++
			}
			if n == 0 {
				return nil
			}
			if err := writeCount(txn, count); err != nil {
				return err
			}
			return applyAggDeltas(txn, sums)
		})
		if err != nil {
			return removed, fmt.Errorf("badgerutxo: remove outputs: %w", err)
		}
		removed += n
	}
	return removed, nil
}

// Sync makes every write committed so far durable.
func (s *Store) Sync() error {
	return s.db.Sync()
}
