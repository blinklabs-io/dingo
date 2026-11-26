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

package badgerutxo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

// ErrRollbackBeyondJournal reports a rollback to a point older than the
// journal reaches. The set cannot be rolled back there and must be rebuilt.
var ErrRollbackBeyondJournal = errors.New(
	"badgerutxo: rollback point is older than the journal",
)

// ErrPointNotInJournal reports a point that is not the tip or an exact
// predecessor retained by the journal.
var ErrPointNotInJournal = errors.New(
	"badgerutxo: point is not in the journal history",
)

// journalRecord is one ApplyBatch segment: its exact point, whether it
// advanced the tip, the tip before it, and the prior state of every output it
// touched. A point written in parts has a record per part; the tip before a
// later part is the point itself.
type journalRecord struct {
	point       ocommon.Point
	prevTip     ocommon.Point
	hadTip      bool
	advancesTip bool
	baseline    bool
	ops         []journalOp
}

// journalOp is one output's state before a batch: absent, or live with cbor.
type journalOp struct {
	ref     Ref
	present bool
	cbor    []byte
}

// RollbackResult is the net change a rollback made: outputs it made live and
// outputs it made not live.
type RollbackResult struct {
	Added   []Output
	Removed []Ref
}

func journalKey(seq uint64) []byte {
	k := make([]byte, 9)
	k[0] = JournalTag
	binary.BigEndian.PutUint64(k[1:], seq)
	return k
}

func encodeRecord(rec journalRecord) []byte {
	size := 8 + 1 + len(rec.point.Hash) + 1 + 1 + 8 + len(rec.prevTip.Hash) + 4
	for _, op := range rec.ops {
		size += refLen + 1 + 4 + len(op.cbor)
	}
	b := make([]byte, 0, size)
	b = binary.BigEndian.AppendUint64(b, rec.point.Slot)
	b = append(b, byte(len(rec.point.Hash))) //nolint:gosec // a block hash is 32 bytes
	b = append(b, rec.point.Hash...)
	flags := byte(0)
	if rec.advancesTip {
		flags |= 1
	}
	if rec.baseline {
		flags |= 2
	}
	b = append(b, flags)
	if rec.hadTip {
		b = append(b, 1, byte(len(rec.prevTip.Hash))) //nolint:gosec // a block hash is 32 bytes
		b = binary.BigEndian.AppendUint64(b, rec.prevTip.Slot)
		b = append(b, rec.prevTip.Hash...)
	} else {
		b = append(b, 0)
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(rec.ops))) //nolint:gosec // bounded by the batch
	for _, op := range rec.ops {
		b = append(b, op.ref[:]...)
		if op.present {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(op.cbor))) //nolint:gosec // one output
		b = append(b, op.cbor...)
	}
	return b
}

func decodeRecord(b []byte) (journalRecord, error) {
	rec, b, err := decodeRecordHead(b)
	if err != nil {
		return rec, err
	}
	bad := errors.New("badgerutxo: truncated journal record")
	need := func(n int) bool { return len(b) >= n }
	if !need(4) {
		return rec, bad
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	rec.ops = make([]journalOp, 0, n)
	for range n {
		if !need(refLen + 5) {
			return rec, bad
		}
		var op journalOp
		copy(op.ref[:], b[:refLen])
		op.present = b[refLen] == 1
		cl := int(binary.BigEndian.Uint32(b[refLen+1:]))
		b = b[refLen+5:]
		if !need(cl) {
			return rec, bad
		}
		if op.present {
			op.cbor = append([]byte(nil), b[:cl]...)
		}
		b = b[cl:]
		rec.ops = append(rec.ops, op)
	}
	return rec, nil
}

// decodeRecordHead decodes a journal record's point, flags and tip before
// it, and returns the undecoded ops after them.
func decodeRecordHead(b []byte) (journalRecord, []byte, error) {
	var rec journalRecord
	bad := errors.New("badgerutxo: truncated journal record")
	need := func(n int) bool { return len(b) >= n }
	if !need(10) {
		return rec, nil, bad
	}
	rec.point.Slot = binary.BigEndian.Uint64(b)
	pointHashLen := int(b[8])
	b = b[9:]
	if !need(pointHashLen + 1) {
		return rec, nil, bad
	}
	rec.point.Hash = append([]byte(nil), b[:pointHashLen]...)
	flags := b[pointHashLen]
	rec.advancesTip = flags&1 != 0
	rec.baseline = flags&2 != 0
	b = b[pointHashLen+1:]
	if !need(1) {
		return rec, nil, bad
	}
	rec.hadTip = b[0] == 1
	b = b[1:]
	if rec.hadTip {
		if !need(1) {
			return rec, nil, bad
		}
		hl := int(b[0])
		b = b[1:]
		if !need(8 + hl) {
			return rec, nil, bad
		}
		rec.prevTip.Slot = binary.BigEndian.Uint64(b)
		rec.prevTip.Hash = append([]byte(nil), b[8:8+hl]...)
		b = b[8+hl:]
	}
	return rec, b, nil
}

// continues reports whether rec is a later part of a point an earlier record
// began: its tip before it is its own point.
func (rec journalRecord) continues() bool {
	return rec.hadTip && samePoint(rec.prevTip, rec.point)
}

// splitRecordsAfter reports whether a record after slot continues a point,
// so undoing that point takes more than one transaction.
func splitRecordsAfter(txn *badger.Txn, slot uint64) (bool, error) {
	opts := badger.DefaultIteratorOptions
	// Prefetching starts a goroutine per item, and the records scanned are
	// as many as the rollback undoes.
	opts.PrefetchValues = false
	opts.Reverse = true
	opts.Prefix = []byte{JournalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(append([]byte{JournalTag},
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))
	for ; it.Valid(); it.Next() {
		var rec journalRecord
		err := it.Item().Value(func(v []byte) error {
			var err error
			rec, _, err = decodeRecordHead(v)
			return err
		})
		if err != nil {
			return false, err
		}
		if rec.point.Slot <= slot {
			return false, nil
		}
		if rec.continues() {
			return true, nil
		}
	}
	return false, nil
}

func readTip(txn *badger.Txn) (ocommon.Point, bool, error) {
	item, err := txn.Get(tipKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ocommon.Point{}, false, nil
	}
	if err != nil {
		return ocommon.Point{}, false, err
	}
	var p ocommon.Point
	err = item.Value(func(v []byte) error {
		p = decodePoint(v)
		return nil
	})
	return p, true, err
}

// topSeeks counts the scans of the journal and the state journal that
// latestRecord and rollbackRowsWithin start at the top of their prefix. Each
// such scan walks every entry deleted there since the last compaction, so a
// rollback that starts one per undone record costs time quadratic in the
// records; tests read it to show a rollback starts a bounded number.
var topSeeks atomic.Uint64

// journalSeekBelow returns the key a newest-first scan of the journal may start
// at after the record keyed undone was deleted, or nil when it must start at
// the top. Deleting a record leaves the next sequence number alone, so the scan
// may skip what was deleted only while the next sequence number is the one the
// rollback first saw: a later one means a newer record may exist. It returns
// the next sequence number it read.
func journalSeekBelow(
	txn *badger.Txn,
	undone []byte,
	firstNext uint64,
) (below []byte, next uint64, err error) {
	item, err := txn.Get(seqKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	if err := item.Value(func(v []byte) error {
		next = binary.BigEndian.Uint64(v)
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if len(undone) != 9 || next != firstNext {
		return nil, next, nil
	}
	return undone, next, nil
}

func appendJournal(txn *badger.Txn, rec journalRecord) error {
	var seq uint64
	item, err := txn.Get(seqKey)
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
	case err != nil:
		return err
	default:
		if err := item.Value(func(v []byte) error {
			seq = binary.BigEndian.Uint64(v)
			return nil
		}); err != nil {
			return err
		}
	}
	if err := txn.Set(journalKey(seq), encodeRecord(rec)); err != nil {
		return err
	}
	return txn.Set(seqKey, binary.BigEndian.AppendUint64(nil, seq+1))
}

// latestRecord returns the newest journal record and its key. It reads one
// item, so the iterator does not prefetch values, which costs a goroutine per
// prefetched item.
//
// below, when not nil, is the key of a record the caller has just deleted and
// that was the newest at the time. The scan starts there rather than at the
// top of the prefix, which would walk every entry the caller has deleted since
// the journal last compacted. The caller must pass nil when a record newer
// than below may exist.
func latestRecord(
	txn *badger.Txn,
	below []byte,
) ([]byte, journalRecord, bool, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	opts.Prefix = []byte{JournalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	if below != nil {
		it.Seek(below)
	} else {
		topSeeks.Add(1)
		it.Seek(
			append(
				[]byte{JournalTag},
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
			),
		)
	}
	if !it.Valid() {
		return nil, journalRecord{}, false, nil
	}
	item := it.Item()
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, journalRecord{}, false, err
	}
	rec, err := decodeRecord(v)
	if err != nil {
		return nil, journalRecord{}, false, err
	}
	return item.KeyCopy(nil), rec, true, nil
}

// journalReaches returns nil only when point is the exact tip or an exact
// predecessor retained by the journal.
func journalReaches(txn *badger.Txn, point ocommon.Point) error {
	return visitJournalAfter(txn, point, nil)
}

// rollbackReaches is journalReaches for a rollback, which also needs the
// state journal: a row family loaded at the tip has no entries before it, so a
// rollback to a point below the journal floor, other than to the tip itself,
// would leave the loaded rows of the blocks it undoes. A caller treats it as a
// rollback past the journal, which rebuilds the set.
func rollbackReaches(txn *badger.Txn, point ocommon.Point) error {
	if err := journalReaches(txn, point); err != nil {
		return err
	}
	tip, ok, err := readTip(txn)
	if err != nil {
		return err
	}
	if ok && samePoint(tip, point) {
		return nil
	}
	floor, err := readLoadFloor(txn)
	if err != nil {
		return err
	}
	if point.Slot+1 < floor {
		return ErrRollbackBeyondJournal
	}
	return nil
}

func readJournalFloor(txn *badger.Txn) (uint64, error) {
	return readFloor(txn, journalFloorKey)
}

// readLoadFloor returns the load floor, or zero when no family a load can
// have left without journal entries holds a row: a rollback below the load
// then orphans nothing.
func readLoadFloor(txn *badger.Txn) (uint64, error) {
	floor, err := readFloor(txn, loadFloorKey)
	if err != nil || floor == 0 {
		return 0, err
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = nil
	it := txn.NewIterator(opts)
	defer it.Close()
	for _, tag := range journaledFamilyTags {
		it.Seek([]byte{tag})
		if it.Valid() && it.Item().Key()[0] == tag {
			return floor, nil
		}
	}
	return 0, nil
}

func readFloor(txn *badger.Txn, key []byte) (uint64, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var floor uint64
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return errors.New("badgerutxo: malformed journal floor")
		}
		floor = binary.BigEndian.Uint64(v)
		return nil
	})
	return floor, err
}

// raiseJournalFloor raises the floor to at least floor. It runs before the
// journal entries below it are dropped, so a reader that finds the entries
// gone also finds the floor.
func (s *Store) raiseJournalFloor(floor uint64) error {
	return s.raiseFloor(journalFloorKey, floor)
}

func (s *Store) raiseFloor(key []byte, floor uint64) error {
	return s.update(func(txn *badger.Txn) error {
		cur, err := readFloor(txn, key)
		if err != nil {
			return err
		}
		if cur >= floor {
			return nil
		}
		return txn.Set(key, binary.BigEndian.AppendUint64(nil, floor))
	})
}

// StateReachesSlot returns ErrRollbackBeyondJournal unless the computed rows
// read by RowAt and RowsAt are exact as of slot: the UTxO journal reaches it
// and the state journal holds every change after it. A slot at or past the
// tip is always reachable.
func (s *Store) StateReachesSlot(slot uint64) error {
	return s.view(func(txn *badger.Txn) error {
		if err := journalReachesSlot(txn, slot); err != nil {
			return err
		}
		tip, ok, err := readTip(txn)
		if err != nil || !ok || slot >= tip.Slot {
			return err
		}
		floor, err := readJournalFloor(txn)
		if err != nil {
			return err
		}
		if slot+1 < floor {
			return ErrRollbackBeyondJournal
		}
		return nil
	})
}

// journalReachesSlot returns ErrRollbackBeyondJournal unless the journal holds
// every change after slot: slot is at or past the tip, or at or past the point
// the oldest record's batch started from. Unlike journalReaches it takes no
// hash, since the rows it guards are keyed by slot. A journal with no records,
// as after a bootstrap, covers only the tip itself.
func journalReachesSlot(txn *badger.Txn, slot uint64) error {
	tip, ok, err := readTip(txn)
	if err != nil || !ok || slot >= tip.Slot {
		return err
	}
	oldest, ok, err := oldestRecord(txn)
	if err != nil {
		return err
	}
	if !ok || (oldest.point.Slot > slot &&
		(!oldest.hadTip || oldest.prevTip.Slot > slot)) {
		return ErrRollbackBeyondJournal
	}
	return nil
}

// oldestRecord returns the oldest journal record.
func oldestRecord(txn *badger.Txn) (journalRecord, bool, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = []byte{JournalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Rewind()
	if !it.Valid() {
		return journalRecord{}, false, nil
	}
	v, err := it.Item().ValueCopy(nil)
	if err != nil {
		return journalRecord{}, false, err
	}
	rec, err := decodeRecord(v)
	return rec, true, err
}

// visitJournalAfter visits records after point, newest first, while proving
// from the same snapshot that point is in the retained full-point lineage.
func visitJournalAfter(
	txn *badger.Txn,
	point ocommon.Point,
	visit func(journalRecord) error,
) error {
	tip, ok, err := readTip(txn)
	if err != nil {
		return err
	}
	if !ok {
		if point.Slot == 0 && len(point.Hash) == 0 {
			return nil
		}
		return ErrPointNotInJournal
	}
	if point.Slot > tip.Slot {
		return ErrPointNotInJournal
	}
	targetIsTip := samePoint(tip, point)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	opts.Prefix = []byte{JournalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(
		append(
			[]byte{JournalTag},
			0xff,
			0xff,
			0xff,
			0xff,
			0xff,
			0xff,
			0xff,
			0xff,
		),
	)
	oldestBoundary := tip
	foundPredecessor := false
	for ; it.Valid(); it.Next() {
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		rec, err := decodeRecord(v)
		if err != nil {
			return err
		}
		if rec.advancesTip && samePoint(rec.point, point) {
			return nil
		}
		if rec.baseline {
			if point.Slot == 0 && len(point.Hash) == 0 {
				return nil
			}
			break
		}
		if visit != nil {
			if err := visit(rec); err != nil {
				return err
			}
		}
		if rec.advancesTip {
			if rec.hadTip {
				oldestBoundary = rec.prevTip
				foundPredecessor = foundPredecessor ||
					samePoint(rec.prevTip, point)
			} else {
				oldestBoundary = ocommon.Point{}
				foundPredecessor = foundPredecessor ||
					(point.Slot == 0 && len(point.Hash) == 0)
			}
		}
	}
	if targetIsTip || foundPredecessor {
		return nil
	}
	if point.Slot < oldestBoundary.Slot {
		return ErrRollbackBeyondJournal
	}
	return ErrPointNotInJournal
}

// RollbackReaches reports whether the journal reaches back to point: false
// when Rollback(point) would return ErrRollbackBeyondJournal. A point that is
// ahead of the tip, or that the retained lineage does not hold, is not one
// the journal is too short for.
func (s *Store) RollbackReaches(point ocommon.Point) (bool, error) {
	err := s.view(func(txn *badger.Txn) error {
		return rollbackReaches(txn, point)
	})
	switch {
	case err == nil, errors.Is(err, ErrPointNotInJournal):
		return true, nil
	case errors.Is(err, ErrRollbackBeyondJournal):
		return false, nil
	}
	return false, err
}

// Rollback undoes, newest first, every journaled batch after point in the
// retained lineage, then records point as the tip, and returns the net change.
// Each batch is undone in its own transaction that also moves the tip back to the
// tip before that batch, so a crash part way leaves the set at a point it
// really held. It returns ErrRollbackBeyondJournal, changing nothing, when
// the journal does not reach back to point.
//
// A rollback that cannot stay inside such transactions records point as
// pending first: when rows under a range tombstone it undoes are deleted
// ahead of its transactions, when the state journal entries it undoes are
// more than one transaction takes, which it then undoes in several, newest
// first, before any batch, or when it undoes a point written in parts, whose
// tip stays on the point until its first part is undone. Until the last transaction clears the mark the
// set holds a mix of states, so a store that starts with the mark finishes
// the rollback (ResumeRollback), and ApplyLedgerBatch refuses until then.
// Readers wait while the mark is set.
func (s *Store) Rollback(point ocommon.Point) (RollbackResult, error) {
	if _, err := s.ResumeRollback(); err != nil {
		return RollbackResult{}, err
	}
	if err := s.view(func(txn *badger.Txn) error {
		return rollbackReaches(txn, point)
	}); err != nil {
		return RollbackResult{}, err
	}
	return s.rollback(point, false)
}

// ResumeRollback finishes a rollback a crash or an error stopped between its
// transactions, and reports whether there was one.
func (s *Store) ResumeRollback() (bool, error) {
	var (
		target  ocommon.Point
		pending bool
	)
	if err := s.view(func(txn *badger.Txn) error {
		var err error
		target, pending, err = readPendingRollback(txn)
		if err != nil || !pending {
			return err
		}
		return rollbackReaches(txn, target)
	}); err != nil {
		return false, fmt.Errorf("badgerutxo: resume rollback: %w", err)
	}
	if !pending {
		return false, nil
	}
	if _, err := s.rollback(target, true); err != nil {
		return false, err
	}
	return true, nil
}

func readPendingRollback(txn *badger.Txn) (ocommon.Point, bool, error) {
	item, err := txn.Get(rollbackPendingKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ocommon.Point{}, false, nil
	}
	if err != nil {
		return ocommon.Point{}, false, err
	}
	var p ocommon.Point
	err = item.Value(func(v []byte) error {
		if len(v) < 8 {
			return fmt.Errorf(
				"badgerutxo: pending rollback mark is %d bytes", len(v),
			)
		}
		p = decodePoint(v)
		return nil
	})
	return p, true, err
}

// rollback undoes the batches after point. marked is whether the pending
// mark is already set.
func (s *Store) rollback(point ocommon.Point, marked bool) (RollbackResult, error) {
	defer s.rewriteRows()()
	entries, maxBytes := s.txnBudget()
	prefixes, err := s.tombstonedPrefixes(point.Slot)
	if err != nil {
		return RollbackResult{}, err
	}
	var heavy, split bool
	if err := s.view(func(txn *badger.Txn) error {
		var err error
		heavy, err = rowsAfterExceed(txn, point.Slot, entries, maxBytes)
		if err != nil {
			return err
		}
		// A credit or withdrawal record per account, as many as the rows.
		heavy = heavy ||
			slotKeysAfterExceed(txn, RewardRecordTag, point.Slot, entries)
		// Undoing a point written in parts leaves its tip on the point
		// until the last part is undone, a state the set never held.
		split, err = splitRecordsAfter(txn, point.Slot)
		return err
	}); err != nil {
		return RollbackResult{}, err
	}
	hook := s.newStepHook()
	// cur is where the state journal scan resumes between transactions.
	var cur rowsCursor
	if marked || len(prefixes) > 0 || heavy || split {
		if !marked {
			if err := s.update(func(txn *badger.Txn) error {
				return txn.Set(rollbackPendingKey, encodePoint(point))
			}); err != nil {
				return RollbackResult{}, fmt.Errorf(
					"badgerutxo: mark rollback to slot %d pending: %w",
					point.Slot, err,
				)
			}
			marked = true
		}
		if err := s.deletePrefixes(prefixes); err != nil {
			return RollbackResult{}, err
		}
		if err := hook(); err != nil {
			return RollbackResult{}, err
		}
		s.gate.Lock()
		defer s.gate.Unlock()
		if heavy {
			last, err := s.drainRows(point.Slot, entries, maxBytes, hook)
			if err != nil {
				return RollbackResult{}, err
			}
			cur.below = last
		}
	}
	// before is each output's state before the rollback, recorded the first
	// time an undone batch touches it; after is its latest state.
	type state struct {
		live bool
		cbor []byte
	}
	before := make(map[Ref]state)
	after := make(map[Ref]state)
	var order []Ref
	// undone is the key of the record the last committed transaction deleted.
	var (
		undone    []byte
		firstNext uint64
	)
	for {
		done := false
		var (
			deleted []byte
			seen    uint64
		)
		err := s.update(func(txn *badger.Txn) error {
			deleted = nil
			current, hasTip, err := readTip(txn)
			if err != nil {
				return err
			}
			from, next, err := journalSeekBelow(txn, undone, firstNext)
			if err != nil {
				return err
			}
			seen = next
			if undone != nil && from == nil {
				// A record newer than the last undone may exist, and with it
				// state journal entries newer than the ones undone.
				cur.below = nil
			}
			key, rec, ok, err := latestRecord(txn, from)
			if err != nil {
				return err
			}
			atPoint := (!hasTip && point.Slot == 0 && len(point.Hash) == 0) ||
				(hasTip && samePoint(current, point))
			atRetainedRecord := ok && rec.advancesTip &&
				samePoint(rec.point, point)
			atBaseline := ok && rec.baseline &&
				point.Slot == 0 && len(point.Hash) == 0
			if atRetainedRecord || atBaseline || (!ok && atPoint) {
				done = true
				// Each undone record already moved the tip back to the tip
				// before it, so the tip is point.
				err := rollbackLedgerState(txn, point.Slot, &cur)
				if err != nil {
					return err
				}
				if marked {
					return txn.Delete(rollbackPendingKey)
				}
				return nil
			}
			if !ok {
				return ErrPointNotInJournal
			}
			count, err := readCount(txn)
			if err != nil {
				return err
			}
			sums := aggDeltas{}
			for _, op := range rec.ops {
				k := utxoKey(op.ref)
				item, err := txn.Get(k)
				cur := state{}
				switch {
				case errors.Is(err, badger.ErrKeyNotFound):
				case err != nil:
					return err
				default:
					cur.live = true
					if cur.cbor, err = item.ValueCopy(nil); err != nil {
						return err
					}
				}
				if _, seen := before[op.ref]; !seen {
					before[op.ref] = cur
					order = append(order, op.ref)
				}
				after[op.ref] = state{live: op.present, cbor: op.cbor}
				switch {
				case op.present && !cur.live:
					count++
				case !op.present && cur.live:
					count--
				}
				if cur.live {
					s.addOutput(sums, cur.cbor, -1)
					if err := s.deleteIndex(txn, op.ref, cur.cbor); err != nil {
						return err
					}
				}
				if op.present {
					s.addOutput(sums, op.cbor, 1)
				}
				if op.present {
					if err := txn.Set(k, op.cbor); err != nil {
						return err
					}
					if err := s.setIndex(txn, op.ref, op.cbor); err != nil {
						return err
					}
				} else if cur.live {
					if err := txn.Delete(k); err != nil {
						return err
					}
				}
			}
			if err := writeCount(txn, count); err != nil {
				return err
			}
			if err := applyAggDeltas(txn, sums); err != nil {
				return err
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
			deleted = key
			if !rec.advancesTip {
				return nil
			}
			if rec.hadTip {
				// Nonces above the restored tip belong to undone blocks.
				err := rollbackLedgerState(txn, rec.prevTip.Slot, &cur)
				if err != nil {
					return err
				}
				return txn.Set(tipKey, encodePoint(rec.prevTip))
			}
			return txn.Set(tipKey, encodePoint(ocommon.Point{}))
		})
		if err != nil {
			return RollbackResult{}, fmt.Errorf(
				"badgerutxo: rollback to slot %d: %w", point.Slot, err,
			)
		}
		if done {
			break
		}
		if undone == nil {
			firstNext = seen
		}
		undone = deleted
		if cur.undone != nil {
			cur.below = cur.undone
		}
		if marked {
			if err := hook(); err != nil {
				return RollbackResult{}, err
			}
		}
	}
	var res RollbackResult
	for _, r := range order {
		b, a := before[r], after[r]
		switch {
		// A changed value is reported as added, which a consumer applies
		// as an overwrite.
		case a.live && (!b.live || !bytes.Equal(a.cbor, b.cbor)):
			res.Added = append(res.Added, Output{Ref: r, Cbor: a.cbor})
		case !a.live && b.live:
			res.Removed = append(res.Removed, r)
		}
	}
	return res, nil
}

// PruneJournal deletes the journal records of batches, and the account
// journal entries of changes, before slot, which no rollback will reach. Records are in slot order, so it stops at the first
// record at or after slot.
func (s *Store) PruneJournal(slot uint64) error {
	// The journal floor never passes the anchor, so a rollback to any point
	// in the volatile window, and the view of the anchor itself, stay
	// reachable.
	if a := s.anchor.Load(); a != nil && slot > a.Slot {
		slot = a.Slot
	}
	if err := s.raiseJournalFloor(slot); err != nil {
		return fmt.Errorf("badgerutxo: raise journal floor: %w", err)
	}
	var keys [][]byte
	if err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{JournalTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) < 8 || binary.BigEndian.Uint64(v) >= slot {
				return nil
			}
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: scan journal: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			return fmt.Errorf("badgerutxo: prune journal: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return err
	}
	if err := s.pruneStateJournal(slot); err != nil {
		return err
	}
	if err := s.pruneRewardRecords(slot); err != nil {
		return err
	}
	if err := s.pruneGovernanceBoundaries(slot); err != nil {
		return err
	}
	return s.pruneClosureUtxoEffects(slot)
}

// RollbackPreview returns the net change Rollback(point) would make, without
// changing the store, so a caller can see the rolled-back set before it
// rolls back. It returns ErrRollbackBeyondJournal when the journal does not
// reach point.
func (s *Store) RollbackPreview(point ocommon.Point) (RollbackResult, error) {
	var res RollbackResult
	err := s.view(func(txn *badger.Txn) error {
		if !slotOnly(point) {
			if err := rollbackReaches(txn, point); err != nil {
				return err
			}
		}
		var err error
		res, err = previewIn(txn, point)
		return err
	})
	return res, err
}

// slotOnly reports whether point names a slot rather than a block: it has no
// hash and is not the empty origin point. A read as of such a point, such as
// the stake at an epoch boundary slot that need hold no block, takes the set
// as it stood after the last commit at or before the slot; every other point
// must be one the journal's lineage holds.
func slotOnly(point ocommon.Point) bool {
	return len(point.Hash) == 0 && point.Slot != 0
}

// visitJournalAfterSlot visits the records of commits after slot, newest
// first, once journalReachesSlot has shown the journal holds them all.
func visitJournalAfterSlot(
	txn *badger.Txn,
	slot uint64,
	visit func(journalRecord) error,
) error {
	if err := journalReachesSlot(txn, slot); err != nil {
		return err
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	opts.Prefix = []byte{JournalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(append([]byte{JournalTag},
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))
	for ; it.Valid(); it.Next() {
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		rec, err := decodeRecord(v)
		if err != nil {
			return err
		}
		if rec.point.Slot <= slot {
			return nil
		}
		if err := visit(rec); err != nil {
			return err
		}
	}
	return nil
}

// previewIn is RollbackPreview as txn sees the store.
func previewIn(txn *badger.Txn, point ocommon.Point) (RollbackResult, error) {
	var res RollbackResult
	// final is each touched output's state before the oldest undone
	// record that touched it: its state once rolled back.
	type state struct {
		live bool
		cbor []byte
	}
	final := make(map[Ref]state)
	var order []Ref
	visit := func(rec journalRecord) error {
		for _, op := range rec.ops {
			if _, ok := final[op.ref]; !ok {
				order = append(order, op.ref)
			}
			final[op.ref] = state{live: op.present, cbor: op.cbor}
		}
		return nil
	}
	var err error
	if slotOnly(point) {
		err = visitJournalAfterSlot(txn, point.Slot, visit)
	} else {
		err = visitJournalAfter(txn, point, visit)
	}
	if err != nil {
		return res, err
	}
	for _, r := range order {
		f := final[r]
		item, err := txn.Get(utxoKey(r))
		var cur []byte
		live := true
		switch {
		case errors.Is(err, badger.ErrKeyNotFound):
			live = false
		case err != nil:
			return res, err
		default:
			if cur, err = item.ValueCopy(nil); err != nil {
				return res, err
			}
		}
		switch {
		case f.live && (!live || !bytes.Equal(f.cbor, cur)):
			res.Added = append(res.Added, Output{Ref: r, Cbor: f.cbor})
		case !f.live && live:
			res.Removed = append(res.Removed, r)
		}
	}
	return res, nil
}

// AsOf is a read-only view of the set as it was at a point the journal
// still reaches. Every read comes from one snapshot of the store, so a
// commit during the reads cannot mix two states. Close releases the
// snapshot.
type AsOf struct {
	txn       *badger.Txn
	release   func()
	closeOnce sync.Once
	// changed holds each output whose state at the point differs from the
	// snapshot's: its CBOR, or nil when it was not live.
	changed map[Ref][]byte
}

// AsOf opens a view of the set as of point. A point at or past the tip sees
// the set as it is. It returns ErrRollbackBeyondJournal when the journal does
// not reach point. On a snapshot Store, the view shares the snapshot's commit
// boundary and must be closed before the parent Store's Close can finish.
func (s *Store) AsOf(point ocommon.Point) (*AsOf, error) {
	var txn *badger.Txn
	var release func()
	var err error
	if s.snap != nil {
		txn, err = s.snap.acquire()
		if err != nil {
			return nil, err
		}
		release = s.snap.done
	} else {
		txn, err = s.readTxn()
		if err != nil {
			return nil, err
		}
		release = txn.Discard
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	a, err := asOfIn(txn, point)
	if err != nil {
		return nil, err
	}
	a.release = release
	transferred = true
	return a, nil
}

// asOfIn builds the view of point over txn, which the view then owns.
func asOfIn(txn *badger.Txn, point ocommon.Point) (*AsOf, error) {
	if err := checkBootstrapComplete(txn); err != nil {
		return nil, err
	}
	res, err := previewIn(txn, point)
	if err != nil {
		return nil, err
	}
	changed := make(map[Ref][]byte, len(res.Added)+len(res.Removed))
	for _, o := range res.Added {
		changed[o.Ref] = o.Cbor
	}
	for _, r := range res.Removed {
		changed[r] = nil
	}
	return &AsOf{txn: txn, changed: changed, release: txn.Discard}, nil
}

// Close releases the view's snapshot.
func (a *AsOf) Close() {
	a.closeOnce.Do(a.release)
}

// Get returns the CBOR of r if it was live at the view's point.
func (a *AsOf) Get(r Ref) ([]byte, bool, error) {
	if c, ok := a.changed[r]; ok {
		return c, c != nil, nil
	}
	item, err := a.txn.Get(utxoKey(r))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	v, err := item.ValueCopy(nil)
	return v, err == nil, err
}

// Each calls fn for every output live at the view's point, in reference
// order: the snapshot's outputs the point left alone, merged with the ones the
// journal restored.
func (a *AsOf) Each(fn func(Output) error) error {
	restored := make([]Output, 0, len(a.changed))
	for r, c := range a.changed {
		if c != nil {
			restored = append(restored, Output{Ref: r, Cbor: c})
		}
	}
	slices.SortFunc(restored, func(x, y Output) int {
		return bytes.Compare(x.Ref[:], y.Ref[:])
	})
	it := a.txn.NewIterator(badger.IteratorOptions{
		Prefix:         []byte{UTxOTag},
		PrefetchValues: true,
	})
	defer it.Close()
	for it.Seek([]byte{UTxOTag}); it.Valid(); it.Next() {
		var r Ref
		copy(r[:], it.Item().Key()[1:])
		if _, ok := a.changed[r]; ok {
			continue
		}
		for len(restored) > 0 && bytes.Compare(restored[0].Ref[:], r[:]) < 0 {
			if err := fn(restored[0]); err != nil {
				return err
			}
			restored = restored[1:]
		}
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := fn(Output{Ref: r, Cbor: v}); err != nil {
			return err
		}
	}
	for _, o := range restored {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}

// ErrRollbackPending reports a batch refused while a rollback is part done.
var ErrRollbackPending = errors.New("badgerutxo: a rollback is pending")

// drainRows undoes the state journal entries after slot in transactions of
// their own, newest first, calling step after each. A transaction that
// proves too big for the plan is retried with half of it.
func (s *Store) drainRows(
	slot uint64,
	entries, maxBytes int64,
	step func() error,
) ([]byte, error) {
	// undone is the last state journal entry a committed transaction undid.
	var undone []byte
	for {
		var (
			more bool
			last []byte
		)
		err := s.update(func(txn *badger.Txn) error {
			if err := bumpRollbackSeq(txn); err != nil {
				return err
			}
			var err error
			more, last, err = rollbackRowsWithin(
				txn, slot, undone, entries, maxBytes,
			)
			if err != nil || more {
				return err
			}
			more, err = deleteSlotKeysWithin(
				txn, RewardRecordTag, slot, entries,
			)
			return err
		})
		if errors.Is(err, badger.ErrTxnTooBig) && (entries > 1 || maxBytes > 1) {
			entries, maxBytes = max(entries/2, 1), max(maxBytes/2, 1)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf(
				"badgerutxo: rollback to slot %d: %w", slot, err,
			)
		}
		if last != nil {
			undone = last
		}
		if err := step(); err != nil {
			return nil, err
		}
		if !more {
			return undone, nil
		}
	}
}

// newStepHook returns the function a rollback calls between its transactions,
// which numbers the steps for the test seam.
func (s *Store) newStepHook() func() error {
	step := 0
	return func() error {
		if s.afterRollbackStep == nil {
			return nil
		}
		err := s.afterRollbackStep(step)
		step++
		return err
	}
}

// priorMark is the failure mark a batch found before it set its own, which
// its last transaction puts back.
type priorMark struct {
	slot uint64
	had  bool
}

// beginBatchRollback prepares a batch that carries a rollback to slot. The
// rows under the range tombstones it undoes and the state journal entries
// above slot, when they are more than the batch's own transaction takes, are
// dropped and undone ahead of it in transactions of their own. Those leave the
// set between two states, so a failure mark at slot+1 goes in first: a start
// that finds it rewinds to the tip before the oldest commit after slot, which
// the rollback was going to, and Rollback finishes what was begun. The batch's
// last transaction puts back the mark found here. It returns nil when the
// batch needs none of that.
func (s *Store) beginBatchRollback(slot uint64) (*priorMark, error) {
	entries, maxBytes := s.txnBudget()
	prefixes, err := s.tombstonedPrefixes(slot)
	if err != nil {
		return nil, err
	}
	var heavy bool
	if err := s.view(func(txn *badger.Txn) error {
		var err error
		heavy, err = rowsAfterExceed(txn, slot, entries, maxBytes)
		heavy = heavy || slotKeysAfterExceed(txn, RewardRecordTag, slot, entries)
		return err
	}); err != nil {
		return nil, err
	}
	if len(prefixes) == 0 && !heavy {
		return nil, nil
	}
	var pre priorMark
	if err := s.update(func(txn *badger.Txn) error {
		var err error
		pre.slot, pre.had, err = markInFlight(txn, slot+1)
		return err
	}); err != nil {
		return nil, err
	}
	hook := s.newStepHook()
	if err := s.deletePrefixes(prefixes); err != nil {
		return nil, err
	}
	if err := hook(); err != nil {
		return nil, err
	}
	if heavy {
		s.gate.Lock()
		defer s.gate.Unlock()
		if _, err := s.drainRows(slot, entries, maxBytes, hook); err != nil {
			return nil, err
		}
	}
	return &pre, nil
}
