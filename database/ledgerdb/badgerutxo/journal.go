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

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

// ErrRollbackBeyondJournal reports a rollback to a point older than the
// journal reaches. The set cannot be rolled back there and must be rebuilt.
var ErrRollbackBeyondJournal = errors.New(
	"badgerutxo: rollback point is older than the journal",
)

// journalRecord is one ApplyBatch: the slot its changes belong to, the tip
// before it, and the state before it of every output it touched.
type journalRecord struct {
	slot    uint64
	prevTip ocommon.Point
	hadTip  bool
	ops     []journalOp
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
	k[0] = journalTag
	binary.BigEndian.PutUint64(k[1:], seq)
	return k
}

func encodeRecord(rec journalRecord) []byte {
	size := 8 + 1 + 1 + 8 + len(rec.prevTip.Hash) + 4
	for _, op := range rec.ops {
		size += refLen + 1 + 4 + len(op.cbor)
	}
	b := make([]byte, 0, size)
	b = binary.BigEndian.AppendUint64(b, rec.slot)
	if rec.hadTip {
		b = append(
			b,
			1,
			byte(len(rec.prevTip.Hash)),
		) //nolint:gosec // a block hash is 32 bytes
		b = binary.BigEndian.AppendUint64(b, rec.prevTip.Slot)
		b = append(b, rec.prevTip.Hash...)
	} else {
		b = append(b, 0)
	}
	b = binary.BigEndian.AppendUint32(
		b,
		uint32(len(rec.ops)),
	) //nolint:gosec // bounded by the batch
	for _, op := range rec.ops {
		b = append(b, op.ref[:]...)
		if op.present {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		b = binary.BigEndian.AppendUint32(
			b,
			uint32(len(op.cbor)),
		) //nolint:gosec // one output
		b = append(b, op.cbor...)
	}
	return b
}

func decodeRecord(b []byte) (journalRecord, error) {
	var rec journalRecord
	bad := errors.New("badgerutxo: truncated journal record")
	need := func(n int) bool { return len(b) >= n }
	if !need(9) {
		return rec, bad
	}
	rec.slot = binary.BigEndian.Uint64(b)
	rec.hadTip = b[8] == 1
	b = b[9:]
	if rec.hadTip {
		if !need(1) {
			return rec, bad
		}
		hl := int(b[0])
		b = b[1:]
		if !need(8 + hl) {
			return rec, bad
		}
		rec.prevTip.Slot = binary.BigEndian.Uint64(b)
		rec.prevTip.Hash = append([]byte(nil), b[8:8+hl]...)
		b = b[8+hl:]
	}
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

// latestRecord returns the newest journal record and its key.
func latestRecord(txn *badger.Txn) ([]byte, journalRecord, bool, error) {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.Prefix = []byte{journalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(
		append(
			[]byte{journalTag},
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

// oldestRecord returns the oldest journal record.
// journalReaches returns ErrRollbackBeyondJournal unless the set's state at
// point can be restored: point is at or past the tip, or the journal holds
// every change after it. Records cover the tip's history from the oldest
// record's previous tip, so a journal with no records, as after a bootstrap,
// covers only the tip itself.
func journalReaches(txn *badger.Txn, point ocommon.Point) error {
	item, err := txn.Get(tipKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var tip ocommon.Point
	if err := item.Value(func(v []byte) error {
		tip = decodePoint(v)
		return nil
	}); err != nil {
		return err
	}
	if point.Slot >= tip.Slot {
		return nil
	}
	oldest, ok, err := oldestRecord(txn)
	if err != nil {
		return err
	}
	if !ok || (oldest.slot > point.Slot &&
		(!oldest.hadTip || oldest.prevTip.Slot > point.Slot)) {
		return ErrRollbackBeyondJournal
	}
	return nil
}

func oldestRecord(txn *badger.Txn) (journalRecord, bool, error) {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{journalTag}
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

// Rollback undoes, newest first, every journaled batch whose slot is after
// point's, then records point as the tip, and returns the net change. Each
// batch is undone in its own transaction that also moves the tip back to the
// tip before that batch, so a crash part way leaves the set at a point it
// really held. It returns ErrRollbackBeyondJournal, changing nothing, when
// the journal does not reach back to point.
func (s *Store) Rollback(point ocommon.Point) (RollbackResult, error) {
	if err := s.db.View(func(txn *badger.Txn) error {
		return journalReaches(txn, point)
	}); err != nil {
		return RollbackResult{}, err
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
	for {
		done := false
		err := s.db.Update(func(txn *badger.Txn) error {
			key, rec, ok, err := latestRecord(txn)
			if err != nil {
				return err
			}
			if !ok || rec.slot <= point.Slot {
				done = true
				return txn.Set(tipKey, encodePoint(point))
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
				}
				if op.present {
					s.addOutput(sums, op.cbor, 1)
				}
				if op.present {
					if err := txn.Set(k, op.cbor); err != nil {
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
			if rec.hadTip {
				return txn.Set(tipKey, encodePoint(rec.prevTip))
			}
			return nil
		})
		if err != nil {
			return RollbackResult{}, fmt.Errorf(
				"badgerutxo: rollback to slot %d: %w", point.Slot, err,
			)
		}
		if done {
			break
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

// PruneJournal deletes the journal records of batches before slot, which no
// rollback will reach. Records are in slot order, so it stops at the first
// record at or after slot.
func (s *Store) PruneJournal(slot uint64) error {
	var keys [][]byte
	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{journalTag}
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
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			return fmt.Errorf("badgerutxo: prune journal: %w", err)
		}
	}
	return wb.Flush()
}

// RollbackPreview returns the net change Rollback(point) would make, without
// changing the store, so a caller can see the rolled-back set before it
// rolls back. It returns ErrRollbackBeyondJournal when the journal does not
// reach point.
func (s *Store) RollbackPreview(point ocommon.Point) (RollbackResult, error) {
	var res RollbackResult
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		res, err = previewIn(txn, point)
		return err
	})
	return res, err
}

// previewIn is RollbackPreview as txn sees the store.
func previewIn(txn *badger.Txn, point ocommon.Point) (RollbackResult, error) {
	var res RollbackResult
	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	opts.Prefix = []byte{journalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	// final is each touched output's state before the oldest undone
	// record that touched it: its state once rolled back.
	type state struct {
		live bool
		cbor []byte
	}
	final := make(map[Ref]state)
	var order []Ref
	if err := journalReaches(txn, point); err != nil {
		return res, err
	}
	it.Seek(
		append(
			[]byte{journalTag},
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
	for ; it.Valid(); it.Next() {
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return res, err
		}
		rec, err := decodeRecord(v)
		if err != nil {
			return res, err
		}
		if rec.slot <= point.Slot {
			break
		}
		for _, op := range rec.ops {
			if _, ok := final[op.ref]; !ok {
				order = append(order, op.ref)
			}
			final[op.ref] = state{live: op.present, cbor: op.cbor}
		}
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
	txn *badger.Txn
	// changed holds each output whose state at the point differs from the
	// snapshot's: its CBOR, or nil when it was not live.
	changed map[Ref][]byte
}

// AsOf opens a view of the set as of point. A point at or past the tip sees
// the set as it is. It returns ErrRollbackBeyondJournal when the journal does
// not reach point.
func (s *Store) AsOf(point ocommon.Point) (*AsOf, error) {
	txn := s.db.NewTransaction(false)
	res, err := previewIn(txn, point)
	if err != nil {
		txn.Discard()
		return nil, err
	}
	changed := make(map[Ref][]byte, len(res.Added)+len(res.Removed))
	for _, o := range res.Added {
		changed[o.Ref] = o.Cbor
	}
	for _, r := range res.Removed {
		changed[r] = nil
	}
	return &AsOf{txn: txn, changed: changed}, nil
}

// Close releases the view's snapshot.
func (a *AsOf) Close() {
	a.txn.Discard()
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

// Each calls fn for every output live at the view's point.
func (a *AsOf) Each(fn func(Output) error) error {
	it := a.txn.NewIterator(badger.IteratorOptions{
		Prefix:         []byte{utxoTag},
		PrefetchValues: true,
	})
	defer it.Close()
	for it.Seek([]byte{utxoTag}); it.Valid(); it.Next() {
		var r Ref
		copy(r[:], it.Item().Key()[1:])
		if _, ok := a.changed[r]; ok {
			continue
		}
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := fn(Output{Ref: r, Cbor: v}); err != nil {
			return err
		}
	}
	for r, c := range a.changed {
		if c == nil {
			continue
		}
		if err := fn(Output{Ref: r, Cbor: c}); err != nil {
			return err
		}
	}
	return nil
}
