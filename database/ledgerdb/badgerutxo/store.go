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
//	m tip                      the point of the last applied change
//	m count                    the number of live outputs
package badgerutxo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

const (
	refLen     = 36
	utxoTag    = 'u'
	journalTag = 'j'
	hashLen    = 32
)

var (
	tipKey   = []byte("mtip")
	countKey = []byte("mcount")
	// seqKey holds the next journal sequence number.
	seqKey = []byte("mseq")
	// formatKey holds formatVersion once the store's contents, journal and
	// sums are all in the current format.
	formatKey = []byte("mfmt")
)

// formatVersion is the layout this code writes: outputs, the journal, and
// the running sums of the aggregator.
const formatVersion = 2

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
}

// Open opens or creates the store in dir.
func Open(dir string, options ...Option) (*Store, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badgerutxo: open %s: %w", dir, err)
	}
	s := &Store{db: db}
	for _, o := range options {
		o(s)
	}
	// An empty store is in the current format by definition; one written in
	// an older format keeps no mark and reports Stale until rebuilt.
	if err := db.Update(func(txn *badger.Txn) error {
		if _, ok, err := readTip(txn); err != nil || ok {
			return err
		}
		return txn.Set(formatKey, []byte{formatVersion})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("badgerutxo: open %s: %w", dir, err)
	}
	return s, nil
}

// Stale reports whether the store was written in an older format, so its
// journal or sums cannot be trusted and it must be rebuilt.
func (s *Store) Stale() (bool, error) {
	var stale bool
	err := s.db.View(func(txn *badger.Txn) error {
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

// Close closes the store.
func (s *Store) Close() error {
	return s.db.Close()
}

func utxoKey(r Ref) []byte {
	k := make([]byte, 1+refLen)
	k[0] = utxoTag
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
	err := s.db.View(func(txn *badger.Txn) error {
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
	err := s.db.View(func(txn *badger.Txn) error {
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
	err := s.db.View(func(txn *badger.Txn) error {
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
	// Clear the markers first, in one transaction: until the final commit
	// below the contents are partial, and a store with no tip and no format
	// marker is rebuilt rather than trusted.
	if err := s.db.Update(func(txn *badger.Txn) error {
		for _, key := range [][]byte{tipKey, countKey, seqKey, formatKey} {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: clear markers for bootstrap: %w", err)
	}
	// The journal describes history the new contents do not share.
	if err := s.db.DropPrefix(
		[]byte{utxoTag}, []byte{journalTag}, []byte{aggTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop for bootstrap: %w", err)
	}
	wb := s.db.NewWriteBatch()
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
	return s.db.Update(func(txn *badger.Txn) error {
		if err := writeCount(txn, n); err != nil {
			return err
		}
		if err := txn.Set(formatKey, []byte{formatVersion}); err != nil {
			return err
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
	var res ApplyResult
	err := s.db.Update(func(txn *badger.Txn) error {
		count, err := readCount(txn)
		if err != nil {
			return err
		}
		var rec journalRecord
		if rec.prevTip, rec.hadTip, err = readTip(txn); err != nil {
			return err
		}
		baseline := tip == nil && !rec.hadTip
		rec.baseline = baseline
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
					advancesTip: tip != nil,
					baseline:    baseline,
				}
				seen = make(map[Ref]struct{})
			}
			open = true
			segment = point
			rec.point = point
			rec.advancesTip = tip != nil
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
		if tip != nil && (!open || !samePoint(*tip, segment)) {
			if err := startRecord(*tip); err != nil {
				return err
			}
		}
		if open {
			if err := appendJournal(txn, rec); err != nil {
				return err
			}
		}
		if tip == nil {
			return nil
		}
		return txn.Set(tipKey, encodePoint(*tip))
	})
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

// Each calls fn for every live output, in key order, until fn returns an
// error.
func (s *Store) Each(fn func(Output) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         []byte{utxoTag},
			PrefetchValues: true,
		})
		defer it.Close()
		for it.Seek([]byte{utxoTag}); it.Valid(); it.Next() {
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
	start[0] = utxoTag
	for i := 1; i < len(start); i++ {
		start[i] = byte(rng.Uint32()) //nolint:gosec // truncation is the point
	}
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         []byte{utxoTag},
			PrefetchValues: true,
		})
		defer it.Close()
		for pass, from := range [][]byte{start, {utxoTag}} {
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
