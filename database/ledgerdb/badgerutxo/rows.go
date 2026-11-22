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
	"math"
	"slices"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

// RowPut sets the row under Key to Value, or deletes it when Delete is
// set, as of the change at Slot. Key is the row's full primary key.
type RowPut struct {
	Slot   uint64
	Key    []byte
	Value  []byte
	Delete bool
	// WipeRewardEpoch, with Key the epoch as 8 bytes BE, deletes every
	// reward output row of that epoch, without a journal entry. It orders
	// with the other puts of the batch.
	WipeRewardEpoch bool
	// TombstonePrefix, with Key a prefix no row of the store holds yet,
	// records that the rows written under it from this batch on did not
	// exist before Slot: a rollback past Slot deletes every row under Key, and
	// RowAt and RowsAt as of an earlier slot find none. One journal entry
	// stands for the whole prefix, however many rows later fill it.
	TombstonePrefix bool
	// WipePrefix deletes every row under Key, without a journal entry.
	WipePrefix bool
	// SkipJournal writes the row with no journal entry of its own, which is
	// exact only under a prefix the batch or an earlier one tombstoned at
	// or before Slot: a rollback then drops the row with the prefix.
	SkipJournal bool
}

// tombstoneMark is the first byte of a state journal entry that records a
// range tombstone; an entry of a row starts with 0 or 1.
const tombstoneMark byte = 2

func stateJournalKey(slot uint64, pk []byte) []byte {
	k := make([]byte, 0, 9+len(pk))
	k = append(k, StateJournalTag)
	k = binary.BigEndian.AppendUint64(k, slot)
	return append(k, pk...)
}

// getRow returns the value of the row under pk.
func getRow(txn *badger.Txn, pk []byte) ([]byte, bool, error) {
	item, err := txn.Get(pk)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	v, err := item.ValueCopy(nil)
	return v, err == nil, err
}

// setRow sets the row under pk to value, or deletes it for a nil value. An
// account keeps its secondary keys in step.
func setRow(txn *badger.Txn, pk, value []byte, present bool) error {
	if len(pk) > 0 && pk[0] == AccountTag {
		cred := pk[1:]
		if len(cred) < 1 {
			return errMalformedAccount
		}
		if !present {
			return setAccount(txn, cred[0], cred[1:], nil)
		}
		r, err := decodeAccount(cred, value)
		if err != nil {
			return err
		}
		return setAccount(txn, cred[0], cred[1:], &r)
	}
	if len(pk) > 0 && pk[0] == SnapshotTag {
		if !present {
			return deleteSnapshotRows(txn, [][]byte{pk})
		}
		return setSnapshotRow(txn, pk, value)
	}
	if !present {
		return txn.Delete(pk)
	}
	return txn.Set(pk, value)
}

// journalRow records the row under pk as it is before its first change at
// slot.
func journalRow(txn *badger.Txn, slot uint64, pk []byte) error {
	jk := stateJournalKey(slot, pk)
	_, err := txn.Get(jk)
	if err == nil {
		return nil
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	old, had, err := getRow(txn, pk)
	if err != nil {
		return err
	}
	before := []byte{0}
	if had {
		before = append([]byte{1}, old...)
	}
	return txn.Set(jk, before)
}

func putRows(txn *badger.Txn, puts []RowPut) error {
	for _, p := range puts {
		if p.WipeRewardEpoch {
			if len(p.Key) != 8 {
				return errors.New(
					"badgerutxo: a reward epoch wipe needs an 8-byte epoch",
				)
			}
			if err := wipeRewardEpoch(txn, binary.BigEndian.Uint64(p.Key)); err != nil {
				return err
			}
			continue
		}
		if p.TombstonePrefix || p.WipePrefix {
			if len(p.Key) == 0 || p.Key[0] == AccountTag ||
				(p.TombstonePrefix && p.WipePrefix) {
				return errors.New(
					"badgerutxo: a prefix put needs one operation on a non-account prefix",
				)
			}
			if p.TombstonePrefix {
				if err := tombstonePrefix(txn, p.Slot, p.Key); err != nil {
					return err
				}
				continue
			}
			if err := deletePrefixKeys(txn, p.Key); err != nil {
				return err
			}
			continue
		}
		if len(p.Key) == 0 || p.Key[0] == AccountTag {
			return errors.New("badgerutxo: a row put needs a non-account key")
		}
		if !p.SkipJournal {
			if err := journalRow(txn, p.Slot, p.Key); err != nil {
				return err
			}
		}
		if err := setRow(txn, p.Key, p.Value, !p.Delete); err != nil {
			return err
		}
	}
	return nil
}

// tombstonePrefix records that nothing under prefix existed before slot.
func tombstonePrefix(txn *badger.Txn, slot uint64, prefix []byte) error {
	return txn.Set(stateJournalKey(slot, prefix), []byte{tombstoneMark})
}

// deletePrefixKeys deletes every row under prefix.
func deletePrefixKeys(txn *badger.Txn, prefix []byte) error {
	var keys [][]byte
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	if len(prefix) > 0 && prefix[0] == SnapshotTag {
		return deleteSnapshotRows(txn, keys)
	}
	return deleteKeys(txn, keys)
}

// tombstonedPrefixes returns the prefixes of the range tombstones after slot.
func (s *Store) tombstonedPrefixes(slot uint64) ([][]byte, error) {
	var prefixes [][]byte
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{StateJournalTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(slotKey(StateJournalTag, slot+1)); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) <= 9 {
				continue
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) > 0 && v[0] == tombstoneMark {
				prefixes = append(prefixes, bytes.Clone(k[9:]))
			}
		}
		return nil
	})
	return prefixes, err
}

// deletePrefixes deletes the rows under each prefix ahead of a rollback that
// undoes their range tombstones, each in as many transactions as it needs, so
// the rollback's own transactions find the prefixes empty. A caller stops
// between this and the rollback only with a mark recorded first.
func (s *Store) deletePrefixes(prefixes [][]byte) error {
	for _, prefix := range prefixes {
		if err := s.deletePrefixRows(prefix); err != nil {
			return err
		}
	}
	return nil
}

// deletePrefixRows deletes every row under prefix in as many transactions as
// it takes.
func (s *Store) deletePrefixRows(prefix []byte) error {
	if len(prefix) > 0 && prefix[0] == SnapshotTag {
		var keys [][]byte
		if err := s.view(func(txn *badger.Txn) error {
			var err error
			keys, err = snapshotKeysUnder(txn, prefix, func([]byte) bool { return true })
			return err
		}); err != nil {
			return err
		}
		for len(keys) > 0 {
			n := min(256, len(keys))
			for {
				// A row and its index entry must disappear in one commit,
				// including when a prefix spans several transactions.
				err := s.update(func(txn *badger.Txn) error {
					return deleteSnapshotRows(txn, keys[:n])
				})
				if errors.Is(err, badger.ErrTxnTooBig) && n > 1 {
					n /= 2
					continue
				}
				if err != nil {
					return err
				}
				break
			}
			keys = keys[n:]
		}
		return nil
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := wb.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return wb.Flush()
}

// Estimated cost of undoing one state journal entry: the row put or delete,
// the secondary keys an account row moves, and the entry's delete. Like the
// forward plan's, it errs high, since a plan too coarse only takes more
// transactions.
const rowUndoEntries = 6

func rowUndoCost(k, v []byte) cost {
	return cost{
		entries: rowUndoEntries,
		bytes:   int64(len(k) + 2*len(v) + 256),
	}
}

// rollbackRows restores every row changed after slot to its state before
// that change, newest change first, and drops those journal entries.
func rollbackRows(txn *badger.Txn, slot uint64) error {
	_, err := rollbackRowsWithin(txn, slot, math.MaxInt64, math.MaxInt64)
	return err
}

// rollbackRowsWithin is rollbackRows for as many of the entries, newest first,
// as fit the budget, and at least one. It reports whether entries after slot
// remain.
func rollbackRowsWithin(
	txn *badger.Txn,
	slot uint64,
	maxEntries, maxBytes int64,
) (bool, error) {
	type entry struct{ key, value []byte }
	entries := make([]entry, 0)
	more := false
	err := func() error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{StateJournalTag}
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		var used cost
		first := slotKey(StateJournalTag, slot+1)
		it.Seek(append([]byte{StateJournalTag},
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))
		for ; it.Valid(); it.Next() {
			k := it.Item().Key()
			if bytes.Compare(k, first) < 0 {
				return nil
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			c := rowUndoCost(k, v)
			if len(entries) > 0 && !used.add(c).fits(maxEntries, maxBytes) {
				more = true
				return nil
			}
			used = used.add(c)
			entries = append(entries, entry{it.Item().KeyCopy(nil), v})
		}
		return nil
	}()
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if len(e.key) < 10 || len(e.value) < 1 {
			return false, errors.New(
				"badgerutxo: malformed state journal entry",
			)
		}
		if e.value[0] == tombstoneMark {
			if err := deletePrefixKeys(txn, e.key[9:]); err != nil {
				return false, err
			}
		} else if err := setRow(
			txn, e.key[9:], e.value[1:], e.value[0] == 1,
		); err != nil {
			return false, err
		}
		if err := txn.Delete(e.key); err != nil {
			return false, err
		}
	}
	return more, nil
}

// rowsAfterExceed reports whether undoing the entries after slot takes more
// than the budget.
func rowsAfterExceed(
	txn *badger.Txn,
	slot uint64,
	maxEntries, maxBytes int64,
) (bool, error) {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{StateJournalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	var used cost
	for it.Seek(slotKey(StateJournalTag, slot+1)); it.Valid(); it.Next() {
		item := it.Item()
		v, err := item.ValueCopy(nil)
		if err != nil {
			return false, err
		}
		used = used.add(rowUndoCost(item.Key(), v))
		if !used.fits(maxEntries, maxBytes) {
			return true, nil
		}
	}
	return false, nil
}

// rowAt returns the row under pk as a rollback to slot would leave it: its
// value before its first change after slot, or its current value.
func rowAt(txn *badger.Txn, pk []byte, slot uint64) ([]byte, bool, error) {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{StateJournalTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(slotKey(StateJournalTag, slot+1)); it.Valid(); it.Next() {
		k := it.Item().Key()
		if len(k) < 9 || !bytes.HasPrefix(pk, k[9:]) {
			continue
		}
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, false, err
		}
		if len(v) < 1 {
			return nil, false, errors.New(
				"badgerutxo: malformed state journal entry",
			)
		}
		if v[0] == tombstoneMark {
			// The row is under a prefix that was empty before the entry.
			return nil, false, nil
		}
		if !bytes.Equal(k[9:], pk) {
			continue
		}
		return v[1:], v[0] == 1, nil
	}
	return getRow(txn, pk)
}

// rowsAt calls fn, in key order, with every row under prefix as a rollback
// to slot would leave it: a row's value before its first change after slot,
// else its current value.
func rowsAt(
	txn *badger.Txn,
	prefix, from, to []byte,
	slot uint64,
	fn func(k, v []byte) error,
) error {
	inRange := func(k []byte) bool {
		return (from == nil || bytes.Compare(k, from) >= 0) &&
			(to == nil || bytes.Compare(k, to) < 0)
	}
	type before struct {
		v       []byte
		present bool
	}
	restored := make(map[string]before)
	// tombstones are the prefixes that were empty before an entry after
	// slot: every row under one is absent as of slot.
	var tombstones [][]byte
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{StateJournalTag}
	it := txn.NewIterator(opts)
	for it.Seek(slotKey(StateJournalTag, slot+1)); it.Valid(); it.Next() {
		k := it.Item().Key()
		if len(k) < 10 ||
			(!bytes.HasPrefix(k[9:], prefix) && !bytes.HasPrefix(prefix, k[9:])) {
			continue
		}
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			it.Close()
			return err
		}
		if len(v) < 1 {
			it.Close()
			return errors.New("badgerutxo: malformed state journal entry")
		}
		if v[0] == tombstoneMark {
			tombstones = append(tombstones, bytes.Clone(k[9:]))
			continue
		}
		if !bytes.HasPrefix(k[9:], prefix) || !inRange(k[9:]) {
			continue
		}
		if _, seen := restored[string(k[9:])]; seen {
			continue
		}
		restored[string(k[9:])] = before{v[1:], v[0] == 1}
	}
	it.Close()
	tombstoned := func(k []byte) bool {
		return slices.ContainsFunc(tombstones, func(p []byte) bool {
			return bytes.HasPrefix(k, p)
		})
	}
	type row struct{ k, v []byte }
	rows := make([]row, 0, len(restored))
	err := eachBetween(txn, prefix, from, to, func(k, v []byte) error {
		if _, ok := restored[string(k)]; !ok && !tombstoned(k) {
			rows = append(rows, row{k, v})
		}
		return nil
	})
	if err != nil {
		return err
	}
	for k, b := range restored {
		if b.present && !tombstoned([]byte(k)) {
			rows = append(rows, row{[]byte(k), b.v})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].k, rows[j].k) < 0
	})
	for _, r := range rows {
		if err := fn(r.k, r.v); err != nil {
			return err
		}
	}
	return nil
}

// pruneStateJournal deletes the journal entries of changes before slot,
// which no rollback will reach.
func (s *Store) pruneStateJournal(slot uint64) error {
	var keys [][]byte
	if err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{StateJournalTag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if len(k) < 9 || binary.BigEndian.Uint64(k[1:9]) >= slot {
				return nil
			}
			keys = append(keys, k)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: scan state journal: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			return fmt.Errorf("badgerutxo: prune state journal: %w", err)
		}
	}
	return wb.Flush()
}

// Row returns the value of the row under pk.
func (s *Store) Row(pk []byte) ([]byte, bool, error) {
	var v []byte
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		var err error
		v, ok, err = getRow(txn, pk)
		return err
	})
	return v, ok, err
}

// RowAt returns the row under pk as a rollback to slot would leave it.
func (s *Store) RowAt(pk []byte, slot uint64) ([]byte, bool, error) {
	var v []byte
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		var err error
		v, ok, err = rowAt(txn, pk, slot)
		return err
	})
	return v, ok, err
}

// RowKeysChangedAfter returns the key of every row under prefix that the state
// journal records a change to after slot, each once, in key order. It is the
// rows whose value as of slot may differ from their current one.
func (s *Store) RowKeysChangedAfter(
	prefix []byte,
	slot uint64,
) ([][]byte, error) {
	seen := make(map[string]struct{})
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{StateJournalTag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		var tombstones [][]byte
		for it.Seek(slotKey(StateJournalTag, slot+1)); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) <= 9 {
				continue
			}
			if bytes.HasPrefix(k[9:], prefix) {
				seen[string(k[9:])] = struct{}{}
			}
			if !bytes.HasPrefix(k[9:], prefix) &&
				!bytes.HasPrefix(prefix, k[9:]) {
				continue
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				it.Close()
				return err
			}
			if len(v) > 0 && v[0] == tombstoneMark {
				tombstones = append(tombstones, bytes.Clone(k[9:]))
			}
		}
		it.Close()
		// A tombstoned prefix stands for every row under it, none of which
		// has a journal entry of its own.
		for _, p := range tombstones {
			delete(seen, string(p))
			narrow := p
			if len(prefix) > len(p) {
				narrow = prefix
			}
			if err := eachPrefix(txn, narrow, func(k, _ []byte) error {
				seen[string(k)] = struct{}{}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(seen))
	for k := range seen {
		keys = append(keys, []byte(k))
	}
	sort.Slice(
		keys,
		func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 },
	)
	return keys, nil
}

// Rows calls fn with the key and value of every row under prefix, in key
// order.
func (s *Store) Rows(prefix []byte, fn func(k, v []byte) error) error {
	return s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, prefix, func(k, v []byte) error {
			s.rangeRowsVisited.Add(1)
			return fn(k, v)
		})
	})
}

// EdgeRows returns the first and the last row under prefix, in key order,
// and false when there is none. It copies the values of those two rows only.
func (s *Store) EdgeRows(
	prefix []byte,
) (firstK, firstV, lastK, lastV []byte, found bool, err error) {
	end := KeyAfter(prefix)
	if end == nil {
		return nil, nil, nil, nil, false, errors.New(
			"badgerutxo: no key follows the prefix",
		)
	}
	err = s.view(func(txn *badger.Txn) error {
		edge := func(reverse bool) (k, v []byte, ok bool, err error) {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			opts.Reverse = reverse
			if reverse {
				// A reverse seek lands on end itself when that key exists,
				// and an iterator bounded by prefix reports it invalid
				// without a way to step past it. Bound this one by the
				// bytes end shares with prefix, which every key between the
				// range and end carries, and step over end explicitly.
				opts.Prefix = end[:len(end)-1]
			}
			it := txn.NewIterator(opts)
			defer it.Close()
			if reverse {
				it.Seek(end)
				if it.Valid() && bytes.Equal(it.Item().Key(), end) {
					it.Next()
				}
			} else {
				it.Rewind()
			}
			s.rangeRowsVisited.Add(1)
			if !it.ValidForPrefix(prefix) {
				return nil, nil, false, nil
			}
			v, err = it.Item().ValueCopy(nil)
			if err != nil {
				return nil, nil, false, err
			}
			return it.Item().KeyCopy(nil), v, true, nil
		}
		var ok bool
		firstK, firstV, ok, err = edge(false)
		if err != nil || !ok {
			return err
		}
		lastK, lastV, ok, err = edge(true)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New(
				"badgerutxo: no last row under a prefix with a first row",
			)
		}
		found = true
		return nil
	})
	return firstK, firstV, lastK, lastV, found, err
}

// EachDistinctEpoch calls fn with each distinct epoch among the keys under
// tag, whose keys lead with the epoch as eight big-endian bytes, in order. It
// reads keys only and seeks past each epoch's rows, so it steps once per
// epoch however many rows an epoch holds.
func (s *Store) EachDistinctEpoch(tag byte, fn func(epoch uint64) error) error {
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{tag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		for it.Valid() {
			s.rangeRowsVisited.Add(1)
			k := it.Item().Key()
			if len(k) < 9 {
				it.Next()
				continue
			}
			epoch := binary.BigEndian.Uint64(k[1:9])
			if err := fn(epoch); err != nil {
				return err
			}
			next := KeyAfter(k[:9])
			if next == nil {
				return nil
			}
			it.Seek(next)
		}
		return nil
	})
}

// RowsInRange calls fn with the key and value of every row under prefix whose
// key is at least from and below to, in key order, seeking to from and
// stopping at to. A nil to is open.
func (s *Store) RowsInRange(
	prefix, from, to []byte,
	fn func(k, v []byte) error,
) error {
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(from); it.Valid(); it.Next() {
			s.rangeRowsVisited.Add(1)
			k := it.Item().Key()
			if to != nil && bytes.Compare(k, to) >= 0 {
				return nil
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

// ErrStopRows is returned by the callback of EachRowAfter to end the walk
// without an error.
var ErrStopRows = errors.New("badgerutxo: stop rows")

// EachRowAfter calls fn with the key and value of each row under prefix whose
// key is strictly after after (the whole prefix when after is nil) and below
// end (open when nil), in key order, seeking to the first. The walk ends when
// fn returns ErrStopRows. It is how a reader resumes a large family after the
// last row it served without stepping over the rows before it.
func (s *Store) EachRowAfter(
	prefix, after, end []byte,
	fn func(k, v []byte) error,
) error {
	start := prefix
	if after != nil {
		start = append(bytes.Clone(after), 0)
	}
	return s.EachRowFrom(prefix, start, end, fn)
}

// EachRowFrom is EachRowAfter for the rows whose key is at least start.
func (s *Store) EachRowFrom(
	prefix, start, end []byte,
	fn func(k, v []byte) error,
) error {
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(start); it.Valid(); it.Next() {
			s.rangeRowsVisited.Add(1)
			k := it.Item().Key()
			if end != nil && bytes.Compare(k, end) >= 0 {
				return nil
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(k, v); err != nil {
				if errors.Is(err, ErrStopRows) {
					return nil
				}
				return err
			}
		}
		return nil
	})
}

// RangeRowsVisited returns how many rows Rows, RowsInRange and EachRowAfter
// have stepped over, and how many epochs EachDistinctEpoch has listed, since
// the store opened, including the row that ended a range. It exists to show
// that a ranged read does not touch the rows outside its range.
func (s *Store) RangeRowsVisited() uint64 {
	return s.rangeRowsVisited.Load()
}

// Keys calls fn with the key of every row under prefix, in key order, without
// reading the values.
func (s *Store) Keys(prefix []byte, fn func(k []byte) error) error {
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := fn(it.Item().Key()); err != nil {
				return err
			}
		}
		return nil
	})
}

// RowsAt calls fn with the key and value of every row under prefix as a
// rollback to slot would leave it, in key order.
func (s *Store) RowsAt(
	prefix []byte,
	slot uint64,
	fn func(k, v []byte) error,
) error {
	return s.RowsAtBetween(prefix, nil, nil, slot, fn)
}

// RowsAtBetween is RowsAt for the rows whose keys are at least from and below
// to; a nil bound is open. The bounds are full keys inside prefix, so a
// caller that wants a slice of a large family reads only that slice.
func (s *Store) RowsAtBetween(
	prefix, from, to []byte,
	slot uint64,
	fn func(k, v []byte) error,
) error {
	return s.view(func(txn *badger.Txn) error {
		return rowsAt(txn, prefix, from, to, slot, fn)
	})
}

// RowsBetween calls fn with the key and value of every row under prefix whose
// key is at least from and below to, in key order; a nil bound is open.
func (s *Store) RowsBetween(
	prefix, from, to []byte,
	fn func(k, v []byte) error,
) error {
	return s.view(func(txn *badger.Txn) error {
		return eachBetween(txn, prefix, from, to, fn)
	})
}

// eachBetween is eachPrefix over the keys from up to, not including, to.
func eachBetween(
	txn *badger.Txn,
	prefix, from, to []byte,
	fn func(k, v []byte) error,
) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	if from == nil {
		it.Rewind()
	} else {
		it.Seek(from)
	}
	for ; it.Valid(); it.Next() {
		if to != nil && bytes.Compare(it.Item().Key(), to) >= 0 {
			return nil
		}
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := fn(it.Item().KeyCopy(nil), v); err != nil {
			return err
		}
	}
	return nil
}

// KeyAfter returns the smallest key greater than every key that has k as a
// prefix, or nil when there is none.
func KeyAfter(k []byte) []byte {
	out := bytes.Clone(k)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

// Computed-row prefixes other than accounts, and their completion marker:
//
//	g <tag:1> <credential>                          DRep
//	l <tag:1> <len:1> <DRep> <tag:1> <key>          live DRep delegator link
//	zdormant                                        DRep dormancy counter
//	mdreps                                          set once they are complete
//
// LoadDrepState replaces the store's DRep rows, links and dormancy counter
// with the ones feed supplies, each as a primary key and value, and marks
// them loaded, for a store from before it kept them.
func (s *Store) LoadDrepState(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.dropPrefix(
		[]byte{DRepTag}, []byte{DRepLinkTag}, []byte{DormancyTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop DRep state: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 ||
			(k[0] != DRepTag && k[0] != DRepLinkTag && k[0] != DormancyTag) {
			return fmt.Errorf("badgerutxo: %x is not a DRep state key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load DRep state: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load DRep state: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(drepsKey, []byte{1})
	})
}

// DrepStateLoaded reports whether the store holds every DRep row, link and
// the dormancy counter up to its tip.
func (s *Store) DrepStateLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(drepsKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// LoadPools replaces the store's pool states with the ones feed supplies,
// each as a primary key and value, and marks them loaded, for a store from
// before it kept them.
func (s *Store) LoadPools(feed func(put func(k, v []byte) error) error) error {
	if err := s.dropPrefix([]byte{PoolTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop pools: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != PoolTag {
			return fmt.Errorf("badgerutxo: %x is not a pool key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load pools: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load pools: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(poolsKey, []byte{1})
	})
}

// PoolsLoaded reports whether the store holds every registered pool's
// state up to its tip.
func (s *Store) PoolsLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(poolsKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// LoadPoolHistory replaces the store's pool certificate events with the ones
// feed supplies, each as a primary key and value, and marks them loaded, for
// a store from before it kept them.
func (s *Store) LoadPoolHistory(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.dropPrefix([]byte{PoolHistoryTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop pool history: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != PoolHistoryTag {
			return fmt.Errorf("badgerutxo: %x is not a pool history key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load pool history: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load pool history: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(legacyPoolHistoryKey); err != nil {
			return err
		}
		return txn.Set(poolHistoryKey, []byte{1})
	})
}

// PoolHistoryLoaded reports whether the store holds every pool certificate
// event up to its tip.
func (s *Store) PoolHistoryLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(poolHistoryKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// Registration history prefixes and their completion marker:
//
//	h <tag:1> <key length:1> <key> <slot:8 BE> <position:8 BE>
//	                                  a stake registration or deregistration
//	i <tag:1> <credential length:1> <credential>
//	                                  a DRep's latest registration deposit
//	mreghist                          set once both are loaded
//
// The position orders certificates inside a slot. Stake events are only
// added and a DRep row is replaced by its next registration; a rollback
// undoes both through the state journal.
// LoadRegistrationHistory replaces the store's registration history with the
// rows feed supplies, each as a primary key and value, and marks it loaded,
// for a store from before it kept them.
func (s *Store) LoadRegistrationHistory(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.dropPrefix(
		[]byte{StakeHistoryTag}, []byte{DRepDepositTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop registration history: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || (k[0] != StakeHistoryTag && k[0] != DRepDepositTag) {
			return fmt.Errorf(
				"badgerutxo: %x is not a registration history key",
				k,
			)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load registration history: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load registration history: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(legacyRegistrationHistoryKey); err != nil {
			return err
		}
		return txn.Set(registrationHistoryKey, []byte{1})
	})
}

// RegistrationHistoryLoaded reports whether the store holds every stake
// registration event and DRep deposit up to its tip.
func (s *Store) RegistrationHistoryLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(registrationHistoryKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// LoadOpCerts replaces the store's op-cert counters with the ones feed
// supplies, each as a primary key and value, and marks them loaded, for a
// store from before it kept them.
func (s *Store) LoadOpCerts(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.dropPrefix([]byte{OpCertTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop op-cert counters: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != OpCertTag {
			return fmt.Errorf("badgerutxo: %x is not an op-cert key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load op-cert counters: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load op-cert counters: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(legacyOpCertsKey); err != nil {
			return err
		}
		return txn.Set(opCertsKey, []byte{1})
	})
}

// OpCertsLoaded reports whether the store holds every pool's op-cert
// counter up to its tip.
func (s *Store) OpCertsLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(opCertsKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// LoadPParamUpdates replaces the store's protocol parameter update
// proposals with the ones feed supplies, each as a primary key and value,
// and marks them loaded, for a store from before it kept them.
func (s *Store) LoadPParamUpdates(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.dropPrefix([]byte{PParamUpdateTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop pparam updates: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != PParamUpdateTag {
			return fmt.Errorf("badgerutxo: %x is not a pparam update key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load pparam updates: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load pparam updates: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(pparamUpdatesKey, []byte{1})
	})
}

// PParamUpdatesLoaded reports whether the store holds every protocol
// parameter update proposal up to its tip.
func (s *Store) PParamUpdatesLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(pparamUpdatesKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// Genesis key delegations and MIR certificates, oldest first, with their
// completion marker:
//
//	f <slot:8 BE> <position:8 BE>   genesis key delegation certificate
//	i <slot:8 BE> <position:8 BE>   MIR certificate
//	mgenmir                         set once both are loaded
//
// The position orders certificates inside a slot. Certificates are only
// added, and a rollback removes those after its target through the state
// journal.
// LoadGenesisMIR replaces the store's genesis key delegations and MIR
// certificates with the ones feed supplies, each as a primary key and value,
// and marks them loaded, for a store from before it kept them.
func (s *Store) LoadGenesisMIR(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.dropPrefix(
		[]byte{GenesisDelegationTag}, []byte{MIRCertTag},
	); err != nil {
		return fmt.Errorf(
			"badgerutxo: drop genesis delegations and MIR certificates: %w",
			err,
		)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || (k[0] != GenesisDelegationTag && k[0] != MIRCertTag) {
			return fmt.Errorf(
				"badgerutxo: %x is not a genesis delegation or MIR certificate key",
				k,
			)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf(
			"badgerutxo: load genesis delegations and MIR certificates: %w",
			err,
		)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf(
			"badgerutxo: load genesis delegations and MIR certificates: %w",
			err,
		)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(genesisMIRKey, []byte{1})
	})
}

// GenesisMIRLoaded reports whether the store holds every genesis key
// delegation and MIR certificate up to its tip.
func (s *Store) GenesisMIRLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(genesisMIRKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// Import baseline and witness slot prefixes and their completion markers:
//
//	B <tag:1> <key>                              an account's import baseline
//	mbaseline                                    set once they are all loaded
//	K <tag:1> <key length:1> <key> <slot:8 BE>   a slot a credential was
//	                                             witnessed at; no value
//	mwitness                                     set once they are all loaded
//
// A baseline row is replaced by the next import of its account; a witness row
// is only added. A rollback undoes both through the state journal.

// loadFamily replaces the rows under tag with the ones feed supplies, each as
// a primary key and value, and sets marker, for a store from before it kept
// the family.
//
// The markers in invalidate are cleared once the old rows are dropped, so a
// stop before the new rows are complete leaves no marker that vouches for the
// partial family.
func (s *Store) loadFamily(
	tag byte,
	marker []byte,
	name string,
	feed func(put func(k, v []byte) error) error,
	invalidate ...[]byte,
) error {
	if err := s.dropPrefix([]byte{tag}); err != nil {
		return fmt.Errorf("badgerutxo: drop %s: %w", name, err)
	}
	if len(invalidate) > 0 {
		if err := s.update(func(txn *badger.Txn) error {
			for _, m := range invalidate {
				if err := txn.Delete(m); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("badgerutxo: clear %s marker: %w", name, err)
		}
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != tag {
			return fmt.Errorf("badgerutxo: %x is not a %s key", k, name)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load %s: %w", name, err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load %s: %w", name, err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(marker, []byte{1})
	})
}

func (s *Store) hasMarker(marker []byte) (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(marker)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// LoadImportBaselines replaces the store's import baselines with the rows
// feed supplies, and marks them and their delegations loaded: the rows feed
// supplies carry both.
func (s *Store) LoadImportBaselines(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.loadFamily(
		ImportBaselineTag,
		importBaselinesKey,
		"import baselines",
		feed,
		importBaselinesKey, importBaselineDelegationsKey,
	); err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(importBaselineDelegationsKey, markerValue(false))
	})
}

// ImportBaselinesLoaded reports whether the store holds every import baseline
// up to its tip.
func (s *Store) ImportBaselinesLoaded() (bool, error) {
	return s.hasMarker(importBaselinesKey)
}

// ImportBaselineDelegationsLoaded reports whether the store has settled what
// its import baselines say about delegations: they hold them, or
// ImportBaselineDelegationsLost says they cannot.
func (s *Store) ImportBaselineDelegationsLoaded() (bool, error) {
	return s.hasMarker(importBaselineDelegationsKey)
}

// ImportBaselineDelegationsLost reports whether the import baselines were
// written before the store kept their delegations, and the metadata store had
// drained the rows they come from.
func (s *Store) ImportBaselineDelegationsLost() (bool, error) {
	return s.markerLost(importBaselineDelegationsKey)
}

// MarkImportBaselineDelegationsLost records that the import baselines have no
// delegations and no source to take them from.
func (s *Store) MarkImportBaselineDelegationsLost() error {
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(importBaselineDelegationsKey, markerValue(true))
	})
}

// RowsAfter calls fn with the key and value of at most limit rows under
// prefix whose key is greater than after, in key order; a nil after starts at
// the first row.
func (s *Store) RowsAfter(
	prefix, after []byte,
	limit int,
	fn func(k, v []byte) error,
) error {
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(prefix)
		if after != nil {
			it.Seek(append(bytes.Clone(after), 0))
		}
		for n := 0; it.Valid() && n < limit; it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(it.Item().KeyCopy(nil), v); err != nil {
				return err
			}
			n++
		}
		return nil
	})
}

// LoadWitnessSlots replaces the store's witness slots with the rows feed
// supplies, and marks them loaded.
func (s *Store) LoadWitnessSlots(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.loadFamily(
		WitnessTag, witnessSlotsKey, "witness slots", feed,
	); err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Delete(legacyWitnessSlotsKey)
	})
}

// WitnessSlotsLoaded reports whether the store holds every witness slot up
// to its tip.
func (s *Store) WitnessSlotsLoaded() (bool, error) {
	return s.hasMarker(witnessSlotsKey)
}

// LastKeyAtOrBelow returns the greatest key under prefix that is at most
// limit, a key of the same family.
func (s *Store) LastKeyAtOrBelow(prefix, limit []byte) ([]byte, bool, error) {
	var out []byte
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(limit)
		if it.Valid() {
			out = it.Item().KeyCopy(nil)
		}
		return nil
	})
	return out, out != nil, err
}
