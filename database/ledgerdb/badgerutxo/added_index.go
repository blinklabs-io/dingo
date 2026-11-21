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
	"cmp"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// addedEntry names a row by the slot it was added at and its key.
type addedEntry struct {
	slot uint64
	pk   []byte
}

func compareAdded(a, b addedEntry) int {
	return cmp.Or(cmp.Compare(a.slot, b.slot), bytes.Compare(a.pk, b.pk))
}

// addedIndex lists the rows of one family whose AddedSlot is below floor, by
// (slot, key), as the transaction at readTs held them. A block writes at a
// slot above the tip, which is at or above the floor, so it only moves a row's
// AddedSlot up: its entry goes stale and never missing, and eachRowAddedIn
// skips a stale entry. The index therefore answers any range below the floor
// it was built at, for a transaction at or after readTs, after a prune has
// raised the floor further. A transaction before readTs may hold a row at the
// slot a later block moved it from, which the index does not name. What can
// bring a row back below the floor, or add one there, is a rollback, a load, a
// restore or a reset: each runs inside rewriteRows, and an index is
// shared only at the generation it was built at, with no rewrite running.
type addedIndex struct {
	floor, gen, readTs uint64
	entries            []addedEntry
}

type addedIndexes struct {
	mu  sync.Mutex
	fam map[byte]*addedIndex
	// rewriting counts the rewrites running. A rewrite writes below the
	// floor across several transactions between its two moves of the
	// generation, so a read during one cannot share an index.
	rewriting atomic.Int64
	// afterGen and beforeIndex pause a read between taking the generation
	// and beginning its transaction, and inside the transaction before the
	// index is looked up. Only tests set them.
	afterGen, beforeIndex func()
}

// rewriteRows begins an operation that can put a row below the floor, and
// returns the function that ends it. Both move the generation.
func (s *Store) rewriteRows() (end func()) {
	s.added.rewriting.Add(1)
	s.rowsRewritten.Add(1)
	return func() {
		// The generation moves before the count drops, so a read that
		// finds no rewrite running also finds this one's generation.
		s.rowsRewritten.Add(1)
		s.added.rewriting.Add(-1)
	}
}

// rowsUnchangedSince reports, for a read that took gen before its transaction
// began and calls it after, that no rewrite wrote between the two or runs
// now, so the transaction holds the rows every other such read at gen holds,
// up to the blocks between them.
func (s *Store) rowsUnchangedSince(gen uint64) bool {
	return s.added.rewriting.Load() == 0 && s.rowsRewritten.Load() == gen
}

// addedIndexFor returns an index of family that reaches through and answers
// txn, which began after gen was read. It reuses the shared one when that
// does, and otherwise builds one at floor from txn, which it shares only when
// the rows did not change since gen.
func (s *Store) addedIndexFor(
	txn *badger.Txn,
	family byte,
	addedSlot func([]byte) (uint64, error),
	floor, gen, through uint64,
) (*addedIndex, error) {
	readTs := txn.ReadTs()
	s.added.mu.Lock()
	defer s.added.mu.Unlock()
	if idx := s.added.fam[family]; idx != nil &&
		idx.gen == gen && readTs >= idx.readTs && through < idx.floor &&
		s.rowsUnchangedSince(gen) {
		return idx, nil
	}
	idx := &addedIndex{floor: floor, gen: gen, readTs: readTs}
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{family}
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		slot, err := addedSlot(v)
		if err != nil {
			return nil, err
		}
		if slot < floor {
			idx.entries = append(idx.entries, addedEntry{
				slot: slot, pk: it.Item().KeyCopy(nil),
			})
		}
	}
	slices.SortFunc(idx.entries, compareAdded)
	if s.rowsUnchangedSince(gen) {
		if s.added.fam == nil {
			s.added.fam = map[byte]*addedIndex{}
		}
		s.added.fam[family] = idx
	}
	return idx, nil
}

// eachRowAddedIn is EachRowChangedIn for a range wholly below the floor: it
// passes each row of family whose AddedSlot is in [from, through], in
// (slot, key) order. Its positions are shaped like a journal entry's, so a
// cursor that began in the journal resumes after the last row served.
func (s *Store) eachRowAddedIn(
	txn *badger.Txn,
	family byte,
	addedSlot func([]byte) (uint64, error),
	floor, gen, from, through uint64,
	after []byte,
	maxVisit int,
	fn func(slot uint64, key, value, pos []byte) error,
) (next []byte, covered, done bool, err error) {
	start := addedEntry{slot: from}
	if after != nil {
		if len(after) < 9 {
			return nil, false, false, nil
		}
		start = addedEntry{
			slot: binary.BigEndian.Uint64(after[1:9]),
			pk:   after[9:],
		}
	}
	if s.added.beforeIndex != nil {
		s.added.beforeIndex()
	}
	idx, err := s.addedIndexFor(
		txn, family, addedSlot, floor, gen, through,
	)
	if err != nil {
		return nil, false, false, err
	}
	i, found := slices.BinarySearchFunc(idx.entries, start, compareAdded)
	if found && after != nil {
		i++
	}
	visited := 0
	for ; i < len(idx.entries); i++ {
		e := idx.entries[i]
		if e.slot > through {
			return nil, true, true, nil
		}
		s.rangeRowsVisited.Add(1)
		visited++
		v, present, err := getRow(txn, e.pk)
		if err != nil {
			return nil, false, false, err
		}
		if present {
			cur, err := addedSlot(v)
			if err != nil {
				return nil, false, false, err
			}
			if cur == e.slot {
				pos := stateJournalKey(e.slot, e.pk)
				if err := fn(e.slot, e.pk, v, pos); err != nil {
					if errors.Is(err, ErrStopRows) {
						return pos, true, false, nil
					}
					return nil, false, false, err
				}
			}
		}
		if visited >= maxVisit {
			return stateJournalKey(e.slot, e.pk), true, false, nil
		}
	}
	return nil, true, true, nil
}
