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

	"github.com/dgraph-io/badger/v4"
)

// ensureExportFloor gives a store opened without an export floor one at its
// tip plus one: it may hold rows a load or merge wrote with no entry at their
// AddedSlot, so only slots after the tip are known to be named by the journal.
func (s *Store) ensureExportFloor() error {
	return s.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(exportFloorKey); err == nil {
			return nil
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		var floor uint64
		tip, ok, err := readTip(txn)
		if err != nil {
			return err
		}
		if ok {
			floor = tip.Slot + 1
		}
		return txn.Set(exportFloorKey, binary.BigEndian.AppendUint64(nil, floor))
	})
}

// changedFloor is the lowest slot from which the state journal names every
// account and DRep row by its AddedSlot: pruning drops entries below the
// journal floor, which a load raises to its tip, and a merge writes rows no
// entry at their AddedSlot describes.
func changedFloor(txn *badger.Txn) (uint64, error) {
	// A load before any tip leaves no floor, but its rows have no entries.
	floor := uint64(1)
	for _, k := range [][]byte{journalFloorKey, exportFloorKey} {
		f, err := readFloor(txn, k)
		if err != nil {
			return 0, err
		}
		floor = max(floor, f)
	}
	return floor, nil
}

// EachRowChangedIn calls fn with the slot, key and current value of each row
// of family (AccountTag or DRepTag), with its journal position, that the state journal records a change
// to at a slot in [from, through], in journal order and resuming after the
// journal position after (the start of the range when nil). A row changed at
// several slots is passed once per slot, and a row since deleted is skipped.
// Its cost is the journal entries in the range, not the size of the family.
//
// A range wholly below the floor is served from the rows themselves: addedSlot
// reads the AddedSlot out of a row's value, and the rows added in the range are
// named by an index of the family, built on first use. It serves every range
// below the floor it was built at, until a rollback, load, merge, restore or
// reset could have put a row there. A nil addedSlot leaves such a range
// uncovered.
//
// It stops after fn returns ErrStopRows, or after stepping over maxVisit
// journal entries of any family, and returns the position to resume after.
// covered is false when the journal cannot name every row the range
// holds, because the range or the position is below the floor the journal is
// complete from; nothing has then been passed to fn, and the caller reads the
// family whole. done is true when the range is exhausted.
func (s *Store) EachRowChangedIn(
	family byte,
	addedSlot func(value []byte) (uint64, error),
	from, through uint64,
	after []byte,
	maxVisit int,
	fn func(slot uint64, key, value, pos []byte) error,
) (next []byte, covered, done bool, err error) {
	covered = true
	// Read before the transaction begins, so a rewrite after this point shows
	// as a different generation than the one an index is stamped with.
	gen := s.rowsRewritten.Load()
	err = s.view(func(txn *badger.Txn) error {
		floor, err := changedFloor(txn)
		if err != nil {
			return err
		}
		if through < floor && addedSlot != nil {
			next, covered, done, err = s.eachRowAddedIn(
				txn, family, addedSlot, floor, gen,
				from, through, after, maxVisit, fn,
			)
			return err
		}
		start := slotKey(StateJournalTag, from)
		if after != nil {
			if len(after) < 9 {
				covered = false
				return nil
			}
			if binary.BigEndian.Uint64(after[1:9]) < floor {
				covered = false
				return nil
			}
			start = append(bytes.Clone(after), 0)
		} else if from < floor {
			covered = false
			return nil
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{StateJournalTag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		visited := 0
		done = true
		for it.Seek(start); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) <= 9 {
				continue
			}
			slot := binary.BigEndian.Uint64(k[1:9])
			if slot > through {
				return nil
			}
			s.rangeRowsVisited.Add(1)
			visited++
			if k[9] == family {
				pk := bytes.Clone(k[9:])
				v, ok, err := getRow(txn, pk)
				if err != nil {
					return err
				}
				if ok {
					if err := fn(slot, pk, v, k); err != nil {
						if errors.Is(err, ErrStopRows) {
							next, done = bytes.Clone(k), false
							return nil
						}
						return err
					}
				}
			}
			if visited >= maxVisit {
				next, done = bytes.Clone(k), false
				return nil
			}
		}
		return nil
	})
	return next, covered, done, err
}

// EachAccountChangedIn is EachRowChangedIn for accounts.
func (s *Store) EachAccountChangedIn(
	from, through uint64,
	after []byte,
	maxVisit int,
	fn func(slot uint64, r AccountRow, pos []byte) error,
) (next []byte, covered, done bool, err error) {
	return s.EachRowChangedIn(AccountTag, accountAddedSlot, from, through, after, maxVisit,
		func(slot uint64, key, value, pos []byte) error {
			if len(key) < 2 {
				return errMalformedAccount
			}
			r, err := decodeAccount(key[1:], value)
			if err != nil {
				return err
			}
			return fn(slot, r, bytes.Clone(pos))
		})
}
