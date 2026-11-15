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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

// MarkFailed records that the ledger could not apply a commit at slot. The
// lowest slot recorded since the last ClearFailed is kept: every commit from
// it on is suspect.
func (s *Store) MarkFailed(slot uint64) error {
	return s.update(func(txn *badger.Txn) error {
		if cur, ok, err := readFailed(txn); err != nil {
			return err
		} else if ok && cur <= slot {
			return nil
		}
		return txn.Set(failedKey, binary.BigEndian.AppendUint64(nil, slot))
	})
}

// Failed returns the lowest slot MarkFailed recorded; ok is false when none
// is recorded.
func (s *Store) Failed() (slot uint64, ok bool, err error) {
	err = s.view(func(txn *badger.Txn) error {
		slot, ok, err = readFailed(txn)
		return err
	})
	return slot, ok, err
}

// ClearFailed removes the mark MarkFailed left.
func (s *Store) ClearFailed() error {
	return s.update(func(txn *badger.Txn) error {
		if _, ok, err := readFailed(txn); err != nil || !ok {
			return err
		}
		return txn.Delete(failedKey)
	})
}

func readFailed(txn *badger.Txn) (uint64, bool, error) {
	item, err := txn.Get(failedKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var slot uint64
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("badgerutxo: failure mark is %d bytes", len(v))
		}
		slot = binary.BigEndian.Uint64(v)
		return nil
	})
	return slot, true, err
}

// PointBefore returns the tip the store held before its oldest journaled
// commit at or after slot, which Rollback restores exactly. ok is false when
// no journaled commit is at or after slot, so the store never took one. It
// returns ErrRollbackBeyondJournal when the journal does not reach a tip
// below slot.
func (s *Store) PointBefore(slot uint64) (ocommon.Point, bool, error) {
	var (
		point ocommon.Point
		found bool
	)
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.Prefix = []byte{JournalTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(append([]byte{JournalTag},
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))
		var oldest journalRecord
		for ; it.Valid(); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			rec, err := decodeRecord(v)
			if err != nil {
				return err
			}
			if rec.point.Slot < slot {
				break
			}
			oldest, found = rec, true
		}
		if !found {
			return nil
		}
		if !oldest.hadTip || oldest.prevTip.Slot >= slot {
			return ErrRollbackBeyondJournal
		}
		floor, err := readLoadFloor(txn)
		if err != nil {
			return err
		}
		if oldest.prevTip.Slot+1 < floor {
			return ErrRollbackBeyondJournal
		}
		point = oldest.prevTip
		return nil
	})
	return point, found, err
}

// HasJournalPoint reports whether Rollback(point) restores a set that held
// exactly point: point is the current tip, or the tip before a journaled
// commit. A point that merely lies below the tip is not one, since Rollback
// would record it as the tip of a state the set never held.
func (s *Store) HasJournalPoint(point ocommon.Point) (bool, error) {
	has := false
	err := s.view(func(txn *badger.Txn) error {
		tip, ok, err := readTip(txn)
		if err != nil {
			return err
		}
		if ok && tip.Slot == point.Slot && bytes.Equal(tip.Hash, point.Hash) {
			has = true
			return nil
		}
		floor, err := readLoadFloor(txn)
		if err != nil {
			return err
		}
		if point.Slot+1 < floor {
			return nil
		}
		opts := badger.DefaultIteratorOptions
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
			if rec.hadTip && rec.prevTip.Slot == point.Slot &&
				rec.prevTip.Slot < rec.point.Slot &&
				bytes.Equal(rec.prevTip.Hash, point.Hash) {
				has = true
				return nil
			}
			if rec.point.Slot <= point.Slot {
				return nil
			}
		}
		return nil
	})
	return has, err
}

// JournalFloor returns the oldest tip a Rollback reaches: the tip before the
// oldest journaled commit. ok is false when the store holds no tip, and the
// floor is the tip itself when the journal is empty.
func (s *Store) JournalFloor() (point ocommon.Point, ok bool, err error) {
	err = s.view(func(txn *badger.Txn) error {
		tip, hasTip, err := readTip(txn)
		if err != nil || !hasTip {
			return err
		}
		ok = true
		point = tip
		floor, err := readLoadFloor(txn)
		if err != nil {
			return err
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{JournalTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		// The oldest tip before a record whose state the state journal
		// still reaches; a record before the floor is not one.
		for it.Rewind(); it.Valid(); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			rec, err := decodeRecord(v)
			if err != nil {
				return err
			}
			if !rec.hadTip {
				return nil
			}
			if rec.prevTip.Slot+1 >= floor {
				point = rec.prevTip
				return nil
			}
		}
		return nil
	})
	return point, ok, err
}

// Backup streams a consistent copy of the whole store to w. The copy is a
// Badger backup, so Restore loads it into an empty store.
func (s *Store) Backup(ctx context.Context, w io.Writer) error {
	if s.snap != nil {
		return ErrSnapshotReadOnly
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.db.Backup(w, 0)
	return err
}

// Restore loads a Backup stream into the store, which must be empty.
func (s *Store) Restore(ctx context.Context, r io.Reader) error {
	if s.snap != nil {
		return ErrSnapshotReadOnly
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Load(r, 256)
}
