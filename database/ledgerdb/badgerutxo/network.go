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

	"github.com/dgraph-io/badger/v4"
)

// Network state keys:
//
//	t <slot:8 BE>   treasury and reserves recorded at slot
//	d <slot:8 BE>   the donations of the block at slot: epoch, amount
//
// NetworkStateRow is the treasury and reserves recorded at a slot.
type NetworkStateRow struct {
	Slot     uint64
	Treasury uint64
	Reserves uint64
}

// DonationRow is the treasury donations of the block at a slot.
type DonationRow struct {
	Slot   uint64
	Epoch  uint64
	Amount uint64
}

func slotKey(tag byte, slot uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{tag}, slot)
}

func encodePair(a, b uint64) []byte {
	return binary.BigEndian.AppendUint64(
		binary.BigEndian.AppendUint64(nil, a), b,
	)
}

func decodeSlotPair(k, v []byte) (slot, a, b uint64, err error) {
	if len(k) != 9 || len(v) != 16 {
		return 0, 0, 0, errors.New("badgerutxo: malformed network state entry")
	}
	return binary.BigEndian.Uint64(k[1:]),
		binary.BigEndian.Uint64(v),
		binary.BigEndian.Uint64(v[8:]),
		nil
}

// putNetworkState and putDonation replace any row at the same slot, as the
// metadata store's upserts do.
func putNetworkState(txn *badger.Txn, r NetworkStateRow) error {
	return txn.Set(
		slotKey(NetworkStateTag, r.Slot), encodePair(r.Treasury, r.Reserves),
	)
}

func putDonation(txn *badger.Txn, r DonationRow) error {
	return txn.Set(slotKey(DonationTag, r.Slot), encodePair(r.Epoch, r.Amount))
}

// deleteSlotKeysAfter deletes the keys under tag whose slot is after slot.
func deleteSlotKeysAfter(txn *badger.Txn, tag byte, slot uint64) error {
	var keys [][]byte
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{tag}
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Seek(slotKey(tag, slot+1)); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	if tag == RewardRecordTag {
		if err := deleteRewardSeqIndexFor(txn, keys); err != nil {
			return err
		}
	}
	return deleteKeys(txn, keys)
}

// deleteSlotKeysWithin is deleteSlotKeysAfter for at most limit keys; it reports whether keys after slot remain.
func deleteSlotKeysWithin(
	txn *badger.Txn,
	tag byte,
	slot uint64,
	limit int64,
) (bool, error) {
	var keys [][]byte
	more := false
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{tag}
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Seek(slotKey(tag, slot+1)); it.Valid(); it.Next() {
		if int64(len(keys)) >= limit {
			more = true
			break
		}
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	if tag == RewardRecordTag {
		if err := deleteRewardSeqIndexFor(txn, keys); err != nil {
			return false, err
		}
	}
	return more, deleteKeys(txn, keys)
}

// slotKeysAfterExceed reports whether more than limit keys of tag follow slot.
func slotKeysAfterExceed(
	txn *badger.Txn,
	tag byte,
	slot uint64,
	limit int64,
) bool {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{tag}
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	var n int64
	for it.Seek(slotKey(tag, slot+1)); it.Valid(); it.Next() {
		if n++; n > limit {
			return true
		}
	}
	return false
}

// NetworkStateAt returns the newest network state row at or before slot.
func (s *Store) NetworkStateAt(slot uint64) (NetworkStateRow, bool, error) {
	var row NetworkStateRow
	var found bool
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{NetworkStateTag}
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(slotKey(NetworkStateTag, slot))
		if !it.Valid() {
			return nil
		}
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		row.Slot, row.Treasury, row.Reserves, err = decodeSlotPair(
			it.Item().KeyCopy(nil), v,
		)
		found = err == nil
		return err
	})
	return row, found, err
}

// NetworkStates returns every network state row, in slot order.
func (s *Store) NetworkStates() ([]NetworkStateRow, error) {
	var out []NetworkStateRow
	err := s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, []byte{NetworkStateTag}, func(k, v []byte) error {
			slot, treasury, reserves, err := decodeSlotPair(k, v)
			if err != nil {
				return err
			}
			out = append(out, NetworkStateRow{
				Slot: slot, Treasury: treasury, Reserves: reserves,
			})
			return nil
		})
	})
	return out, err
}

// Donations returns every donation row, in slot order.
func (s *Store) Donations() ([]DonationRow, error) {
	var out []DonationRow
	err := s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, []byte{DonationTag}, func(k, v []byte) error {
			slot, epoch, amount, err := decodeSlotPair(k, v)
			if err != nil {
				return err
			}
			out = append(out, DonationRow{
				Slot: slot, Epoch: epoch, Amount: amount,
			})
			return nil
		})
	})
	return out, err
}

// LoadNetworkState replaces the store's network state and donation rows
// with the given ones and marks them loaded, for a store from before it
// kept them.
func (s *Store) LoadNetworkState(
	states []NetworkStateRow,
	donations []DonationRow,
) error {
	if err := s.dropPrefix(
		[]byte{NetworkStateTag}, []byte{DonationTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop network state: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	for _, r := range states {
		err := wb.Set(
			slotKey(NetworkStateTag, r.Slot),
			encodePair(r.Treasury, r.Reserves),
		)
		if err != nil {
			return fmt.Errorf("badgerutxo: load network state: %w", err)
		}
	}
	for _, r := range donations {
		err := wb.Set(slotKey(DonationTag, r.Slot), encodePair(r.Epoch, r.Amount))
		if err != nil {
			return fmt.Errorf("badgerutxo: load donations: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load network state: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(networkStateKey, []byte{1})
	})
}

// NetworkStateLoaded reports whether the store holds every network state
// and donation row up to its tip.
func (s *Store) NetworkStateLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(networkStateKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// DeleteDonationsForEpoch deletes the donation rows of epoch. A ledger-state
// import replaces them with the snapshot's total for the epoch; the set
// cannot roll back below the snapshot, so nothing is journaled.
func (s *Store) DeleteDonationsForEpoch(epoch uint64) error {
	return s.update(func(txn *badger.Txn) error {
		var keys [][]byte
		err := eachPrefix(txn, []byte{DonationTag}, func(k, v []byte) error {
			_, e, _, err := decodeSlotPair(k, v)
			if err != nil {
				return err
			}
			if e == epoch {
				keys = append(keys, bytes.Clone(k))
			}
			return nil
		})
		if err != nil {
			return err
		}
		return deleteKeys(txn, keys)
	})
}
