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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// DRep history rows are the rows the metadata store keeps beside the DRep
// table to answer a read as of an earlier slot. Every row is keyed by the
// slot it was recorded at, first, so a rollback that deletes the rows after a
// slot is the journal's restore of the rows written after it:
//
//	T <slot:8> 0 <tag:1> <credential>   a DRep's activity and expiry epochs
//	                                    from the slot on, 8 BE each
//	T <slot:8> 1 <previous:8>           the dormancy counter before the change
//	                                    at the slot, 8 BE
//	T <slot:8> 2                        a dormant DRep expiry bump the epoch
//	                                    boundary at the slot applied
//	T <slot:8> 3 <tag:1> <staking key>  an account whose DRep delegation was
//	                                    cleared at the slot
const (
	drepHistoryExpiryKind   byte = 0
	drepHistoryDormancyKind byte = 1
	drepHistoryEventKind    byte = 2
	drepHistoryClearKind    byte = 3
)

func drepHistoryKey(slot uint64, kind byte, rest ...[]byte) []byte {
	k := binary.BigEndian.AppendUint64([]byte{DRepHistoryTag}, slot)
	k = append(k, kind)
	for _, r := range rest {
		k = append(k, r...)
	}
	return k
}

// DRepExpiryHistoryKey keys the activity and expiry a DRep holds from slot.
func DRepExpiryHistoryKey(slot uint64, tag uint8, credential []byte) []byte {
	return drepHistoryKey(slot, drepHistoryExpiryKind, []byte{tag}, credential)
}

// DRepDormancyHistoryKey keys the dormancy counter in force before the change
// at slot. Two changes at one slot always leave different counters before
// them, so the counter is part of the key.
func DRepDormancyHistoryKey(slot, previous uint64) []byte {
	return drepHistoryKey(
		slot, drepHistoryDormancyKind,
		binary.BigEndian.AppendUint64(nil, previous),
	)
}

// DRepExpiryEventKey keys the dormant expiry bump applied at slot.
func DRepExpiryEventKey(slot uint64) []byte {
	return drepHistoryKey(slot, drepHistoryEventKind)
}

// DRepClearKey keys the clearing of an account's DRep delegation at slot.
func DRepClearKey(slot uint64, tag uint8, stakingKey []byte) []byte {
	return drepHistoryKey(slot, drepHistoryClearKind, []byte{tag}, stakingKey)
}

// EncodeDRepExpiryHistoryValue encodes the activity and expiry epochs.
func EncodeDRepExpiryHistoryValue(activity, expiry uint64) []byte {
	return binary.BigEndian.AppendUint64(
		binary.BigEndian.AppendUint64(nil, activity), expiry,
	)
}

// DRepHistoryRowKind names what a DRep history row holds.
type DRepHistoryRowKind uint8

// The DRep history row kinds.
const (
	DRepHistoryExpiry DRepHistoryRowKind = iota
	DRepHistoryDormancy
	DRepHistoryEvent
	DRepHistoryClear
)

// DRepHistoryRow is a decoded DRep history row. Which fields it sets depends
// on Kind: Tag and Key name the DRep of an expiry row and the account of a
// clear; Activity and Expiry are an expiry row's; Previous is a dormancy
// row's.
type DRepHistoryRow struct {
	Kind     DRepHistoryRowKind
	Slot     uint64
	Tag      uint8
	Key      []byte
	Activity uint64
	Expiry   uint64
	Previous uint64
}

var errMalformedDRepHistory = errors.New(
	"badgerutxo: malformed DRep history row",
)

// DecodeDRepHistoryRow decodes a DRep history row from its key and value.
func DecodeDRepHistoryRow(k, v []byte) (DRepHistoryRow, error) {
	if len(k) < 10 || k[0] != DRepHistoryTag {
		return DRepHistoryRow{}, errMalformedDRepHistory
	}
	row := DRepHistoryRow{Slot: binary.BigEndian.Uint64(k[1:9])}
	rest := k[10:]
	switch k[9] {
	case drepHistoryExpiryKind:
		if len(rest) < 1 || len(v) != 16 {
			return DRepHistoryRow{}, errMalformedDRepHistory
		}
		row.Kind, row.Tag = DRepHistoryExpiry, rest[0]
		row.Key = append([]byte(nil), rest[1:]...)
		row.Activity = binary.BigEndian.Uint64(v)
		row.Expiry = binary.BigEndian.Uint64(v[8:])
	case drepHistoryDormancyKind:
		if len(rest) != 8 {
			return DRepHistoryRow{}, errMalformedDRepHistory
		}
		row.Kind, row.Previous = DRepHistoryDormancy, binary.BigEndian.Uint64(rest)
	case drepHistoryEventKind:
		if len(rest) != 0 {
			return DRepHistoryRow{}, errMalformedDRepHistory
		}
		row.Kind = DRepHistoryEvent
	case drepHistoryClearKind:
		if len(rest) < 1 {
			return DRepHistoryRow{}, errMalformedDRepHistory
		}
		row.Kind, row.Tag = DRepHistoryClear, rest[0]
		row.Key = append([]byte(nil), rest[1:]...)
	default:
		return DRepHistoryRow{}, fmt.Errorf(
			"badgerutxo: unknown DRep history row kind %d", k[9],
		)
	}
	return row, nil
}

// LoadDRepHistory replaces the store's DRep history rows with the ones feed
// supplies, each as a primary key and value, and marks them loaded. When the
// metadata store had drained the rows before they could be read, lost is set:
// the store is marked as holding the history from the tip on only, so a
// reader can tell its rows are not the whole history.
func (s *Store) LoadDRepHistory(
	lost bool,
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.dropPrefix([]byte{DRepHistoryTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop DRep history: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != DRepHistoryTag {
			return fmt.Errorf("badgerutxo: %x is not a DRep history key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load DRep history: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load DRep history: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(drepHistoryLoadedKey, markerValue(lost))
	})
}

// DRepHistoryLoaded reports whether the store holds the DRep history rows up
// to its tip.
func (s *Store) DRepHistoryLoaded() (bool, error) {
	return s.hasMarker(drepHistoryLoadedKey)
}

// DRepHistoryLost reports whether the metadata store had drained the DRep
// history before the store could load it, so the rows below the load are
// gone.
func (s *Store) DRepHistoryLost() (bool, error) {
	return s.markerLost(drepHistoryLoadedKey)
}

func markerValue(lost bool) []byte {
	if lost {
		return []byte{markerLost}
	}
	return []byte{1}
}

// markerLost is the value of a load marker whose rows the metadata store had
// already drained.
const markerLost byte = 2

func (s *Store) markerLost(marker []byte) (bool, error) {
	var lost bool
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(marker)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			lost = len(v) == 1 && v[0] == markerLost
			return nil
		})
	})
	return lost, err
}
