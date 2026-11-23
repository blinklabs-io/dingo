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
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// The delegator inactivity activation and the pending ratification record
// are ledger state the metadata store kept under sync_state keys:
//
//	0 0                          the epoch the activation ran at (8 BE)
//	1                            the pending ratification record
//
// Both are journaled as rows. The credentials the activation stamped are
// marked on their account rows.
const inactivityEpochKind byte = 0

// InactivityEpochKey keys the epoch the delegator inactivity activation ran
// at.
func InactivityEpochKey() []byte {
	return []byte{InactivityTag, inactivityEpochKind}
}

// RatifyPendingKey keys the pending ratification record.
func RatifyPendingKey() []byte {
	return []byte{RatifyPendingTag}
}

// loadTagged replaces the rows under tag with the ones feed supplies and
// sets marker. A loaded row has no journal entry, so feed names the slot a
// rollback drops it below through tombstone.
func (s *Store) loadTagged(
	tag byte,
	marker []byte,
	feed func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error,
) error {
	// A drop raises the journal floor, so only a load that stopped part way
	// pays for one.
	var held bool
	if err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{tag}
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		held = it.Valid()
		return nil
	}); err != nil {
		return err
	}
	if held {
		if err := s.dropPrefix([]byte{tag}); err != nil {
			return fmt.Errorf("badgerutxo: drop %q rows: %w", tag, err)
		}
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != tag {
			return fmt.Errorf("badgerutxo: %x is not a %q key", k, tag)
		}
		return wb.Set(k, v)
	}, wb.tombstoneFor([]byte{tag}))
	if err != nil {
		return fmt.Errorf("badgerutxo: load %q rows: %w", tag, err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load %q rows: %w", tag, err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(marker, []byte{1})
	})
}

// LoadInactivity replaces the delegator inactivity activation rows with the
// ones feed supplies and marks them loaded, for a store from before it kept
// them.
func (s *Store) LoadInactivity(
	feed func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error,
) error {
	return s.loadTagged(InactivityTag, inactivityKey, feed)
}

// InactivityLoaded reports whether the store holds the delegator inactivity
// activation up to its tip.
func (s *Store) InactivityLoaded() (bool, error) {
	return s.hasMarker(inactivityKey)
}

// LoadRatifyPending replaces the pending ratification record with the one
// feed supplies and marks it loaded, for a store from before it kept one.
func (s *Store) LoadRatifyPending(
	feed func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error,
) error {
	return s.loadTagged(RatifyPendingTag, ratifyPendingKey, feed)
}

// RatifyPendingLoaded reports whether the store holds the pending
// ratification record up to its tip.
func (s *Store) RatifyPendingLoaded() (bool, error) {
	return s.hasMarker(ratifyPendingKey)
}
