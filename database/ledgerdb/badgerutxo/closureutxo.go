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
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// ClosureUtxoKey is the key of a closure output effect: the slot it was
// applied at, whether the output was spent (kind 1) rather than created
// (kind 0), and the output.
func ClosureUtxoKey(applied uint64, spent bool, ref Ref) []byte {
	k := make([]byte, 0, 1+8+1+len(ref))
	k = append(k, ClosureUtxoTag)
	k = binary.BigEndian.AppendUint64(k, applied)
	kind := byte(0)
	if spent {
		kind = 1
	}
	k = append(k, kind)
	return append(k, ref[:]...)
}

// DecodeClosureUtxoKey splits a closure output effect key.
func DecodeClosureUtxoKey(
	k []byte,
) (applied uint64, spent bool, ref Ref, err error) {
	if len(k) != 1+8+1+len(ref) || k[0] != ClosureUtxoTag {
		return 0, false, ref, fmt.Errorf(
			"badgerutxo: malformed closure output key %x", k,
		)
	}
	copy(ref[:], k[10:])
	return binary.BigEndian.Uint64(k[1:9]), k[9] == 1, ref, nil
}

// pruneClosureUtxoEffects drops the effects applied before slot, which only
// a read as of an earlier slot would use, once the journal no longer reaches
// it.
func (s *Store) pruneClosureUtxoEffects(slot uint64) error {
	var keys [][]byte
	if err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{ClosureUtxoTag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) < 9 || binary.BigEndian.Uint64(k[1:9]) >= slot {
				return nil
			}
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: scan closure output effects: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			return fmt.Errorf("badgerutxo: prune closure output effects: %w", err)
		}
	}
	return wb.Flush()
}

// LoadClosureUtxoEffects replaces the store's closure output effects with the
// rows feed supplies, and marks them loaded.
func (s *Store) LoadClosureUtxoEffects(
	feed func(put func(k, v []byte) error) error,
) error {
	return s.loadFamily(
		ClosureUtxoTag, closureUtxoKey, "closure output effects", feed,
	)
}

// ClosureUtxoEffectsLoaded reports whether the store holds every closure
// output effect up to its tip.
func (s *Store) ClosureUtxoEffectsLoaded() (bool, error) {
	return s.hasMarker(closureUtxoKey)
}
