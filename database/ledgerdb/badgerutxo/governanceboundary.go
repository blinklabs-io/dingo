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

// GovernanceBoundaryKey is the row key of epoch's boundary snapshot.
func GovernanceBoundaryKey(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{GovernanceBoundaryTag}, epoch)
}

// PutGovernanceBoundary writes the boundary row of epoch without a journal
// entry, for a ledger-state import: the row lies at or below the imported
// point, which no rollback crosses.
func (s *Store) PutGovernanceBoundary(epoch uint64, value []byte) error {
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(GovernanceBoundaryKey(epoch), value)
	})
}

// pruneGovernanceBoundaries drops the boundary rows no read inside the
// rollback window uses once the journal floor reaches slot. The newest row
// whose boundary lies below slot is kept: it holds the epoch the floor sits
// in, which every slot of that epoch still reads.
func (s *Store) pruneGovernanceBoundaries(slot uint64) error {
	var below [][]byte
	if err := s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, []byte{GovernanceBoundaryTag}, func(k, v []byte) error {
			if len(v) < 8 {
				return fmt.Errorf("badgerutxo: malformed governance boundary row %x", k)
			}
			if binary.BigEndian.Uint64(v) < slot {
				below = append(below, k)
			}
			return nil
		})
	}); err != nil {
		return fmt.Errorf("badgerutxo: scan governance boundaries: %w", err)
	}
	if len(below) < 2 {
		return nil
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	for _, k := range below[:len(below)-1] {
		if err := wb.Delete(k); err != nil {
			return fmt.Errorf(
				"badgerutxo: prune governance boundaries: %w",
				err,
			)
		}
	}
	return wb.Flush()
}
