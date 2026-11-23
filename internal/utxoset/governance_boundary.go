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

package utxoset

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
)

var errMalformedGovernanceBoundary = errors.New(
	"utxoset: malformed governance boundary row",
)

// encodeGovernanceBoundary is the boundary slot, 8 bytes big-endian, then
// the snapshot as JSON. The store reads the slot alone when it prunes.
func encodeGovernanceBoundary(
	s *models.GovernanceBoundarySnapshot,
) ([]byte, error) {
	body, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("utxoset: encode governance boundary: %w", err)
	}
	return append(
		binary.BigEndian.AppendUint64(nil, s.BoundarySlot),
		body...), nil
}

func decodeGovernanceBoundary(
	v []byte,
) (*models.GovernanceBoundarySnapshot, error) {
	if len(v) < 8 {
		return nil, errMalformedGovernanceBoundary
	}
	var s models.GovernanceBoundarySnapshot
	if err := json.Unmarshal(v[8:], &s); err != nil {
		return nil, fmt.Errorf("%w: %w", errMalformedGovernanceBoundary, err)
	}
	if s.BoundarySlot != binary.BigEndian.Uint64(v) {
		return nil, errMalformedGovernanceBoundary
	}
	return &s, nil
}

// governanceBoundaryRows converts a commit's boundary snapshots to rows
// journaled at their boundary slots, so a rollback below a boundary removes
// its row even when a deferred RATIFY wrote it after the boundary committed.
func governanceBoundaryRows(
	lc ledger.LedgerChanges,
) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0, len(lc.GovernanceBoundaries))
	for i := range lc.GovernanceBoundaries {
		s := &lc.GovernanceBoundaries[i]
		v, err := encodeGovernanceBoundary(s)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  s.BoundarySlot,
			Key:   badgerutxo.GovernanceBoundaryKey(s.Epoch),
			Value: v,
		})
	}
	return out, nil
}

// GovernanceBoundarySnapshotAt implements ledger.GovernanceBoundaryReader.
func (s *Set) GovernanceBoundarySnapshotAt(
	epoch uint64,
	at *uint64,
) (*models.GovernanceBoundarySnapshot, bool, error) {
	key := badgerutxo.GovernanceBoundaryKey(epoch)
	var (
		v   []byte
		ok  bool
		err error
	)
	if at == nil {
		v, ok, err = s.store.Row(key)
	} else {
		v, ok, err = s.store.RowAt(key, *at)
	}
	if err != nil || !ok {
		return nil, false, err
	}
	snapshot, err := decodeGovernanceBoundary(v)
	if err != nil {
		return nil, false, err
	}
	return snapshot, true, nil
}

// LoadGovernanceBoundarySnapshot implements
// ledgerstate.GovernanceBoundaryLoader: it replaces the row of
// snapshot.Epoch without a journal entry, since an imported boundary lies at
// or below the imported point.
func (s *Set) LoadGovernanceBoundarySnapshot(
	snapshot *models.GovernanceBoundarySnapshot,
) error {
	v, err := encodeGovernanceBoundary(snapshot)
	if err != nil {
		return err
	}
	if err := s.store.PutGovernanceBoundary(snapshot.Epoch, v); err != nil {
		return fmt.Errorf("utxoset: load governance boundary: %w", err)
	}
	return nil
}

var _ ledger.GovernanceBoundaryReader = (*Set)(nil)
