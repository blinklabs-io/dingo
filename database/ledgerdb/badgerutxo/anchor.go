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
	"errors"
	"fmt"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrAnchorUnknown is returned by AnchorView before SetAnchor has recorded an
// immutable tip.
var ErrAnchorUnknown = errors.New("badgerutxo: no anchor recorded")

// ErrAnchorBelowJournalFloor is returned by AnchorView when the anchor is
// older than the journal reaches, so the set as of the anchor cannot be
// rebuilt. It always wraps ErrRollbackBeyondJournal.
var ErrAnchorBelowJournalFloor = errors.New(
	"badgerutxo: anchor is below the journal floor",
)

// SetAnchor records the immutable tip: the newest point no rollback goes
// behind. PruneJournal then never raises the journal floor past it, which
// keeps the set able to roll back to every point in the volatile window and
// to answer AnchorView. A caller records a newer anchor as the chain
// advances.
func (s *Store) SetAnchor(point ocommon.Point) {
	p := point
	s.anchor.Store(&p)
}

// Anchor returns the point recorded by SetAnchor; ok is false before one is.
func (s *Store) Anchor() (point ocommon.Point, ok bool) {
	if a := s.anchor.Load(); a != nil {
		return *a, true
	}
	return ocommon.Point{}, false
}

// AnchorView opens a read-only view of the set as of the anchor, for export
// paths that need the immutable state while blocks keep applying. It is the
// set's journal read as of the anchor (AsOf): one Badger snapshot with the
// journaled prior states of every output changed after the anchor laid over
// it, so the live set is untouched and a commit during the reads cannot mix
// two states. Close releases the snapshot, which a caller holds for one job.
//
// It returns ErrAnchorUnknown before an anchor is recorded, and
// ErrAnchorBelowJournalFloor, wrapping ErrRollbackBeyondJournal, when the
// journal floor is past the anchor.
func (s *Store) AnchorView() (*AsOf, error) {
	anchor, ok := s.Anchor()
	if !ok {
		return nil, ErrAnchorUnknown
	}
	txn, err := s.readTxn()
	if err != nil {
		return nil, err
	}
	floor, err := readJournalFloor(txn)
	if err != nil {
		txn.Discard()
		return nil, err
	}
	// Records below the floor are gone; a rollback to a point needs every
	// record from the one after it.
	if anchor.Slot+1 < floor {
		txn.Discard()
		return nil, fmt.Errorf(
			"%w: anchor slot %d, journal floor %d: %w",
			ErrAnchorBelowJournalFloor, anchor.Slot, floor,
			ErrRollbackBeyondJournal,
		)
	}
	a, err := asOfIn(txn, anchor)
	if err != nil {
		txn.Discard()
		return nil, err
	}
	return a, nil
}
