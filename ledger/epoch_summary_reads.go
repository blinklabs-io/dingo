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

package ledger

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// EpochSummaryReader serves epoch summary rows from a live UTxO set that
// keeps epoch state.
type EpochSummaryReader interface {
	// EpochSummariesLoaded reports whether the reader holds every summary
	// row; until it does, the set does not serve the reads.
	EpochSummariesLoaded() (bool, error)
	// EpochSummary returns epoch's summary row, or nil.
	EpochSummary(epoch uint64) (*models.EpochSummary, error)
}

// EpochSummaryLister lists every summary row of a set that keeps them.
type EpochSummaryLister interface {
	EpochSummaries() ([]models.EpochSummary, error)
}

// epochSummaryReader returns the set's epoch state and summary readers and
// a true indicator when both readers are available from the set. The total active stake read falls back to snapshot rows, so both
// readers must hold their rows.
func (ls *LedgerState) epochSummaryReader(txn ledgerdb.Txn) (
	EpochStateReader,
	EpochSummaryReader,
	bool,
) {
	er, mode := ls.epochStateReader(txn)
	if !mode || er == nil {
		return nil, nil, false
	}
	sr, ok := ls.readSet(txn).(EpochSummaryReader)
	if !ok {
		return nil, nil, false
	}
	if !ls.epochSummarySetLoaded.Load() {
		loaded, err := sr.EpochSummariesLoaded()
		if err != nil || !loaded {
			return nil, nil, false
		}
		ls.epochSummarySetLoaded.Store(true)
	}
	return er, sr, mode
}

// summaryViewLocked is epoch's summary row as txn sees the set, or nil. The
// caller holds utxoSetApplyMu for reading.
func (ls *LedgerState) summaryViewLocked(
	er EpochStateReader,
	sr EpochSummaryReader,
	txn ledgerdb.Txn,
	epoch uint64,
) (*models.EpochSummary, error) {
	ops := ls.stagedEpochStateOps(txn)
	row, err := sr.EpochSummary(epoch)
	if err != nil {
		return nil, err
	}
	starts, err := stagedRollbackEpochStarts(er, ops)
	if err != nil {
		return nil, err
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			newest, found := rollbackEpochStarts(starts, *o.rollbackTo)
			if found && epoch > newest {
				row = nil
			}
		}
		for _, e := range o.epochs {
			starts[e.EpochId] = e.StartSlot
		}
		for _, sm := range o.summaries {
			if sm.Epoch == epoch {
				c := sm
				row = &c
			}
		}
	}
	return row, nil
}

// totalActiveStakeFrom answers GetTotalActiveStake from a summary row and the
// snapshot rows of the queried epoch and type, as the metadata store does:
// a ready summary answers a mark query, and the rows' sum answers the rest.
func totalActiveStakeFrom(
	sm *models.EpochSummary,
	snapshotType string,
	snapshots []*models.PoolStakeSnapshot,
) (uint64, error) {
	if snapshotType == models.PoolStakeSnapshotTypeMark &&
		sm != nil && sm.SnapshotReady {
		return uint64(sm.TotalActiveStake), nil
	}
	var total uint64
	for _, sn := range snapshots {
		v := uint64(sn.TotalStake)
		if ^uint64(0)-total < v {
			return 0, errors.New("amount sum overflow")
		}
		total += v
	}
	return total, nil
}

func sameSummaryPtr(a, b *models.EpochSummary) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	x, y := *a, *b
	x.ID, y.ID = 0, 0
	return fmt.Sprintf("%+v", x) == fmt.Sprintf("%+v", y)
}

func (s stagingLedgerDB) GetEpochSummary(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.EpochSummary, error) {
	er, sr, mode := s.ls.epochSummaryReader(txn)
	if !mode {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilySummaries)
	}
	if er == nil || sr == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilySummaries)
	}
	s.ls.utxoSetApplyMu.RLock()
	fromSet, setErr := s.ls.summaryViewLocked(er, sr, txn, epoch)
	s.ls.utxoSetApplyMu.RUnlock()
	return fromSet, setErr
}

func (s stagingLedgerDB) GetTotalActiveStake(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (uint64, error) {
	er, sr, mode := s.ls.epochSummaryReader(txn)
	if !mode {
		return 0, s.ls.setFamilyUnserved(nodesettings.FamilySummaries)
	}
	if er == nil || sr == nil {
		return 0, s.ls.setFamilyUnserved(nodesettings.FamilySummaries)
	}
	// One lock hold, so a commit's set apply cannot land between the summary
	// and the snapshot rows.
	s.ls.utxoSetApplyMu.RLock()
	fromSet, setErr := func() (uint64, error) {
		sm, err := s.ls.summaryViewLocked(er, sr, txn, epoch)
		if err != nil {
			return 0, err
		}
		rows, err := s.ls.snapshotViewLocked(er, txn, epoch, snapshotType, 0)
		if err != nil {
			return 0, err
		}
		return totalActiveStakeFrom(sm, snapshotType, rows)
	}()
	s.ls.utxoSetApplyMu.RUnlock()
	return fromSet, setErr
}

// summaryListViewLocked is every summary row as txn sees the set. The caller
// holds utxoSetApplyMu for reading.
func (ls *LedgerState) summaryListViewLocked(
	er EpochStateReader,
	sr EpochSummaryLister,
	txn ledgerdb.Txn,
) (map[uint64]models.EpochSummary, error) {
	ops := ls.stagedEpochStateOps(txn)
	rows, err := sr.EpochSummaries()
	if err != nil {
		return nil, err
	}
	starts, err := stagedRollbackEpochStarts(er, ops)
	if err != nil {
		return nil, err
	}
	view := make(map[uint64]models.EpochSummary, len(rows))
	for _, sm := range rows {
		view[sm.Epoch] = sm
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			newest, found := rollbackEpochStarts(starts, *o.rollbackTo)
			if found {
				for id := range view {
					if id > newest {
						delete(view, id)
					}
				}
			}
		}
		for _, e := range o.epochs {
			starts[e.EpochId] = e.StartSlot
		}
		for _, sm := range o.summaries {
			view[sm.Epoch] = sm
		}
	}
	return view, nil
}

// GetLatestEpochSummary is the set's highest-epoch summary, as txn sees it,
// once the summaries family is drained; it is otherwise the metadata store's.
func (s stagingLedgerDB) GetLatestEpochSummary(
	txn ledgerdb.Txn,
) (*models.EpochSummary, error) {
	er, sr, _ := s.ls.epochSummaryReader(txn)
	lister, ok := sr.(EpochSummaryLister)
	if er == nil || sr == nil || !ok {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilySummaries)
	}
	s.ls.utxoSetApplyMu.RLock()
	view, err := s.ls.summaryListViewLocked(er, lister, txn)
	s.ls.utxoSetApplyMu.RUnlock()
	if err != nil {
		return nil, err
	}
	var latest *models.EpochSummary
	for _, sm := range view {
		if latest == nil || sm.Epoch > latest.Epoch {
			c := sm
			latest = &c
		}
	}
	return latest, nil
}
