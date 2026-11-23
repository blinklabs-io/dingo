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
	"github.com/dgraph-io/badger/v4"
)

// DecodeLegacyAccountForTest decodes r through the account encoding that
// predates CIP-0163 activation provenance.
func DecodeLegacyAccountForTest(r AccountRow) (AccountRow, error) {
	v := encodeAccount(r)
	v = v[:len(v)-9]
	return decodeAccount(credKey(r.Tag, r.Key), v)
}

// DeleteFormatForTest removes the format mark, as a store written before it
// existed lacks it.
func (s *Store) DeleteFormatForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(formatKey)
	})
}

// UseLegacyGovernanceMarkerForTest leaves only the marker a store wrote
// before proposal and vote rows carried row IDs.
func (s *Store) UseLegacyGovernanceMarkerForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(governanceKey); err != nil {
			return err
		}
		return txn.Set(legacyGovernanceKey, []byte{1})
	})
}

// HasLegacyGovernanceMarkerForTest reports whether the pre-row-ID marker is
// set.
func (s *Store) HasLegacyGovernanceMarkerForTest() (bool, error) {
	var ok bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(legacyGovernanceKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// SetBudgetForTest replaces Badger's per-transaction entry and byte limits in
// the plan of a chunked batch. The real limits still apply at commit.
func (s *Store) SetBudgetForTest(entries, bytes int64) {
	s.budgetEntries, s.budgetBytes = entries, bytes
}

// SetAfterChunkForTest sets fn to run between the transactions of a chunked
// batch, after done of them; an error from it stops the batch there.
func (s *Store) SetAfterChunkForTest(fn func(done int) error) {
	s.afterChunk = fn
}

// MidBatchForTest is what a crash between the transactions of a chunked
// batch leaves: the tip, the number of nonce rows and the failure mark. It
// reads without waiting for the batch, which the AfterChunk hook runs inside.
type MidBatchForTest struct {
	Tip      ocommon.Point
	HasTip   bool
	Nonces   int
	Failed   bool
	FailedAt uint64
}

// MidBatchStateForTest reads the state a crash would leave right now.
func (s *Store) MidBatchStateForTest() (MidBatchForTest, error) {
	var m MidBatchForTest
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		if m.Tip, m.HasTip, err = readTip(txn); err != nil {
			return err
		}
		if m.FailedAt, m.Failed, err = readFailed(txn); err != nil {
			return err
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{NonceTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			m.Nonces++
		}
		return nil
	})
	return m, err
}

// UseLegacyRewardBlocksMarkerForTest leaves only the marker a store wrote
// before its reward block rows carried the overlay-inclusive counts.
func (s *Store) UseLegacyRewardBlocksMarkerForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(rewardBlocksKey); err != nil {
			return err
		}
		return txn.Set([]byte("mblkfee"), []byte{1})
	})
}

// HasLegacyRewardBlocksMarkerForTest reports whether the earlier reward
// block marker is set.
func (s *Store) HasLegacyRewardBlocksMarkerForTest() (bool, error) {
	var ok bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("mblkfee"))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// UseLegacyPoolHistoryMarkerForTest leaves only the marker a store wrote
// before its pool events carried an effective slot.
func (s *Store) UseLegacyPoolHistoryMarkerForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(poolHistoryKey); err != nil {
			return err
		}
		return txn.Set([]byte("mpoolhist"), []byte{1})
	})
}

// HasLegacyPoolHistoryMarkerForTest reports whether the earlier pool history
// marker is set.
func (s *Store) HasLegacyPoolHistoryMarkerForTest() (bool, error) {
	var ok bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("mpoolhist"))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// UseLegacyOpCertsMarkerForTest leaves only the marker a store wrote before
// its op-cert counters carried the sequence of the latest observation.
func (s *Store) UseLegacyOpCertsMarkerForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(opCertsKey); err != nil {
			return err
		}
		return txn.Set([]byte("mopcerts"), []byte{1})
	})
}

// HasLegacyOpCertsMarkerForTest reports whether the earlier op-cert marker is
// set.
func (s *Store) HasLegacyOpCertsMarkerForTest() (bool, error) {
	var ok bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("mopcerts"))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// UseLegacyRegistrationHistoryMarkerForTest leaves only the marker a store
// wrote before its stake events carried an effective slot.
func (s *Store) UseLegacyRegistrationHistoryMarkerForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(registrationHistoryKey); err != nil {
			return err
		}
		return txn.Set([]byte("mreghist"), []byte{1})
	})
}

// UseLegacyWitnessSlotsMarkerForTest leaves only the marker a store wrote
// before its witness rows carried an effective slot.
func (s *Store) UseLegacyWitnessSlotsMarkerForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(witnessSlotsKey); err != nil {
			return err
		}
		return txn.Set([]byte("mwitness"), []byte{1})
	})
}

// HasKeyForTest reports whether key is set.
func (s *Store) HasKeyForTest(key []byte) (bool, error) {
	var ok bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// SetAfterRollbackStepForTest sets fn to run between the transactions of a
// rollback that needs a pending mark, after done of them; an error from it
// stops the rollback there. Step 0 is after the rows under the undone range
// tombstones are dropped.
func (s *Store) SetAfterRollbackStepForTest(fn func(done int) error) {
	s.afterRollbackStep = fn
}

// PendingRollbackForTest reads the rollback-in-progress mark a crash would
// leave right now.
func (s *Store) PendingRollbackForTest() (ocommon.Point, bool, error) {
	var (
		p  ocommon.Point
		ok bool
	)
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		p, ok, err = readPendingRollback(txn)
		return err
	})
	return p, ok, err
}

// SetAfterBootstrapFlushForTest sets fn to run after a bootstrap has written
// its new contents and before it writes the tip; an error from it stops the
// bootstrap there, as a crash would.
func (s *Store) SetAfterBootstrapFlushForTest(fn func() error) {
	s.afterBootstrapFlush = fn
}

// WriteLegacyRewardRecordsForTest stores records as a store from before
// sequences and the sequence index wrote them: the amount only, no number, no
// index entry, and clears the marks that say the index is built.
func (s *Store) WriteLegacyRewardRecordsForTest(recs []RewardRecord) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, r := range recs {
			v := make([]byte, rewardRecordAmountLen)
			v[7] = byte(r.Amount) //nolint:gosec // a small test amount
			k, err := rewardRecordKey(r)
			if err != nil {
				return err
			}
			if err := txn.Set(k, v); err != nil {
				return err
			}
		}
		if err := txn.Delete(rewardSeqKey); err != nil {
			return err
		}
		return txn.Delete(rewardIndexedKey)
	})
}

// DeleteExportFloorForTest removes the export floor, as a store written
// before it existed lacks it.
func (s *Store) DeleteExportFloorForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(exportFloorKey)
	})
}

// PutLegacySnapshotRowForTest stores r as a store wrote it before capture
// slots were kept: the value alone, with no index entry.
func (s *Store) PutLegacySnapshotRowForTest(r SnapshotRow) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(snapshotKey(r), r.Value)
	})
}

// PutUnindexedSnapshotRowForTest stores r as an interrupted backfill leaves
// it: with its capture slot and no index entry.
func (s *Store) PutUnindexedSnapshotRowForTest(r SnapshotRow) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(snapshotKey(r), encodeSnapshotValue(r.CapturedSlot, r.Value))
	})
}

// ClearSnapshotSlotsMarkerForTest removes the marker that says every
// snapshot row is indexed.
func (s *Store) ClearSnapshotSlotsMarkerForTest() error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(snapshotSlotsKey)
	})
}

// SnapshotIndexMismatchForTest compares the capture index with the rows that
// carry a capture slot and returns what differs: a row with no entry, or an
// entry with no row or the wrong slot. It is empty when they agree.
func (s *Store) SnapshotIndexMismatchForTest() ([]string, error) {
	var out []string
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(snapshotSlotsKey)
		marked := err == nil
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		want := map[string]bool{}
		err = eachPrefix(txn, []byte{SnapshotTag}, func(k, v []byte) error {
			idx, err := snapshotSlotKeyOf(k, v)
			if idx != nil {
				want[string(idx)] = true
			} else if err == nil && marked {
				out = append(out, fmt.Sprintf("indexed store has legacy row: %x", k))
			}
			return err
		})
		if err != nil {
			return err
		}
		err = eachPrefix(txn, []byte{SnapshotSlotTag}, func(k, _ []byte) error {
			if !want[string(k)] {
				out = append(out, fmt.Sprintf("entry without row: %x", k))
			}
			delete(want, string(k))
			return nil
		})
		for k := range want {
			out = append(out, fmt.Sprintf("row without entry: %x", k))
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AddedIndexFloorForTest returns the floor the family's index of rows below
// the floor was built at, and whether one is held.
func (s *Store) AddedIndexFloorForTest(family byte) (uint64, bool) {
	s.added.mu.Lock()
	defer s.added.mu.Unlock()
	idx := s.added.fam[family]
	if idx == nil {
		return 0, false
	}
	return idx.floor, true
}

// SetAfterRowsGenerationForTest sets fn to run when EachRowChangedIn has read
// the rows' generation and not yet begun its transaction.
func (s *Store) SetAfterRowsGenerationForTest(fn func()) {
	s.added.afterGen = fn
}

// SetBeforeAddedIndexForTest sets fn to run inside the transaction of a read
// below the floor, before it looks up the family's index.
func (s *Store) SetBeforeAddedIndexForTest(fn func()) {
	s.added.beforeIndex = fn
}
