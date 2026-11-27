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

// The layout migration brings a store written by an earlier release to the
// layout this release reads, in place and once. Open runs it before the store
// serves anything, so no read or write sees a half-migrated store.
//
// Every step is idempotent and each leaves the store consistent for the next:
// a stop anywhere, however it happens, reruns the whole migration at the next
// Open, and a step finds the part of its work already done. layoutKey is
// written last.
//
// The migration deletes what only an IndexerDB writer or API storage mode
// read, and the rows API storage mode kept beyond the retention window. It
// has no reverse.

const (
	// snapshotHeaderMark started a pool stake snapshot value that carried its
	// capture slot ahead of the encoded row: the mark, then 8 bytes of slot.
	snapshotHeaderMark = 0x00
	snapshotHeaderLen  = 9
	// retiredRewardRecordLen is the length of a reward record value that
	// carried a sequence number after the amount and flag.
	retiredRewardRecordLen = rewardRecordLen + 8
	// baselineKeptLen is the length of an import baseline value without the
	// delegations the import recorded, which a longer value appends.
	baselineKeptLen = 1 + 8 + 1 + 8
)

// Migration steps, as the hook of a test sees them.
const (
	MigrationStepMarkers   = "markers"
	MigrationStepFamilies  = "families"
	MigrationStepRecords   = "records"
	MigrationStepSnapshots = "snapshots"
	MigrationStepBaselines = "baselines"
	MigrationStepJournal   = "journal"
	MigrationStepWindow    = "window"
)

// Retention is how many epochs before the newest the layout migration keeps.
// A zero field leaves its rows alone.
type Retention struct {
	// RewardEpochs bounds the reward stake input and derived reward rows.
	RewardEpochs uint64
	// SnapshotEpochs bounds the pool stake snapshot rows.
	SnapshotEpochs uint64
}

// WithRetention sets the window the layout migration prunes to.
func WithRetention(r Retention) Option {
	return func(s *Store) { s.retention = r }
}

func (s *Store) migrationStep(name string) error {
	if s.migrationHook == nil {
		return nil
	}
	return s.migrationHook(name)
}

// migrateLayout runs the layout migration unless the store holds the layout
// already. A store that holds a partial bootstrap is left alone: its rows are
// replaced whole by the bootstrap that completes it.
func (s *Store) migrateLayout() error {
	var done bool
	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkBootstrapComplete(txn); err != nil {
			return err
		}
		_, err := txn.Get(layoutKey)
		done = err == nil
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
	if errors.Is(err, ErrBootstrapIncomplete) {
		return nil
	}
	if err != nil || done {
		return err
	}
	steps := []struct {
		name string
		run  func() error
	}{
		{MigrationStepMarkers, s.dropRetiredMarkers},
		{MigrationStepFamilies, s.dropRetiredFamilies},
		{MigrationStepRecords, s.trimRewardRecords},
		{MigrationStepSnapshots, s.stripSnapshotHeaders},
		{MigrationStepBaselines, s.trimImportBaselines},
		{MigrationStepJournal, s.trimStateJournal},
		{MigrationStepWindow, s.dropRowsBeyondWindow},
	}
	for _, st := range steps {
		if err := st.run(); err != nil {
			return fmt.Errorf("%s: %w", st.name, err)
		}
		if err := s.migrationStep(st.name); err != nil {
			return err
		}
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(layoutKey, []byte{1})
	})
}

func (s *Store) dropRetiredMarkers() error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, k := range retiredMarkers {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) dropRetiredFamilies() error {
	return s.db.DropPrefix(retiredPrefixes...)
}

// rewriteUnder calls fix with every row under prefix and writes the value it
// returns when that differs from the row's. A row is rewritten only after the
// whole family was read through one snapshot, so a stop leaves each row
// either as it was or fixed.
func (s *Store) rewriteUnder(
	prefix []byte,
	fix func(k, v []byte) ([]byte, error),
) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			fixed, err := fix(k, v)
			if err != nil {
				return err
			}
			if fixed != nil && !bytes.Equal(fixed, v) {
				if err := wb.Set(k, fixed); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return wb.Flush()
}

// trimRewardRecords drops the sequence number from every reward record.
func (s *Store) trimRewardRecords() error {
	return s.rewriteUnder([]byte{RewardRecordTag}, func(_, v []byte) ([]byte, error) {
		if len(v) == retiredRewardRecordLen {
			return v[:rewardRecordLen], nil
		}
		return nil, nil
	})
}

// stripSnapshotHeaders drops the capture slot header from every pool stake
// snapshot row: the encoded row carries the capture slot itself. No encoded
// row starts with the mark, the CBOR encoding of the integer zero.
func (s *Store) stripSnapshotHeaders() error {
	return s.rewriteUnder([]byte{SnapshotTag}, func(_, v []byte) ([]byte, error) {
		if len(v) >= snapshotHeaderLen && v[0] == snapshotHeaderMark {
			return v[snapshotHeaderLen:], nil
		}
		return nil, nil
	})
}

// trimImportBaselines drops the delegations from every import baseline.
func (s *Store) trimImportBaselines() error {
	return s.rewriteUnder([]byte{ImportBaselineTag}, func(_, v []byte) ([]byte, error) {
		if len(v) > baselineKeptLen {
			return v[:baselineKeptLen], nil
		}
		return nil, nil
	})
}

// trimStateJournal brings the state journal entries of the rows migrated above
// to their layout, and deletes those of the dropped families, so a rollback
// restores no row the store no longer holds. An entry is k <slot:8> <row key>
// valued 0 for an absent row, else 1 and the row's value.
func (s *Store) trimStateJournal() error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{StateJournalTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) <= 9 {
				continue
			}
			switch k[9] {
			case retiredDRepHistoryTag:
				if err := wb.Delete(it.Item().KeyCopy(nil)); err != nil {
					return err
				}
			case ImportBaselineTag:
				v, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}
				if len(v) > 1+baselineKeptLen && v[0] == 1 {
					if err := wb.Set(
						it.Item().KeyCopy(nil), v[:1+baselineKeptLen],
					); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return wb.Flush()
}

// dropRowsBeyondWindow deletes the reward stake input, derived reward and
// pool stake snapshot rows of epochs below the retention window, which a store
// that kept its history for API queries still holds. The pool stake snapshot
// window is the backstop the ledger's retention guard never exceeds: whatever
// a deferred header still needs below the default window, the ledger's own
// prune releases at its next epoch boundary.
func (s *Store) dropRowsBeyondWindow() error {
	if s.retention == (Retention{}) {
		return nil
	}
	newest, ok, err := s.newestEpoch()
	if err != nil || !ok {
		return err
	}
	var prefixes [][]byte
	if s.retention.RewardEpochs > 0 && newest > s.retention.RewardEpochs {
		below := newest - s.retention.RewardEpochs
		for _, tag := range []byte{
			RewardStakeInputTag, RewardDerivedTag, RewardCursorTag,
		} {
			p, err := s.epochPrefixesBelow(tag, below)
			if err != nil {
				return err
			}
			prefixes = append(prefixes, p...)
		}
	}
	if s.retention.SnapshotEpochs > 0 && newest > s.retention.SnapshotEpochs {
		p, err := s.epochPrefixesBelow(
			SnapshotTag, newest-s.retention.SnapshotEpochs,
		)
		if err != nil {
			return err
		}
		prefixes = append(prefixes, p...)
	}
	if len(prefixes) == 0 {
		return nil
	}
	return s.db.DropPrefix(prefixes...)
}

// newestEpoch returns the number of the newest epoch row.
func (s *Store) newestEpoch() (uint64, bool, error) {
	var newest uint64
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		opts.Prefix = []byte{EpochTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(binary.BigEndian.AppendUint64([]byte{EpochTag}, ^uint64(0)))
		if it.ValidForPrefix([]byte{EpochTag}) {
			newest = binary.BigEndian.Uint64(it.Item().Key()[1:])
			found = true
		}
		return nil
	})
	return newest, found, err
}

// epochPrefixesBelow lists tag <epoch:8 BE> for every epoch below below that
// holds a row under tag, reading one key per epoch.
func (s *Store) epochPrefixesBelow(tag byte, below uint64) ([][]byte, error) {
	var out [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{tag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); {
			k := it.Item().Key()
			if len(k) < 9 {
				it.Next()
				continue
			}
			epoch := binary.BigEndian.Uint64(k[1:9])
			if epoch >= below {
				return nil
			}
			out = append(out, bytes.Clone(k[:9]))
			it.Seek(binary.BigEndian.AppendUint64([]byte{tag}, epoch+1))
		}
		return nil
	})
	return out, err
}
