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

// Epoch summary keys:
//
//	f <epoch:8 BE>   epoch summary row, the caller's encoded row
//	mesummary        set once every summary up to the tip is held
//
// A rollback deletes the summaries of epochs above the newest surviving
// epoch row, as it does the pool stake snapshots.
// SummaryRow is an epoch's summary: its epoch and encoded contents.
type SummaryRow struct {
	Epoch uint64
	Value []byte
}

func summaryRowKey(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{SummaryTag}, epoch)
}

func putSummary(txn *badger.Txn, r SummaryRow) error {
	return txn.Set(summaryRowKey(r.Epoch), r.Value)
}

// deleteSummariesAfter deletes the summaries of epochs above newest.
func deleteSummariesAfter(txn *badger.Txn, newest uint64) error {
	var gone [][]byte
	err := eachPrefix(txn, []byte{SummaryTag}, func(k, _ []byte) error {
		if binary.BigEndian.Uint64(k[1:9]) > newest {
			gone = append(gone, k)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return deleteKeys(txn, gone)
}

// Summary returns epoch's summary row, or false when the store has none.
func (s *Store) Summary(epoch uint64) (SummaryRow, bool, error) {
	var out SummaryRow
	var found bool
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(summaryRowKey(epoch))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		out, found = SummaryRow{Epoch: epoch, Value: v}, true
		return nil
	})
	return out, found, err
}

// Summaries returns every epoch summary row, in epoch order.
func (s *Store) Summaries() ([]SummaryRow, error) {
	var out []SummaryRow
	err := s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, []byte{SummaryTag}, func(k, v []byte) error {
			out = append(out, SummaryRow{
				Epoch: binary.BigEndian.Uint64(k[1:9]), Value: v,
			})
			return nil
		})
	})
	return out, err
}

// LoadSummaries replaces the store's epoch summary rows with the given ones
// and marks them loaded, for a store from before it kept them.
func (s *Store) LoadSummaries(rows []SummaryRow) error {
	if err := s.dropPrefix([]byte{SummaryTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop epoch summaries: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	for _, r := range rows {
		if err := wb.Set(summaryRowKey(r.Epoch), r.Value); err != nil {
			return fmt.Errorf("badgerutxo: load epoch summaries: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load epoch summaries: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(summaryKey, []byte{1})
	})
}

// SummariesLoaded reports whether the store holds every epoch summary row
// up to its tip.
func (s *Store) SummariesLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(summaryKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}
