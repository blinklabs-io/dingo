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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
)

// ensureDRepHistory loads the DRep history the metadata store holds into a
// set from before it kept it. A drained metadata store holds none, and the
// load then marks the set as having lost it, so the set refuses to export
// the history rather than export none.
func (s *Set) ensureDRepHistory(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.DRepHistoryLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the DRep history marker: %w", err)
	}
	if loaded {
		return nil
	}
	var rows ledgerdb.DRepHistoryRows
	lost := false
	if r, ok := ldb.(ledgerdb.DRepHistoryRowsReader); ok {
		txn := ldb.Begin(ctx, false)
		rows, err = r.DRepHistoryRows(txn)
		txn.Release()
		if errors.Is(err, database.ErrNoMetadataStore) {
			lost, err = true, nil
		}
		if err != nil {
			return fmt.Errorf("utxoset: read DRep history: %w", err)
		}
	}
	err = s.store.LoadDRepHistory(lost, func(put func(k, v []byte) error) error {
		for _, r := range rows.Expiry {
			err := put(
				badgerutxo.DRepExpiryHistoryKey(r.Slot, r.Tag, r.Credential),
				badgerutxo.EncodeDRepExpiryHistoryValue(r.Activity, r.Expiry),
			)
			if err != nil {
				return err
			}
		}
		for _, r := range rows.Dormancy {
			if err := put(
				badgerutxo.DRepDormancyHistoryKey(r.Slot, r.Previous), nil,
			); err != nil {
				return err
			}
		}
		for _, slot := range rows.Events {
			if err := put(badgerutxo.DRepExpiryEventKey(slot), nil); err != nil {
				return err
			}
		}
		for _, r := range rows.Clears {
			if err := put(
				badgerutxo.DRepClearKey(r.Slot, r.Tag, r.StakingKey), nil,
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load DRep history: %w", err)
	}
	s.logger.Info(
		"loaded DRep history into the live UTxO set",
		"expiry", len(rows.Expiry), "dormancy", len(rows.Dormancy),
		"events", len(rows.Events), "clears", len(rows.Clears),
	)
	return nil
}

// DRepHistoryLoaded reports whether the set holds the DRep history up to its
// tip.
func (s *Set) DRepHistoryLoaded() (bool, error) {
	return s.store.DRepHistoryLoaded()
}

// DRepHistoryLost reports whether the metadata store had drained the DRep
// history before the set could load it.
func (s *Set) DRepHistoryLost() (bool, error) {
	return s.store.DRepHistoryLost()
}

// DRepHistory lists the DRep history rows the set holds.
func (s *Set) DRepHistory() (ledgerdb.DRepHistoryRows, error) {
	return s.DRepHistoryIn(0, math.MaxUint64)
}

// DRepHistoryIn lists the DRep history rows dated in [from, through], reading
// only that slot range of the history.
func (s *Set) DRepHistoryIn(from, through uint64) (ledgerdb.DRepHistoryRows, error) {
	var out ledgerdb.DRepHistoryRows
	prefix := []byte{badgerutxo.DRepHistoryTag}
	start := binary.BigEndian.AppendUint64(bytes.Clone(prefix), from)
	var end []byte
	if through < math.MaxUint64 {
		end = binary.BigEndian.AppendUint64(bytes.Clone(prefix), through+1)
	}
	err := s.store.RowsInRange(
		prefix, start, end,
		func(k, v []byte) error {
			r, err := badgerutxo.DecodeDRepHistoryRow(k, v)
			if err != nil {
				return err
			}
			switch r.Kind {
			case badgerutxo.DRepHistoryExpiry:
				out.Expiry = append(out.Expiry, ledgerdb.DRepExpiryHistoryRow{
					Tag: r.Tag, Credential: r.Key, Slot: r.Slot,
					Activity: r.Activity, Expiry: r.Expiry,
				})
			case badgerutxo.DRepHistoryDormancy:
				out.Dormancy = append(out.Dormancy, ledgerdb.DRepDormancyHistoryRow{
					Slot: r.Slot, Previous: r.Previous,
				})
			case badgerutxo.DRepHistoryEvent:
				out.Events = append(out.Events, r.Slot)
			case badgerutxo.DRepHistoryClear:
				out.Clears = append(out.Clears, ledgerdb.DRepClearRow{
					Tag: r.Tag, StakingKey: r.Key, Slot: r.Slot,
				})
			}
			return nil
		},
	)
	return out, err
}

// historyAfter walks the history rows dated in [from, through] after the key
// after (the start of the range when nil), in key order, which is slot order.
func (s *Set) historyAfter(
	from, through uint64,
	after []byte,
	fn func(r badgerutxo.DRepHistoryRow, key []byte) error,
) error {
	prefix := []byte{badgerutxo.DRepHistoryTag}
	start := binary.BigEndian.AppendUint64(bytes.Clone(prefix), from)
	var end []byte
	if through < math.MaxUint64 {
		end = binary.BigEndian.AppendUint64(bytes.Clone(prefix), through+1)
	}
	walk := func(k, v []byte) error {
		r, err := badgerutxo.DecodeDRepHistoryRow(k, v)
		if err != nil {
			return err
		}
		return stopRows(fn(r, bytes.Clone(k)))
	}
	if after == nil || bytes.Compare(after, start) < 0 {
		return s.store.EachRowFrom(prefix, start, end, walk)
	}
	return s.store.EachRowAfter(prefix, after, end, walk)
}

// DRepExpiryHistoryAfter calls fn with each expiry history row dated in
// [from, through] after the key after, in key order, until fn returns
// ledger.ErrStopRows.
func (s *Set) DRepExpiryHistoryAfter(
	from, through uint64,
	after []byte,
	fn func(r ledgerdb.DRepExpiryHistoryRow, key []byte) error,
) error {
	return s.historyAfter(from, through, after, func(r badgerutxo.DRepHistoryRow, key []byte) error {
		if r.Kind != badgerutxo.DRepHistoryExpiry {
			return nil
		}
		return fn(ledgerdb.DRepExpiryHistoryRow{
			Tag: r.Tag, Credential: r.Key, Slot: r.Slot,
			Activity: r.Activity, Expiry: r.Expiry,
		}, key)
	})
}

// DRepDormancyHistoryAfter is DRepExpiryHistoryAfter for the dormancy counter
// history.
func (s *Set) DRepDormancyHistoryAfter(
	from, through uint64,
	after []byte,
	fn func(r ledgerdb.DRepDormancyHistoryRow, key []byte) error,
) error {
	return s.historyAfter(from, through, after, func(r badgerutxo.DRepHistoryRow, key []byte) error {
		if r.Kind != badgerutxo.DRepHistoryDormancy {
			return nil
		}
		return fn(ledgerdb.DRepDormancyHistoryRow{
			Slot: r.Slot, Previous: r.Previous,
		}, key)
	})
}
