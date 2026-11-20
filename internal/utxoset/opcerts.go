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
	"context"
	"encoding/binary"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/ledger"
)

func opCertKey(pool []byte) []byte {
	return append([]byte{badgerutxo.OpCertTag}, pool...)
}

func encodeOpCert(st ledger.OpCertState) []byte {
	v := binary.BigEndian.AppendUint64(nil, st.Sequence)
	v = binary.BigEndian.AppendUint64(v, st.Slot)
	return binary.BigEndian.AppendUint64(v, st.LastSequence)
}

func decodeOpCert(pool, v []byte) (*ledger.OpCertState, error) {
	if len(v) != 24 {
		return nil, fmt.Errorf(
			"utxoset: malformed op-cert counter of %d bytes",
			len(v),
		)
	}
	return &ledger.OpCertState{
		Pool:         pool,
		Sequence:     binary.BigEndian.Uint64(v),
		Slot:         binary.BigEndian.Uint64(v[8:]),
		LastSequence: binary.BigEndian.Uint64(v[16:]),
	}, nil
}

func opCertRows(lc ledger.LedgerChanges) []badgerutxo.RowPut {
	out := make([]badgerutxo.RowPut, 0, len(lc.OpCerts))
	for _, c := range lc.OpCerts {
		out = append(out, badgerutxo.RowPut{
			Slot: c.Slot, Key: opCertKey(c.State.Pool), Value: encodeOpCert(c.State),
		})
	}
	return out
}

// ensureOpCerts loads every pool's op-cert counter from the metadata store
// into a set from before it kept them.
func (s *Set) ensureOpCerts(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.OpCertsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the op-cert marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	counters, err := ldb.LatestPoolOpCertCounters(ctx, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read op-cert counters: %w", err)
	}
	err = s.store.LoadOpCerts(func(put func(k, v []byte) error) error {
		for _, c := range counters {
			v := encodeOpCert(ledger.OpCertState{
				Pool: c.PoolKeyHash, Sequence: c.Sequence, Slot: c.Slot,
				LastSequence: c.LastSequence,
			})
			if err := put(opCertKey(c.PoolKeyHash), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load op-cert counters: %w", err)
	}
	s.logger.Info(
		"loaded op-cert counters into the live UTxO set",
		"pools",
		len(counters),
	)
	return nil
}

// OpCertsLoaded reports whether the set holds every pool's op-cert counter.
func (s *Set) OpCertsLoaded() (bool, error) {
	return s.store.OpCertsLoaded()
}

// OpCertState returns pool's counter, or nil when it has none.
func (s *Set) OpCertState(pool []byte) (*ledger.OpCertState, error) {
	v, ok, err := s.store.Row(opCertKey(pool))
	if err != nil || !ok {
		return nil, err
	}
	return decodeOpCert(pool, v)
}

// OpCertStateAt returns pool's counter as a rollback to slot would leave
// it. The set keeps history only from its load and within its journal, so
// the oldest counter it holds answers every earlier slot, and its Slot may be
// after slot.
func (s *Set) OpCertStateAt(
	pool []byte,
	slot uint64,
) (*ledger.OpCertState, error) {
	v, ok, err := s.store.RowAt(opCertKey(pool), slot)
	if err != nil || !ok {
		return nil, err
	}
	return decodeOpCert(pool, v)
}

// OpCertStates calls fn with every pool's counter.
func (s *Set) OpCertStates(fn func(*ledger.OpCertState) error) error {
	return s.store.Rows([]byte{badgerutxo.OpCertTag}, func(k, v []byte) error {
		st, err := decodeOpCert(k[1:], v)
		if err != nil {
			return err
		}
		return fn(st)
	})
}
