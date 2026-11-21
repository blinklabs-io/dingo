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
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type poolEventEnc struct {
	cbor.StructAsArray
	Retirement bool
	Reg        []poolRegEnc
	Epoch      uint64
	Effective  uint64
}

func poolHistoryPrefix(pool []byte) []byte {
	k := make([]byte, 0, 2+len(pool))
	k = append(k, badgerutxo.PoolHistoryTag, byte(len(pool))) //nolint:gosec // poolEventRows bounds a pool key to a byte
	return append(k, pool...)
}

func poolEventKey(pool []byte, slot, position uint64) []byte {
	k := poolHistoryPrefix(pool)
	k = binary.BigEndian.AppendUint64(k, slot)
	return binary.BigEndian.AppendUint64(k, position)
}

func encodePoolEvent(ev ledger.PoolEvent) ([]byte, error) {
	e := poolEventEnc{
		Retirement: ev.Registration == nil,
		Reg:        encodePoolReg(ev.Registration),
		Epoch:      ev.RetiringEpoch,
		Effective:  ev.EffectiveSlot,
	}
	return cbor.Encode(&e)
}

func decodePoolEvent(pool, k, v []byte) (ledger.PoolEvent, error) {
	var e poolEventEnc
	if _, err := cbor.Decode(v, &e); err != nil {
		return ledger.PoolEvent{}, fmt.Errorf("utxoset: decode pool event: %w", err)
	}
	if len(k) < 16 {
		return ledger.PoolEvent{}, fmt.Errorf("utxoset: malformed pool event key %x", k)
	}
	ev := ledger.PoolEvent{
		Slot:          binary.BigEndian.Uint64(k[len(k)-16:]),
		Position:      binary.BigEndian.Uint64(k[len(k)-8:]),
		RetiringEpoch: e.Epoch,
		EffectiveSlot: e.Effective,
	}
	if !e.Retirement {
		reg, err := decodePoolReg(pool, e.Reg)
		if err != nil {
			return ledger.PoolEvent{}, err
		}
		ev.Registration = reg
	}
	return ev, nil
}

// poolEventRows converts a commit's pool events to journaled rows.
func poolEventRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0, len(lc.PoolEvents))
	for _, c := range lc.PoolEvents {
		if len(c.Pool) == 0 || len(c.Pool) > math.MaxUint8 {
			return nil, fmt.Errorf("utxoset: pool key of %d bytes", len(c.Pool))
		}
		v, err := encodePoolEvent(c.Event)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Event.Slot,
			Key:   poolEventKey(c.Pool, c.Event.Slot, c.Event.Position),
			Value: v,
		})
	}
	return out, nil
}

// ensurePoolHistory loads every pool's registrations and retirements from
// the metadata store into a set from before it kept them.
func (s *Set) ensurePoolHistory(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.PoolHistoryLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the pool history marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	all, err := ldb.ListPoolKeyHashes(txn)
	if err != nil {
		return fmt.Errorf("utxoset: list pools: %w", err)
	}
	pkhs := make([]lcommon.PoolKeyHash, 0, len(all))
	for _, a := range all {
		pkhs = append(pkhs, lcommon.PoolKeyHash(a))
	}
	pools, err := ldb.GetPools(ctx, pkhs, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read pools: %w", err)
	}
	events := 0
	err = s.store.LoadPoolHistory(func(put func(k, v []byte) error) error {
		for i := range pools {
			for _, ev := range loadedPoolEvents(&pools[i]) {
				v, err := encodePoolEvent(ev)
				if err != nil {
					return err
				}
				pool := pools[i].PoolKeyHash
				if err := put(poolEventKey(pool, ev.Slot, ev.Position), v); err != nil {
					return err
				}
				events++
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load pool history: %w", err)
	}
	s.logger.Info("loaded pool history into the live UTxO set",
		"pools", len(pools), "events", events)
	return nil
}

// loadedPoolEvents returns pool's events with their positions. The rows
// carry no block or certificate index, so an event's position is its rank
// within its slot by certificate ID, which the store assigns in block and
// certificate order; an import's retirement, which has no certificate,
// ranks last.
func loadedPoolEvents(pool *models.Pool) []ledger.PoolEvent {
	type row struct {
		ev        ledger.PoolEvent
		synthetic bool
		certID    uint
		index     uint
		id        uint
	}
	rows := make([]row, 0, len(pool.Registration)+len(pool.Retirement))
	for i := range pool.Registration {
		r := &pool.Registration[i]
		rows = append(rows, row{
			ev: ledger.PoolEvent{
				Slot:          r.AddedSlot,
				Registration:  ledger.CleanPoolRegistration(r),
				EffectiveSlot: r.EffectiveSlot,
			},
			certID: r.CertificateID, index: r.RegistrationIndex, id: r.ID,
		})
		rows[len(rows)-1].ev.Registration.PoolKeyHash = pool.PoolKeyHash
	}
	for i := range pool.Retirement {
		r := &pool.Retirement[i]
		rows = append(rows, row{
			ev: ledger.PoolEvent{
				Slot: r.AddedSlot, RetiringEpoch: r.Epoch,
				EffectiveSlot: r.EffectiveSlot,
			},
			synthetic: r.CertificateID == 0,
			certID:    r.CertificateID, id: r.ID,
		})
	}
	slices.SortFunc(rows, func(a, b row) int {
		if c := cmp.Compare(a.ev.Slot, b.ev.Slot); c != 0 {
			return c
		}
		if a.synthetic != b.synthetic {
			if a.synthetic {
				return 1
			}
			return -1
		}
		if c := cmp.Compare(a.certID, b.certID); c != 0 {
			return c
		}
		if c := cmp.Compare(a.index, b.index); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	out := make([]ledger.PoolEvent, 0, len(rows))
	rank := uint64(0)
	for i, r := range rows {
		if i > 0 && rows[i-1].ev.Slot != r.ev.Slot {
			rank = 0
		}
		r.ev.Position = rank
		if r.synthetic {
			r.ev.Position = ledger.PoolEventSyntheticPosition
		}
		rank++
		out = append(out, r.ev)
	}
	return out
}

// PoolHistoryLoaded reports whether the set holds every pool certificate
// event.
func (s *Set) PoolHistoryLoaded() (bool, error) {
	return s.store.PoolHistoryLoaded()
}

// PoolHistory returns every event of pool, oldest first.
func (s *Set) PoolHistory(pool []byte) ([]ledger.PoolEvent, error) {
	var out []ledger.PoolEvent
	err := s.store.Rows(poolHistoryPrefix(pool), func(k, v []byte) error {
		ev, err := decodePoolEvent(pool, k, v)
		if err != nil {
			return err
		}
		out = append(out, ev)
		return nil
	})
	return out, err
}

// PoolHistoryPools returns the key hash of every pool the history holds an
// event for, in key order.
func (s *Set) PoolHistoryPools() ([][]byte, error) {
	var out [][]byte
	err := s.store.Rows([]byte{badgerutxo.PoolHistoryTag}, func(k, _ []byte) error {
		if len(k) < 2 || len(k) < 2+int(k[1])+16 {
			return fmt.Errorf("utxoset: malformed pool event key %x", k)
		}
		pool := k[2 : 2+int(k[1])]
		if n := len(out); n == 0 || !bytes.Equal(out[n-1], pool) {
			out = append(out, bytes.Clone(pool))
		}
		return nil
	})
	return out, err
}
