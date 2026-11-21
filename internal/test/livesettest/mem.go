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

package livesettest

import (
	"bytes"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type memKey struct {
	txID  string
	index uint32
}

// memUndo restores key to cbor, or removes it when live is false.
type memUndo struct {
	key  memKey
	cbor []byte
	live bool
}

type memEntry struct {
	slot uint64
	undo []memUndo
}

// Mem is an in-memory live UTxO set that rolls back from its own journal.
// It holds the UTxO set only, so a ledger using it keeps every other
// ledger-state family in the metadata store, which suits tests that seed
// those families there.
type Mem struct {
	mu      sync.Mutex
	outputs map[memKey][]byte
	journal []memEntry
}

var (
	_ ledger.UtxoSetWriter     = (*Mem)(nil)
	_ ledger.UtxoSetReader     = (*Mem)(nil)
	_ ledger.UtxoSetRollbacker = (*Mem)(nil)
	_ ledger.UtxoSetIterator   = (*Mem)(nil)
	_ ledger.UtxoSetSums       = (*Mem)(nil)
)

// New returns an empty Mem.
func New() *Mem {
	return &Mem{outputs: map[memKey][]byte{}}
}

func keyOf(txID []byte, index uint32) memKey {
	return memKey{txID: string(txID), index: index}
}

// ApplyUtxoChanges applies changes in order, journaling each under its
// point's slot, or tip's when the change has none. A rollback change drops
// the journal above its point.
func (m *Mem) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range changes {
		slot := c.Point.Slot
		if slot == 0 && tip != nil {
			slot = tip.Slot
		}
		var undo []memUndo
		record := func(k memKey) {
			old, live := m.outputs[k]
			undo = append(undo, memUndo{key: k, cbor: old, live: live})
		}
		for _, r := range c.Removed {
			k := keyOf(r.Hash, r.Idx)
			record(k)
			delete(m.outputs, k)
		}
		for _, o := range c.Added {
			k := keyOf(o.Ref.Hash, o.Ref.Idx)
			record(k)
			m.outputs[k] = slices.Clone(o.Cbor)
		}
		if c.Cause == ledger.UtxoChangeRollback {
			m.journal = slices.DeleteFunc(m.journal, func(e memEntry) bool {
				return e.slot > c.Point.Slot
			})
			continue
		}
		m.journal = append(m.journal, memEntry{slot: slot, undo: undo})
	}
	return nil
}

// Get returns txID#index's output CBOR when it is live.
func (m *Mem) Get(txID []byte, index uint32) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.outputs[keyOf(txID, index)]
	return c, ok, nil
}

// Sum returns the lovelace of the live outputs staked to key.
func (m *Mem) Sum(key []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n uint64
	for _, c := range m.outputs {
		if k, l, ok := ledger.UtxoStakeOf(c); ok && bytes.Equal(k, key) {
			n += l
		}
	}
	return n, nil
}

// EachSum calls fn for every stake key with lovelace in live outputs.
func (m *Mem) EachSum(fn func(key []byte, lovelace uint64) error) error {
	m.mu.Lock()
	sums := map[string]uint64{}
	for _, c := range m.outputs {
		if k, l, ok := ledger.UtxoStakeOf(c); ok {
			sums[string(k)] += l
		}
	}
	m.mu.Unlock()
	for k, l := range sums {
		if err := fn([]byte(k), l); err != nil {
			return err
		}
	}
	return nil
}

// EachUtxo calls fn for every live output.
func (m *Mem) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	m.mu.Lock()
	outputs := make(map[memKey][]byte, len(m.outputs))
	maps.Copy(outputs, m.outputs)
	m.mu.Unlock()
	for k, c := range outputs {
		if err := fn([]byte(k.txID), k.index, c); err != nil {
			return err
		}
	}
	return nil
}

// RollbackUtxoSet undoes every journaled change above point and returns the
// net change.
func (m *Mem) RollbackUtxoSet(point ocommon.Point) (ledger.UtxoEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	evt, outputs, keep := m.rollback(point)
	m.outputs = outputs
	m.journal = keep
	return evt, nil
}

// PreviewUtxoSetRollback returns RollbackUtxoSet(point)'s change without
// making it.
func (m *Mem) PreviewUtxoSetRollback(
	point ocommon.Point,
) (ledger.UtxoEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	evt, _, _ := m.rollback(point)
	return evt, nil
}

func (m *Mem) rollback(
	point ocommon.Point,
) (ledger.UtxoEvent, map[memKey][]byte, []memEntry) {
	outputs := make(map[memKey][]byte, len(m.outputs))
	maps.Copy(outputs, m.outputs)
	cut := len(m.journal)
	for cut > 0 && m.journal[cut-1].slot > point.Slot {
		cut--
	}
	touched := map[memKey]struct{}{}
	for i := len(m.journal) - 1; i >= cut; i-- {
		undo := m.journal[i].undo
		for _, u := range slices.Backward(undo) {
			touched[u.key] = struct{}{}
			if u.live {
				outputs[u.key] = u.cbor
			} else {
				delete(outputs, u.key)
			}
		}
	}
	evt := ledger.UtxoEvent{Point: point, Cause: ledger.UtxoChangeRollback}
	for k := range touched {
		before, wasLive := m.outputs[k]
		after, isLive := outputs[k]
		ref := models.UtxoId{Hash: []byte(k.txID), Idx: k.index}
		switch {
		case wasLive && !isLive:
			evt.Removed = append(evt.Removed, ref)
		case isLive && (!wasLive || !bytes.Equal(before, after)):
			evt.Added = append(evt.Added, ledger.UtxoOutput{Ref: ref, Cbor: after})
		}
	}
	return evt, outputs, slices.Clone(m.journal[:cut])
}

var (
	_ ledger.UtxoSetSnapshotter = (*Mem)(nil)
	_ ledger.UtxoSetHistory     = (*Mem)(nil)
)

// memView is a frozen copy of a Mem: it answers reads as of the snapshot and
// refuses writes.
type memView struct{ *Mem }

func (memView) ApplyUtxoChanges(*ocommon.Point, []ledger.UtxoEvent) error {
	return errors.New("livesettest: a snapshot is read-only")
}

// SnapshotUtxoSet returns a read-only copy of the set's current outputs.
func (m *Mem) SnapshotUtxoSet() (ledger.UtxoSetWriter, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return memView{&Mem{
		outputs: maps.Clone(m.outputs),
		journal: slices.Clone(m.journal),
	}}, func() {}, nil
}

// UtxoSetAt returns the set as it stood at point, undoing the journaled
// changes above it.
func (m *Mem) UtxoSetAt(point ocommon.Point) (ledger.UtxoSetView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, outputs, _ := m.rollback(point)
	return &Mem{outputs: outputs}, nil
}

// Close releases a view returned by UtxoSetAt; a Mem holds nothing to release.
func (m *Mem) Close() {}
