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
	"bytes"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/utxoref"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type querySetHistory struct {
	readErr   error
	rawRows   []models.Utxo
	live      map[utxoref.Key][]byte
	snapshots map[uint64]map[utxoref.Key][]byte
}

func newQuerySetHistory() *querySetHistory {
	return &querySetHistory{
		live:      make(map[utxoref.Key][]byte),
		snapshots: make(map[uint64]map[utxoref.Key][]byte),
	}
}

func (s *querySetHistory) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []UtxoEvent,
) error {
	for _, change := range changes {
		for _, added := range change.Added {
			s.live[utxoSetKey(added.Ref.Hash, added.Ref.Idx)] = slices.Clone(added.Cbor)
		}
		for _, removed := range change.Removed {
			delete(s.live, utxoSetKey(removed.Hash, removed.Idx))
		}
	}
	if tip != nil {
		s.snapshots[tip.Slot] = cloneQueryUtxos(s.live)
	}
	return nil
}

func (s *querySetHistory) Get(txID []byte, index uint32) ([]byte, bool, error) {
	if s.readErr != nil {
		return nil, false, s.readErr
	}
	cbor, ok := s.live[utxoSetKey(txID, index)]
	return slices.Clone(cbor), ok, nil
}

func (s *querySetHistory) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	for _, row := range s.rawRows {
		if err := fn(row.TxId, row.OutputIdx, row.Cbor); err != nil {
			return err
		}
	}
	if s.readErr != nil {
		return s.readErr
	}
	for key, cbor := range s.live {
		if err := fn(key.TxId.Bytes(), key.Index, slices.Clone(cbor)); err != nil {
			return err
		}
	}
	return nil
}

func (s *querySetHistory) Sum(key []byte) (uint64, error) {
	var n uint64
	for _, c := range s.live {
		if k, l, ok := UtxoStakeOf(c); ok && bytes.Equal(k, key) {
			n += l
		}
	}
	return n, nil
}

// EachSum calls fn for every stake key with lovelace in the live outputs.
func (s *querySetHistory) EachSum(fn func(key []byte, lovelace uint64) error) error {
	sums := map[string]uint64{}
	for _, c := range s.live {
		if k, l, ok := UtxoStakeOf(c); ok {
			sums[string(k)] += l
		}
	}
	for k, l := range sums {
		if err := fn([]byte(k), l); err != nil {
			return err
		}
	}
	return nil
}

// RollbackUtxoSet reports no change: the fixtures that roll a ledger back on
// this set hold no outputs above the rollback point.
func (s *querySetHistory) RollbackUtxoSet(point ocommon.Point) (UtxoEvent, error) {
	return UtxoEvent{Point: point, Cause: UtxoChangeRollback}, nil
}

func (s *querySetHistory) PreviewUtxoSetRollback(
	point ocommon.Point,
) (UtxoEvent, error) {
	return s.RollbackUtxoSet(point)
}

func (s *querySetHistory) SnapshotUtxoSet() (UtxoSetWriter, func(), error) {
	view := newQuerySetHistory()
	view.live = cloneQueryUtxos(s.live)
	view.readErr, view.rawRows = s.readErr, slices.Clone(s.rawRows)
	for slot, rows := range s.snapshots {
		view.snapshots[slot] = cloneQueryUtxos(rows)
	}
	return view, func() {}, nil
}

func (s *querySetHistory) UtxoSetAt(
	point ocommon.Point,
) (UtxoSetView, error) {
	var slot uint64
	found := false
	for candidate := range s.snapshots {
		if candidate <= point.Slot && (!found || candidate > slot) {
			slot = candidate
			found = true
		}
	}
	if !found {
		return nil, ErrUtxoAsOfUnavailable
	}
	return queryUtxoView{utxos: s.snapshots[slot]}, nil
}

type queryUtxoView struct {
	utxos map[utxoref.Key][]byte
}

func (v queryUtxoView) Get(txID []byte, index uint32) ([]byte, bool, error) {
	cbor, ok := v.utxos[utxoSetKey(txID, index)]
	return slices.Clone(cbor), ok, nil
}

func (v queryUtxoView) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	for key, cbor := range v.utxos {
		if err := fn(key.TxId.Bytes(), key.Index, slices.Clone(cbor)); err != nil {
			return err
		}
	}
	return nil
}

func (queryUtxoView) Close() {}

func cloneQueryUtxos(src map[utxoref.Key][]byte) map[utxoref.Key][]byte {
	clone := make(map[utxoref.Key][]byte, len(src))
	for key, cbor := range src {
		clone[key] = slices.Clone(cbor)
	}
	return clone
}

// attachQueryLiveUtxoSet puts the outputs db's metadata rows hold live into
// the ledger's LedgerDB set, committed at db's tip, and returns the set. A
// ledger built without a set gets an in-memory one that serves outputs only.
func attachQueryLiveUtxoSet(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
) UtxoSetWriter {
	t.Helper()
	if ls.config.UtxoSet == nil {
		ls.config.UtxoSet = newQuerySetHistory()
	}
	set := ls.config.UtxoSet
	var added []UtxoOutput
	err := db.IterateLiveUtxos(t.Context(), nil, func(utxo *models.Utxo) error {
		added = append(added, UtxoOutput{
			Ref:  models.UtxoId{Hash: slices.Clone(utxo.TxId), Idx: utxo.OutputIdx},
			Cbor: slices.Clone(utxo.Cbor),
		})
		return nil
	})
	require.NoError(t, err)
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	point := ocommon.Point{
		Slot: tip.Point.Slot, Hash: slices.Clone(tip.Point.Hash),
	}
	require.NoError(t, set.ApplyUtxoChanges(
		&point, []UtxoEvent{{Point: point, Added: added}},
	))
	return set
}

func attachQueryUtxoLifecycle(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	txID []byte,
	index uint32,
	createdSlot uint64,
	spentSlot uint64,
) {
	t.Helper()
	events := map[uint64][]UtxoEvent{0: {}}
	cbor := queryTestOutputCBOR(t, db, txID, index)
	events[createdSlot] = []UtxoEvent{{Added: []UtxoOutput{{
		Ref:  models.UtxoId{Hash: slices.Clone(txID), Idx: index},
		Cbor: cbor,
	}}}}
	if spentSlot != 0 {
		events[spentSlot] = []UtxoEvent{{Removed: []models.UtxoId{{
			Hash: slices.Clone(txID), Idx: index,
		}}}}
	}
	attachQueryUtxoHistory(t, ls, events)
}

// attachQueryUtxoHistory commits events to the fixture's LedgerDB set in slot
// order, one commit per slot, so the set's journal answers a read as of any
// of them. A slot db holds a block for is committed at that block's point.
func attachQueryUtxoHistory(
	t *testing.T,
	ls *LedgerState,
	events map[uint64][]UtxoEvent,
) UtxoSetWriter {
	t.Helper()
	if ls.config.UtxoSet == nil {
		attachTestLiveSet(t, ls)
	}
	set := ls.config.UtxoSet
	slots := make([]uint64, 0, len(events))
	for slot := range events {
		slots = append(slots, slot)
	}
	slices.Sort(slots)
	for _, slot := range slots {
		point := ocommon.Point{Slot: slot}
		if block, err := database.BlockBySlot(t.Context(), ls.db, slot); err == nil {
			point.Hash = slices.Clone(block.Hash)
		}
		changes := slices.Clone(events[slot])
		for i := range changes {
			changes[i].Point = point
		}
		if len(changes) == 0 {
			changes = []UtxoEvent{{Point: point}}
		}
		require.NoError(t, set.ApplyUtxoChanges(&point, changes))
	}
	return set
}

var _ UtxoSetHistory = (*querySetHistory)(nil)
var _ UtxoSetReader = (*querySetHistory)(nil)
var _ UtxoSetIterator = (*querySetHistory)(nil)
var _ UtxoSetWriter = (*querySetHistory)(nil)

func queryTestOutputCBOR(t *testing.T, db *database.Database, txID []byte, index uint32) []byte {
	t.Helper()
	txn := db.BlobTxn(false)
	defer txn.Release()
	cbor, err := db.Blob().GetUtxo(txn.Blob(), txID, index)
	require.NoError(t, err)
	return cbor
}
