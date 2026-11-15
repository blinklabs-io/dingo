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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
)

func pparamUpdateKey(slot uint64, index uint32, genesis []byte) []byte {
	k := binary.BigEndian.AppendUint64([]byte{badgerutxo.PParamUpdateTag}, slot)
	k = binary.BigEndian.AppendUint32(k, index)
	return append(k, genesis...)
}

func encodePParamUpdate(u models.PParamUpdate) []byte {
	return append(binary.BigEndian.AppendUint64(nil, u.Epoch), u.Cbor...)
}

func decodePParamUpdate(k, v []byte) (models.PParamUpdate, error) {
	if len(k) < 13 || len(v) < 8 {
		return models.PParamUpdate{}, fmt.Errorf(
			"utxoset: malformed pparam update of %d key and %d value bytes",
			len(k), len(v),
		)
	}
	return models.PParamUpdate{
		GenesisHash: k[13:],
		Cbor:        v[8:],
		AddedSlot:   binary.BigEndian.Uint64(k[1:]),
		Epoch:       binary.BigEndian.Uint64(v),
	}, nil
}

func pparamUpdateRows(lc ledger.LedgerChanges) []badgerutxo.RowPut {
	out := make([]badgerutxo.RowPut, 0, len(lc.PParamUpdates))
	for _, c := range lc.PParamUpdates {
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Update.AddedSlot,
			Key:   pparamUpdateKey(c.Update.AddedSlot, c.Index, c.Update.GenesisHash),
			Value: encodePParamUpdate(c.Update),
		})
	}
	return out
}

// ensurePParamUpdates loads the metadata store's protocol parameter update
// proposals into a set from before it kept them.
func (s *Set) ensurePParamUpdates(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.PParamUpdatesLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the pparam update marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	updates, err := ldb.ListPParamUpdates(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read pparam updates: %w", err)
	}
	err = s.store.LoadPParamUpdates(func(put func(k, v []byte) error) error {
		// The metadata store keeps no transaction index; the position
		// within a slot, in row order, gives the same order.
		next := map[uint64]uint32{}
		for _, u := range updates {
			index := next[u.AddedSlot]
			next[u.AddedSlot] = index + 1
			err := put(
				pparamUpdateKey(u.AddedSlot, index, u.GenesisHash),
				encodePParamUpdate(u),
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load pparam updates: %w", err)
	}
	s.logger.Info("loaded protocol parameter updates into the live UTxO set",
		"proposals", len(updates))
	return nil
}

// PParamUpdatesLoaded reports whether the set holds every protocol parameter
// update proposal.
func (s *Set) PParamUpdatesLoaded() (bool, error) {
	return s.store.PParamUpdatesLoaded()
}

// PParamUpdates calls fn with every protocol parameter update proposal in
// the order they were stored, without row IDs.
func (s *Set) PParamUpdates(fn func(models.PParamUpdate) error) error {
	return EachPParamUpdate(s.store, fn)
}

// EachPParamUpdate calls fn with every protocol parameter update proposal
// store holds, in the order they were stored, without row IDs.
func EachPParamUpdate(
	store *badgerutxo.Store,
	fn func(models.PParamUpdate) error,
) error {
	return store.Rows([]byte{badgerutxo.PParamUpdateTag}, func(k, v []byte) error {
		u, err := decodePParamUpdate(k, v)
		if err != nil {
			return err
		}
		return fn(u)
	})
}
