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
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
)

// isCredentialSumKey reports whether key is the stake sum key of a stake
// credential, ledger.CredentialStakeKey. A pointer's key does not take part in
// the closure effects, since the store's pointer stake queries read applied
// slots.
func isCredentialSumKey(key []byte) bool {
	return len(key) > 0 && key[0] == 'c'
}

func encodeClosureUtxoEffect(effective, lovelace uint64, stakeKey []byte) []byte {
	v := make([]byte, 0, 16+len(stakeKey))
	v = binary.BigEndian.AppendUint64(v, effective)
	v = binary.BigEndian.AppendUint64(v, lovelace)
	return append(v, stakeKey...)
}

func decodeClosureUtxoEffect(k, v []byte) (ledger.ClosureUtxoEffect, error) {
	applied, spent, ref, err := badgerutxo.DecodeClosureUtxoKey(k)
	if err != nil {
		return ledger.ClosureUtxoEffect{}, err
	}
	if len(v) < 17 {
		return ledger.ClosureUtxoEffect{}, fmt.Errorf(
			"utxoset: malformed closure output effect of %d bytes", len(v),
		)
	}
	return ledger.ClosureUtxoEffect{
		Ref: models.UtxoId{
			Hash: bytes.Clone(ref[:32]),
			Idx:  binary.BigEndian.Uint32(ref[32:]),
		},
		Applied:   applied,
		Effective: binary.BigEndian.Uint64(v),
		Spent:     spent,
		StakeKey:  bytes.Clone(v[16:]),
		Lovelace:  binary.BigEndian.Uint64(v[8:]),
	}, nil
}

// closureUtxoRows records the staked outputs the commit's closure
// transactions created and spent. Only an event carrying an effective slot
// writes a row, so an output outside such a closure costs no byte. A spent
// output's stake is read from the output itself, before its delete, from an
// output the commit created earlier or else from the set.
func (s *Set) closureUtxoRows(
	lc ledger.LedgerChanges,
) ([]badgerutxo.RowPut, error) {
	var out []badgerutxo.RowPut
	var created map[badgerutxo.Ref][]byte
	for _, evt := range lc.Utxo {
		if evt.EffectiveSlot == 0 || evt.EffectiveSlot == evt.Point.Slot ||
			evt.Cause != ledger.UtxoChangeTransaction {
			continue
		}
		if created == nil {
			created = map[badgerutxo.Ref][]byte{}
			for _, e := range lc.Utxo {
				for _, o := range e.Added {
					created[badgerutxo.NewRef(o.Ref.Hash, o.Ref.Idx)] = o.Cbor
				}
			}
		}
		for _, o := range evt.Added {
			key, lovelace, ok := ledger.UtxoStakeOf(o.Cbor)
			if !ok || !isCredentialSumKey(key) {
				continue
			}
			ref := badgerutxo.NewRef(o.Ref.Hash, o.Ref.Idx)
			out = append(out, badgerutxo.RowPut{
				Slot: evt.Point.Slot,
				Key:  badgerutxo.ClosureUtxoKey(evt.Point.Slot, false, ref),
				Value: encodeClosureUtxoEffect(
					evt.EffectiveSlot, lovelace, key,
				),
			})
		}
		for _, r := range evt.Removed {
			ref := badgerutxo.NewRef(r.Hash, r.Idx)
			cbor, ok := created[ref]
			if !ok {
				var err error
				cbor, ok, err = s.Get(r.Hash, r.Idx)
				if err != nil {
					return nil, fmt.Errorf(
						"utxoset: read spent closure output: %w", err,
					)
				}
			}
			if !ok {
				continue
			}
			key, lovelace, ok := ledger.UtxoStakeOf(cbor)
			if !ok || !isCredentialSumKey(key) {
				continue
			}
			out = append(out, badgerutxo.RowPut{
				Slot: evt.Point.Slot,
				Key:  badgerutxo.ClosureUtxoKey(evt.Point.Slot, true, ref),
				Value: encodeClosureUtxoEffect(
					evt.EffectiveSlot, lovelace, key,
				),
			})
		}
	}
	return out, nil
}

// ensureClosureUtxoEffects loads the closure output effects from the
// metadata store into a set from before it kept them. A store that keeps no
// UTxO rows yields none, and the set keeps the effects from then on.
func (s *Set) ensureClosureUtxoEffects(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.ClosureUtxoEffectsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the closure output marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	n := 0
	err = s.store.LoadClosureUtxoEffects(
		func(put func(k, v []byte) error) error {
			return ldb.IterateLeiosClosureUtxoEffects(txn,
				func(e models.LeiosClosureUtxoEffect) error {
					n++
					ref := badgerutxo.NewRef(e.TxHash, e.Index)
					return put(
						badgerutxo.ClosureUtxoKey(e.Applied, e.Spent, ref),
						encodeClosureUtxoEffect(
							e.Effective, e.Amount,
							ledger.CredentialStakeKey(
								e.CredentialTag, e.StakingKey,
							),
						),
					)
				})
		})
	if err != nil {
		return fmt.Errorf("utxoset: load closure output effects: %w", err)
	}
	s.logger.Info(
		"loaded closure output effects into the live UTxO set", "effects", n,
	)
	return nil
}

// ClosureUtxoEffectsLoaded reports whether the set holds every closure
// output effect.
func (s *Set) ClosureUtxoEffectsLoaded() (bool, error) {
	return s.store.ClosureUtxoEffectsLoaded()
}

// ClosureUtxoEffectsAfter calls fn with every closure output effect applied
// after slot, in applied order.
func (s *Set) ClosureUtxoEffectsAfter(
	slot uint64,
	fn func(ledger.ClosureUtxoEffect) error,
) error {
	return s.store.Rows(
		[]byte{badgerutxo.ClosureUtxoTag},
		func(k, v []byte) error {
			e, err := decodeClosureUtxoEffect(k, v)
			if err != nil {
				return err
			}
			if e.Applied <= slot {
				return nil
			}
			return fn(e)
		},
	)
}

var _ ledger.ClosureUtxoEffectReader = (*Set)(nil)
