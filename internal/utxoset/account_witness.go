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
	"math"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
)

// importBaselineLen is the length of the part of an encoded import baseline
// every version carries: the active flag, the added slot, whether a deposit is
// recorded and the deposit. A baseline written once the set kept the imported
// delegations follows it with a marker byte, the DRep type, and the pool and
// DRep, each behind a one-byte length; an older one stops there and says
// nothing about its delegations.
const importBaselineLen = 1 + 8 + 1 + 8

const importBaselineDelegations = 1

func importBaselineKey(ref models.StakeCredentialRef) []byte {
	k := make([]byte, 0, 2+len(ref.Key))
	k = append(k, badgerutxo.ImportBaselineTag, ref.Tag)
	return append(k, ref.Key...)
}

func encodeImportBaseline(b ledger.ImportBaseline) ([]byte, error) {
	if len(b.Pool) > math.MaxUint8 || len(b.Drep) > math.MaxUint8 {
		return nil, fmt.Errorf(
			"utxoset: import baseline delegation of %d and %d bytes",
			len(b.Pool), len(b.Drep),
		)
	}
	v := make([]byte, 0, importBaselineLen+11+len(b.Pool)+len(b.Drep))
	v = append(v, boolByte(b.Active))
	v = binary.BigEndian.AppendUint64(v, b.AddedSlot)
	if b.Deposit == nil {
		v = binary.BigEndian.AppendUint64(append(v, 0), 0)
	} else {
		v = binary.BigEndian.AppendUint64(append(v, 1), *b.Deposit)
	}
	v = append(v, importBaselineDelegations)
	v = binary.BigEndian.AppendUint64(v, b.DrepType)
	v = append(v, byte(len(b.Pool))) //nolint:gosec // bounded above
	v = append(v, b.Pool...)
	v = append(v, byte(len(b.Drep))) //nolint:gosec // bounded above
	return append(v, b.Drep...), nil
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func decodeImportBaseline(
	ref models.StakeCredentialRef,
	v []byte,
) (*ledger.ImportBaseline, error) {
	if len(v) < importBaselineLen {
		return nil, fmt.Errorf(
			"utxoset: malformed import baseline of %d bytes", len(v),
		)
	}
	b := &ledger.ImportBaseline{
		Ref:       ref,
		Active:    v[0] == 1,
		AddedSlot: binary.BigEndian.Uint64(v[1:]),
	}
	if v[9] == 1 {
		d := binary.BigEndian.Uint64(v[10:])
		b.Deposit = &d
	}
	rest := v[importBaselineLen:]
	if len(rest) == 0 {
		return b, nil
	}
	malformed := fmt.Errorf(
		"utxoset: malformed import baseline of %d bytes", len(v),
	)
	if rest[0] != importBaselineDelegations || len(rest) < 1+8+1 {
		return nil, malformed
	}
	b.DrepType = binary.BigEndian.Uint64(rest[1:])
	rest = rest[9:]
	n := int(rest[0])
	if len(rest) < 1+n+1 {
		return nil, malformed
	}
	if n > 0 {
		b.Pool = bytes.Clone(rest[1 : 1+n])
	}
	rest = rest[1+n:]
	m := int(rest[0])
	if len(rest) != 1+m {
		return nil, malformed
	}
	if m > 0 {
		b.Drep = bytes.Clone(rest[1 : 1+m])
	}
	return b, nil
}

func witnessPrefix(ref models.StakeCredentialRef) []byte {
	k := make([]byte, 0, 3+len(ref.Key)+8)
	keyLen := byte(len(ref.Key)) //nolint:gosec // credentialKey bounds a credential to a byte
	k = append(k, badgerutxo.WitnessTag, ref.Tag, keyLen)
	return append(k, ref.Key...)
}

func witnessKey(ref models.StakeCredentialRef, slot uint64) []byte {
	return binary.BigEndian.AppendUint64(witnessPrefix(ref), slot)
}

// witnessEffectiveKey is the key of a witness of a Leios closure applied at
// slot that takes effect at effective, which is before slot: the applied
// slot's key with the effective slot appended. A witness that takes effect
// where it was applied keeps the key witnessKey gives.
func witnessEffectiveKey(
	ref models.StakeCredentialRef,
	slot, effective uint64,
) []byte {
	k := witnessKey(ref, slot)
	if effective == 0 || effective == slot {
		return k
	}
	return binary.BigEndian.AppendUint64(k, effective)
}

// witnessWithdrawalKey is the key of the withdrawal a witness of a Leios
// closure carries: the witness's key with a marker byte appended. It is a row
// of its own, beside the witness, because a closure's certificates witness the
// credential at the same slots and the two cannot share a key.
func witnessWithdrawalKey(
	ref models.StakeCredentialRef,
	slot, effective uint64,
) []byte {
	return append(witnessEffectiveKey(ref, slot, effective), 1)
}

// witnessKeySlots splits a witness key under prefix into the slot it was
// applied at and the slot it takes effect at. withdrawal is set for the key
// witnessWithdrawalKey gives, which marks a witness row as also a withdrawal.
func witnessKeySlots(
	prefix, k []byte,
) (slot, effective uint64, withdrawal bool, err error) {
	switch len(k) - len(prefix) {
	case 8:
		slot = binary.BigEndian.Uint64(k[len(prefix):])
		return slot, slot, false, nil
	case 16:
		return binary.BigEndian.Uint64(k[len(prefix):]),
			binary.BigEndian.Uint64(k[len(prefix)+8:]), false, nil
	case 17:
		return binary.BigEndian.Uint64(k[len(prefix):]),
			binary.BigEndian.Uint64(k[len(prefix)+8:]), true, nil
	}
	return 0, 0, false, fmt.Errorf("utxoset: malformed witness key %x", k)
}

// accountWitnessRows converts a commit's import baselines and witness slots
// to journaled rows.
func accountWitnessRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make(
		[]badgerutxo.RowPut,
		0,
		len(lc.ImportBaselines)+2*len(lc.Witnesses),
	)
	for _, c := range lc.ImportBaselines {
		if err := credentialKey(c.Baseline.Ref.Key); err != nil {
			return nil, err
		}
		value, err := encodeImportBaseline(c.Baseline)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Slot,
			Key:   importBaselineKey(c.Baseline.Ref),
			Value: value,
		})
	}
	for _, c := range lc.Witnesses {
		if err := credentialKey(c.Ref.Key); err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Slot,
			Key:   witnessEffectiveKey(c.Ref, c.Slot, c.EffectiveSlot),
			Value: []byte{},
		})
		if c.Withdrawal && c.EffectiveSlot != 0 && c.EffectiveSlot != c.Slot {
			out = append(out, badgerutxo.RowPut{
				Slot:  c.Slot,
				Key:   witnessWithdrawalKey(c.Ref, c.Slot, c.EffectiveSlot),
				Value: []byte{},
			})
		}
	}
	return out, nil
}

// ensureImportBaselines loads every account import baseline, with the
// delegations the import recorded, from the v0.94.2 metadata store into a set
// that does not hold them.
func (s *Set) ensureImportBaselines(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.ImportBaselinesLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the import baseline marker: %w", err)
	}
	if loaded {
		settled, err := s.store.ImportBaselineDelegationsLoaded()
		if err != nil {
			return fmt.Errorf(
				"utxoset: read the import baseline delegation marker: %w",
				err,
			)
		}
		if settled {
			return nil
		}
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	n := 0
	err = s.store.LoadImportBaselines(func(put func(k, v []byte) error) error {
		return ldb.IterateAccountImportBaselines(txn,
			func(r models.AccountImportBaselineRow) error {
				if err := credentialKey(r.StakingKey); err != nil {
					return err
				}
				ref := models.NewStakeCredentialRef(
					r.CredentialTag,
					r.StakingKey,
				)
				v, err := encodeImportBaseline(ledger.ImportBaseline{
					Ref: ref, Active: r.Active, AddedSlot: r.AddedSlot,
					Deposit: r.Deposit, Pool: r.Pool, Drep: r.Drep,
					DrepType: r.DrepType,
				})
				if err != nil {
					return err
				}
				n++
				return put(importBaselineKey(ref), v)
			})
	})
	if err != nil {
		return fmt.Errorf("utxoset: load import baselines: %w", err)
	}
	s.logger.Info(
		"loaded account import baselines into the live UTxO set",
		"baselines",
		n,
	)
	return nil
}

// ImportBaselinesAfter returns at most limit import baselines whose
// credential follows after (tag, then key; nil to start), in that order, and
// the credential to continue from, nil at the end.
func (s *Set) ImportBaselinesAfter(
	after *models.StakeCredentialRef,
	limit int,
) ([]ledger.ImportBaseline, *models.StakeCredentialRef, error) {
	var from []byte
	if after != nil {
		from = importBaselineKey(*after)
	}
	var out []ledger.ImportBaseline
	err := s.store.RowsAfter(
		[]byte{badgerutxo.ImportBaselineTag}, from, limit,
		func(k, v []byte) error {
			if len(k) < 2 {
				return fmt.Errorf("utxoset: malformed import baseline key %x", k)
			}
			b, err := decodeImportBaseline(
				models.NewStakeCredentialRef(k[1], bytes.Clone(k[2:])), v,
			)
			if err != nil {
				return err
			}
			out = append(out, *b)
			return nil
		},
	)
	if err != nil || len(out) < limit {
		return out, nil, err
	}
	last := out[len(out)-1].Ref
	return out, &last, nil
}

// ensureWitnessSlots loads every slot a credential was witnessed at from the
// metadata store into a set from before it kept them.
func (s *Set) ensureWitnessSlots(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.WitnessSlotsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the witness slot marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	n := 0
	err = s.store.LoadWitnessSlots(func(put func(k, v []byte) error) error {
		err := ldb.IterateAccountWitnessEffectiveSlots(txn,
			func(ref models.StakeCredentialRef, slot, effective uint64) error {
				if err := credentialKey(ref.Key); err != nil {
					return err
				}
				n++
				return put(
					witnessEffectiveKey(ref, slot, effective), []byte{},
				)
			})
		if err != nil {
			return err
		}
		return ldb.IterateAccountWithdrawalEffectiveSlots(txn,
			func(ref models.StakeCredentialRef, slot, effective uint64) error {
				if err := credentialKey(ref.Key); err != nil {
					return err
				}
				return put(
					witnessWithdrawalKey(ref, slot, effective), []byte{},
				)
			})
	})
	if err != nil {
		return fmt.Errorf("utxoset: load witness slots: %w", err)
	}
	s.logger.Info(
		"loaded account witness slots into the live UTxO set",
		"witnesses",
		n,
	)
	return nil
}

// ImportBaselinesLoaded reports whether the set holds every account import
// baseline.
func (s *Set) ImportBaselinesLoaded() (bool, error) {
	return s.store.ImportBaselinesLoaded()
}

func (s *Set) importBaselineFrom(
	ref models.StakeCredentialRef,
	v []byte,
	ok bool,
	err error,
) (*ledger.ImportBaseline, error) {
	if err != nil || !ok {
		return nil, err
	}
	return decodeImportBaseline(ref, v)
}

// ImportBaseline returns ref's import baseline, or nil when it has none.
func (s *Set) ImportBaseline(
	ref models.StakeCredentialRef,
) (*ledger.ImportBaseline, error) {
	v, ok, err := s.store.Row(importBaselineKey(ref))
	return s.importBaselineFrom(ref, v, ok, err)
}

// ImportBaselineAt returns ImportBaseline as a rollback to slot would leave
// it.
func (s *Set) ImportBaselineAt(
	ref models.StakeCredentialRef,
	slot uint64,
) (*ledger.ImportBaseline, error) {
	v, ok, err := s.store.RowAt(importBaselineKey(ref), slot)
	return s.importBaselineFrom(ref, v, ok, err)
}

// WitnessSlotsLoaded reports whether the set holds every witness slot.
func (s *Set) WitnessSlotsLoaded() (bool, error) {
	return s.store.WitnessSlotsLoaded()
}

// LastWitnessSlot returns the greatest slot at or below maxSlot at which ref
// was witnessed.
func (s *Set) LastWitnessSlot(
	ref models.StakeCredentialRef,
	maxSlot uint64,
) (uint64, bool, error) {
	prefix := witnessPrefix(ref)
	// A closure's witness key carries 8 more bytes after the slot, so the
	// limit runs to the end of the slot's keys.
	limit := binary.BigEndian.AppendUint64(append([]byte{}, prefix...), maxSlot)
	limit = binary.BigEndian.AppendUint64(limit, math.MaxUint64)
	k, ok, err := s.store.LastKeyAtOrBelow(prefix, limit)
	if err != nil || !ok {
		return 0, false, err
	}
	slot, _, _, err := witnessKeySlots(prefix, k)
	if err != nil {
		return 0, false, err
	}
	return slot, true, nil
}

// LastWitnessEffectiveSlot returns the greatest slot at or below maxSlot at
// which ref's witnesses take effect: the context slot of a Leios closure's
// witness, the applied slot of any other. It reads every witness of ref.
func (s *Set) LastWitnessEffectiveSlot(
	ref models.StakeCredentialRef,
	maxSlot uint64,
) (uint64, bool, error) {
	prefix := witnessPrefix(ref)
	var best uint64
	var found bool
	err := s.store.Rows(prefix, func(k, _ []byte) error {
		_, effective, _, err := witnessKeySlots(prefix, k)
		if err != nil {
			return err
		}
		if effective <= maxSlot && (!found || effective > best) {
			best, found = effective, true
		}
		return nil
	})
	return best, found, err
}

// EachWitness calls fn with the applied and effective slot of every witness of
// ref.
func (s *Set) EachWitness(
	ref models.StakeCredentialRef,
	fn func(slot, effective uint64) error,
) error {
	prefix := witnessPrefix(ref)
	return s.store.Rows(prefix, func(k, _ []byte) error {
		slot, effective, withdrawal, err := witnessKeySlots(prefix, k)
		if err != nil || withdrawal {
			return err
		}
		return fn(slot, effective)
	})
}

// EachWithdrawalWitness calls fn with the applied and effective slot of every
// reward withdrawal of a Leios closure that takes effect before it was applied.
func (s *Set) EachWithdrawalWitness(
	ref models.StakeCredentialRef,
	fn func(slot, effective uint64) error,
) error {
	prefix := witnessPrefix(ref)
	return s.store.Rows(prefix, func(k, _ []byte) error {
		slot, effective, withdrawal, err := witnessKeySlots(prefix, k)
		if err != nil || !withdrawal {
			return err
		}
		return fn(slot, effective)
	})
}

var _ ledger.WitnessEachReader = (*Set)(nil)
