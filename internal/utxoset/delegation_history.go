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

func delegationPrefix(ref models.StakeCredentialRef) []byte {
	k := make([]byte, 0, 3+len(ref.Key))
	k = append(k, badgerutxo.DelegationTag, ref.Tag, byte(len(ref.Key))) //nolint:gosec // credentialKey bounds a credential to a byte
	return append(k, ref.Key...)
}

func delegationKey(ref models.StakeCredentialRef, slot, position uint64) []byte {
	k := delegationPrefix(ref)
	k = binary.BigEndian.AppendUint64(k, slot)
	return binary.BigEndian.AppendUint64(k, position)
}

func encodeDelegation(effective uint64, pool []byte) []byte {
	return append(binary.BigEndian.AppendUint64(nil, effective), pool...)
}

func decodeDelegation(k, v []byte) (ledger.DelegationEvent, error) {
	if len(k) < 16 || len(v) < 8 {
		return ledger.DelegationEvent{}, fmt.Errorf(
			"utxoset: malformed delegation row %x", k,
		)
	}
	return ledger.DelegationEvent{
		Slot:          binary.BigEndian.Uint64(k[len(k)-16:]),
		Position:      binary.BigEndian.Uint64(k[len(k)-8:]),
		EffectiveSlot: binary.BigEndian.Uint64(v),
		Pool:          bytes.Clone(v[8:]),
	}, nil
}

// delegationRows converts a commit's delegations to journaled rows.
func delegationRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0, len(lc.DelegationEvents))
	for _, c := range lc.DelegationEvents {
		if err := credentialKey(c.Ref.Key); err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Event.Slot,
			Key:   delegationKey(c.Ref, c.Event.Slot, c.Event.Position),
			Value: encodeDelegation(c.Event.EffectiveSlot, c.Event.Pool),
		})
	}
	return out, nil
}

// ensureDelegations loads every pool delegation certificate from the
// metadata store into a set from before it kept them.
func (s *Set) ensureDelegations(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.DelegationsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the delegation marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	n := 0
	err = s.store.LoadDelegations(func(put func(k, v []byte) error) error {
		return ldb.IterateStakeDelegationEvents(txn,
			func(r models.StakeDelegationEventRow) error {
				if err := credentialKey(r.StakingKey); err != nil {
					return err
				}
				n++
				ref := models.NewStakeCredentialRef(r.CredentialTag, r.StakingKey)
				return put(
					delegationKey(
						ref, r.AddedSlot,
						ledger.PoolEventPosition(r.BlockIndex, r.CertIndex),
					),
					encodeDelegation(r.EffectiveSlot, r.PoolKeyHash),
				)
			})
	})
	if err != nil {
		return fmt.Errorf("utxoset: load delegations: %w", err)
	}
	s.logger.Info("loaded pool delegations into the live UTxO set", "events", n)
	return nil
}

// DelegationHistoryLoaded reports whether the set holds every pool
// delegation certificate.
func (s *Set) DelegationHistoryLoaded() (bool, error) {
	return s.store.DelegationsLoaded()
}

// DelegationHistory returns every pool delegation of ref, oldest first.
func (s *Set) DelegationHistory(
	ref models.StakeCredentialRef,
) ([]ledger.DelegationEvent, error) {
	var out []ledger.DelegationEvent
	err := s.store.Rows(delegationPrefix(ref), func(k, v []byte) error {
		ev, err := decodeDelegation(k, v)
		if err != nil {
			return err
		}
		out = append(out, ev)
		return nil
	})
	return out, err
}

// DelegationHistoryRefs returns every credential with a pool delegation.
func (s *Set) DelegationHistoryRefs() ([]models.StakeCredentialRef, error) {
	var out []models.StakeCredentialRef
	var last []byte
	err := s.store.Rows([]byte{badgerutxo.DelegationTag},
		func(k, _ []byte) error {
			// D <tag:1> <len:1> <credential> <slot:8> <position:8>
			if len(k) < 3+16 || len(k) != 3+int(k[2])+16 {
				return fmt.Errorf("utxoset: malformed delegation row %x", k)
			}
			cred := k[:3+int(k[2])]
			if bytes.Equal(cred, last) {
				return nil
			}
			last = bytes.Clone(cred)
			out = append(out, models.NewStakeCredentialRef(
				k[1], bytes.Clone(k[3:3+int(k[2])]),
			))
			return nil
		})
	return out, err
}

var _ ledger.DelegationHistoryReader = (*Set)(nil)
