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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
)

// StateReachesSlot implements ledger.StateHistory: it returns an error
// wrapping ledger.ErrLedgerStateAsOfUnavailable unless the computed rows are
// exact as of slot.
func (s *Set) StateReachesSlot(slot uint64) error {
	err := s.store.StateReachesSlot(slot)
	if errors.Is(err, badgerutxo.ErrRollbackBeyondJournal) {
		return fmt.Errorf("%w: %w", ledger.ErrLedgerStateAsOfUnavailable, err)
	}
	return err
}

// DrepsAt calls fn with every DRep row, active or not, as a rollback to slot
// would leave it.
func (s *Set) DrepsAt(slot uint64, fn func(*models.Drep) error) error {
	return s.store.RowsAt(
		[]byte{badgerutxo.DRepTag},
		slot,
		func(k, v []byte) error {
			d, err := decodeDrep(k, v)
			if err != nil {
				return err
			}
			return fn(d)
		},
	)
}

// AccountRefsChangedAfter returns the credential of every account the state
// journal records a change to after slot: the accounts whose state as of slot
// may differ from their current one.
func (s *Set) AccountRefsChangedAfter(
	slot uint64,
) ([]models.StakeCredentialRef, error) {
	keys, err := s.store.RowKeysChangedAfter(
		[]byte{badgerutxo.AccountTag},
		slot,
	)
	if err != nil {
		return nil, err
	}
	refs := make([]models.StakeCredentialRef, 0, len(keys))
	for _, k := range keys {
		if len(k) < 2 {
			return nil, fmt.Errorf("utxoset: malformed account key %x", k)
		}
		refs = append(refs, models.NewStakeCredentialRef(k[1], k[2:]))
	}
	return refs, nil
}

// PoolKeysChangedAfter returns every pool the state journal records a change
// to after slot, one the state removed after slot included.
func (s *Set) PoolKeysChangedAfter(slot uint64) ([][]byte, error) {
	keys, err := s.store.RowKeysChangedAfter([]byte{badgerutxo.PoolTag}, slot)
	if err != nil {
		return nil, err
	}
	pools := make([][]byte, 0, len(keys))
	for _, k := range keys {
		if len(k) < 2 {
			return nil, fmt.Errorf("utxoset: malformed pool key %x", k)
		}
		pools = append(pools, bytes.Clone(k[1:]))
	}
	return pools, nil
}

// DRepLinksChangedAfter returns the (DRep, delegator) pair of every link the
// state journal records a change to after slot, a link closed after slot
// included.
func (s *Set) DRepLinksChangedAfter(
	slot uint64,
) ([][2]models.StakeCredentialRef, error) {
	keys, err := s.store.RowKeysChangedAfter([]byte{badgerutxo.DRepLinkTag}, slot)
	if err != nil {
		return nil, err
	}
	out := make([][2]models.StakeCredentialRef, 0, len(keys))
	for _, k := range keys {
		d, st, err := decodeDrepLinkKey(k)
		if err != nil {
			return nil, err
		}
		out = append(out, [2]models.StakeCredentialRef{d, st})
	}
	return out, nil
}

// DRepDepositsAt calls fn with the deposit of every DRep's latest
// registration as a rollback to slot would leave it. deposit is nil when it
// is unknown.
func (s *Set) DRepDepositsAt(
	slot uint64,
	fn func(tag uint8, cred []byte, deposit *uint64) error,
) error {
	return s.store.RowsAt(
		[]byte{badgerutxo.DRepDepositTag},
		slot,
		func(k, v []byte) error {
			if len(k) < 3 || len(k) != 3+int(k[2]) {
				return fmt.Errorf("utxoset: malformed DRep deposit key %x", k)
			}
			deposit, err := s.drepDepositFrom(v, true, nil)
			if err != nil {
				return err
			}
			return fn(k[1], k[3:], deposit)
		},
	)
}

// PoolStatesAt calls fn with every registered pool's state as a rollback to
// slot would leave it.
func (s *Set) PoolStatesAt(
	slot uint64,
	fn func(*ledger.PoolState) error,
) error {
	return s.store.RowsAt(
		[]byte{badgerutxo.PoolTag},
		slot,
		func(k, v []byte) error {
			st, err := decodePoolState(k[1:], v)
			if err != nil {
				return err
			}
			return fn(st)
		},
	)
}
