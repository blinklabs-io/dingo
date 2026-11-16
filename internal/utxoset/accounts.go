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
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
)

func accountRow(a models.Account) badgerutxo.AccountRow {
	return badgerutxo.AccountRow{
		Tag:             a.CredentialTag,
		Key:             a.StakingKey,
		Pool:            a.Pool,
		Drep:            a.Drep,
		DrepType:        uint8(a.DrepType), //nolint:gosec // a DRep type code
		Active:          a.Active,
		Reward:          uint64(a.Reward),
		AddedSlot:       a.AddedSlot,
		CreatedSlot:     a.CreatedSlot,
		ExpirationEpoch: a.ExpirationEpoch,
	}
}

func accountModel(r badgerutxo.AccountRow) *models.Account {
	return &models.Account{
		StakingKey:      r.Key,
		CredentialTag:   r.Tag,
		Pool:            r.Pool,
		Drep:            r.Drep,
		DrepType:        uint64(r.DrepType),
		Active:          r.Active,
		Reward:          types.Uint64(r.Reward),
		AddedSlot:       r.AddedSlot,
		CreatedSlot:     r.CreatedSlot,
		ExpirationEpoch: r.ExpirationEpoch,
	}
}

func accountPuts(changes []ledger.AccountChange) []badgerutxo.AccountPut {
	puts := make([]badgerutxo.AccountPut, 0, len(changes))
	for _, c := range changes {
		puts = append(puts, badgerutxo.AccountPut{
			Slot: c.Slot, Row: accountRow(c.Account),
		})
	}
	return puts
}

// ensureAccounts loads the metadata store's accounts into a set from before
// it kept them.
func (s *Set) ensureAccounts(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.AccountsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the accounts marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	n := 0
	err = s.store.LoadAccounts(func(put func(badgerutxo.AccountRow) error) error {
		return ldb.IterateAccounts(txn, func(a *models.Account) error {
			n++
			s.merge.noteAccount(a.CredentialTag, a.StakingKey)
			row := accountRow(*a)
			if s.merge != nil && s.mergeOverDrained.Load() {
				// The metadata store lost the rows a drain moved out, so the
				// import wrote the account over none. One it holds keeps
				// what an import of an account the ledger already holds does:
				// its delegations, activity and reward update and the slots
				// and expiry it was first written with stay.
				held, ok, err := s.store.Account(row.Tag, row.Key)
				if err != nil {
					return err
				}
				if ok {
					row.AddedSlot = held.AddedSlot
					row.CreatedSlot = held.CreatedSlot
					row.ExpirationEpoch = held.ExpirationEpoch
				}
			}
			return put(row)
		})
	})
	if err != nil {
		return fmt.Errorf("utxoset: load accounts: %w", err)
	}
	s.logger.Info("loaded accounts into the live UTxO set", "accounts", n)
	return nil
}

// AccountsLoaded reports whether the set holds every account.
func (s *Set) AccountsLoaded() (bool, error) {
	return s.store.AccountsLoaded()
}

// Account returns the account of (tag, key), active or not, or nil.
func (s *Set) Account(tag uint8, key []byte) (*models.Account, error) {
	r, ok, err := s.store.Account(tag, key)
	if err != nil || !ok {
		return nil, err
	}
	return accountModel(r), nil
}

// AccountAt returns the account of (tag, key) as a rollback to slot would
// leave it, or nil.
func (s *Set) AccountAt(tag uint8, key []byte, slot uint64) (*models.Account, error) {
	r, ok, err := s.store.AccountAt(tag, key, slot)
	if err != nil || !ok {
		return nil, err
	}
	return accountModel(r), nil
}

// Accounts calls fn with every account the set holds.
func (s *Set) Accounts(fn func(*models.Account) error) error {
	return s.store.Accounts(func(r badgerutxo.AccountRow) error {
		return fn(accountModel(r))
	})
}

func rewardRecord(r ledger.RewardRecord) badgerutxo.RewardRecord {
	return badgerutxo.RewardRecord{
		Slot: r.Slot, Withdrawal: r.Withdrawal, Source: r.Source,
		Tag: r.Ref.Tag, Key: r.Ref.Key, Amount: r.Amount,
	}
}

func rewardRecords(rs []ledger.RewardRecord) []badgerutxo.RewardRecord {
	out := make([]badgerutxo.RewardRecord, 0, len(rs))
	for _, r := range rs {
		out = append(out, rewardRecord(r))
	}
	return out
}

// HasRewardRecord reports whether the set applied r.
func (s *Set) HasRewardRecord(r ledger.RewardRecord) (bool, error) {
	return s.store.HasRewardRecord(rewardRecord(r))
}

// WithdrawnAt returns the lovelace withdrawn at slot per credential, and
// false when a withdrawal at slot was recorded without its amount.
func (s *Set) WithdrawnAt(slot uint64) (map[string]uint64, bool, error) {
	return s.store.WithdrawnAt(slot)
}

// AccountsByPool returns the active accounts delegating to pool.
func (s *Set) AccountsByPool(pool []byte) ([]models.StakeCredentialRef, error) {
	var refs []models.StakeCredentialRef
	err := s.store.AccountsByPool(pool, func(r badgerutxo.AccountRow) error {
		refs = append(refs, models.NewStakeCredentialRef(r.Tag, r.Key))
		return nil
	})
	return refs, err
}

// ActiveAccountRefs returns the credentials of every active account.
func (s *Set) ActiveAccountRefs() ([]models.StakeCredentialRef, error) {
	var refs []models.StakeCredentialRef
	err := s.store.Accounts(func(r badgerutxo.AccountRow) error {
		if r.Active {
			refs = append(refs, models.NewStakeCredentialRef(r.Tag, r.Key))
		}
		return nil
	})
	return refs, err
}

// AccountsByDrep returns the active accounts delegating to the DRep of typ
// and drep.
func (s *Set) AccountsByDrep(typ uint64, drep []byte) ([]models.StakeCredentialRef, error) {
	var refs []models.StakeCredentialRef
	err := s.store.AccountsByDrep(uint8(typ), drep, func(r badgerutxo.AccountRow) error { //nolint:gosec // a DRep type code
		refs = append(refs, models.NewStakeCredentialRef(r.Tag, r.Key))
		return nil
	})
	return refs, err
}

// AccountsWithDrep returns every account, active or not, that names a DRep.
func (s *Set) AccountsWithDrep() ([]models.StakeCredentialRef, error) {
	var refs []models.StakeCredentialRef
	err := s.store.Accounts(func(r badgerutxo.AccountRow) error {
		if len(r.Drep) > 0 {
			refs = append(refs, models.NewStakeCredentialRef(r.Tag, r.Key))
		}
		return nil
	})
	return refs, err
}
