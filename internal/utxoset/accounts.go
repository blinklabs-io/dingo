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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
)

func accountRow(a models.Account) badgerutxo.AccountRow {
	return badgerutxo.AccountRow{
		Tag:                       a.CredentialTag,
		Key:                       a.StakingKey,
		Pool:                      a.Pool,
		Drep:                      a.Drep,
		DrepType:                  uint8(a.DrepType), //nolint:gosec // a DRep type code
		Active:                    a.Active,
		Reward:                    uint64(a.Reward),
		AddedSlot:                 a.AddedSlot,
		CreatedSlot:               a.CreatedSlot,
		ExpirationEpoch:           a.ExpirationEpoch,
		InactivityActivation:      a.InactivityActivation,
		InactivityActivationEpoch: a.InactivityActivationEpoch,
	}
}

func accountModel(r badgerutxo.AccountRow) *models.Account {
	return &models.Account{
		StakingKey:                r.Key,
		CredentialTag:             r.Tag,
		Pool:                      r.Pool,
		Drep:                      r.Drep,
		DrepType:                  uint64(r.DrepType),
		Active:                    r.Active,
		Reward:                    types.Uint64(r.Reward),
		AddedSlot:                 r.AddedSlot,
		CreatedSlot:               r.CreatedSlot,
		ExpirationEpoch:           r.ExpirationEpoch,
		InactivityActivation:      r.InactivityActivation,
		InactivityActivationEpoch: r.InactivityActivationEpoch,
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
			row := accountRow(*a)
			return put(row)
		})
	})
	if err != nil {
		return fmt.Errorf("utxoset: load accounts: %w", err)
	}
	s.logger.Info("loaded accounts into the live UTxO set", "accounts", n)
	return nil
}

// ensureAccountInactivityActivation upgrades legacy set account rows with the
// CIP-0163 activation membership and floor. The SQL relation is authoritative
// when retained; a set that activated after the account family was drained
// reconstructs the same membership from its rollback state immediately before
// the activation boundary.
func (s *Set) ensureAccountInactivityActivation(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.AccountInactivityActivationLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read account inactivity activation marker: %w", err)
	}
	if loaded {
		return nil
	}
	// The activation epoch is loaded before the accounts' membership.
	epoch, ran, err := s.InactivityActivation(nil)
	if err != nil {
		return fmt.Errorf("utxoset: read delegator-inactivity activation: %w", err)
	}
	if !ran {
		return s.store.LoadAccountInactivityActivation(0, 0, nil)
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	epochs, err := s.Epochs()
	if err != nil {
		return fmt.Errorf("utxoset: read activation epoch: %w", err)
	}
	var boundary uint64
	var foundBoundary bool
	for _, e := range epochs {
		if e.EpochId == epoch {
			boundary, foundBoundary = e.StartSlot, true
			break
		}
	}
	if !foundBoundary {
		return fmt.Errorf("utxoset: activation epoch %d is absent from epoch state", epoch)
	}
	var refs []models.StakeCredentialRef
	members := make(map[string]struct{})
	if err := s.Accounts(func(a *models.Account) error {
		ref := models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
		refs = append(refs, ref)
		if a.InactivityActivation {
			members[ref.MapKey()] = struct{}{}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("utxoset: list accounts for activation provenance: %w", err)
	}
	if !s.store.LoadsRefusedFor(nodesettings.FamilyAccounts) {
		for start := 0; start < len(refs); start += 400 {
			end := min(start+400, len(refs))
			batch, err := ldb.AccountInactivityActivationMembership(ctx, refs[start:end], txn)
			if err != nil {
				return fmt.Errorf("utxoset: read account activation membership: %w", err)
			}
			for key := range batch {
				members[key] = struct{}{}
			}
		}
	}
	if boundary > 0 {
		floor, covered, err := s.JournalFloor()
		if err != nil {
			return fmt.Errorf("utxoset: read journal floor for activation provenance: %w", err)
		}
		if covered && floor.Slot <= boundary-1 {
			for _, ref := range refs {
				a, err := s.AccountAt(ref.Tag, ref.Key, boundary-1)
				if err != nil {
					return fmt.Errorf("utxoset: reconstruct account activation membership: %w", err)
				}
				if a != nil && a.Active {
					members[ref.MapKey()] = struct{}{}
				}
			}
		} else if len(members) == 0 {
			for _, ref := range refs {
				a, err := s.Account(ref.Tag, ref.Key)
				if err != nil {
					return fmt.Errorf("utxoset: inspect account activation provenance: %w", err)
				}
				if a != nil && a.CreatedSlot < boundary && a.ExpirationEpoch != 0 {
					return fmt.Errorf("utxoset: account activation membership is unavailable below journal floor %d", floor.Slot)
				}
			}
		}
	}
	if err := s.store.LoadAccountInactivityActivation(epoch, boundary, members); err != nil {
		return fmt.Errorf("utxoset: load account inactivity activation: %w", err)
	}
	return nil
}

// AccountsLoaded reports whether the set holds every account.
func (s *Set) AccountsLoaded() (bool, error) {
	return s.store.AccountsLoaded()
}

// AccountInactivityActivationLoaded reports whether the set's accounts carry
// the one-time CIP-0163 activation membership and floor.
func (s *Set) AccountInactivityActivationLoaded() (bool, error) {
	return s.store.AccountInactivityActivationLoaded()
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

// stopRows maps the ledger's end-of-page sentinel to the store's.
func stopRows(err error) error {
	if errors.Is(err, ledger.ErrStopRows) {
		return badgerutxo.ErrStopRows
	}
	return err
}

// AccountsAfter calls fn with each account whose key is strictly after the
// account key after (every account when nil), in key order, with that key,
// until fn returns ledger.ErrStopRows. It seeks to after rather than reading
// the accounts before it.
func (s *Set) AccountsAfter(
	after []byte,
	fn func(a *models.Account, key []byte) error,
) error {
	return s.store.AccountsAfter(after, func(r badgerutxo.AccountRow, key []byte) error {
		return stopRows(fn(accountModel(r), key))
	})
}

// AccountsChangedIn calls fn with each account the state journal records a
// change to at a slot in [from, through], with that slot and the journal
// position to resume after, stopping after fn returns ledger.ErrStopRows or
// maxVisit journal entries. It reports whether the journal covers the range;
// see badgerutxo.Store.EachRowChangedIn.
func (s *Set) AccountsChangedIn(
	from, through uint64,
	after []byte,
	maxVisit int,
	fn func(a *models.Account, slot uint64, pos []byte) error,
) (next []byte, covered, done bool, err error) {
	return s.store.EachAccountChangedIn(from, through, after, maxVisit,
		func(slot uint64, r badgerutxo.AccountRow, pos []byte) error {
			return stopRows(fn(accountModel(r), slot, pos))
		})
}

// RangeRowsVisited returns how many rows and journal entries the set's
// ranged reads have stepped over.
func (s *Set) RangeRowsVisited() uint64 {
	return s.store.RangeRowsVisited()
}

func rewardRecord(r ledger.RewardRecord) badgerutxo.RewardRecord {
	return badgerutxo.RewardRecord{
		Slot: r.Slot, Withdrawal: r.Withdrawal, Source: r.Source,
		Tag: r.Ref.Tag, Key: r.Ref.Key, Amount: r.Amount,
		PostSnapshot: r.PostSnapshot,
	}
}

func rewardRecords(rs []ledger.RewardRecord) []badgerutxo.RewardRecord {
	out := make([]badgerutxo.RewardRecord, 0, len(rs))
	for _, r := range rs {
		out = append(out, rewardRecord(r))
	}
	return out
}

// RewardRecordsIn returns the reward records the set holds dated in [from,
// through], oldest write first.
func (s *Set) RewardRecordsIn(from, through uint64) ([]ledger.RewardRecord, error) {
	recs, err := s.store.RewardRecordsIn(from, through)
	if err != nil {
		return nil, err
	}
	out := make([]ledger.RewardRecord, 0, len(recs))
	for _, r := range recs {
		out = append(out, ledger.RewardRecord{
			Slot: r.Slot, Withdrawal: r.Withdrawal, Source: r.Source,
			Ref:    models.NewStakeCredentialRef(r.Tag, r.Key),
			Amount: r.Amount, PostSnapshot: r.PostSnapshot, Seq: r.Seq,
		})
	}
	return out, nil
}

// RewardCreditsAfter returns the credits dated in [from, through], oldest
// write first, those numbered above cursor when it is positive, at most limit
// of them; more reports another follows.
func (s *Set) RewardCreditsAfter(
	from, through, cursor uint64,
	limit int,
) ([]ledger.RewardRecord, bool, error) {
	recs, more, err := s.store.RewardCreditsAfter(from, through, cursor, limit)
	if err != nil {
		return nil, false, err
	}
	out := make([]ledger.RewardRecord, 0, len(recs))
	for _, r := range recs {
		out = append(out, ledger.RewardRecord{
			Slot:         r.Slot,
			Source:       r.Source,
			Ref:          models.NewStakeCredentialRef(r.Tag, r.Key),
			Amount:       r.Amount,
			PostSnapshot: r.PostSnapshot,
			Seq:          r.Seq,
		})
	}
	return out, more, nil
}

// EachRewardRecordIn calls fn with each reward record the set holds dated in
// [from, through], without holding them all.
func (s *Set) EachRewardRecordIn(
	from, through uint64,
	fn func(ledger.RewardRecord) error,
) error {
	return s.store.EachRewardRecordIn(from, through, func(r badgerutxo.RewardRecord) error {
		return fn(ledger.RewardRecord{
			Slot: r.Slot, Withdrawal: r.Withdrawal, Source: r.Source,
			Ref:    models.NewStakeCredentialRef(r.Tag, r.Key),
			Amount: r.Amount, PostSnapshot: r.PostSnapshot, Seq: r.Seq,
		})
	})
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

// ensureRewardRecords loads the metadata store's reward journal into a set
// that has not held it. A drained store holds none, and the load then marks the
// records as lost: what the drain removed cannot be read back.
func (s *Set) ensureRewardRecords(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.RewardRecordsLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the reward record marker: %w", err)
	}
	if loaded {
		return nil
	}
	r, ok := ldb.(ledgerdb.RewardJournalReader)
	if !ok {
		return nil
	}
	lost := false
	n := 0
	err = s.store.LoadRewardRecords(false, func(put func(badgerutxo.RewardRecord) error) error {
		txn := ldb.Begin(ctx, false)
		defer txn.Release()
		err := r.EachRewardJournalRow(txn, func(row ledgerdb.RewardJournalRow) error {
			n++
			return put(badgerutxo.RewardRecord{
				Slot: row.Slot, Withdrawal: row.Withdrawal, Source: row.TxHash,
				Tag: row.CredentialTag, Key: row.StakingKey, Amount: row.Amount,
				PostSnapshot: row.PostSnapshot,
			})
		})
		if errors.Is(err, database.ErrFamilyDrained) {
			lost = true
			return nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("utxoset: load reward records: %w", err)
	}
	if lost {
		if err := s.store.MarkRewardRecordsLost(); err != nil {
			return err
		}
	}
	s.logger.Info("loaded reward records into the live UTxO set", "records", n, "lost", lost)
	return nil
}

// RewardRecordsLost reports whether the metadata store had drained its reward
// journal before the set could load it.
func (s *Set) RewardRecordsLost() (bool, error) {
	return s.store.RewardRecordsLost()
}

// RewardRecordHigh returns the sequence number the next record will take.
func (s *Set) RewardRecordHigh() (uint64, error) {
	return s.store.RewardRecordHigh()
}

// AckRewardRecords records that every record numbered below seq has been
// copied by the consumer, with the rows dated through slot.
func (s *Set) AckRewardRecords(seq, slot uint64) error {
	return s.store.AckRewardRecords(seq, slot)
}

// RewardConsumedSlot returns the slot through which the consumer has copied
// the reward rows. ok is false when no consumer is registered.
func (s *Set) RewardConsumedSlot() (slot uint64, ok bool, err error) {
	return s.store.RewardConsumedSlot()
}
