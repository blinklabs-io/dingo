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

package badgerutxo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Account keys:
//
//	c <tag:1> <key>                              account
//	b <len:1> <pool> <tag:1> <key>               active account delegating to pool
//	v <type:1> <len:1> <drep> <tag:1> <key>      active account delegating to a DRep
//
// The secondary keys are written in the same transaction as the account, so
// they always agree with it. The state journal (rows.go) keeps each
// account's state before its first change at each slot.
// AccountRow is one stake credential's account.
type AccountRow struct {
	Tag             uint8
	Key             []byte
	Pool            []byte
	Drep            []byte
	DrepType        uint8
	Active          bool
	Reward          uint64
	AddedSlot       uint64
	CreatedSlot     uint64
	ExpirationEpoch uint64
}

// AccountPut sets an account as of the change at Slot.
type AccountPut struct {
	Slot uint64
	Row  AccountRow
}

func credKey(tag uint8, key []byte) []byte {
	return append([]byte{tag}, key...)
}

func accountKey(tag uint8, key []byte) []byte {
	return append([]byte{AccountTag}, credKey(tag, key)...)
}

func accountPoolKey(r AccountRow) []byte {
	k := []byte{AccountPoolTag, byte(len(r.Pool))} //nolint:gosec // a pool key hash
	k = append(k, r.Pool...)
	return append(k, credKey(r.Tag, r.Key)...)
}

func accountPoolPrefix(pool []byte) []byte {
	return append([]byte{AccountPoolTag, byte(len(pool))}, pool...) //nolint:gosec // a pool key hash
}

func accountDrepKey(r AccountRow) []byte {
	k := accountDrepPrefix(r.DrepType, r.Drep)
	return append(k, credKey(r.Tag, r.Key)...)
}

func accountDrepPrefix(typ uint8, drep []byte) []byte {
	k := make([]byte, 0, 3+len(drep))
	k = append(k, AccountDRepTag, typ, byte(len(drep))) //nolint:gosec // a DRep credential
	return append(k, drep...)
}

// delegatesToPool and delegatesToDrep say which secondary keys r has: an
// inactive account delegates to nothing.
func delegatesToPool(r AccountRow) bool { return r.Active && len(r.Pool) > 0 }

func delegatesToDrep(r AccountRow) bool {
	return r.Active && (len(r.Drep) > 0 || r.DrepType >= 2)
}

func appendBytes(b, v []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(v))) //nolint:gosec // bounded by a credential
	return append(b, v...)
}

func encodeAccount(r AccountRow) []byte {
	b := []byte{r.DrepType, 0}
	if r.Active {
		b[1] = 1
	}
	b = binary.BigEndian.AppendUint64(b, r.Reward)
	b = binary.BigEndian.AppendUint64(b, r.AddedSlot)
	b = binary.BigEndian.AppendUint64(b, r.CreatedSlot)
	b = binary.BigEndian.AppendUint64(b, r.ExpirationEpoch)
	b = appendBytes(b, r.Pool)
	return appendBytes(b, r.Drep)
}

var errMalformedAccount = errors.New("badgerutxo: malformed account entry")

func readBytes(v []byte) ([]byte, []byte, error) {
	if len(v) < 4 {
		return nil, nil, errMalformedAccount
	}
	n := int(binary.BigEndian.Uint32(v))
	if len(v) < 4+n {
		return nil, nil, errMalformedAccount
	}
	if n == 0 {
		return nil, v[4:], nil
	}
	return bytes.Clone(v[4 : 4+n]), v[4+n:], nil
}

// decodeAccount decodes v, the value of the account of cred (tag then key).
func decodeAccount(cred, v []byte) (AccountRow, error) {
	if len(cred) < 1 || len(v) < 34 {
		return AccountRow{}, errMalformedAccount
	}
	r := AccountRow{
		Tag:             cred[0],
		Key:             bytes.Clone(cred[1:]),
		DrepType:        v[0],
		Active:          v[1] == 1,
		Reward:          binary.BigEndian.Uint64(v[2:]),
		AddedSlot:       binary.BigEndian.Uint64(v[10:]),
		CreatedSlot:     binary.BigEndian.Uint64(v[18:]),
		ExpirationEpoch: binary.BigEndian.Uint64(v[26:]),
	}
	var err error
	rest := v[34:]
	if r.Pool, rest, err = readBytes(rest); err != nil {
		return AccountRow{}, err
	}
	if r.Drep, rest, err = readBytes(rest); err != nil {
		return AccountRow{}, err
	}
	if len(rest) != 0 {
		return AccountRow{}, errMalformedAccount
	}
	return r, nil
}

// getAccount returns the account of (tag, key) as txn sees it.
func getAccount(txn *badger.Txn, tag uint8, key []byte) (AccountRow, bool, error) {
	item, err := txn.Get(accountKey(tag, key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return AccountRow{}, false, nil
	}
	if err != nil {
		return AccountRow{}, false, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return AccountRow{}, false, err
	}
	r, err := decodeAccount(credKey(tag, key), v)
	return r, err == nil, err
}

// setAccount replaces the account of (tag, key) with r, or deletes it when
// r is nil, keeping the secondary keys in step.
func setAccount(txn *badger.Txn, tag uint8, key []byte, r *AccountRow) error {
	old, had, err := getAccount(txn, tag, key)
	if err != nil {
		return err
	}
	if had {
		if delegatesToPool(old) {
			if err := txn.Delete(accountPoolKey(old)); err != nil {
				return err
			}
		}
		if delegatesToDrep(old) {
			if err := txn.Delete(accountDrepKey(old)); err != nil {
				return err
			}
		}
	}
	if r == nil {
		if !had {
			return nil
		}
		return txn.Delete(accountKey(tag, key))
	}
	if err := txn.Set(accountKey(tag, key), encodeAccount(*r)); err != nil {
		return err
	}
	if delegatesToPool(*r) {
		if err := txn.Set(accountPoolKey(*r), nil); err != nil {
			return err
		}
	}
	if delegatesToDrep(*r) {
		if err := txn.Set(accountDrepKey(*r), nil); err != nil {
			return err
		}
	}
	return nil
}

// putAccounts applies puts in order, journaling each account's state before
// its first change at each slot.
func putAccounts(txn *badger.Txn, puts []AccountPut) error {
	for _, p := range puts {
		row := p.Row
		if err := journalRow(txn, p.Slot, accountKey(row.Tag, row.Key)); err != nil {
			return err
		}
		if err := setAccount(txn, row.Tag, row.Key, &row); err != nil {
			return err
		}
	}
	return nil
}

// Account returns the account of (tag, key).
func (s *Store) Account(tag uint8, key []byte) (AccountRow, bool, error) {
	var r AccountRow
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		var err error
		r, ok, err = getAccount(txn, tag, key)
		return err
	})
	return r, ok, err
}

// AccountAt returns the account of (tag, key) as a rollback to slot would
// leave it, from the state journal.
func (s *Store) AccountAt(tag uint8, key []byte, slot uint64) (AccountRow, bool, error) {
	v, ok, err := s.RowAt(accountKey(tag, key), slot)
	if err != nil || !ok {
		return AccountRow{}, false, err
	}
	r, err := decodeAccount(credKey(tag, key), v)
	return r, err == nil, err
}

// eachAccountUnder calls fn with the account each secondary key under
// prefix names.
func (s *Store) eachAccountUnder(prefix []byte, fn func(AccountRow) error) error {
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			cred := it.Item().KeyCopy(nil)[len(prefix):]
			if len(cred) < 1 {
				return errMalformedAccount
			}
			r, ok, err := getAccount(txn, cred[0], cred[1:])
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("badgerutxo: secondary key names a missing account %x", cred)
			}
			if err := fn(r); err != nil {
				return err
			}
		}
		return nil
	})
}

// AccountsByPool calls fn with every active account delegating to pool.
func (s *Store) AccountsByPool(pool []byte, fn func(AccountRow) error) error {
	return s.eachAccountUnder(accountPoolPrefix(pool), fn)
}

// AccountsByDrep calls fn with every active account delegating to the DRep
// of typ and credential drep, empty for the AlwaysAbstain and
// AlwaysNoConfidence types.
func (s *Store) AccountsByDrep(
	typ uint8,
	drep []byte,
	fn func(AccountRow) error,
) error {
	return s.eachAccountUnder(accountDrepPrefix(typ, drep), fn)
}

// Accounts calls fn with every account, in key order.
func (s *Store) Accounts(fn func(AccountRow) error) error {
	return s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, []byte{AccountTag}, func(k, v []byte) error {
			r, err := decodeAccount(k[1:], v)
			if err != nil {
				return err
			}
			return fn(r)
		})
	})
}

// LoadAccounts replaces the store's accounts with the ones feed supplies and
// marks them loaded, for a store from before it kept them.
func (s *Store) LoadAccounts(feed func(put func(AccountRow) error) error) error {
	if err := s.dropPrefix(
		[]byte{AccountTag}, []byte{AccountPoolTag}, []byte{AccountDRepTag},
		[]byte{RewardRecordTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop accounts: %w", err)
	}
	if !s.merging.Load() {
		if err := s.dropAccountJournal(); err != nil {
			return fmt.Errorf("badgerutxo: drop accounts: %w", err)
		}
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	n := 0
	err := feed(func(r AccountRow) error {
		n++
		if err := wb.Set(accountKey(r.Tag, r.Key), encodeAccount(r)); err != nil {
			return err
		}
		if wb.wb == nil {
			// A merge writes the account through setAccount, which keeps
			// the secondary keys in step and removes the ones the account
			// it overwrites held.
			return nil
		}
		if delegatesToPool(r) {
			if err := wb.Set(accountPoolKey(r), nil); err != nil {
				return err
			}
		}
		if delegatesToDrep(r) {
			return wb.Set(accountDrepKey(r), nil)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load accounts: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load accounts: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(legacyAccountsKey); err != nil {
			return err
		}
		return txn.Set(accountsKey, []byte{1})
	})
}

// AccountsLoaded reports whether the store holds every account up to its
// tip.
func (s *Store) AccountsLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(accountsKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// RewardRecord identifies one reward credit, by its source, or one
// withdrawal, by its transaction, applied to an account at Slot.
type RewardRecord struct {
	Slot       uint64
	Withdrawal bool
	Source     []byte
	Tag        uint8
	Key        []byte
	// Amount is the lovelace credited or withdrawn. A record written
	// without one holds no value.
	Amount uint64
}

func rewardRecordKey(r RewardRecord) []byte {
	k := make([]byte, 0, 12+len(r.Source)+len(r.Key))
	k = append(k, RewardRecordTag)
	k = binary.BigEndian.AppendUint64(k, r.Slot)
	kind := byte(0)
	if r.Withdrawal {
		kind = 1
	}
	k = append(k, kind, byte(len(r.Source))) //nolint:gosec // a hash
	k = append(k, r.Source...)
	return append(k, credKey(r.Tag, r.Key)...)
}

// HasRewardRecord reports whether r was applied.
func (s *Store) HasRewardRecord(r RewardRecord) (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(rewardRecordKey(r))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

func putRewardRecords(txn *badger.Txn, records []RewardRecord) error {
	for _, r := range records {
		if err := txn.Set(
			rewardRecordKey(r), binary.BigEndian.AppendUint64(nil, r.Amount),
		); err != nil {
			return err
		}
	}
	return nil
}

// WithdrawnAt returns the lovelace withdrawn at slot per credential (tag then
// key), from the records the set holds. ok is false when a withdrawal at slot
// was recorded without its amount, which only a record from before amounts
// were kept is.
func (s *Store) WithdrawnAt(slot uint64) (out map[string]uint64, ok bool, err error) {
	out = make(map[string]uint64)
	ok = true
	prefix := binary.BigEndian.AppendUint64([]byte{RewardRecordTag}, slot)
	err = s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			rest := k[len(prefix):]
			if len(rest) < 2 || rest[0] != 1 || len(rest) < 2+int(rest[1]) {
				continue
			}
			cred := rest[2+int(rest[1]):]
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) != 8 {
				ok = false
				continue
			}
			out[string(cred)] += binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return out, ok, err
}

// pruneRewardRecords deletes the records before slot, which no rollback
// will reach and no replay will repeat.
func (s *Store) pruneRewardRecords(slot uint64) error {
	var keys [][]byte
	if err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{RewardRecordTag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if len(k) < 9 || binary.BigEndian.Uint64(k[1:9]) >= slot {
				return nil
			}
			keys = append(keys, k)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: scan reward records: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			return fmt.Errorf("badgerutxo: prune reward records: %w", err)
		}
	}
	return wb.Flush()
}

// dropAccountJournal deletes the state journal entries of account rows, which
// the rows a load writes have no history for, and leaves every other family's.
func (s *Store) dropAccountJournal() error {
	wb := s.newWriteBatch()
	defer wb.Cancel()
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{StateJournalTag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) > 9 && k[9] == AccountTag {
				if err := wb.Delete(it.Item().KeyCopy(nil)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return wb.Flush()
}
