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
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

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
	// InactivityActivation and its epoch preserve the one-time CIP-0163
	// activation membership and floor with the set-owned account.
	InactivityActivation      bool
	InactivityActivationEpoch uint64
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

// ErrKeyPartTooLong reports a hash too long for the one-byte length a key
// stores it behind.
var ErrKeyPartTooLong = errors.New("badgerutxo: key part longer than 255 bytes")

// keyPartLen returns the length byte of v in a key. A longer part would wrap
// to the length of a shorter one and be filed under that part's keys.
func keyPartLen(v []byte) (byte, error) {
	n := len(v)
	if n > math.MaxUint8 {
		return 0, fmt.Errorf("%w: %d bytes", ErrKeyPartTooLong, n)
	}
	return byte(n), nil
}

func accountPoolKey(r AccountRow) ([]byte, error) {
	k, err := accountPoolPrefix(r.Pool)
	if err != nil {
		return nil, err
	}
	return append(k, credKey(r.Tag, r.Key)...), nil
}

func accountPoolPrefix(pool []byte) ([]byte, error) {
	n, err := keyPartLen(pool)
	if err != nil {
		return nil, err
	}
	return append([]byte{AccountPoolTag, n}, pool...), nil
}

func accountDrepKey(r AccountRow) ([]byte, error) {
	k, err := accountDrepPrefix(r.DrepType, r.Drep)
	if err != nil {
		return nil, err
	}
	return append(k, credKey(r.Tag, r.Key)...), nil
}

func accountDrepPrefix(typ uint8, drep []byte) ([]byte, error) {
	n, err := keyPartLen(drep)
	if err != nil {
		return nil, err
	}
	return append([]byte{AccountDRepTag, typ, n}, drep...), nil
}

// accountSecondaryKeys returns the secondary keys r has.
func accountSecondaryKeys(r AccountRow) ([][]byte, error) {
	var keys [][]byte
	if delegatesToPool(r) {
		k, err := accountPoolKey(r)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	if delegatesToDrep(r) {
		k, err := accountDrepKey(r)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, nil
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
	b = appendBytes(b, r.Drep)
	if r.InactivityActivation {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return binary.BigEndian.AppendUint64(b, r.InactivityActivationEpoch)
}

// accountAddedSlot reads the AddedSlot out of an account's value.
func accountAddedSlot(v []byte) (uint64, error) {
	if len(v) < 34 {
		return 0, errMalformedAccount
	}
	return binary.BigEndian.Uint64(v[10:]), nil
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
	if len(rest) == 0 {
		return r, nil
	}
	if len(rest) != 9 || rest[0] > 1 {
		return AccountRow{}, errMalformedAccount
	}
	r.InactivityActivation = rest[0] == 1
	r.InactivityActivationEpoch = binary.BigEndian.Uint64(rest[1:])
	if !r.InactivityActivation && r.InactivityActivationEpoch != 0 {
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
		stale, err := accountSecondaryKeys(old)
		if err != nil {
			return err
		}
		for _, k := range stale {
			if err := txn.Delete(k); err != nil {
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
	secondary, err := accountSecondaryKeys(*r)
	if err != nil {
		return err
	}
	if err := txn.Set(accountKey(tag, key), encodeAccount(*r)); err != nil {
		return err
	}
	for _, k := range secondary {
		if err := txn.Set(k, nil); err != nil {
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
	prefix, err := accountPoolPrefix(pool)
	if err != nil {
		return err
	}
	return s.eachAccountUnder(prefix, fn)
}

// AccountsByDrep calls fn with every active account delegating to the DRep
// of typ and credential drep, empty for the AlwaysAbstain and
// AlwaysNoConfidence types.
func (s *Store) AccountsByDrep(
	typ uint8,
	drep []byte,
	fn func(AccountRow) error,
) error {
	prefix, err := accountDrepPrefix(typ, drep)
	if err != nil {
		return err
	}
	return s.eachAccountUnder(prefix, fn)
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

// AccountsAfter calls fn with each account whose key is strictly after the
// account key after (every account when nil), in key order, with that key,
// until fn returns ErrStopRows.
func (s *Store) AccountsAfter(
	after []byte,
	fn func(r AccountRow, key []byte) error,
) error {
	return s.EachRowAfter([]byte{AccountTag}, after, nil, func(k, v []byte) error {
		r, err := decodeAccount(k[1:], v)
		if err != nil {
			return err
		}
		return fn(r, bytes.Clone(k))
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
	if err := s.dropAccountJournal(); err != nil {
		return fmt.Errorf("badgerutxo: drop accounts: %w", err)
	}
	if err := s.dropRewardSeqIndex(); err != nil {
		return fmt.Errorf("badgerutxo: drop accounts: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	n := 0
	err := feed(func(r AccountRow) error {
		n++
		secondary, err := accountSecondaryKeys(r)
		if err != nil {
			return err
		}
		if err := wb.Set(accountKey(r.Tag, r.Key), encodeAccount(r)); err != nil {
			return err
		}
		for _, k := range secondary {
			if err := wb.Set(k, nil); err != nil {
				return err
			}
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
		if err := txn.Delete(accountInactivityActivationKey); err != nil {
			return err
		}
		return txn.Set(accountsKey, []byte{1})
	})
}

// LoadAccountInactivityActivation adds the one-time CIP-0163 activation
// provenance to legacy account rows. A journal entry at slot S stores the
// state before S, so only entries after the activation boundary carry it.
func (s *Store) LoadAccountInactivityActivation(
	epoch, boundary uint64,
	members map[string]struct{},
) error {
	wb := s.newWriteBatch()
	defer wb.Cancel()
	patch := func(cred, value []byte) ([]byte, error) {
		if len(cred) < 1 {
			return nil, errMalformedAccount
		}
		if _, ok := members[string(cred)]; !ok {
			return nil, nil
		}
		r, err := decodeAccount(cred, value)
		if err != nil {
			return nil, err
		}
		r.InactivityActivation = true
		r.InactivityActivationEpoch = epoch
		return encodeAccount(r), nil
	}
	if err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{AccountTag}
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				it.Close()
				return err
			}
			v, err = patch(k[1:], v)
			if err != nil {
				it.Close()
				return err
			}
			if v != nil {
				if err := wb.Set(k, v); err != nil {
					it.Close()
					return err
				}
			}
		}
		it.Close()

		opts.Prefix = []byte{StateJournalTag}
		it = txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if len(k) < 11 || k[9] != AccountTag ||
				binary.BigEndian.Uint64(k[1:9]) <= boundary {
				continue
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) < 2 || v[0] != 1 {
				continue
			}
			encoded, err := patch(k[10:], v[1:])
			if err != nil {
				return err
			}
			if encoded != nil {
				if err := wb.Set(k, append([]byte{1}, encoded...)); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: load account inactivity activation: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load account inactivity activation: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(accountInactivityActivationKey, []byte{1})
	})
}

// AccountInactivityActivationLoaded reports whether account rows carry the
// CIP-0163 activation provenance relation.
func (s *Store) AccountInactivityActivationLoaded() (bool, error) {
	return s.hasMarker(accountInactivityActivationKey)
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
	// PostSnapshot marks a credit a boundary applied after its SNAP point.
	PostSnapshot bool
	// Seq orders records by when the store wrote them, whatever their slot:
	// a credit journaled lazily under an old boundary slot is newer than
	// every record written before it. The store assigns it; it is zero for a
	// record written before sequences were kept, and is ignored on write.
	Seq uint64
}

// rewardRecordValue is a record's value: its amount, then a flag byte, then
// its sequence number, 8 bytes big-endian each. A record from before flags
// and sequences hold only the amount.
const (
	rewardRecordAmountLen = 8
	rewardRecordLen       = 17
)

func readRewardSeq(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get(rewardSeqKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("badgerutxo: malformed reward record sequence")
	}
	return binary.BigEndian.Uint64(v), nil
}

func rewardRecordKey(r RewardRecord) ([]byte, error) {
	n, err := keyPartLen(r.Source)
	if err != nil {
		return nil, err
	}
	k := binary.BigEndian.AppendUint64([]byte{RewardRecordTag}, r.Slot)
	kind := byte(0)
	if r.Withdrawal {
		kind = 1
	}
	k = append(k, kind, n)
	k = append(k, r.Source...)
	return append(k, credKey(r.Tag, r.Key)...), nil
}

// HasRewardRecord reports whether r was applied.
func (s *Store) HasRewardRecord(r RewardRecord) (bool, error) {
	k, err := rewardRecordKey(r)
	if err != nil {
		return false, err
	}
	var ok bool
	err = s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

func putRewardRecords(txn *badger.Txn, records []RewardRecord) error {
	if len(records) == 0 {
		return nil
	}
	next, err := readRewardSeq(txn)
	if err != nil {
		return err
	}
	for _, r := range records {
		k, err := rewardRecordKey(r)
		if err != nil {
			return err
		}
		seq := next
		item, err := txn.Get(k)
		switch {
		case errors.Is(err, badger.ErrKeyNotFound):
			next++
		case err != nil:
			return err
		default:
			// The same record written again keeps its place in the order.
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) == rewardRecordLen {
				seq = binary.BigEndian.Uint64(v[9:])
			} else {
				next++
			}
		}
		v := binary.BigEndian.AppendUint64(nil, r.Amount)
		flag := byte(0)
		if r.PostSnapshot {
			flag = 1
		}
		v = append(v, flag)
		if err := txn.Set(
			k, binary.BigEndian.AppendUint64(v, seq),
		); err != nil {
			return err
		}
		if err := setRewardSeqIndex(txn, k, seq); err != nil {
			return err
		}
	}
	return txn.Set(rewardSeqKey, binary.BigEndian.AppendUint64(nil, next))
}

// RewardRecordsIn returns the records dated in [from, through], each with
// its sequence number, oldest write first.
func (s *Store) RewardRecordsIn(from, through uint64) ([]RewardRecord, error) {
	var out []RewardRecord
	err := s.EachRewardRecordIn(from, through, func(r RewardRecord) error {
		out = append(out, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(out, func(a, b RewardRecord) int {
		return cmp.Compare(a.Seq, b.Seq)
	})
	return out, nil
}

// EachRewardRecordIn calls fn with each record dated in [from, through], in
// slot order, without holding them all.
func (s *Store) EachRewardRecordIn(
	from, through uint64,
	fn func(RewardRecord) error,
) error {
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{RewardRecordTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(slotKey(RewardRecordTag, from)); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) < 11 {
				continue
			}
			slot := binary.BigEndian.Uint64(k[1:9])
			if slot > through {
				return nil
			}
			srcLen := int(k[10])
			if len(k) < 11+srcLen+1 {
				return fmt.Errorf("badgerutxo: malformed reward record key %x", k)
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) < rewardRecordAmountLen {
				return fmt.Errorf("badgerutxo: malformed reward record at slot %d", slot)
			}
			rec := RewardRecord{
				Slot:       slot,
				Withdrawal: k[9] == 1,
				Source:     bytes.Clone(k[11 : 11+srcLen]),
				Tag:        k[11+srcLen],
				Key:        bytes.Clone(k[11+srcLen+1:]),
				Amount:     binary.BigEndian.Uint64(v),
			}
			if len(v) == rewardRecordLen {
				rec.PostSnapshot = v[8] == 1
				rec.Seq = binary.BigEndian.Uint64(v[9:])
			}
			if err := fn(rec); err != nil {
				return err
			}
		}
		return nil
	})
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
			if len(v) < rewardRecordAmountLen {
				ok = false
				continue
			}
			out[string(cred)] += binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return out, ok, err
}

// pruneRewardRecords deletes the records before slot, which no rollback will
// reach and no replay will repeat. While a consumer is registered it keeps the
// records it has not yet copied: those numbered at or above its consumed mark.
func (s *Store) pruneRewardRecords(slot uint64) error {
	var keys, indexKeys [][]byte
	if err := s.view(func(txn *badger.Txn) error {
		consumed, held, err := rewardConsumed(txn)
		if err != nil {
			return err
		}
		var through uint64
		if held {
			if through, err = readConsumedSlot(txn); err != nil {
				return err
			}
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{RewardRecordTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if len(k) < 9 || binary.BigEndian.Uint64(k[1:9]) >= slot {
				return nil
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if held {
				if binary.BigEndian.Uint64(k[1:9]) > through {
					continue
				}
				var seq uint64
				if len(v) == rewardRecordLen {
					seq = binary.BigEndian.Uint64(v[9:])
				}
				if seq >= consumed {
					continue
				}
			}
			keys = append(keys, k)
			if rewardKeyIsCredit(k) && len(v) == rewardRecordLen {
				indexKeys = append(indexKeys, rewardSeqIndexKey(binary.BigEndian.Uint64(v[9:])))
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("badgerutxo: scan reward records: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	for _, k := range slices.Concat(keys, indexKeys) {
		if err := wb.Delete(k); err != nil {
			return fmt.Errorf("badgerutxo: prune reward records: %w", err)
		}
	}
	return wb.Flush()
}

// rewardConsumed returns the consumed mark and whether a consumer is
// registered.
func rewardConsumed(txn *badger.Txn) (uint64, bool, error) {
	if _, err := txn.Get(rewardConsumerKey); errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}
	item, err := txn.Get(rewardConsumedKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil || len(v) != 8 {
		return 0, false, errors.New("badgerutxo: malformed reward consumed mark")
	}
	return binary.BigEndian.Uint64(v), true, nil
}

// SetRewardConsumer registers, or with on false removes, a reader that copies
// the reward records and needs every one it has not yet acknowledged.
func (s *Store) SetRewardConsumer(on bool) error {
	return s.update(func(txn *badger.Txn) error {
		_, err := txn.Get(rewardConsumerKey)
		held := err == nil
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		switch {
		case on && !held:
			return txn.Set(rewardConsumerKey, []byte{1})
		case !on && held:
			if err := txn.Delete(rewardConsumedKey); err != nil {
				return err
			}
			if err := txn.Delete(rewardConsumedSlotKey); err != nil {
				return err
			}
			return txn.Delete(rewardConsumerKey)
		}
		return nil
	})
}

// RewardRecordHigh returns the sequence number the next record will take:
// every record held so far is numbered below it.
func (s *Store) RewardRecordHigh() (uint64, error) {
	var n uint64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		n, err = readRewardSeq(txn)
		return err
	})
	return n, err
}

// AckRewardRecords records that the consumer has copied every record numbered
// below seq and dated through slot, and the reward rows dated through slot.
// The marks never move back.
func (s *Store) AckRewardRecords(seq, slot uint64) error {
	return s.update(func(txn *badger.Txn) error {
		cur, held, err := rewardConsumed(txn)
		if err != nil || !held {
			return err
		}
		if seq > cur {
			err := txn.Set(rewardConsumedKey, binary.BigEndian.AppendUint64(nil, seq))
			if err != nil {
				return err
			}
		}
		prev, err := readConsumedSlot(txn)
		if err != nil || slot <= prev {
			return err
		}
		return txn.Set(rewardConsumedSlotKey, binary.BigEndian.AppendUint64(nil, slot))
	})
}

func readConsumedSlot(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get(rewardConsumedSlotKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil || len(v) != 8 {
		return 0, errors.New("badgerutxo: malformed reward consumed slot")
	}
	return binary.BigEndian.Uint64(v), nil
}

// RewardConsumedSlot returns the slot through which the consumer has copied
// the reward rows, and whether a consumer is registered.
func (s *Store) RewardConsumedSlot() (slot uint64, held bool, err error) {
	err = s.view(func(txn *badger.Txn) error {
		if _, e := txn.Get(rewardConsumerKey); errors.Is(e, badger.ErrKeyNotFound) {
			return nil
		} else if e != nil {
			return e
		}
		held = true
		slot, err = readConsumedSlot(txn)
		return err
	})
	return slot, held, err
}

// LoadRewardRecords loads the metadata store's reward journal into a store
// from before it kept the records, numbering them in the order feed supplies
// them, and marks them loaded.
func (s *Store) LoadRewardRecords(
	feed func(put func(RewardRecord) error) error,
) error {
	var batch []RewardRecord
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		recs := batch
		batch = nil
		return s.update(func(txn *badger.Txn) error {
			return putRewardRecords(txn, recs)
		})
	}
	err := feed(func(r RewardRecord) error {
		batch = append(batch, r)
		if len(batch) >= 2000 {
			return flush()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load reward records: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("badgerutxo: load reward records: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(rewardRecordsKey, loadedMarker)
	})
}

// RewardRecordsLoaded reports whether the store holds the reward records of
// the metadata store's journal.
func (s *Store) RewardRecordsLoaded() (bool, error) {
	return s.hasMarker(rewardRecordsKey)
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
