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

package badgerutxo_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func acct(key byte, pool, drep byte, active bool, reward uint64) badgerutxo.AccountRow {
	r := badgerutxo.AccountRow{
		Tag: 0, Key: []byte{key, key}, Active: active, Reward: reward,
	}
	if pool != 0 {
		r.Pool = []byte{pool}
	}
	if drep != 0 {
		r.Drep = []byte{drep}
	}
	return r
}

func applyAccounts(
	t *testing.T,
	s *badgerutxo.Store,
	tip ocommon.Point,
	rows ...badgerutxo.AccountRow,
) {
	t.Helper()
	puts := make([]badgerutxo.AccountPut, 0, len(rows))
	for _, r := range rows {
		puts = append(puts, badgerutxo.AccountPut{Slot: tip.Slot, Row: r})
	}
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Accounts: puts})
	require.NoError(t, err)
}

func byPool(t *testing.T, s *badgerutxo.Store, pool byte) []byte {
	t.Helper()
	var keys []byte
	require.NoError(t, s.AccountsByPool([]byte{pool}, func(r badgerutxo.AccountRow) error {
		keys = append(keys, r.Key[0])
		return nil
	}))
	return keys
}

func byDrep(t *testing.T, s *badgerutxo.Store, drep byte) []byte {
	t.Helper()
	var keys []byte
	require.NoError(t, s.AccountsByDrep(0, []byte{drep}, func(r badgerutxo.AccountRow) error {
		keys = append(keys, r.Key[0])
		return nil
	}))
	return keys
}

// The secondary keys follow an account's delegation and drop it when it
// stops being active.
func TestAccountSecondaryKeysFollowTheAccount(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyAccounts(t, s, point(10, 0x10),
		acct(1, 'p', 'd', true, 5), acct(2, 'p', 0, true, 0))
	require.Equal(t, []byte{1, 2}, byPool(t, s, 'p'))
	require.Equal(t, []byte{1}, byDrep(t, s, 'd'))
	applyAccounts(t, s, point(20, 0x20),
		acct(1, 'q', 'd', true, 5), acct(2, 'p', 0, false, 0))
	require.Empty(t, byPool(t, s, 'p'))
	require.Equal(t, []byte{1}, byPool(t, s, 'q'))
	r, ok, err := s.Account(0, []byte{2, 2})
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, r.Active)
}

// Activation provenance survives restart, while an account encoded by the
// previous format remains readable with no invented membership.
func TestAccountInactivityActivationEncoding(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir)
	bootstrap(t, s, point(1, 0x01))
	r := acct(1, 'p', 0, true, 5)
	r.InactivityActivation = true
	r.InactivityActivationEpoch = 7
	applyAccounts(t, s, point(10, 0x10), r)
	require.NoError(t, s.Close())

	s = open(t, dir)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	got, ok, err := s.Account(r.Tag, r.Key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, r, got)

	legacy, err := badgerutxo.DecodeLegacyAccountForTest(r)
	require.NoError(t, err)
	require.False(t, legacy.InactivityActivation)
	require.Zero(t, legacy.InactivityActivationEpoch)
}

// Migrating a legacy account patches only journal states after activation, so
// rollback keeps the membership at and after the boundary and removes it
// before the boundary.
func TestLoadAccountInactivityActivationPreservesRollback(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	before := acct(1, 'p', 0, true, 5)
	applyAccounts(t, s, point(5, 0x05), before)
	stamped := before
	stamped.ExpirationEpoch = 3
	applyAccounts(t, s, point(10, 0x10), stamped)
	after := stamped
	after.Active = false
	applyAccounts(t, s, point(20, 0x20), after)

	members := map[string]struct{}{string(append([]byte{before.Tag}, before.Key...)): {}}
	require.NoError(t, s.LoadAccountInactivityActivation(1, 10, members))
	at15, ok, err := s.AccountAt(before.Tag, before.Key, 15)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, at15.InactivityActivation)
	require.Equal(t, uint64(1), at15.InactivityActivationEpoch)
	at9, ok, err := s.AccountAt(before.Tag, before.Key, 9)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, at9.InactivityActivation)
	require.Zero(t, at9.InactivityActivationEpoch)
}

// A rollback restores every account to its state at the target, newest
// change first, in both rollback paths, including removing an account
// created after it.
func TestRollbackRestoresAccounts(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		applyAccounts(t, s, point(10, 0x10), acct(1, 'p', 0, true, 5))
		applyAccounts(t, s, point(20, 0x20),
			acct(1, 'q', 'd', true, 7), acct(2, 'p', 0, true, 0))
		applyAccounts(t, s, point(30, 0x1e), acct(1, 'r', 0, false, 9))
		if viaJournal {
			_, err := s.Rollback(point(10, 0x10))
			require.NoError(t, err)
		} else {
			to := uint64(15)
			tip := point(15, 0x0f)
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
				DeleteNoncesAfter: &to,
			})
			require.NoError(t, err)
		}
		r, ok, err := s.Account(0, []byte{1, 1})
		require.NoError(t, err)
		require.True(t, ok, "journal=%t", viaJournal)
		require.Equal(t, acct(1, 'p', 0, true, 5), r, "journal=%t", viaJournal)
		_, ok, err = s.Account(0, []byte{2, 2})
		require.NoError(t, err)
		require.False(t, ok, "journal=%t", viaJournal)
		require.Equal(t, []byte{1}, byPool(t, s, 'p'), "journal=%t", viaJournal)
		require.Empty(t, byPool(t, s, 'q'), "journal=%t", viaJournal)
		require.Empty(t, byPool(t, s, 'r'), "journal=%t", viaJournal)
		require.Empty(t, byDrep(t, s, 'd'), "journal=%t", viaJournal)
	}
}

// A pruned journal no longer undoes changes before the prune slot.
func TestPruneJournalDropsOldAccountEntries(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyAccounts(t, s, point(10, 0x10), acct(1, 'p', 0, true, 5))
	applyAccounts(t, s, point(20, 0x20), acct(1, 'q', 0, true, 6))
	require.NoError(t, s.PruneJournal(15))
	to := uint64(5)
	tip := point(5, 0x05)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &to})
	require.NoError(t, err)
	r, ok, err := s.Account(0, []byte{1, 1})
	require.NoError(t, err)
	require.True(t, ok, "the change at slot 10 is past the pruned journal")
	require.Equal(t, acct(1, 'p', 0, true, 5), r)
}

func TestLoadAccountsAndBootstrapMarker(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.AccountsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, s.LoadAccounts(func(put func(badgerutxo.AccountRow) error) error {
		return put(acct(3, 'p', 'd', true, 4))
	}))
	loaded, err = s.AccountsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	require.Equal(t, []byte{3}, byPool(t, s, 'p'))
	require.Equal(t, []byte{3}, byDrep(t, s, 'd'))

	bootstrap(t, s, point(5, 0x05))
	loaded, err = s.AccountsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	n := 0
	require.NoError(t, s.Accounts(func(badgerutxo.AccountRow) error { n++; return nil }))
	require.Zero(t, n)
	require.Empty(t, byPool(t, s, 'p'))
}

// AccountAt answers from the journal what a rollback to the slot would
// leave.
func TestAccountAtSlot(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyAccounts(t, s, point(10, 0x10), acct(1, 'p', 0, true, 5))
	applyAccounts(t, s, point(20, 0x20), acct(1, 'q', 0, true, 6))
	for _, tc := range []struct {
		slot uint64
		ok   bool
		pool byte
	}{{5, false, 0}, {10, true, 'p'}, {15, true, 'p'}, {20, true, 'q'}, {99, true, 'q'}} {
		r, ok, err := s.AccountAt(0, []byte{1, 1}, tc.slot)
		require.NoError(t, err)
		require.Equal(t, tc.ok, ok, "slot %d", tc.slot)
		if ok {
			require.Equal(t, []byte{tc.pool}, r.Pool, "slot %d", tc.slot)
		}
	}
}

// Reward records are written with the batch, deleted by a rollback past
// their slot and pruned with the journal.
func TestRewardRecordsFollowRollbackAndPrune(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	rec := func(slot uint64, withdrawal bool) badgerutxo.RewardRecord {
		return badgerutxo.RewardRecord{
			Slot: slot, Withdrawal: withdrawal, Source: []byte{7},
			Tag: 0, Key: []byte{1, 1},
		}
	}
	for _, r := range []badgerutxo.RewardRecord{rec(10, false), rec(20, true)} {
		tip := point(r.Slot, byte(r.Slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			RewardRecords: []badgerutxo.RewardRecord{r},
		})
		require.NoError(t, err)
	}
	has := func(r badgerutxo.RewardRecord) bool {
		ok, err := s.HasRewardRecord(r)
		require.NoError(t, err)
		return ok
	}
	require.True(t, has(rec(10, false)))
	require.False(t, has(rec(10, true)), "a withdrawal is not a credit")
	require.True(t, has(rec(20, true)))
	to := uint64(15)
	tip := point(15, 0x0f)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &to})
	require.NoError(t, err)
	require.True(t, has(rec(10, false)))
	require.False(t, has(rec(20, true)))
	require.NoError(t, s.PruneJournal(11))
	require.False(t, has(rec(10, false)))
}

// WithdrawnAt sums the withdrawals recorded at one slot per credential and
// leaves out credits and the withdrawals of other slots.
func TestWithdrawnAtSumsTheWithdrawalsOfOneSlot(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	rec := func(slot uint64, withdrawal bool, source byte, key byte, amount uint64) badgerutxo.RewardRecord {
		return badgerutxo.RewardRecord{
			Slot: slot, Withdrawal: withdrawal, Source: []byte{source},
			Tag: 0, Key: []byte{key}, Amount: amount,
		}
	}
	tip := point(20, 0x14)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		RewardRecords: []badgerutxo.RewardRecord{
			rec(20, true, 1, 1, 5),
			rec(20, true, 2, 1, 7),
			rec(20, true, 3, 2, 11),
			rec(20, false, 4, 1, 100),
			rec(21, true, 5, 1, 1000),
		},
	})
	require.NoError(t, err)
	got, complete, err := s.WithdrawnAt(20)
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, map[string]uint64{
		string([]byte{0, 1}): 12,
		string([]byte{0, 2}): 11,
	}, got)
}

// The store numbers each record in the order it wrote them, whatever the
// slot: a credit written later under an old slot follows every earlier one,
// and a record written again keeps its number.
func TestRewardRecordsAreNumberedByWriteOrder(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	credit := func(slot uint64, source byte, post bool) badgerutxo.RewardRecord {
		return badgerutxo.RewardRecord{
			Slot: slot, Source: []byte{source}, Tag: 1, Key: []byte{9, 9},
			Amount: uint64(source) * 10, PostSnapshot: post,
		}
	}
	apply := func(slot uint64, recs ...badgerutxo.RewardRecord) {
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{RewardRecords: recs})
		require.NoError(t, err)
	}
	apply(20, credit(20, 1, false), credit(20, 2, true))
	apply(30, credit(10, 3, false))
	apply(31, credit(20, 1, false))

	got, err := s.RewardRecordsIn(0, 100)
	require.NoError(t, err)
	require.Len(t, got, 3)
	var order []byte
	for i, r := range got {
		order = append(order, r.Source[0])
		require.Equal(t, uint64(i), r.Seq, "numbers run from zero without a gap")
		require.Equal(t, uint64(r.Source[0])*10, r.Amount)
		require.Equal(t, r.Source[0] == 2, r.PostSnapshot)
		require.Equal(t, uint8(1), r.Tag)
		require.Equal(t, []byte{9, 9}, r.Key)
	}
	require.Equal(t, []byte{1, 2, 3}, order,
		"the credit written last, at the oldest slot, is last")

	inRange, err := s.RewardRecordsIn(15, 25)
	require.NoError(t, err)
	require.Len(t, inRange, 2)
	require.Equal(t, uint64(0), inRange[0].Seq)
}

// While a consumer is registered a prune keeps the reward records it has not
// copied: those it has not acknowledged, and those dated after the slot it
// copied through. Without one every record below the slot goes.
func TestRewardRecordPruneKeepsWhatTheConsumerHasNotCopied(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T, consumer bool) *badgerutxo.Store {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		require.NoError(t, s.SetRewardConsumer(consumer))
		tip := point(100, 0x64)
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			RewardRecords: []badgerutxo.RewardRecord{
				{Slot: 10, Source: []byte{1}, Key: []byte{1}, Amount: 1},
				{Slot: 20, Source: []byte{2}, Key: []byte{1}, Amount: 2},
				{Slot: 30, Source: []byte{3}, Key: []byte{1}, Amount: 3},
			},
		})
		require.NoError(t, err)
		return s
	}
	slots := func(t *testing.T, s *badgerutxo.Store) []uint64 {
		recs, err := s.RewardRecordsIn(0, 1000)
		require.NoError(t, err)
		var out []uint64
		for _, r := range recs {
			out = append(out, r.Slot)
		}
		return out
	}

	s := setup(t, false)
	require.NoError(t, s.PruneJournal(25))
	require.Equal(t, []uint64{30}, slots(t, s), "no consumer: everything below 25 goes")

	s = setup(t, true)
	require.NoError(t, s.PruneJournal(25))
	require.Equal(t, []uint64{10, 20, 30}, slots(t, s), "nothing is acknowledged")

	// Records 0 and 1 are acknowledged, copied through slot 20.
	require.NoError(t, s.AckRewardRecords(2, 20))
	require.NoError(t, s.PruneJournal(25))
	require.Equal(t, []uint64{30}, slots(t, s))

	s = setup(t, true)
	// Everything is acknowledged, but only slots through 10 were copied.
	require.NoError(t, s.AckRewardRecords(3, 10))
	require.NoError(t, s.PruneJournal(25))
	require.Equal(t, []uint64{20, 30}, slots(t, s),
		"a record dated after the copied slot stays")
}

// A secondary key holds a pool or DRep hash behind a one-byte length, so a
// longer hash is refused where it enters the store instead of being filed
// under the key of a shorter one.
func TestAccountRejectsDelegationHashLongerThanItsKeyLength(t *testing.T) {
	t.Parallel()
	hash := func(n int) []byte { return bytes.Repeat([]byte{0xab}, n) }
	row := func(pool, drep []byte) badgerutxo.AccountRow {
		return badgerutxo.AccountRow{
			Tag: 0, Key: []byte{1, 1}, Active: true, Pool: pool, Drep: drep,
		}
	}
	apply := func(s *badgerutxo.Store, r badgerutxo.AccountRow) error {
		tip := point(10, 0x10)
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			Accounts: []badgerutxo.AccountPut{{Slot: tip.Slot, Row: r}},
		})
		return err
	}
	load := func(s *badgerutxo.Store, r badgerutxo.AccountRow) error {
		return s.LoadAccounts(func(put func(badgerutxo.AccountRow) error) error {
			return put(r)
		})
	}
	count := func(t *testing.T, walk func(func(badgerutxo.AccountRow) error) error) int {
		t.Helper()
		n := 0
		require.NoError(t, walk(func(badgerutxo.AccountRow) error { n++; return nil }))
		return n
	}
	for name, write := range map[string]func(*badgerutxo.Store, badgerutxo.AccountRow) error{
		"batch": apply, "load": load,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := open(t, t.TempDir())
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			bootstrap(t, s, point(1, 0x01))
			require.ErrorIs(t, write(s, row(hash(256), nil)), badgerutxo.ErrKeyPartTooLong)
			require.ErrorIs(t, write(s, row(nil, hash(256))), badgerutxo.ErrKeyPartTooLong)
			require.NoError(t, write(s, row(hash(255), hash(255))))
			require.Equal(t, 1, count(t, func(fn func(badgerutxo.AccountRow) error) error {
				return s.AccountsByPool(hash(255), fn)
			}))
			require.Equal(t, 1, count(t, func(fn func(badgerutxo.AccountRow) error) error {
				return s.AccountsByDrep(0, hash(255), fn)
			}))
			// A 256-byte hash truncates to the length of an empty one.
			require.Zero(t, count(t, func(fn func(badgerutxo.AccountRow) error) error {
				return s.AccountsByPool(nil, fn)
			}))
			abstain := row(nil, nil)
			abstain.DrepType = 2
			require.NoError(t, write(s, abstain))
			require.Equal(t, 1, count(t, func(fn func(badgerutxo.AccountRow) error) error {
				return s.AccountsByDrep(2, nil, fn)
			}))
		})
	}
	t.Run("read", func(t *testing.T) {
		t.Parallel()
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		none := func(badgerutxo.AccountRow) error { return nil }
		require.ErrorIs(t, s.AccountsByPool(hash(256), none), badgerutxo.ErrKeyPartTooLong)
		require.ErrorIs(t, s.AccountsByDrep(0, hash(256), none), badgerutxo.ErrKeyPartTooLong)
	})
}

// A reward record's key holds its source behind a one-byte length.
func TestRewardRecordRejectsSourceLongerThanItsKeyLength(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	rec := func(n int) badgerutxo.RewardRecord {
		return badgerutxo.RewardRecord{
			Slot: 10, Source: bytes.Repeat([]byte{7}, n), Tag: 0, Key: []byte{1, 1},
		}
	}
	apply := func(r badgerutxo.RewardRecord) error {
		tip := point(10, 0x10)
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			RewardRecords: []badgerutxo.RewardRecord{r},
		})
		return err
	}
	require.ErrorIs(t, apply(rec(256)), badgerutxo.ErrKeyPartTooLong)
	_, err := s.HasRewardRecord(rec(256))
	require.ErrorIs(t, err, badgerutxo.ErrKeyPartTooLong)
	require.NoError(t, apply(rec(255)))
	ok, err := s.HasRewardRecord(rec(255))
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = s.HasRewardRecord(rec(0))
	require.NoError(t, err)
	require.False(t, ok, "a 256-byte source must not be filed as an empty one")
}
