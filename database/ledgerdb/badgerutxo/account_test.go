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
