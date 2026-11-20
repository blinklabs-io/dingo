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
	"github.com/stretchr/testify/require"
)

func accountAt(key byte, slot uint64) badgerutxo.AccountRow {
	r := acct(key, 0, 0, true, 0)
	r.AddedSlot = slot
	return r
}

// changedIn reads every account the journal names in [from, through], one
// journal entry at a time, and reports whether the journal covered the range.
func changedIn(
	t *testing.T,
	s *badgerutxo.Store,
	from, through uint64,
) (keys []byte, covered bool) {
	t.Helper()
	var after []byte
	for {
		next, ok, done, err := s.EachAccountChangedIn(from, through, after, 1,
			func(slot uint64, r badgerutxo.AccountRow, _ []byte) error {
				if r.AddedSlot == slot {
					keys = append(keys, r.Key[0])
				}
				return nil
			})
		require.NoError(t, err)
		if !ok {
			return nil, false
		}
		if done {
			return keys, true
		}
		after = next
	}
}

// The journal names the accounts of a slot range at a cost of the entries in
// it: the rows outside the range are not stepped over.
func TestAccountsChangedInStepsOverOnlyTheRange(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	const n = 200
	for i := 1; i <= n; i++ {
		slot := uint64(10 + i) //nolint:gosec // a small index
		applyAccounts(t, s, point(slot, byte(i)), accountAt(byte(i), slot))
	}
	before := s.RangeRowsVisited()
	keys, covered := changedIn(t, s, 100, 103)
	require.True(t, covered)
	require.Equal(t, []byte{90, 91, 92, 93}, keys)
	require.LessOrEqual(t, s.RangeRowsVisited()-before, uint64(8))
}

// A range below what the journal holds is not covered: pruning drops the
// entries below its slot, a load writes rows with none, and a store that opens
// without a floor takes its tip. Nothing is served from a range that is not
// covered.
func TestAccountsChangedInIsNotCoveredBelowTheJournalsFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir)
	bootstrap(t, s, point(1, 0x01))
	for i := 1; i <= 5; i++ {
		slot := uint64(10 * i) //nolint:gosec // a small index
		applyAccounts(t, s, point(slot, byte(i)), accountAt(byte(i), slot))
	}
	keys, covered := changedIn(t, s, 20, 40)
	require.True(t, covered)
	require.Equal(t, []byte{2, 3, 4}, keys)

	require.NoError(t, s.PruneJournal(30))
	_, covered = changedIn(t, s, 20, 40)
	require.False(t, covered, "the entry at slot 20 is pruned")
	keys, covered = changedIn(t, s, 30, 40)
	require.True(t, covered)
	require.Equal(t, []byte{3, 4}, keys)

	require.NoError(t, s.LoadAccounts(func(put func(badgerutxo.AccountRow) error) error {
		return put(accountAt(9, 5))
	}))
	_, covered = changedIn(t, s, 51, 60)
	require.True(t, covered, "a load at the tip names the slots after it")
	applyAccounts(t, s, point(60, 6), accountAt(6, 60))
	_, covered = changedIn(t, s, 51, 60)
	require.True(t, covered)
	_, covered = changedIn(t, s, 0, 60)
	require.False(t, covered)

	// A store from before the floor existed takes its tip.
	require.NoError(t, s.DeleteExportFloorForTest())
	require.NoError(t, s.Close())
	s = open(t, dir)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, covered = changedIn(t, s, 60, 70)
	require.False(t, covered, "a row from before the floor may lack its entry")
	_, covered = changedIn(t, s, 61, 70)
	require.True(t, covered)
}

// A set loaded from a snapshot holds rows below the journal's floor, and a
// replay reads them one segment at a time. Each segment is served from the
// rows added in it, so its cost is those rows and not the table.
func TestAccountsChangedInServesLoadedRowsBelowTheFloorByRange(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	const n = 200
	bootstrap(t, s, point(n+1, 0x01))
	require.NoError(t, s.LoadAccounts(func(put func(badgerutxo.AccountRow) error) error {
		for i := 1; i <= n; i++ {
			if err := put(accountAt(byte(i), uint64(i))); err != nil { //nolint:gosec // a small index
				return err
			}
		}
		return nil
	}))

	const width = 10
	for from := uint64(1); from <= n; from += width {
		before := s.RangeRowsVisited()
		keys, covered := changedIn(t, s, from, from+width-1)
		require.True(t, covered, "segment from %d", from)
		want := make([]byte, 0, width)
		for k := from; k < from+width; k++ {
			want = append(want, byte(k)) //nolint:gosec // a small index
		}
		require.Equal(t, want, keys, "segment from %d", from)
		require.LessOrEqual(t, s.RangeRowsVisited()-before, uint64(2*width),
			"segment from %d stepped over rows outside it", from)
	}
}

func loadAccountsAtSlots(t *testing.T, s *badgerutxo.Store, keys ...byte) {
	t.Helper()
	require.NoError(t, s.LoadAccounts(func(put func(badgerutxo.AccountRow) error) error {
		for i, k := range keys {
			if err := put(accountAt(k, uint64(i+1))); err != nil { //nolint:gosec // a small index
				return err
			}
		}
		return nil
	}))
}

// A row a block changed after the load is no longer added in its old range,
// and a rollback that restores it puts it back there, however early the
// index of that range was built.
func TestAccountsChangedInBelowTheFloorFollowsRewrites(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(11, 0x01))
	loadAccountsAtSlots(t, s, 1, 2, 3, 4, 5)

	applyAccounts(t, s, point(20, 0x02), accountAt(3, 20))
	keys, covered := changedIn(t, s, 1, 10)
	require.True(t, covered)
	require.Equal(t, []byte{1, 2, 4, 5}, keys, "account 3 was added at slot 20")

	_, err := s.Rollback(point(11, 0x01))
	require.NoError(t, err)
	keys, covered = changedIn(t, s, 1, 10)
	require.True(t, covered)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, keys, "the rollback restored account 3")

	loadAccountsAtSlots(t, s, 7, 8)
	keys, covered = changedIn(t, s, 1, 10)
	require.True(t, covered)
	require.Equal(t, []byte{7, 8}, keys, "the load replaced the accounts")
}
