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

// Journaled rows are restored by a rollback, newest change first, on both
// rollback paths, including a row created and one deleted after the target;
// RowAt answers what the rollback would leave.
func TestRollbackRestoresRows(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		put := func(slot uint64, rows ...badgerutxo.RowPut) {
			for i := range rows {
				rows[i].Slot = slot
			}
			tip := point(slot, byte(slot))
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: rows})
			require.NoError(t, err)
		}
		a, b := []byte("ga"), []byte("gb")
		put(10, badgerutxo.RowPut{Key: a, Value: []byte{1}})
		put(20, badgerutxo.RowPut{Key: a, Value: []byte{2}},
			badgerutxo.RowPut{Key: b, Value: []byte{9}})
		put(30, badgerutxo.RowPut{Key: a, Delete: true})
		v, ok, err := s.RowAt(a, 15)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte{1}, v)
		if viaJournal {
			_, err := s.Rollback(point(10, 0x0a))
			require.NoError(t, err)
		} else {
			to := uint64(15)
			tip := point(15, 0x0f)
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &to})
			require.NoError(t, err)
		}
		v, ok, err = s.Row(a)
		require.NoError(t, err)
		require.True(t, ok, "journal=%t", viaJournal)
		require.Equal(t, []byte{1}, v, "journal=%t", viaJournal)
		_, ok, err = s.Row(b)
		require.NoError(t, err)
		require.False(t, ok, "journal=%t", viaJournal)
	}
}

// RowsAt lists every row under a prefix as a rollback would leave it: a row
// created after the slot is absent, one changed after it has its earlier
// value, one deleted after it is back, and rows under other prefixes are
// left out.
func TestRowsAtListsRowsAsARollbackLeavesThem(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	put := func(slot uint64, rows ...badgerutxo.RowPut) {
		for i := range rows {
			rows[i].Slot = slot
		}
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: rows})
		require.NoError(t, err)
	}
	put(10, badgerutxo.RowPut{Key: []byte("ga"), Value: []byte{1}},
		badgerutxo.RowPut{Key: []byte("gb"), Value: []byte{1}},
		badgerutxo.RowPut{Key: []byte("wa"), Value: []byte{7}})
	put(20, badgerutxo.RowPut{Key: []byte("ga"), Value: []byte{2}},
		badgerutxo.RowPut{Key: []byte("gc"), Value: []byte{2}})
	put(30, badgerutxo.RowPut{Key: []byte("gb"), Delete: true})
	list := func(slot uint64) map[string]byte {
		t.Helper()
		got := map[string]byte{}
		require.NoError(t, s.RowsAt([]byte("g"), slot, func(k, v []byte) error {
			got[string(k)] = v[0]
			return nil
		}))
		return got
	}
	require.Equal(t, map[string]byte{"ga": 2, "gc": 2}, list(30))
	require.Equal(t, map[string]byte{"ga": 2, "gb": 1, "gc": 2}, list(25))
	require.Equal(t, map[string]byte{"ga": 1, "gb": 1}, list(15))
	require.Empty(t, list(5))
}

// A store whose committee rows are loaded reports it, and a bootstrap
// clears the rows and the marker.
func TestCommitteeRowsLoadAndBootstrapClearsThem(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.CommitteeLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.Error(t, s.LoadCommittee(func(put func(k, v []byte) error) error {
		return put([]byte("ga"), []byte{1})
	}), "a key outside the committee prefixes is refused")
	require.NoError(t, s.LoadCommittee(func(put func(k, v []byte) error) error {
		return put([]byte("Nrow"), []byte{1})
	}))
	loaded, err = s.CommitteeLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	_, ok, err := s.Row([]byte("Nrow"))
	require.NoError(t, err)
	require.True(t, ok)
	bootstrap(t, s, point(2, 0x02))
	loaded, err = s.CommitteeLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	_, ok, err = s.Row([]byte("Nrow"))
	require.NoError(t, err)
	require.False(t, ok)
}

// StateReachesSlot reports where the computed rows are exact: the slots the
// state journal still holds every later change of. Pruning and a row load
// each cut it, and a slot at or past the tip is always exact.
func TestStateReachesSlotFollowsPruningAndLoads(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(100, 0x01))
	key := []byte("ga")
	for _, slot := range []uint64{110, 120, 130} {
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			Rows: []badgerutxo.RowPut{
				{Slot: slot, Key: key, Value: []byte{byte(slot)}},
			},
		})
		require.NoError(t, err)
	}
	require.ErrorIs(
		t,
		s.StateReachesSlot(99),
		badgerutxo.ErrRollbackBeyondJournal,
	)
	for _, slot := range []uint64{100, 115, 130, 500} {
		require.NoError(t, s.StateReachesSlot(slot), "slot %d", slot)
	}
	v, ok, err := s.RowAt(key, 115)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{110}, v)

	require.NoError(t, s.PruneJournal(120))
	for _, slot := range []uint64{100, 118} {
		require.ErrorIs(
			t, s.StateReachesSlot(slot), badgerutxo.ErrRollbackBeyondJournal,
			"slot %d", slot,
		)
	}
	for _, slot := range []uint64{119, 125, 130} {
		require.NoError(t, s.StateReachesSlot(slot), "slot %d", slot)
	}

	require.NoError(t, s.LoadAccounts(
		func(func(badgerutxo.AccountRow) error) error { return nil },
	))
	require.ErrorIs(
		t, s.StateReachesSlot(125), badgerutxo.ErrRollbackBeyondJournal,
	)
	require.NoError(t, s.StateReachesSlot(130))
}

// RowKeysChangedAfter names each row under a prefix the journal records a
// change to after a slot, once however often it changed, and no other.
func TestRowKeysChangedAfterNamesTheRowsChangedSince(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	put := func(slot uint64, keys ...string) {
		rows := make([]badgerutxo.RowPut, 0, len(keys))
		for _, k := range keys {
			rows = append(
				rows,
				badgerutxo.RowPut{Slot: slot, Key: []byte(k), Value: []byte{1}},
			)
		}
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: rows})
		require.NoError(t, err)
	}
	put(10, "ga", "gb")
	put(20, "gb", "gc", "wx")
	put(30, "gb")
	keys := func(prefix string, slot uint64) []string {
		got, err := s.RowKeysChangedAfter([]byte(prefix), slot)
		require.NoError(t, err)
		out := make([]string, 0, len(got))
		for _, k := range got {
			out = append(out, string(k))
		}
		return out
	}
	require.Equal(t, []string{"ga", "gb", "gc"}, keys("g", 5))
	require.Equal(t, []string{"gb", "gc"}, keys("g", 10))
	require.Equal(t, []string{"gb"}, keys("g", 20))
	require.Empty(t, keys("g", 30))
	require.Equal(t, []string{"wx"}, keys("w", 10))
}

// EdgeRows reads the first and last rows under the prefix and none outside
// it, including a row whose key is exactly the prefix's successor, which a
// reverse seek to that successor lands on before stepping into the range.
func TestEdgeRowsExcludesThePrefixSuccessor(t *testing.T) {
	t.Parallel()
	type row struct{ k, v string }
	for _, tc := range []struct {
		name        string
		rows        []row
		prefix      string
		first, last string
		found       bool
	}{
		{
			name:   "successor follows one row",
			rows:   []row{{"g\x10\x01", "a"}, {"g\x11", "z"}},
			prefix: "g\x10", first: "g\x10\x01", last: "g\x10\x01", found: true,
		},
		{
			name: "successor follows several rows",
			rows: []row{
				{"g\x10\x01", "a"}, {"g\x10\x02", "b"},
				{"g\x11", "z"}, {"g\x12", "y"},
			},
			prefix: "g\x10", first: "g\x10\x01", last: "g\x10\x02", found: true,
		},
		{
			name:   "prefix ends in 0xff",
			rows:   []row{{"g\x10\xff\x01", "a"}, {"g\x10\xff\x02", "b"}, {"g\x11", "z"}},
			prefix: "g\x10\xff", first: "g\x10\xff\x01", last: "g\x10\xff\x02", found: true,
		},
		{
			name:   "range empty, successor present",
			rows:   []row{{"g\x0f", "x"}, {"g\x11", "z"}},
			prefix: "g\x10",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := open(t, t.TempDir())
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			bootstrap(t, s, point(1, 0x01))
			puts := make([]badgerutxo.RowPut, 0, len(tc.rows))
			for _, r := range tc.rows {
				puts = append(puts, badgerutxo.RowPut{
					Key: []byte(r.k), Value: []byte(r.v), Slot: 10,
				})
			}
			tip := point(10, 0x0a)
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: puts})
			require.NoError(t, err)
			fk, fv, lk, lv, found, err := s.EdgeRows([]byte(tc.prefix))
			require.NoError(t, err)
			require.Equal(t, tc.found, found)
			if !tc.found {
				return
			}
			value := map[string]string{}
			for _, r := range tc.rows {
				value[r.k] = r.v
			}
			require.Equal(t, []byte(tc.first), fk)
			require.Equal(t, []byte(value[tc.first]), fv)
			require.Equal(t, []byte(tc.last), lk)
			require.Equal(t, []byte(value[tc.last]), lv)
		})
	}
}
