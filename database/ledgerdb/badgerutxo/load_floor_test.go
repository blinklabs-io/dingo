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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func delegationKey(slot uint64) []byte {
	k := []byte{badgerutxo.DelegationTag, 0, 28}
	k = append(k, make([]byte, 28)...)
	k = binary.BigEndian.AppendUint64(k, slot)
	return binary.BigEndian.AppendUint64(k, 0)
}

// A family loaded at the tip has no state journal entries before it, so the
// store cannot undo the loaded rows of a block before the load. A rollback to
// a point below the load is refused whatever the UTxO journal still holds, and
// one to the load's tip or after it works.
func TestRollbackBelowAFamilyLoadIsBeyondTheJournal(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(10, 1), out(1, 0))
	_, err := s.Apply(point(20, 2), []badgerutxo.Output{out(2, 0)}, nil)
	require.NoError(t, err)
	loaded := delegationKey(20)
	require.NoError(t, s.LoadDelegations(func(put func(k, v []byte) error) error {
		return put(loaded, make([]byte, 8+28))
	}))

	_, err = s.Rollback(point(10, 1))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	_, err = s.RollbackPreview(point(10, 1))
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	has, err := s.HasJournalPoint(point(10, 1))
	require.NoError(t, err)
	require.False(t, has)
	_, _, err = s.PointBefore(15)
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
	floor, ok, err := s.JournalFloor()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(20), floor.Slot)
	requireTip(t, s, point(20, 2))

	_, err = s.Apply(point(30, 3), []badgerutxo.Output{out(3, 0)}, nil)
	require.NoError(t, err)
	has, err = s.HasJournalPoint(point(20, 2))
	require.NoError(t, err)
	require.True(t, has)
	_, err = s.Rollback(point(20, 2))
	require.NoError(t, err)
	requireTip(t, s, point(20, 2))
	_, present, err := s.Row(loaded)
	require.NoError(t, err)
	require.True(t, present, "a row the load wrote at the tip is kept")
}

// Loading the accounts drops their own journal entries, whose history the
// load does not have, and no other family's.
func TestLoadAccountsKeepsOtherFamiliesJournal(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(10, 1), out(1, 0))
	drep := append([]byte{badgerutxo.DRepTag, 0}, make([]byte, 28)...)
	p := point(20, 2)
	_, err := s.ApplyLedgerBatch(&p, badgerutxo.LedgerBatch{
		Changes: []badgerutxo.Change{{
			Point: p, Added: []badgerutxo.Output{out(2, 0)},
		}},
		Rows: []badgerutxo.RowPut{{Slot: 20, Key: drep, Value: []byte{1}}},
	})
	require.NoError(t, err)
	require.NoError(t, s.LoadAccounts(
		func(func(badgerutxo.AccountRow) error) error { return nil },
	))
	_, present, err := s.RowAt(drep, 10)
	require.NoError(t, err)
	require.False(t, present, "the DRep's journal entry survived the load")
}
