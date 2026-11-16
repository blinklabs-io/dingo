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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A store marked loaded before the rows carried row IDs holds rows without
// them, so it must read as not loaded, and a reload must replace the rows
// and clear the old marker.
func TestGovernanceRowsReloadWhenTheMarkerIsFromBeforeRowIDs(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	oldRow := append([]byte{'P'}, make([]byte, 36)...)
	load := func(value string) {
		require.NoError(t, s.LoadGovernance(
			func(put func(k, v []byte) error) error {
				return put(oldRow, []byte(value))
			},
		))
	}
	load("old")
	loaded, err := s.GovernanceLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	require.NoError(t, s.UseLegacyGovernanceMarkerForTest())
	loaded, err = s.GovernanceLoaded()
	require.NoError(t, err)
	require.False(t, loaded, "rows without row IDs must be reloaded")

	load("new")
	loaded, err = s.GovernanceLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	legacy, err := s.HasLegacyGovernanceMarkerForTest()
	require.NoError(t, err)
	require.False(t, legacy)
	row, ok, err := s.Row(oldRow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("new"), row)
}

func govRow(id uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, id)
}

func govKey(tag byte, seed byte) []byte {
	return append([]byte{tag}, bytes.Repeat([]byte{seed}, 36)...)
}

// The ID sequence continues from the highest loaded ID, rises with the IDs a
// batch puts, and never falls back when a rollback removes the rows.
func TestGovernanceIDSequenceNeverFallsBack(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.LoadGovernance(
		func(put func(k, v []byte) error) error {
			for i, id := range []uint64{4, 9, 2} {
				if err := put(govKey(badgerutxo.ProposalTag, byte(i)), govRow(id)); err != nil {
					return err
				}
			}
			return put(govKey(badgerutxo.VoteTag, 1), govRow(30))
		},
	))
	ids, err := s.GovernanceNextIDs()
	require.NoError(t, err)
	require.Equal(t, badgerutxo.GovernanceIDs{Proposal: 10, Vote: 31}, ids,
		"the next IDs continue from the highest loaded ones")

	apply := func(slot uint64, ids badgerutxo.GovernanceIDs, key byte) {
		point := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		_, err := s.ApplyLedgerBatch(&point, badgerutxo.LedgerBatch{
			Rows: []badgerutxo.RowPut{{
				Slot: slot, Key: govKey(badgerutxo.ProposalTag, key),
				Value: govRow(ids.Proposal),
			}},
			GovernanceIDs: &ids,
		})
		require.NoError(t, err)
	}
	apply(5, badgerutxo.GovernanceIDs{Proposal: 12, Vote: 31}, 7)
	ids, err = s.GovernanceNextIDs()
	require.NoError(t, err)
	require.Equal(t, badgerutxo.GovernanceIDs{Proposal: 13, Vote: 32}, ids)

	apply(6, badgerutxo.GovernanceIDs{Proposal: 3}, 8)
	ids, err = s.GovernanceNextIDs()
	require.NoError(t, err)
	require.Equal(t, badgerutxo.GovernanceIDs{Proposal: 13, Vote: 32}, ids,
		"a lower ID does not lower the sequence")

	point := ocommon.NewPoint(4, bytes.Repeat([]byte{4}, 32))
	_, err = s.ApplyLedgerBatch(&point, badgerutxo.LedgerBatch{
		DeleteNoncesAfter: &point.Slot,
	})
	require.NoError(t, err)
	_, ok, err := s.Row(govKey(badgerutxo.ProposalTag, 7))
	require.NoError(t, err)
	require.False(t, ok, "the rollback removed the row")
	ids, err = s.GovernanceNextIDs()
	require.NoError(t, err)
	require.Equal(t, badgerutxo.GovernanceIDs{Proposal: 13, Vote: 32}, ids,
		"the rollback does not hand an ID out twice")
}

// A store that predates the sequence derives it from its rows.
func TestGovernanceIDSequenceIsDerivedFromRowsWithoutOne(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	point := ocommon.NewPoint(5, bytes.Repeat([]byte{5}, 32))
	_, err := s.ApplyLedgerBatch(&point, badgerutxo.LedgerBatch{
		Rows: []badgerutxo.RowPut{{
			Slot: 5, Key: govKey(badgerutxo.ProposalTag, 1), Value: govRow(6),
		}},
	})
	require.NoError(t, err)

	ids, err := s.GovernanceNextIDs()

	require.NoError(t, err)
	require.Equal(t, badgerutxo.GovernanceIDs{Proposal: 7, Vote: 1}, ids)
}
