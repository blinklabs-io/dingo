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

func applyNetworkState(
	t *testing.T,
	s *badgerutxo.Store,
	tip ocommon.Point,
	states []badgerutxo.NetworkStateRow,
	donations ...badgerutxo.DonationRow,
) {
	t.Helper()
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		NetworkStates: states, Donations: donations,
	})
	require.NoError(t, err)
}

func netState(slot, treasury uint64) badgerutxo.NetworkStateRow {
	return badgerutxo.NetworkStateRow{
		Slot: slot, Treasury: treasury, Reserves: 1000 - treasury,
	}
}

// NetworkStateAt answers as GetNetworkStateAsOfSlot does: the newest row at
// or before the slot, and a write at a slot already held replaces it.
func TestNetworkStateAtSlot(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	_, ok, err := s.NetworkStateAt(^uint64(0))
	require.NoError(t, err)
	require.False(t, ok)
	applyNetworkState(t, s, point(10, 0x10),
		[]badgerutxo.NetworkStateRow{netState(0, 1), netState(10, 2)})
	applyNetworkState(t, s, point(20, 0x20),
		[]badgerutxo.NetworkStateRow{netState(20, 3), netState(20, 4)})
	for _, tc := range []struct{ at, slot, treasury uint64 }{
		{0, 0, 1}, {9, 0, 1}, {10, 10, 2}, {19, 10, 2}, {20, 20, 4},
		{^uint64(0), 20, 4},
	} {
		got, ok, err := s.NetworkStateAt(tc.at)
		require.NoError(t, err)
		require.True(t, ok, "at %d", tc.at)
		require.Equal(t, netState(tc.slot, tc.treasury), got, "at %d", tc.at)
	}
}

// A rollback deletes the network states and donations after its point, in
// both rollback paths.
func TestRollbackUndoesNetworkState(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		applyNetworkState(t, s, point(10, 0x10),
			[]badgerutxo.NetworkStateRow{netState(10, 1)},
			badgerutxo.DonationRow{Slot: 10, Epoch: 0, Amount: 5})
		applyNetworkState(t, s, point(110, 0x6e),
			[]badgerutxo.NetworkStateRow{netState(110, 2)},
			badgerutxo.DonationRow{Slot: 110, Epoch: 1, Amount: 7})
		if viaJournal {
			_, err := s.Rollback(point(10, 0x10))
			require.NoError(t, err)
		} else {
			to := uint64(50)
			tip := point(50, 0x32)
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
				DeleteNoncesAfter: &to,
			})
			require.NoError(t, err)
		}
		states, err := s.NetworkStates()
		require.NoError(t, err)
		require.Equal(t, []badgerutxo.NetworkStateRow{netState(10, 1)}, states,
			"journal=%t", viaJournal)
		donations, err := s.Donations()
		require.NoError(t, err)
		require.Equal(t,
			[]badgerutxo.DonationRow{{Slot: 10, Epoch: 0, Amount: 5}},
			donations, "journal=%t", viaJournal)
	}
}

func TestLoadNetworkStateAndBootstrapMarker(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.NetworkStateLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, s.LoadNetworkState(
		[]badgerutxo.NetworkStateRow{netState(0, 1)},
		[]badgerutxo.DonationRow{{Slot: 3, Epoch: 0, Amount: 9}},
	))
	loaded, err = s.NetworkStateLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	donations, err := s.Donations()
	require.NoError(t, err)
	require.Len(t, donations, 1)

	bootstrap(t, s, point(5, 0x05))
	loaded, err = s.NetworkStateLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	states, err := s.NetworkStates()
	require.NoError(t, err)
	require.Empty(t, states)
	donations, err = s.Donations()
	require.NoError(t, err)
	require.Empty(t, donations)
}
