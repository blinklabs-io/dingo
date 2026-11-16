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

func marker(t *testing.T, s *badgerutxo.Store, name string) (string, bool) {
	t.Helper()
	v, ok, err := s.Marker(name)
	require.NoError(t, err)
	return v, ok
}

func applyMarkers(
	t *testing.T,
	s *badgerutxo.Store,
	tip ocommon.Point,
	writes ...badgerutxo.MarkerWrite,
) {
	t.Helper()
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Markers: writes})
	require.NoError(t, err)
}

// Markers are set, replaced and deleted by name, are loaded only once
// LoadMarkers ran, and refuse a name the store does not keep.
func TestMarkersAreWrittenAndLoaded(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.MarkersLoaded()
	require.NoError(t, err)
	require.False(t, loaded)

	require.NoError(t, s.LoadMarkers(map[string]string{
		badgerutxo.SyntheticV2Marker: "true",
	}))
	loaded, err = s.MarkersLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	v, ok := marker(t, s, badgerutxo.SyntheticV2Marker)
	require.True(t, ok)
	require.Equal(t, "true", v)
	_, ok = marker(t, s, badgerutxo.SyntheticV2ClearedMarker)
	require.False(t, ok)

	applyMarkers(
		t,
		s,
		point(10, 0x10),
		badgerutxo.MarkerWrite{
			Name:  badgerutxo.SyntheticV2Marker,
			Value: "false",
		},
		badgerutxo.MarkerWrite{
			Name:  badgerutxo.SyntheticV2ClearedMarker,
			Value: "3",
		},
	)
	v, _ = marker(t, s, badgerutxo.SyntheticV2Marker)
	require.Equal(t, "false", v)
	v, _ = marker(t, s, badgerutxo.SyntheticV2ClearedMarker)
	require.Equal(t, "3", v)

	applyMarkers(t, s, point(20, 0x20), badgerutxo.MarkerWrite{
		Name: badgerutxo.SyntheticV2Marker, Delete: true,
	})
	_, ok = marker(t, s, badgerutxo.SyntheticV2Marker)
	require.False(t, ok)

	require.NoError(t, s.LoadMarkers(nil))
	_, ok = marker(t, s, badgerutxo.SyntheticV2ClearedMarker)
	require.False(t, ok, "a load replaces the markers")

	_, _, err = s.Marker("other")
	require.Error(t, err)
	tip := point(30, 0x30)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Markers: []badgerutxo.MarkerWrite{{Name: "other", Value: "1"}},
	})
	require.Error(t, err)
}

// A rollback that leaves an epoch before the one real data was confirmed at
// drops both synthetic markers, in both rollback paths; one that leaves that
// epoch, or a later one, keeps them.
func TestRollbackRevertsTheSyntheticMarkersAcrossTheConfirmingEpoch(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		rollbackTo uint64
		// heldSlot and heldHash are the tip the journal restores: the last
		// point the set held at or before rollbackTo.
		heldSlot uint64
		heldHash byte
		kept     bool
	}{
		{"before the confirming epoch", 150, 110, 0x6e, false},
		{"at the confirming epoch", 250, 220, 0xdc, true},
		{"after the confirming epoch", 310, 310, 0x36, true},
	} {
		for _, viaJournal := range []bool{true, false} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				s := open(t, t.TempDir())
				t.Cleanup(func() { require.NoError(t, s.Close()) })
				bootstrap(t, s, point(1, 0x01))
				applyEpochState(t, s, point(10, 0x10),
					[]badgerutxo.EpochRow{epochRow(0, 0)})
				applyEpochState(t, s, point(110, 0x6e),
					[]badgerutxo.EpochRow{epochRow(1, 100)})
				applyEpochState(t, s, point(210, 0xd2),
					[]badgerutxo.EpochRow{epochRow(2, 200)})
				applyMarkers(t, s, point(220, 0xdc),
					badgerutxo.MarkerWrite{
						Name: badgerutxo.SyntheticV2Marker, Value: "false",
					},
					badgerutxo.MarkerWrite{
						Name: badgerutxo.SyntheticV2ClearedMarker, Value: "2",
					},
				)
				applyEpochState(t, s, point(310, 0x36),
					[]badgerutxo.EpochRow{epochRow(3, 300)})

				if viaJournal {
					_, err := s.Rollback(point(tc.heldSlot, tc.heldHash))
					require.NoError(t, err)
				} else {
					to := tc.rollbackTo
					tip := point(tc.rollbackTo, 0x99)
					_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
						DeleteNoncesAfter: &to,
					})
					require.NoError(t, err)
				}
				_, hasBool := marker(t, s, badgerutxo.SyntheticV2Marker)
				_, hasEpoch := marker(t, s, badgerutxo.SyntheticV2ClearedMarker)
				require.Equal(t, tc.kept, hasBool)
				require.Equal(t, tc.kept, hasEpoch)
			})
		}
	}
}
