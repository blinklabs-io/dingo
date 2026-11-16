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

func pp(era, epoch, slot uint64, tag byte) badgerutxo.PParamsRow {
	return badgerutxo.PParamsRow{
		Era: era, Epoch: epoch, AddedSlot: slot, Cbor: []byte{tag},
	}
}

func applyPParams(
	t *testing.T,
	s *badgerutxo.Store,
	tip ocommon.Point,
	rows ...badgerutxo.PParamsRow,
) {
	t.Helper()
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{PParams: rows})
	require.NoError(t, err)
}

func pparamsAt(t *testing.T, s *badgerutxo.Store, era, epoch uint64) byte {
	t.Helper()
	r, ok, err := s.PParamsAt(era, epoch)
	require.NoError(t, err)
	if !ok {
		return 0
	}
	return r.Cbor[0]
}

// PParamsAt picks as GetPParams does: within the era, the latest epoch at
// or before the one asked, and of that epoch's rows the last written.
func TestPParamsAtPicksTheLatestWrite(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyPParams(t, s, point(10, 0x10),
		pp(5, 1, 10, 'a'), pp(6, 1, 10, 'b'), pp(5, 1, 10, 'c'))
	applyPParams(t, s, point(20, 0x20), pp(5, 3, 20, 'd'))
	for _, tc := range []struct {
		era, epoch uint64
		want       byte
	}{
		{5, 0, 0}, {5, 1, 'c'}, {5, 2, 'c'}, {5, 3, 'd'}, {5, 99, 'd'},
		{6, 1, 'b'}, {6, 5, 'b'}, {7, 5, 0},
	} {
		require.Equal(t, tc.want, pparamsAt(t, s, tc.era, tc.epoch),
			"era %d epoch %d", tc.era, tc.epoch)
	}
	rows, err := s.PParamsForEra(5)
	require.NoError(t, err)
	var order []byte
	for _, r := range rows {
		order = append(order, r.Cbor[0])
	}
	require.Equal(t, []byte("acd"), order)
}

// A rollback deletes the rows added after its point, in both rollback
// paths, and later writes still order after the rows left.
func TestRollbackUndoesPParams(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		applyPParams(t, s, point(10, 0x10), pp(5, 1, 10, 'a'))
		applyPParams(t, s, point(110, 0x6e), pp(5, 1, 110, 'b'), pp(6, 2, 110, 'c'))
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
		require.Equal(t, byte('a'), pparamsAt(t, s, 5, 9), "journal=%t", viaJournal)
		require.Equal(t, byte(0), pparamsAt(t, s, 6, 9), "journal=%t", viaJournal)
		applyPParams(t, s, point(120, 0x78), pp(5, 1, 120, 'e'))
		require.Equal(t, byte('e'), pparamsAt(t, s, 5, 9), "journal=%t", viaJournal)
	}
}

func TestLoadPParamsAndBootstrapMarker(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.PParamsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, s.LoadPParams([]badgerutxo.PParamsRow{
		pp(5, 1, 10, 'a'), pp(5, 1, 10, 'b'),
	}))
	loaded, err = s.PParamsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	require.Equal(t, byte('b'), pparamsAt(t, s, 5, 1))
	applyPParams(t, s, point(20, 0x20), pp(5, 1, 20, 'c'))
	require.Equal(t, byte('c'), pparamsAt(t, s, 5, 1),
		"a write after the load orders after the loaded rows")

	bootstrap(t, s, point(5, 0x05))
	loaded, err = s.PParamsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.Equal(t, byte(0), pparamsAt(t, s, 5, 1))
}
