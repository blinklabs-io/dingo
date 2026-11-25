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

func epochRow(epoch, start uint64) badgerutxo.EpochRow {
	return badgerutxo.EpochRow{
		Epoch: epoch, StartSlot: start, Value: []byte{byte(epoch)},
	}
}

func snap(epoch uint64, typ string, pool byte) badgerutxo.SnapshotOp {
	return badgerutxo.SnapshotOp{
		Kind: badgerutxo.SnapshotPut,
		Row: badgerutxo.SnapshotRow{
			Epoch: epoch, Type: typ, Pool: []byte{pool}, Value: []byte{pool},
		},
	}
}

func applyEpochState(
	t *testing.T,
	s *badgerutxo.Store,
	tip ocommon.Point,
	epochs []badgerutxo.EpochRow,
	ops ...badgerutxo.SnapshotOp,
) {
	t.Helper()
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Epochs: epochs, SnapshotOps: ops,
	})
	require.NoError(t, err)
}

func snapshotEpochs(t *testing.T, s *badgerutxo.Store, epochs ...uint64) []string {
	t.Helper()
	var out []string
	for _, e := range epochs {
		rows, err := s.Snapshots(e, "", 0)
		require.NoError(t, err)
		for _, r := range rows {
			out = append(out, string(rune('0'+r.Epoch))+r.Type+string(r.Pool))
		}
	}
	return out
}

func TestSnapshotOpsApplyInOrder(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyEpochState(t, s, point(10, 0x10), nil,
		snap(1, "mark", 'a'), snap(1, "mark", 'b'), snap(1, "set", 'a'),
		snap(2, "mark", 'a'),
	)
	applyEpochState(t, s, point(20, 0x20), nil,
		badgerutxo.SnapshotOp{
			Kind: badgerutxo.SnapshotDeleteEpochType,
			Row:  badgerutxo.SnapshotRow{Epoch: 1, Type: "mark"},
		},
		snap(1, "mark", 'c'),
	)
	require.Equal(t, []string{"1seta", "1markc", "2marka"},
		snapshotEpochs(t, s, 1, 2))
	applyEpochState(t, s, point(30, 0x30), nil,
		badgerutxo.SnapshotOp{
			Kind: badgerutxo.SnapshotDeleteBefore,
			Row:  badgerutxo.SnapshotRow{Epoch: 2},
		},
	)
	require.Equal(t, []string{"2marka"}, snapshotEpochs(t, s, 1, 2))
}

// A rollback deletes the epochs that start after its point and the
// snapshots of epochs after the newest epoch left, in both rollback paths.
func TestRollbackUndoesEpochState(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		applyEpochState(t, s, point(10, 0x10),
			[]badgerutxo.EpochRow{epochRow(0, 0)}, snap(0, "mark", 'a'))
		applyEpochState(t, s, point(110, 0x6e),
			[]badgerutxo.EpochRow{epochRow(1, 100)}, snap(1, "mark", 'a'))
		applyEpochState(t, s, point(210, 0xd2),
			[]badgerutxo.EpochRow{epochRow(2, 200)}, snap(2, "mark", 'a'))

		if viaJournal {
			// The journal rolls back only to a point the set held.
			_, err := s.Rollback(point(110, 0x6e))
			require.NoError(t, err)
		} else {
			to := uint64(150)
			tip := point(150, 0x96)
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
				DeleteNoncesAfter: &to,
			})
			require.NoError(t, err)
		}
		epochs, err := s.Epochs()
		require.NoError(t, err)
		require.Len(t, epochs, 2, "journal=%t", viaJournal)
		require.Equal(t, uint64(1), epochs[1].Epoch)
		require.Equal(t, []string{"0marka", "1marka"},
			snapshotEpochs(t, s, 0, 1, 2), "journal=%t", viaJournal)
	}
}

func TestLoadEpochStateAndBootstrapMarker(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.EpochStateLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, s.LoadEpochState(
		[]badgerutxo.EpochRow{epochRow(3, 300)},
		[]badgerutxo.SnapshotRow{snap(3, "go", 'z').Row},
		nil,
	))
	loaded, err = s.EpochStateLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	require.Equal(t, []string{"3goz"}, snapshotEpochs(t, s, 3))

	bootstrap(t, s, point(5, 0x05))
	loaded, err = s.EpochStateLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	epochs, err := s.Epochs()
	require.NoError(t, err)
	require.Empty(t, epochs)
}

// A rollback to before a mid-epoch era switch gives the switched epoch's row
// its source era back and drops the record, in both rollback paths; one that
// keeps the switch's block leaves both.
func TestRollbackRevertsMidEpochEraSwitch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		viaJournal bool
		to         uint64
		wantEra    uint64
	}{
		// The journal rolls back only to points it recorded.
		{"journal before switch", true, 110, 4},
		{"batch before switch", false, 150, 4},
		{"journal at switch", true, 160, 5},
	} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		row := epochRow(1, 100)
		row.Era = 4
		applyEpochState(t, s, point(110, 0x6e), []badgerutxo.EpochRow{row})
		row.Era = 5
		tip := point(160, 0xa0)
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			Epochs: []badgerutxo.EpochRow{row},
			EraSwitch: &badgerutxo.EraSwitch{
				Epoch: 1, Slot: 160, FromEra: 4, ToEra: 5,
			},
		})
		require.NoError(t, err)
		applyEpochState(t, s, point(180, 0xb4), nil)

		rollbackTo := func(slot uint64) {
			p := point(slot, byte(slot))
			if tc.viaJournal {
				_, err := s.Rollback(p)
				require.NoError(t, err)
				return
			}
			_, err := s.ApplyLedgerBatch(&p, badgerutxo.LedgerBatch{
				DeleteNoncesAfter: &slot,
			})
			require.NoError(t, err)
		}
		rollbackTo(tc.to)
		epochs, err := s.Epochs()
		require.NoError(t, err)
		require.Len(t, epochs, 1, tc.name)
		require.Equal(t, tc.wantEra, epochs[0].Era, tc.name)
		require.Equal(t, []byte{1}, epochs[0].Value, tc.name)

		// A second rollback below the switch reverts only if the record
		// survived the first.
		rollbackTo(110)
		epochs, err = s.Epochs()
		require.NoError(t, err)
		require.Equal(t, uint64(4), epochs[0].Era, tc.name)
	}
}

func summaryEpochs(t *testing.T, s *badgerutxo.Store, epochs ...uint64) []string {
	t.Helper()
	var out []string
	for _, e := range epochs {
		r, ok, err := s.Summary(e)
		require.NoError(t, err)
		if ok {
			out = append(out, string(rune('0'+e))+string(r.Value))
		}
	}
	return out
}

// A rollback keeps the summaries of the epochs that survive it, whichever
// path undoes it, and drops those of later epochs.
func TestRollbackUndoesEpochSummaries(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		for i, start := range []uint64{0, 100, 200} {
			epoch := uint64(i)
			tip := point(start+10, byte(start+10))
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
				Epochs: []badgerutxo.EpochRow{epochRow(epoch, start)},
				Summaries: []badgerutxo.SummaryRow{
					{Epoch: epoch, Value: []byte{'a' + byte(epoch)}},
				},
			})
			require.NoError(t, err)
		}
		if viaJournal {
			// The journal rolls back only to a point the set held.
			_, err := s.Rollback(point(110, 0x6e))
			require.NoError(t, err)
		} else {
			to := uint64(150)
			tip := point(150, 0x96)
			_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
				DeleteNoncesAfter: &to,
			})
			require.NoError(t, err)
		}
		require.Equal(t, []string{"0a", "1b"}, summaryEpochs(t, s, 0, 1, 2),
			"journal=%t", viaJournal)
	}
}

func TestLoadSummariesAndBootstrapMarker(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.SummariesLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, s.LoadSummaries([]badgerutxo.SummaryRow{
		{Epoch: 3, Value: []byte("x")},
	}))
	loaded, err = s.SummariesLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	require.Equal(t, []string{"3x"}, summaryEpochs(t, s, 3))

	bootstrap(t, s, point(5, 0x05))
	loaded, err = s.SummariesLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.Empty(t, summaryEpochs(t, s, 3))
}

// EachSnapshot visits the rows of every epoch and type in key order.
func TestEachSnapshotVisitsEveryEpochAndType(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	applyEpochState(t, s, point(10, 0x10), nil,
		snap(2, "mark", 'a'), snap(1, "set", 'b'), snap(1, "mark", 'a'),
	)
	var got []string
	require.NoError(t, s.EachSnapshot(func(r badgerutxo.SnapshotRow) error {
		got = append(got, string(rune('0'+r.Epoch))+r.Type+string(r.Pool))
		return nil
	}))
	require.Equal(t, []string{"1setb", "1marka", "2marka"}, got)
}
