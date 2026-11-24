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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

var errInterrupted = errors.New("interrupted")

// retiredLayout is what WriteRetiredLayoutForTest wrote, for the checks.
type retiredLayout struct {
	record      badgerutxo.RewardRecord
	snapshot    badgerutxo.SnapshotRow
	baselineKey []byte
	baseline    []byte
	slot        uint64
}

func writeRetiredLayout(t *testing.T, dir string) retiredLayout {
	t.Helper()
	l := retiredLayout{
		record: badgerutxo.RewardRecord{
			Slot: 10, Source: []byte{1}, Tag: 1, Key: []byte{9}, Amount: 42,
		},
		snapshot: badgerutxo.SnapshotRow{
			Epoch: 4, Type: "mark", Pool: []byte{7, 7}, Value: []byte{0xa1, 1, 2},
		},
		baselineKey: append([]byte{badgerutxo.ImportBaselineTag, 0}, 5, 5),
		slot:        12,
	}
	// active, added slot 3, deposit recorded: 2 ADA; then a recorded
	// delegation tail.
	l.baseline = []byte{1}
	l.baseline = binary.BigEndian.AppendUint64(l.baseline, 3)
	l.baseline = append(l.baseline, 1)
	l.baseline = binary.BigEndian.AppendUint64(l.baseline, 2_000_000)
	l.baseline = append(l.baseline, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 8, 8, 0)
	s := open(t, dir)
	bootstrap(t, s, point(1, 0x01))
	require.NoError(t, s.WriteRetiredLayoutForTest(
		l.record, l.snapshot, l.baselineKey, l.baseline, l.slot,
	))
	require.NoError(t, s.Close())
	return l
}

func requireCurrentLayout(t *testing.T, s *badgerutxo.Store, l retiredLayout) {
	t.Helper()
	retired, err := s.RetiredKeysForTest()
	require.NoError(t, err)
	require.Empty(t, retired)

	rk, err := badgerutxo.RewardRecordKeyForTest(l.record)
	require.NoError(t, err)
	v, ok, err := s.RawValueForTest(rk)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, append(binary.BigEndian.AppendUint64(nil, 42), 0), v,
		"the record keeps its amount and flag and loses its number")
	applied, err := s.HasRewardRecord(l.record)
	require.NoError(t, err)
	require.True(t, applied)

	v, ok, err = s.RawValueForTest(badgerutxo.SnapshotKeyForTest(l.snapshot))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, l.snapshot.Value, v, "the snapshot row is its encoded value alone")

	v, ok, err = s.RawValueForTest(l.baselineKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, l.baseline[:18], v)
	v, ok, err = s.RawValueForTest(
		badgerutxo.StateJournalKeyForTest(l.slot, l.baselineKey),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, append([]byte{1}, l.baseline[:18]...), v)
}

func TestLayoutMigrationRemovesRetiredKeys(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	l := writeRetiredLayout(t, dir)

	s := open(t, dir)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	requireCurrentLayout(t, s, l)
}

func TestLayoutMigrationResumesAfterInterruption(t *testing.T) {
	t.Parallel()
	steps := []string{
		badgerutxo.MigrationStepMarkers, badgerutxo.MigrationStepFamilies,
		badgerutxo.MigrationStepRecords, badgerutxo.MigrationStepSnapshots,
		badgerutxo.MigrationStepBaselines, badgerutxo.MigrationStepJournal,
		badgerutxo.MigrationStepWindow,
	}
	for _, stop := range steps {
		t.Run(stop, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			l := writeRetiredLayout(t, dir)

			_, err := badgerutxo.Open(dir, badgerutxo.WithMigrationHookForTest(
				func(step string) error {
					if step == stop {
						return errInterrupted
					}
					return nil
				},
			))
			require.ErrorIs(t, err, errInterrupted)

			s := open(t, dir)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			requireCurrentLayout(t, s, l)
		})
	}
}

// A store that holds the current layout is not migrated again.
func TestLayoutMigrationRunsOnce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Close())
	_, err := badgerutxo.Open(dir, badgerutxo.WithMigrationHookForTest(
		func(string) error { return errInterrupted },
	))
	require.NoError(t, err)
}

func epochRowKey(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{badgerutxo.EpochTag}, epoch)
}

func TestLayoutMigrationDropsRowsBeyondTheWindow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir)
	bootstrap(t, s, point(1, 0x01))
	const newest = 40
	for e := uint64(0); e <= newest; e++ {
		require.NoError(t, s.PutRawForTest(epochRowKey(e), make([]byte, 16)))
		for _, tag := range []byte{
			badgerutxo.RewardStakeInputTag, badgerutxo.RewardDerivedTag,
			badgerutxo.RewardCursorTag,
		} {
			k := append(binary.BigEndian.AppendUint64([]byte{tag}, e), 1)
			require.NoError(t, s.PutRawForTest(k, []byte{1}))
		}
		sk := badgerutxo.SnapshotKeyForTest(badgerutxo.SnapshotRow{
			Epoch: e, Type: "mark", Pool: []byte{1},
		})
		require.NoError(t, s.PutRawForTest(sk, []byte{0xa0}))
	}
	// The layout mark is absent as in a store from before the migration.
	require.NoError(t, s.WriteRetiredLayoutForTest(
		badgerutxo.RewardRecord{Slot: 10, Source: []byte{1}, Key: []byte{9}},
		badgerutxo.SnapshotRow{Epoch: 100, Type: "mark", Pool: []byte{2}, Value: []byte{0xa0}},
		[]byte{badgerutxo.ImportBaselineTag, 0, 5}, make([]byte, 18), 12,
	))
	require.NoError(t, s.Close())

	s, err := badgerutxo.Open(dir, badgerutxo.WithRetention(badgerutxo.Retention{
		RewardEpochs: 3, SnapshotEpochs: 10,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	held := func(tag byte, e uint64) bool {
		ok, err := s.HasKeyForTest(append(binary.BigEndian.AppendUint64([]byte{tag}, e), 1))
		require.NoError(t, err)
		return ok
	}
	snapshotHeld := func(e uint64) bool {
		ok, err := s.HasKeyForTest(badgerutxo.SnapshotKeyForTest(badgerutxo.SnapshotRow{
			Epoch: e, Type: "mark", Pool: []byte{1},
		}))
		require.NoError(t, err)
		return ok
	}
	for e := uint64(0); e <= newest; e++ {
		for _, tag := range []byte{
			badgerutxo.RewardStakeInputTag, badgerutxo.RewardDerivedTag,
			badgerutxo.RewardCursorTag,
		} {
			require.Equal(t, e >= newest-3, held(tag, e), "tag %c epoch %d", tag, e)
		}
		require.Equal(t, e >= newest-10, snapshotHeld(e), "snapshot epoch %d", e)
	}
	require.True(t, func() bool {
		ok, err := s.HasKeyForTest(epochRowKey(0))
		require.NoError(t, err)
		return ok
	}(), "epoch rows are not pruned")
}
