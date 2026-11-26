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

package utxoset_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/utxoset"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func loadSlotThree(target utxoset.ReimportTarget) (ocommon.Point, error) {
	point := slotPoint(3)
	err := target.Set.LoadUtxoSet(point, func(put func([]byte, uint32, []byte) error) error {
		for slot := range uint64(3) {
			if err := put(bytes.Repeat([]byte{byte(slot + 1)}, 32), 0, []byte{0x80}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return point, err
	}
	return point, target.Set.FinishImport()
}

func openScratchTestSet(t *testing.T) (*utxoset.Set, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "set")
	set, err := utxoset.Open(utxoset.Config{
		Dir: dir, CheckpointDir: filepath.Join(t.TempDir(), "checkpoints"),
		CheckpointKeep: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	applyRange(t, set, 1, 8)
	return set, dir
}

// TestReimportLoadsACopyAndLeavesTheLiveSetUntouchedUntilCommit requires a
// re-import that fails before or at the swap to leave the live set as it was,
// and one that commits to replace it.
func TestReimportLoadsACopyAndLeavesTheLiveSetUntouchedUntilCommit(t *testing.T) {
	t.Parallel()
	boom := errors.New("the import failed")
	for _, tc := range []struct {
		name    string
		run     utxoset.ScratchReimportFunc
		wantErr bool
		wantTip uint64
	}{
		{
			name: "fails after loading the copy",
			run: func(
				_ context.Context, _ ocommon.Point, target utxoset.ReimportTarget,
			) (ocommon.Point, error) {
				if _, err := loadSlotThree(target); err != nil {
					return ocommon.Point{}, err
				}
				return ocommon.Point{}, boom
			},
			wantErr: true, wantTip: 8,
		},
		{
			name: "commits a point the copy is not at",
			run: func(
				ctx context.Context, _ ocommon.Point, target utxoset.ReimportTarget,
			) (ocommon.Point, error) {
				if _, err := loadSlotThree(target); err != nil {
					return ocommon.Point{}, err
				}
				return ocommon.Point{}, target.Commit(ctx, slotPoint(5))
			},
			wantErr: true, wantTip: 8,
		},
		{
			name: "commits the loaded state",
			run: func(
				ctx context.Context, _ ocommon.Point, target utxoset.ReimportTarget,
			) (ocommon.Point, error) {
				point, err := loadSlotThree(target)
				if err != nil {
					return ocommon.Point{}, err
				}
				return point, target.Commit(ctx, point)
			},
			wantTip: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set, dir := openScratchTestSet(t)
			set.SetScratchReimporter(tc.run)

			_, err := set.Reimport(t.Context(), slotPoint(8))
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			tip, ok, err := set.Tip()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.wantTip, tip.Slot)
			n, err := set.Count()
			require.NoError(t, err)
			require.Equal(t, tc.wantTip, n)
			_, statErr := os.Stat(dir + "-reimport")
			require.ErrorIs(t, statErr, os.ErrNotExist, "the copy must be removed")
		})
	}
}

// TestReimportSwapSurvivesCheckpointRotation requires the imported set's
// checkpoint to outlive the pruning that keeps the newest checkpoints by slot,
// as the point a Mithril re-import loads is older than the live set's.
func TestReimportSwapSurvivesCheckpointRotation(t *testing.T) {
	t.Parallel()
	set, _ := openScratchTestSet(t)
	_, err := set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	applyRange(t, set, 9, 10)
	_, err = set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	set.SetScratchReimporter(func(
		ctx context.Context, _ ocommon.Point, target utxoset.ReimportTarget,
	) (ocommon.Point, error) {
		point, err := loadSlotThree(target)
		if err != nil {
			return ocommon.Point{}, err
		}
		return point, target.Commit(ctx, point)
	})

	_, err = set.Reimport(t.Context(), slotPoint(10))
	require.NoError(t, err)

	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(3), tip.Slot)
	cps, err := set.Checkpoints()
	require.NoError(t, err)
	require.NotEmpty(t, cps)
	for _, cp := range cps {
		require.LessOrEqual(t, cp.Slot, uint64(3), "a checkpoint of the replaced state survives")
	}
}

// TestReimportRefusedBeforeImportWhenDiskIsShort requires the space check to
// run before the import loads anything, as the import writes the metadata
// store the live set can no longer be rolled back against.
func TestReimportRefusedBeforeImportWhenDiskIsShort(t *testing.T) {
	t.Parallel()
	set, _ := openScratchTestSet(t)
	set.SetAvailableDiskForTest(func(string) (uint64, error) { return 0, nil })
	called := false
	set.SetScratchReimporter(func(
		context.Context, ocommon.Point, utxoset.ReimportTarget,
	) (ocommon.Point, error) {
		called = true
		return ocommon.Point{}, nil
	})

	_, err := set.Reimport(t.Context(), slotPoint(8))

	require.ErrorIs(t, err, utxoset.ErrCheckpointNoSpace)
	require.False(t, called, "the import ran before the space check")
}

// TestReimportRefusesAScratchSetItCannotCopyTheLiveSetInto requires a
// re-import whose copy of the live set fails to run no import, in place of
// loading into an empty set that silently drops the history the live set
// holds.
func TestReimportRefusesAScratchSetItCannotCopyTheLiveSetInto(t *testing.T) {
	t.Parallel()
	set, _ := openScratchTestSet(t)
	called := false
	set.SetScratchReimporter(func(
		context.Context, ocommon.Point, utxoset.ReimportTarget,
	) (ocommon.Point, error) {
		called = true
		return ocommon.Point{}, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := set.Reimport(ctx, slotPoint(8))

	require.Error(t, err)
	require.False(t, called, "the import ran against an empty copy")
}

// TestReimportSwapExcludesLiveCheckpointWrites requires the swap to hold the
// live set's checkpoint writers out from before it writes the imported set's
// checkpoint until it has restored it, so a live write cannot prune the
// older checkpoint it is about to read.
func TestReimportSwapExcludesLiveCheckpointWrites(t *testing.T) {
	t.Parallel()
	set, _ := openScratchTestSet(t)
	busy := false
	set.SetAfterSwapCheckpointForTest(func() {
		busy = set.CheckpointWriteBusyForTest()
	})
	set.SetScratchReimporter(func(
		ctx context.Context, _ ocommon.Point, target utxoset.ReimportTarget,
	) (ocommon.Point, error) {
		point, err := loadSlotThree(target)
		if err != nil {
			return ocommon.Point{}, err
		}
		return point, target.Commit(ctx, point)
	})

	_, err := set.Reimport(t.Context(), slotPoint(8))

	require.NoError(t, err)
	require.True(t, busy, "a live checkpoint write could run between the imported set's checkpoint and its restore")
	require.False(t, set.CheckpointWriteBusyForTest(), "the swap releases the writers")
}
