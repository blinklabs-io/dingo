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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// applyRange applies one output per slot in [from, to], each its own commit.
func applyRange(t *testing.T, set *utxoset.Set, from, to uint64) {
	t.Helper()
	for slot := from; slot <= to; slot++ {
		tip := slotPoint(slot)
		require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{{
			Point: tip,
			Added: []ledger.UtxoOutput{{
				Ref: models.UtxoId{
					Hash: bytes.Repeat([]byte{byte(slot)}, 32),
				},
				Cbor: []byte{0x80},
			}},
		}}))
	}
}

func openCheckpointSet(t *testing.T, cfg utxoset.Config) *utxoset.Set {
	t.Helper()
	cfg.CheckpointDir = filepath.Join(t.TempDir(), "checkpoints")
	cfg.CheckpointKeep = 2
	return openRecoverySet(t, cfg)
}

// TestWriteCheckpointKeepsTheNewestAndRefusesAFailedSet requires a rolling
// pair of checkpoints, counted in CheckpointBytes, and no checkpoint of a set
// that carries a failure mark.
func TestWriteCheckpointKeepsTheNewestAndRefusesAFailedSet(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	for _, slot := range []uint64{2, 3, 4} {
		applyRange(t, set, slot, slot)
		cp, err := set.WriteCheckpoint(t.Context())
		require.NoError(t, err)
		require.Equal(t, slot, cp.Slot)
	}
	cps, err := set.Checkpoints()
	require.NoError(t, err)
	require.Len(t, cps, 2)
	require.Equal(t, uint64(4), cps[0].Slot)
	require.Equal(t, uint64(3), cps[1].Slot)
	var want int64
	for _, cp := range cps {
		info, err := os.Stat(cp.Path)
		require.NoError(t, err)
		require.Equal(t, cp.Bytes, info.Size())
		want += info.Size()
	}
	got, err := set.CheckpointBytes()
	require.NoError(t, err)
	require.Greater(t, got, want, "the descriptions are counted with the backups")

	require.NoError(t, set.MarkUtxoSetFailed(4))
	_, err = set.WriteCheckpoint(t.Context())
	require.ErrorContains(t, err, "failure mark")
}

func TestWriteCheckpointNeedsADirectory(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{Only: true})
	applySlots(t, set, 2)
	_, err := set.WriteCheckpoint(t.Context())
	require.ErrorIs(t, err, utxoset.ErrNoCheckpointDir)
}

// recoveryCase is one shape of the failures the journal walk cannot help
// with. setup damages a set at slot 8 whose checkpoint is at slot 4 (when one
// was written) and returns the ledger tip the set is reconciled with.
type recoveryCase struct {
	name  string
	setup func(t *testing.T, set *utxoset.Set) ocommon.Point
}

var recoveryCases = []recoveryCase{
	{
		// The ledger cannot roll back to the set's tip or any point the
		// pruned journal holds, as when the set trails a Mithril trust
		// boundary or the consumed-output prune floor.
		name: "set behind the trust boundary or prune floor",
		setup: func(t *testing.T, set *utxoset.Set) ocommon.Point {
			require.NoError(t, set.PruneHistory(8))
			return slotPoint(10)
		},
	},
	{
		name: "failed commit older than the journal floor",
		setup: func(t *testing.T, set *utxoset.Set) ocommon.Point {
			require.NoError(t, set.PruneHistory(7))
			require.NoError(t, set.MarkUtxoSetFailed(2))
			return slotPoint(8)
		},
	},
	{
		name: "ledger tip below the journal floor",
		setup: func(t *testing.T, set *utxoset.Set) ocommon.Point {
			require.NoError(t, set.PruneHistory(7))
			return slotPoint(5)
		},
	},
}

// resumeOnlyAt accepts a point only when it is the one named, as a ledger that
// can roll back to a checkpoint and to nothing the set's own journal holds.
func resumeOnlyAt(
	slot uint64,
	asked *[]uint64,
) func(context.Context, ocommon.Point) (bool, error) {
	return func(_ context.Context, p ocommon.Point) (bool, error) {
		*asked = append(*asked, p.Slot)
		return p.Slot == slot && bytes.Equal(p.Hash, slotPoint(slot).Hash), nil
	}
}

func requireSetAt(t *testing.T, set *utxoset.Set, slot uint64) {
	t.Helper()
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, slot, tip.Slot)
	n, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, slot, n)
	_, marked, err := set.FailedAt()
	require.NoError(t, err)
	require.False(t, marked, "the failure mark must be gone")
}

// TestReconcileRestoresACheckpointWhenTheJournalCannotHelp requires the newest
// checkpoint at or below the ledger tip to be restored, the ledger to be rolled
// back to its point, and no resync-style error, in each case the journal walk
// leaves with an error.
func TestReconcileRestoresACheckpointWhenTheJournalCannotHelp(t *testing.T) {
	t.Parallel()
	for _, tc := range recoveryCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openCheckpointSet(t, utxoset.Config{Only: true})
			set.SetDrainedFamilies(drainedMask(true))
			applyRange(t, set, 1, 4)
			cp, err := set.WriteCheckpoint(t.Context())
			require.NoError(t, err)
			require.Equal(t, uint64(4), cp.Slot)
			applyRange(t, set, 5, 8)
			ledgerTip := tc.setup(t, set)
			var asked []uint64
			require.NoError(t, set.Reconcile(
				t.Context(),
				dbadapter.New(ledgerAt(t, ledgerTip)),
				resumeOnlyAt(4, &asked),
			))
			require.Contains(t, asked, uint64(4), "the ledger was asked to roll back to the checkpoint")
			requireSetAt(t, set, 4)
		})
	}
}

// reimportOnce is a fake Mithril re-import: it records the ledger tip it is
// asked for and loads a snapshot at slot 3 into the set.
type reimportOnce struct {
	set    *utxoset.Set
	calls  int
	asked  ocommon.Point
	failed error
}

func (r *reimportOnce) fn(
	_ context.Context,
	localTip ocommon.Point,
) (ocommon.Point, error) {
	r.calls++
	r.asked = localTip
	if r.failed != nil {
		return ocommon.Point{}, r.failed
	}
	point := slotPoint(3)
	err := r.set.LoadUtxoSet(point, func(put func([]byte, uint32, []byte) error) error {
		for slot := range uint64(3) {
			if err := put(bytes.Repeat([]byte{byte(slot + 1)}, 32), 0, []byte{0x80}); err != nil {
				return err
			}
		}
		return nil
	})
	return point, err
}

// TestReconcileReimportsFromMithrilWithoutACheckpoint requires a re-import of
// ledger state at or below the ledger tip when no checkpoint exists, a
// checkpoint is corrupt, or every checkpoint is above the ledger tip.
func TestReconcileReimportsFromMithrilWithoutACheckpoint(t *testing.T) {
	t.Parallel()
	type variant struct {
		name  string
		write bool
		// resumeAt is the point the ledger accepts: the checkpoint's, when a
		// checkpoint that loaded would be used, else none.
		resumeAt uint64
		prep     func(t *testing.T, set *utxoset.Set)
	}
	for _, v := range []variant{
		{name: "no checkpoint", resumeAt: 99},
		{
			// The backup is intact; only the digest recorded for it differs,
			// so a restore that skipped the check would load it.
			name: "digest mismatch", write: true, resumeAt: 4,
			prep: func(t *testing.T, set *utxoset.Set) {
				cps, err := set.Checkpoints()
				require.NoError(t, err)
				require.Len(t, cps, 1)
				side := strings.TrimSuffix(cps[0].Path, ".bak") + ".json"
				raw, err := os.ReadFile(side)
				require.NoError(t, err)
				var meta map[string]any
				require.NoError(t, json.Unmarshal(raw, &meta))
				meta["sha256"] = strings.Repeat("0", 64)
				raw, err = json.Marshal(meta)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(side, raw, 0o600))
			},
		},
		{
			name: "corrupt checkpoint", write: true, resumeAt: 4,
			prep: func(t *testing.T, set *utxoset.Set) {
				cps, err := set.Checkpoints()
				require.NoError(t, err)
				require.Len(t, cps, 1)
				raw, err := os.ReadFile(cps[0].Path)
				require.NoError(t, err)
				raw[len(raw)/2] ^= 0xff
				require.NoError(t, os.WriteFile(cps[0].Path, raw, 0o600))
			},
		},
	} {
		for _, tc := range recoveryCases {
			t.Run(v.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				set := openCheckpointSet(t, utxoset.Config{Only: true})
				set.SetDrainedFamilies(drainedMask(true))
				applyRange(t, set, 1, 4)
				if v.write {
					_, err := set.WriteCheckpoint(t.Context())
					require.NoError(t, err)
				}
				applyRange(t, set, 5, 8)
				if v.prep != nil {
					v.prep(t, set)
				}
				ledgerTip := tc.setup(t, set)
				fake := &reimportOnce{set: set}
				set.SetReimporter(fake.fn)
				var asked []uint64
				require.NoError(t, set.Reconcile(
					t.Context(),
					dbadapter.New(ledgerAt(t, ledgerTip)),
					resumeOnlyAt(v.resumeAt, &asked),
				))
				require.Equal(t, 1, fake.calls)
				require.Equal(t, ledgerTip.Slot, fake.asked.Slot)
				tip, _, err := set.Tip()
				require.NoError(t, err)
				require.Equal(t, uint64(3), tip.Slot)
			})
		}
	}
}

// TestReconcileSkipsACheckpointAboveTheLedgerTip requires a checkpoint taken
// after the ledger's tip to be left alone and the older one used.
func TestReconcileSkipsACheckpointAboveTheLedgerTip(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	set.SetDrainedFamilies(drainedMask(true))
	applyRange(t, set, 1, 2)
	_, err := set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	applyRange(t, set, 3, 8)
	_, err = set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	require.NoError(t, set.PruneHistory(8))
	var asked []uint64
	require.NoError(t, set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(5))),
		resumeOnlyAt(2, &asked),
	))
	requireSetAt(t, set, 2)
	// The set's own tip is asked first. The newer checkpoint is never
	// restored, so the ledger is not asked about it; the older one is asked
	// once before the set is replaced and once to roll back.
	require.Equal(t, []uint64{8, 2, 2}, asked)
	cps, err := set.Checkpoints()
	require.NoError(t, err)
	require.Len(t, cps, 2, "recovery must not delete a checkpoint it skipped")
}

// TestReconcileNamesWhatRecoveryLacks requires an error that names the missing
// checkpoint and Mithril source, keeps the journal's own reason, and never
// says resync.
func TestReconcileNamesWhatRecoveryLacks(t *testing.T) {
	t.Parallel()
	for _, tc := range recoveryCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openRecoverySet(t, utxoset.Config{Only: true})
			set.SetDrainedFamilies(drainedMask(true))
			applySlots(t, set, 8)
			ledgerTip := tc.setup(t, set)
			var asked []uint64
			err := set.Reconcile(
				t.Context(),
				dbadapter.New(ledgerAt(t, ledgerTip)),
				resumeOnlyAt(99, &asked),
			)
			require.Error(t, err)
			require.ErrorContains(t, err, "no checkpoint directory is configured")
			require.ErrorContains(t, err, "no Mithril aggregator is available")
			require.NotContains(t, err.Error(), "resync")
		})
	}

	t.Run("a directory with nothing usable", func(t *testing.T) {
		t.Parallel()
		set := openCheckpointSet(t, utxoset.Config{Only: true})
		set.SetDrainedFamilies(drainedMask(true))
		applySlots(t, set, 8)
		require.NoError(t, set.PruneHistory(8))
		err := set.Reconcile(
			t.Context(),
			dbadapter.New(ledgerAt(t, slotPoint(10))),
			neverResume,
		)
		require.ErrorContains(t, err, "holds no checkpoint at or below slot 10")
		require.ErrorContains(t, err, "rollback journal reaches back only to")
	})
}

// TestReconcileReportsAFailedReimport requires the re-import's own error to be
// returned beside the journal's reason.
func TestReconcileReportsAFailedReimport(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{Only: true})
	set.SetDrainedFamilies(drainedMask(true))
	applySlots(t, set, 8)
	require.NoError(t, set.PruneHistory(8))
	cause := errors.New("the aggregator serves no snapshot at or below the tip")
	fake := &reimportOnce{set: set, failed: cause}
	set.SetReimporter(fake.fn)
	err := set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(10))),
		neverResume,
	)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "rollback journal reaches back only to")
}

// TestWriteCheckpointFixesItsVersionInsideTheBarrier requires the barrier to
// be taken and lifted once, with the copy's version fixed between the two: a
// commit made when the barrier is taken is in the checkpoint.
func TestWriteCheckpointFixesItsVersionInsideTheBarrier(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	applyRange(t, set, 1, 4)
	var events []string
	set.SetCheckpointBarrier(func(context.Context) (func(), error) {
		events = append(events, "hold")
		applyRange(t, set, 5, 5)
		return func() { events = append(events, "lift") }, nil
	})
	cp, err := set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"hold", "lift"}, events)
	require.Equal(t, uint64(5), cp.Slot)

	applyRange(t, set, 6, 8)
	require.NoError(t, set.PruneHistory(8))
	var asked []uint64
	require.NoError(t, set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(10))),
		resumeOnlyAt(5, &asked),
	))
	requireSetAt(t, set, 5)
}

// TestWriteCheckpointIsOneSnapshotAfterTheBarrier requires a commit made as the
// barrier is lifted to be absent from the copy and from the tip it is named
// for, so the checkpoint's tip, rows and journal are one state.
func TestWriteCheckpointIsOneSnapshotAfterTheBarrier(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	applyRange(t, set, 1, 4)
	set.SetCheckpointBarrier(func(context.Context) (func(), error) {
		return func() { applyRange(t, set, 5, 5) }, nil
	})
	cp, err := set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(4), cp.Slot)

	applyRange(t, set, 6, 8)
	require.NoError(t, set.PruneHistory(8))
	var asked []uint64
	require.NoError(t, set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(10))),
		resumeOnlyAt(4, &asked),
	))
	requireSetAt(t, set, 4)
}

// TestFailedCheckpointRecoveryLeavesTheSetUntouched requires a set with every
// checkpoint unusable (declined by the ledger, or failing its digest) to keep
// its rows and tip, so the failed recovery destroys nothing.
func TestFailedCheckpointRecoveryLeavesTheSetUntouched(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		corrupt bool
	}{
		{name: "ledger declines the point"},
		{name: "checkpoint fails its digest", corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openCheckpointSet(t, utxoset.Config{Only: true})
			set.SetDrainedFamilies(drainedMask(true))
			applyRange(t, set, 1, 4)
			cp, err := set.WriteCheckpoint(t.Context())
			require.NoError(t, err)
			applyRange(t, set, 5, 8)
			require.NoError(t, set.PruneHistory(7))
			if tc.corrupt {
				require.NoError(t, os.WriteFile(cp.Path, []byte("not a backup"), 0o600))
			}
			err = set.Reconcile(
				t.Context(),
				dbadapter.New(ledgerAt(t, slotPoint(5))),
				neverResume,
			)
			require.Error(t, err)
			tip, ok, err := set.Tip()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, uint64(8), tip.Slot, "the set must keep its tip")
			n, err := set.Count()
			require.NoError(t, err)
			require.Equal(t, uint64(8), n, "the set must keep its rows")
		})
	}
}

// TestReconcileFinishesAnInterruptedRestore requires a start that finds the
// restore marker to redo the restore from the checkpoint it names, never
// using the part-restored set.
func TestReconcileFinishesAnInterruptedRestore(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	set.SetDrainedFamilies(drainedMask(true))
	applyRange(t, set, 1, 4)
	cp, err := set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	applyRange(t, set, 5, 8)
	marker, err := json.Marshal(map[string]any{"slot": cp.Slot, "hash": cp.Hash})
	require.NoError(t, err)
	markerPath := filepath.Join(filepath.Dir(cp.Path), "restore-pending.json")
	require.NoError(t, os.WriteFile(markerPath, marker, 0o600))

	// The set agrees with the ledger at slot 8 and would be trusted, but the
	// marker says it may hold part of a restore of the checkpoint at slot 4.
	var asked []uint64
	require.NoError(t, set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(8))),
		resumeOnlyAt(4, &asked),
	))
	require.Equal(t, []uint64{4}, asked)
	requireSetAt(t, set, 4)
	_, err = os.Stat(markerPath)
	require.ErrorIs(t, err, os.ErrNotExist, "the marker is cleared once the restore is checked")

	// A marker whose checkpoint is gone stops the start.
	require.NoError(t, os.WriteFile(markerPath, marker, 0o600))
	require.NoError(t, os.Remove(cp.Path))
	err = set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(4))),
		neverResume,
	)
	require.ErrorContains(t, err, "was interrupted")
}

// TestRecoveryLogsAnErrorFromACommittedResume requires a resume that rolled
// the ledger back but returned an error to be logged, on the journal walk and
// on the checkpoint restore, rather than dropped.
func TestRecoveryLogsAnErrorFromACommittedResume(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, set *utxoset.Set) ocommon.Point
		ckpt  bool
	}{
		{
			name: "journal walk",
			setup: func(t *testing.T, set *utxoset.Set) ocommon.Point {
				require.NoError(t, set.PruneHistory(5))
				return slotPoint(10)
			},
		},
		{
			name: "checkpoint restore",
			ckpt: true,
			setup: func(t *testing.T, set *utxoset.Set) ocommon.Point {
				require.NoError(t, set.PruneHistory(8))
				return slotPoint(10)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var logs syncBuffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			set := openCheckpointSet(t, utxoset.Config{Only: true, Logger: logger})
			set.SetDrainedFamilies(drainedMask(true))
			applyRange(t, set, 1, 4)
			if tc.ckpt {
				_, err := set.WriteCheckpoint(t.Context())
				require.NoError(t, err)
			}
			applyRange(t, set, 5, 8)
			ledgerTip := tc.setup(t, set)
			committed := errors.New("rollback committed with a follow-up error")
			require.NoError(t, set.Reconcile(
				t.Context(),
				dbadapter.New(ledgerAt(t, ledgerTip)),
				func(ctx context.Context, p ocommon.Point) (bool, error) {
					switch {
					case p.Slot == 8:
						// The set's own tip is not a point the ledger takes.
						return false, nil
					case ledger.ResumeCheckOnly(ctx):
						return true, nil
					}
					return true, committed
				},
			))
			require.Contains(t, logs.String(), "but reported an error")
			require.Contains(t, logs.String(), committed.Error())
		})
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestWriteCheckpointSkipsWhenTheDiskIsFull requires a checkpoint to be refused,
// writing nothing, when the free space cannot hold the copy, and written once
// it can.
func TestWriteCheckpointSkipsWhenTheDiskIsFull(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	applyRange(t, set, 1, 4)
	set.SetAvailableDiskForTest(func(string) (uint64, error) { return 0, nil })
	_, err := set.WriteCheckpoint(t.Context())
	require.ErrorIs(t, err, utxoset.ErrCheckpointNoSpace)
	cps, err := set.Checkpoints()
	require.NoError(t, err)
	require.Empty(t, cps)
	got, err := set.CheckpointBytes()
	require.NoError(t, err)
	require.Zero(t, got, "a skipped checkpoint leaves no temporary file")

	set.SetAvailableDiskForTest(func(string) (uint64, error) { return 1 << 40, nil })
	_, err = set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
}

// TestCheckpointDirectoryDropsAndCountsRestoreScratchStores requires a restore
// check killed mid-way, which leaves a scratch store directory, to be counted
// in the checkpoint disk and removed by the next checkpoint and the next
// start.
func TestCheckpointDirectoryDropsAndCountsRestoreScratchStores(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	applyRange(t, set, 1, 2)
	cp, err := set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	dir := filepath.Dir(cp.Path)
	leak := func(name string) string {
		p := filepath.Join(dir, "tmp-restore-"+name)
		require.NoError(t, os.MkdirAll(filepath.Join(p, "sub"), 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(p, "sub", "000001.vlog"), make([]byte, 4096), 0o600))
		return p
	}
	before, err := set.CheckpointBytes()
	require.NoError(t, err)
	first := leak("one")
	after, err := set.CheckpointBytes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, after-before, int64(4096), "the scratch store is counted")

	applyRange(t, set, 3, 3)
	_, err = set.WriteCheckpoint(t.Context())
	require.NoError(t, err)
	require.NoDirExists(t, first, "a checkpoint removes the scratch store")

	second := leak("two")
	require.NoError(t, set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(3))),
		neverResume,
	))
	require.NoDirExists(t, second, "a start removes the scratch store")
}

// TestWriteCheckpointRefusesASetWithAPendingMerge requires no checkpoint of a
// set whose catch-up has not merged its ledger-state families: restoring it
// would bring back rows that are partly the snapshot's.
func TestWriteCheckpointRefusesASetWithAPendingMerge(t *testing.T) {
	t.Parallel()
	set := openCheckpointSet(t, utxoset.Config{Only: true})
	applyRange(t, set, 1, 4)
	require.NoError(t, set.LoadUtxoSetCatchUp(slotPoint(4), func(
		put func([]byte, uint32, []byte) error,
	) error {
		return put(bytes.Repeat([]byte{0xa1}, 32), 0, []byte{0x80})
	}))

	_, err := set.WriteCheckpoint(t.Context())

	require.ErrorIs(t, err, badgerutxo.ErrNotCheckpointable)
	cps, err := set.Checkpoints()
	require.NoError(t, err)
	require.Empty(t, cps)
}

// TestRecoveryRefusesACheckpointThatCarriesAFailureMark requires recovery to
// leave a set as it was rather than restore a checkpoint that holds a failure
// mark, which restoring would carry into the recovered set.
func TestRecoveryRefusesACheckpointThatCarriesAFailureMark(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "checkpoints")
	set := openRecoverySet(t, utxoset.Config{
		Only: true, CheckpointDir: dir, CheckpointKeep: 2,
	})
	set.SetDrainedFamilies(drainedMask(true))
	applyRange(t, set, 1, 8)
	require.NoError(t, set.PruneHistory(8))

	marked := openRecoverySet(t, utxoset.Config{Only: true})
	applyRange(t, marked, 1, 4)
	require.NoError(t, marked.MarkUtxoSetFailed(4))
	var buf bytes.Buffer
	require.NoError(t, marked.Backup(t.Context(), &buf))
	point := slotPoint(4)
	hash := hex.EncodeToString(point.Hash)
	sum := sha256.Sum256(buf.Bytes())
	require.NoError(t, os.MkdirAll(dir, 0o750))
	name := fmt.Sprintf("ckpt-%020d-%s", point.Slot, hash)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".bak"), buf.Bytes(), 0o600))
	meta, err := json.Marshal(map[string]any{
		"slot": point.Slot, "hash": hash,
		"bytes": buf.Len(), "sha256": hex.EncodeToString(sum[:]),
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), meta, 0o600))

	var asked []uint64
	err = set.Reconcile(
		t.Context(),
		dbadapter.New(ledgerAt(t, slotPoint(10))),
		resumeOnlyAt(4, &asked),
	)

	require.ErrorContains(t, err, "could be restored and resumed from")
	require.NotContains(t, asked, uint64(4), "the ledger was asked to roll back to a checkpoint that cannot be used")
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, tip.Slot, uint64(4), "the checkpoint replaced the set")
}
