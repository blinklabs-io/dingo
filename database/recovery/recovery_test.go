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

package recovery

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRepairer records what recovery asked it to do.
type fakeRepairer struct {
	trimErr        error
	rollbackErr    error
	fenceErr       error
	rolledBackTo   []Point
	trimmedAbove   []uint64
	fenceResets    int
	trimmedRemoved int
}

func (r *fakeRepairer) TrimBlobAbove(slot uint64) (int, error) {
	r.trimmedAbove = append(r.trimmedAbove, slot)
	return r.trimmedRemoved, r.trimErr
}

func (r *fakeRepairer) RollbackTo(point Point) error {
	r.rolledBackTo = append(r.rolledBackTo, point)
	return r.rollbackErr
}

func (r *fakeRepairer) ResetCommitFence() error {
	r.fenceResets++
	return r.fenceErr
}

func newTestManager(t *testing.T, dir string) *Manager {
	t.Helper()
	mgr, err := New(Config{
		Dir:                dir,
		CheckMode:          CheckModeFast,
		SyncJournal:        true,
		CheckpointInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	return mgr
}

// healthySource is a source whose stores agree at a single point.
func healthySource(slot uint64, ts int64) *fakeSource {
	point := Point{Slot: slot, Hash: []byte{byte(slot)}}
	return &fakeSource{
		metadataTip: point,
		blobTip:     point,
		metadataTS:  ts,
		blobTS:      ts,
		recent:      linkedBlocks(4),
		utxos:       UtxoIntegrityResult{Checked: 4},
	}
}

func TestManagerRecoverCleanStateDoesNothing(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	repairer := &fakeRepairer{}
	result, err := mgr.Recover(healthySource(100, 7), repairer)
	require.NoError(t, err)
	assert.Equal(t, OutcomeClean, result.Outcome)
	assert.Empty(t, repairer.trimmedAbove)
	assert.Empty(t, repairer.rolledBackTo)
	assert.Zero(t, repairer.fenceResets)
}

func TestManagerRecoverTrimsWhenBlobIsAhead(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	source := healthySource(100, 7)
	// The signature of a crash between the blob commit and the metadata
	// commit: blob carries the newer fence and the extra blocks.
	source.blobTS = 8
	source.blobTip = Point{Slot: 110, Hash: []byte{110}}
	source.orphans = []BlockRef{{Slot: 105}, {Slot: 110}}
	repairer := &fakeRepairer{trimmedRemoved: 2}

	result, err := mgr.Recover(source, repairer)
	require.NoError(t, err)
	assert.Equal(t, OutcomeRepaired, result.Outcome)
	require.Len(t, repairer.trimmedAbove, 1)
	assert.Equal(t, uint64(100), repairer.trimmedAbove[0])
	assert.Equal(t, 1, repairer.fenceResets)
	assert.Empty(t, repairer.rolledBackTo)
}

func TestManagerRecoverTrimsAboveChainTipNotLedgerTip(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	base := healthySource(100, 7)
	base.blobTS = 8
	base.blobTip = Point{Slot: 300, Hash: []byte{3}}
	// Blocks between the applied tip and the chain tip are legitimately
	// on-chain and only waiting to be applied — the shape a snapshot
	// bootstrap leaves — so the boundary has to be the chain tip.
	source := chainTipSource{
		fakeSource: base,
		chainTip:   Point{Slot: 250, Hash: []byte{2}},
	}
	repairer := &fakeRepairer{}

	result, err := mgr.Recover(source, repairer)
	require.NoError(t, err)
	assert.Equal(t, OutcomeRepaired, result.Outcome)
	require.Len(t, repairer.trimmedAbove, 1)
	assert.Equal(t, uint64(250), repairer.trimmedAbove[0])
}

func TestManagerRecoverRollsBackWhenBlobIsBehind(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	source := healthySource(100, 7)
	// The blob store lost a durable write, so the ledger references blocks
	// that are gone and has to be rewound onto what survives.
	source.blobTS = 6
	source.blobTip = Point{Slot: 90, Hash: []byte{90}}
	repairer := &fakeRepairer{}

	result, err := mgr.Recover(source, repairer)
	require.NoError(t, err)
	assert.Equal(t, OutcomeRepaired, result.Outcome)
	require.Len(t, repairer.rolledBackTo, 1)
	assert.Equal(t, uint64(90), repairer.rolledBackTo[0].Slot)
	assert.Equal(t, 1, repairer.fenceResets)
	assert.Empty(t, repairer.trimmedAbove)
}

func TestManagerRecoverWithoutRepairerReportsOnly(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	source := healthySource(100, 7)
	source.blobTS = 8
	result, err := mgr.Recover(source, nil)
	require.NoError(t, err)
	assert.Equal(t, OutcomeUnrepaired, result.Outcome)
	assert.NotEmpty(t, result.Actions)
}

func TestManagerRecoverPropagatesRepairFailure(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	source := healthySource(100, 7)
	source.blobTS = 8
	repairer := &fakeRepairer{trimErr: errors.New("disk on fire")}
	result, err := mgr.Recover(source, repairer)
	require.Error(t, err)
	require.NotNil(t, result)
	assert.Equal(t, OutcomeUnrepaired, result.Outcome)
}

func TestManagerRecoverSurfacesUnresolvedIntents(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr := newTestManager(t, dir)
	// A begin with no resolution is exactly what a crash inside the commit
	// window leaves in the journal.
	_, err := mgr.Begin(Intent{Kind: IntentBlockAdd, Slot: 105}, 8)
	require.NoError(t, err)

	source := healthySource(100, 7)
	source.blobTS = 8
	source.blobTip = Point{Slot: 105, Hash: []byte{105}}
	repairer := &fakeRepairer{trimmedRemoved: 1}

	result, err := mgr.Recover(source, repairer)
	require.NoError(t, err)
	require.Len(t, result.Pending, 1)
	assert.Equal(t, uint64(105), result.Pending[0].Intent.Slot)
	assert.Equal(t, IntentBlockAdd, result.Pending[0].Intent.Kind)
	assert.Equal(t, OutcomeRepaired, result.Outcome)
}

func TestManagerUnresolvedIntentAloneIsNotARepair(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	// Only begin records are synced, so a crash routinely loses the commit
	// marker of a commit that did land. The stores holding the same fence is
	// the authority: nothing was half applied, so nothing should be repaired.
	_, err := mgr.Begin(Intent{Kind: IntentBlockAdd, Slot: 105}, 7)
	require.NoError(t, err)
	repairer := &fakeRepairer{}

	result, err := mgr.Recover(healthySource(100, 7), repairer)
	require.NoError(t, err)
	assert.Equal(t, OutcomeClean, result.Outcome)
	require.Len(t, result.Pending, 1)
	assert.Empty(t, repairer.trimmedAbove)
	assert.Empty(t, repairer.rolledBackTo)
	assert.Zero(t, repairer.fenceResets)
	assert.NotEmpty(t, result.Actions, "the intents should still be reported")
}

func TestManagerResolvedIntentsAreNotPending(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	committed, err := mgr.Begin(Intent{Kind: IntentBlockAdd, Slot: 1}, 1)
	require.NoError(t, err)
	require.NoError(t, mgr.Commit(committed))
	aborted, err := mgr.Begin(Intent{Kind: IntentBlockAdd, Slot: 2}, 2)
	require.NoError(t, err)
	require.NoError(t, mgr.Abort(aborted))

	result, err := mgr.Recover(healthySource(100, 7), &fakeRepairer{})
	require.NoError(t, err)
	assert.Empty(t, result.Pending)
	assert.Equal(t, OutcomeClean, result.Outcome)
}

func TestManagerRecoverReportsUnrepairableDamage(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	// The stores agree, so there is no interrupted commit to undo, but the
	// blocks beneath the tip do not link up.
	source := healthySource(100, 7)
	blocks := linkedBlocks(6)
	blocks[2].PrevHash = []byte{0xff}
	source.recent = blocks

	result, err := mgr.Recover(source, &fakeRepairer{})
	require.NoError(t, err)
	assert.Equal(t, OutcomeUnrepaired, result.Outcome)
	assert.True(t, result.Report.Failed())
}

func TestManagerCheckpointWritesAndTruncates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr := newTestManager(t, dir)
	for i := range 3 {
		seq, err := mgr.Begin(
			Intent{Kind: IntentBlockAdd, Slot: uint64(i)},
			7,
		)
		require.NoError(t, err)
		require.NoError(t, mgr.Commit(seq))
	}
	source := healthySource(100, 7)
	require.NoError(t, mgr.Checkpoint(source))

	cp, err := mgr.checkpoints.Latest()
	require.NoError(t, err)
	assert.Equal(t, uint64(100), cp.TipSlot)
	assert.Equal(t, int64(7), cp.CommitTimestamp)
	assert.NoError(t, cp.Verify())
	entries, err := os.ReadDir(filepath.Join(dir, checkpointSubdir))
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
}

func TestManagerRecoverRepairsBlobBehindWhenFencesAgree(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	// Equal commit timestamps do not prove the stores are complete. If the
	// blob tip is behind, recovery must rewind the applied state to it.
	source := healthySource(100, 7)
	source.blobTip = Point{Slot: 90, Hash: []byte{90}}
	repairer := &fakeRepairer{}

	result, err := mgr.Recover(source, repairer)
	require.NoError(t, err)
	require.Equal(t, []Point{{Slot: 90, Hash: []byte{90}}}, repairer.rolledBackTo)
	// The tip check still reports the original skew.
	assert.Equal(t, OutcomeRepaired, result.Outcome)
	tipCheck, ok := result.Report.Find(CheckTipConsistency)
	require.True(t, ok)
	assert.Equal(t, SeverityFail, tipCheck.Severity)
}

func TestManagerRecoverRefusesToTrimWithNoKnownTip(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	// Nothing says where the chain ends, but the blob store holds blocks.
	// Trimming above slot zero would erase all of them.
	source := &fakeSource{
		metadataTS: 7,
		blobTS:     8,
		blobTip:    Point{Slot: 500, Hash: []byte{5}},
	}
	repairer := &fakeRepairer{}
	result, err := mgr.Recover(source, repairer)
	require.NoError(t, err)
	assert.Equal(t, OutcomeUnrepaired, result.Outcome)
	assert.Empty(t, repairer.trimmedAbove)
	assert.Zero(t, repairer.fenceResets)
}

func TestNewRejectsNegativeCheckpointInterval(t *testing.T) {
	_, err := New(Config{Dir: t.TempDir(), CheckpointInterval: -time.Second})
	assert.Error(t, err)
}

func TestManagerCheckpointToleratesTransientTimestampSkew(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	source := healthySource(100, 7)
	// The two timestamps are not read atomically, so a commit landing
	// between them looks like divergence on the first read only.
	source.timestampsFn = func(call int) (int64, int64, error) {
		if call == 1 {
			return 6, 7, nil
		}
		return 7, 7, nil
	}
	require.NoError(t, mgr.Checkpoint(source))
	cp, err := mgr.checkpoints.Latest()
	require.NoError(t, err)
	assert.Equal(t, int64(7), cp.CommitTimestamp)
}

func TestManagerCheckpointRefusesDivergedStores(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	source := healthySource(100, 7)
	source.blobTS = 8
	// Anchoring recovery to state the stores do not agree on would be worse
	// than having no anchor at all.
	assert.Error(t, mgr.Checkpoint(source))
}

func TestManagerCheckpointStopsBelowOpenCommits(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	done, err := mgr.Begin(Intent{Kind: IntentBlockAdd, Slot: 1}, 7)
	require.NoError(t, err)
	require.NoError(t, mgr.Commit(done))
	open, err := mgr.Begin(Intent{Kind: IntentBlockAdd, Slot: 2}, 7)
	require.NoError(t, err)

	require.NoError(t, mgr.Checkpoint(healthySource(100, 7)))
	cp, err := mgr.checkpoints.Latest()
	require.NoError(t, err)
	// Covering the open commit would authorise truncating away the very
	// intent record recovery needs if the process died before it resolved.
	assert.Less(t, cp.Seq, open)
}

func TestManagerRecoverAfterCheckpointIgnoresCoveredIntents(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	// A begin whose commit marker was lost, but which a later checkpoint
	// covers, is resolved: the checkpoint asserts the stores agreed past it.
	_, err := mgr.Begin(Intent{Kind: IntentBlockAdd, Slot: 1}, 7)
	require.NoError(t, err)
	mgr.resolve(1)
	require.NoError(t, mgr.Checkpoint(healthySource(100, 7)))

	result, err := mgr.Recover(healthySource(100, 7), &fakeRepairer{})
	require.NoError(t, err)
	assert.Empty(t, result.Pending)
	require.NotNil(t, result.Checkpoint)
	assert.Equal(t, uint64(100), result.Checkpoint.TipSlot)
}

func TestManagerRecoverRequiresSource(t *testing.T) {
	t.Parallel()
	mgr := newTestManager(t, t.TempDir())
	_, err := mgr.Recover(nil, nil)
	assert.Error(t, err)
}

func TestNewManagerRequiresDir(t *testing.T) {
	t.Parallel()
	_, err := New(Config{})
	assert.Error(t, err)
}

func TestNilManagerIsSafe(t *testing.T) {
	t.Parallel()
	// The database holds a nil manager when crash recovery is off, and every
	// call site relies on that being harmless rather than guarding each one.
	var mgr *Manager
	seq, err := mgr.Begin(Intent{}, 1)
	assert.NoError(t, err)
	assert.Zero(t, seq)
	assert.NoError(t, mgr.Commit(1))
	assert.NoError(t, mgr.Abort(1))
	assert.NoError(t, mgr.Checkpoint(nil))
	assert.NoError(t, mgr.Close())
	mgr.Start(nil)
	result, err := mgr.Recover(nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestManagerStartCheckpointsPeriodically(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mgr, err := New(Config{
		Dir:                dir,
		CheckMode:          CheckModeFast,
		CheckpointInterval: 5 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close()) }()

	mgr.Start(healthySource(100, 7))
	require.Eventually(t, func() bool {
		_, err := mgr.checkpoints.Latest()
		return err == nil
	}, 3*time.Second, 5*time.Millisecond)
}

func TestOutcomeAndSeverityStrings(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "clean", OutcomeClean.String())
	assert.Equal(t, "repaired", OutcomeRepaired.String())
	assert.Equal(t, "unrepaired", OutcomeUnrepaired.String())
	assert.Equal(t, "ok", SeverityOK.String())
	assert.Equal(t, "warn", SeverityWarn.String())
	assert.Equal(t, "fail", SeverityFail.String())
	assert.Equal(t, "begin", RecordTypeBegin.String())
	assert.Equal(t, "block_add", IntentBlockAdd.String())
	assert.Equal(t, "rollback", IntentRollback.String())
}
