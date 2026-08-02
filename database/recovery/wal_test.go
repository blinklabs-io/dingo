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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestWAL(t *testing.T, dir string, maxSegment int64) *WAL {
	t.Helper()
	w, err := OpenWAL(WALConfig{
		Dir:             dir,
		MaxSegmentBytes: maxSegment,
		SyncOnBegin:     true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		// Close is idempotent, so a test that closed the journal itself
		// does not fail here.
		require.NoError(t, w.Close())
	})
	return w
}

func replayAll(t *testing.T, w *WAL) []Record {
	t.Helper()
	var records []Record
	require.NoError(t, w.Replay(func(r Record) error {
		records = append(records, r)
		return nil
	}))
	return records
}

func TestWALSequencesIncrease(t *testing.T) {
	t.Parallel()
	w := newTestWAL(t, t.TempDir(), 0)
	first, err := w.Begin(Intent{Kind: IntentBlockAdd, Slot: 1}, 100)
	require.NoError(t, err)
	second, err := w.Begin(Intent{Kind: IntentBlockAdd, Slot: 2}, 101)
	require.NoError(t, err)
	assert.Greater(t, second, first)
	assert.Equal(t, second+1, w.NextSeq())
}

func TestWALReplayReturnsRecordsInOrder(t *testing.T) {
	t.Parallel()
	w := newTestWAL(t, t.TempDir(), 0)
	seq, err := w.Begin(Intent{Kind: IntentBlockAdd, Slot: 7}, 500)
	require.NoError(t, err)
	require.NoError(t, w.Commit(seq))
	records := replayAll(t, w)
	require.Len(t, records, 2)
	assert.Equal(t, RecordTypeBegin, records[0].Type)
	assert.Equal(t, uint64(7), records[0].Intent.Slot)
	assert.Equal(t, int64(500), records[0].CommitTimestamp)
	assert.Equal(t, RecordTypeCommit, records[1].Type)
	assert.Equal(t, seq, records[1].Seq)
}

func TestWALSurvivesReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWAL(t, dir, 0)
	seq, err := w.Begin(Intent{Kind: IntentBlockAdd, Slot: 3}, 42)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	reopened := newTestWAL(t, dir, 0)
	// Sequences must keep increasing across a restart so replay can tell
	// records written before it from those written after.
	assert.Greater(t, reopened.NextSeq(), seq)
	records := replayAll(t, reopened)
	require.Len(t, records, 1)
	assert.Equal(t, seq, records[0].Seq)
}

func TestWALRotatesSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// A tiny segment cap forces a rotation on essentially every record.
	w := newTestWAL(t, dir, 1)
	for i := range 4 {
		seq, err := w.Begin(
			Intent{Kind: IntentBlockAdd, Slot: uint64(i)},
			int64(i),
		)
		require.NoError(t, err)
		require.NoError(t, w.Commit(seq))
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Greater(t, len(entries), 1, "expected more than one segment")
	assert.Len(t, replayAll(t, w), 8)
}

func TestWALTruncateThroughRemovesCoveredSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWAL(t, dir, 1)
	seqs := make([]uint64, 0, 5)
	for i := range 5 {
		seq, err := w.Begin(
			Intent{Kind: IntentBlockAdd, Slot: uint64(i)},
			int64(i),
		)
		require.NoError(t, err)
		require.NoError(t, w.Commit(seq))
		seqs = append(seqs, seq)
	}
	before, err := os.ReadDir(dir)
	require.NoError(t, err)

	removed, err := w.TruncateThrough(seqs[2])
	require.NoError(t, err)
	assert.Positive(t, removed)

	after, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Less(t, len(after), len(before))
	for _, record := range replayAll(t, w) {
		assert.Greater(
			t,
			record.Seq,
			seqs[2]-1,
			"records at or below the truncation point should be gone",
		)
	}
}

func TestWALSequencesSurviveTruncationAndRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWAL(t, dir, 1)
	var highest uint64
	for i := range 5 {
		seq, err := w.Begin(
			Intent{Kind: IntentBlockAdd, Slot: uint64(i)},
			int64(i),
		)
		require.NoError(t, err)
		require.NoError(t, w.Commit(seq))
		highest = seq
	}
	// Truncation leaves only the active segment. Emptying it simulates a
	// crash between creating a segment and flushing the record that prompted
	// it, which is the shape that leaves no record to recover the counter
	// from.
	_, err := w.TruncateThrough(highest)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(
			t,
			os.Truncate(filepath.Join(dir, entry.Name()), 0),
		)
	}

	reopened := newTestWAL(t, dir, 1)
	// Reissuing a sequence a checkpoint already covers would let truncation
	// discard the new records along with the old.
	assert.Greater(t, reopened.NextSeq(), highest)
}

func TestWALSequencesRemainMonotonicAfterPartialTruncation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWAL(t, dir, 1)
	seqs := make([]uint64, 0, 8)
	for i := range 8 {
		seq, err := w.Begin(
			Intent{Kind: IntentBlockAdd, Slot: uint64(i)},
			int64(i),
		)
		require.NoError(t, err)
		require.NoError(t, w.Commit(seq))
		seqs = append(seqs, seq)
	}
	checkpointed := seqs[3]
	require.NoError(t, func() error {
		_, err := w.TruncateThrough(checkpointed)
		return err
	}())
	require.NoError(t, w.Close())

	reopened := newTestWAL(t, dir, 1)
	assert.Greater(t, reopened.NextSeq(), seqs[len(seqs)-1])
	next, err := reopened.Begin(
		Intent{Kind: IntentBlockAdd, Slot: 99},
		99,
	)
	require.NoError(t, err)
	assert.Greater(t, next, seqs[len(seqs)-1])
}

func TestWALTruncateKeepsUncoveredSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWAL(t, dir, 1)
	seq, err := w.Begin(Intent{Kind: IntentBlockAdd, Slot: 1}, 1)
	require.NoError(t, err)
	require.NoError(t, w.Commit(seq))
	// Truncating below the oldest record must not remove anything.
	removed, err := w.TruncateThrough(0)
	require.NoError(t, err)
	assert.Zero(t, removed)
	assert.NotEmpty(t, replayAll(t, w))
}

func TestWALReplayStopsAtTornTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := newTestWAL(t, dir, 0)
	for i := range 3 {
		_, err := w.Begin(
			Intent{Kind: IntentBlockAdd, Slot: uint64(i)},
			int64(i),
		)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	path := filepath.Join(dir, entries[0].Name())
	info, err := os.Stat(path)
	require.NoError(t, err)
	// Chop the final few bytes, which is what an interrupted append leaves.
	require.NoError(t, os.Truncate(path, info.Size()-3))

	reopened := newTestWAL(t, dir, 0)
	records := replayAll(t, reopened)
	assert.Len(
		t,
		records,
		2,
		"the two intact records survive, the torn one does not",
	)
}

func TestWALCheckpointRecordReplays(t *testing.T) {
	t.Parallel()
	w := newTestWAL(t, t.TempDir(), 0)
	require.NoError(t, w.AppendCheckpoint(Checkpoint{
		Seq:         9,
		TipSlot:     1234,
		TipHash:     []byte{0xaa},
		BlobTipSlot: 1234,
	}))
	records := replayAll(t, w)
	require.Len(t, records, 1)
	require.NotNil(t, records[0].Checkpoint)
	assert.Equal(t, uint64(9), records[0].Checkpoint.Seq)
	// AppendCheckpoint seals before writing, so what comes back verifies.
	assert.NoError(t, records[0].Checkpoint.Verify())
}

func TestWALRejectsUseAfterClose(t *testing.T) {
	t.Parallel()
	w := newTestWAL(t, t.TempDir(), 0)
	require.NoError(t, w.Close())
	_, err := w.Begin(Intent{}, 1)
	assert.ErrorIs(t, err, ErrWALClosed)
	assert.ErrorIs(t, w.Commit(1), ErrWALClosed)
	assert.ErrorIs(t, w.Sync(), ErrWALClosed)
	_, truncErr := w.TruncateThrough(1)
	assert.ErrorIs(t, truncErr, ErrWALClosed)
}

func TestOpenWALRequiresDir(t *testing.T) {
	t.Parallel()
	_, err := OpenWAL(WALConfig{})
	assert.Error(t, err)
}

func TestParseSegmentName(t *testing.T) {
	t.Parallel()
	seq, ok := parseSegmentName("wal-00000000000000000042.log")
	assert.True(t, ok)
	assert.Equal(t, uint64(42), seq)
	for _, name := range []string{
		"wal-notanumber.log",
		"other-00000000000000000042.log",
		"wal-00000000000000000042.txt",
	} {
		_, ok := parseSegmentName(name)
		assert.False(t, ok, "should not parse %q", name)
	}
}
