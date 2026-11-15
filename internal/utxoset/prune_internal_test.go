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

package utxoset

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestApplyUtxoChangesSucceedsWhenPruningFails requires a journal prune
// failure after a committed batch not to fail the commit: the ledger stops
// following a set whose commit reports an error.
func TestApplyUtxoChangesSucceedsWhenPruningFails(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir(), HistorySlots: 10})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.UpdateAnchor(ocommon.NewPoint(1, bytes.Repeat([]byte{1}, 32)))
	pruned := 0
	set.pruneJournal = func(uint64) error {
		pruned++
		return errors.New("prune failed")
	}
	for slot := uint64(1); slot <= pruneEvery; slot++ {
		tip := ocommon.NewPoint(slot+10, bytes.Repeat([]byte{1}, 32))
		require.NoError(t, set.ApplyUtxoChanges(&tip, nil))
	}
	require.NotZero(t, pruned, "the prune was never attempted")
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(pruneEvery+10), tip.Slot)
}

// TestPruneKeepsTheImmutableTipReachable requires the stability-window prune
// not to raise the journal floor past the recorded immutable tip, which is
// older than the window here, so the set still opens a view of it.
func TestPruneKeepsTheImmutableTipReachable(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir(), HistorySlots: 10})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.UpdateAnchor(ocommon.NewPoint(15, bytes.Repeat([]byte{1}, 32)))
	require.NoError(t, applyCommits(set, pruneEvery))
	v, err := set.AnchorView()
	require.NoError(t, err)
	v.Close()
}

// TestPruneWaitsForAnAnchor requires no prune while no immutable tip has
// been recorded.
func TestPruneWaitsForAnAnchor(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir(), HistorySlots: 10})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	pruned := 0
	set.pruneJournal = func(uint64) error {
		pruned++
		return nil
	}
	require.NoError(t, applyCommits(set, pruneEvery))
	require.Zero(t, pruned)
}

// TestCommitPathNeedsNoLedgerLock stands a write lock in for the ledger's
// own, held across every commit as the ledger holds it while it runs a write
// transaction, and requires the commits, including a prune, to finish while
// a goroutine that needs the read lock, as the anchor updater does, is
// blocked behind it.
func TestCommitPathNeedsNoLedgerLock(t *testing.T) {
	t.Parallel()
	set, err := Open(Config{Dir: t.TempDir(), HistorySlots: 10})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.UpdateAnchor(ocommon.NewPoint(15, bytes.Repeat([]byte{1}, 32)))
	pruned := make(chan struct{}, 1)
	set.pruneJournal = func(uint64) error {
		select {
		case pruned <- struct{}{}:
		default:
		}
		return nil
	}
	var ledgerMu sync.RWMutex
	ledgerMu.Lock()
	updaterDone := make(chan struct{})
	go func() {
		defer close(updaterDone)
		ledgerMu.RLock()
		defer ledgerMu.RUnlock()
		set.UpdateAnchor(ocommon.NewPoint(20, bytes.Repeat([]byte{1}, 32)))
	}()
	committed := make(chan error, 1)
	go func() { committed <- applyCommits(set, pruneEvery) }()
	require.NoError(t, testutil.RequireReceive(
		t, committed, 20*time.Second, "commits blocked",
	))
	testutil.RequireReceive(t, pruned, time.Second, "no prune ran")
	ledgerMu.Unlock()
	testutil.RequireReceive(t, updaterDone, 5*time.Second, "updater stuck")
}

func applyCommits(set *Set, n uint64) error {
	for slot := uint64(1); slot <= n; slot++ {
		tip := ocommon.NewPoint(slot+10, bytes.Repeat([]byte{1}, 32))
		if err := set.ApplyUtxoChanges(&tip, nil); err != nil {
			return err
		}
	}
	return nil
}
