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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// TestSnapshotReadsTheStoreAsOfTheCall requires a snapshot to answer from the
// state at its creation: a later apply is invisible to it, visible to the
// store, and a second snapshot sees it.
func TestSnapshotReadsTheStoreAsOfTheCall(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	bootstrap(t, s, point(10, 1), out(1, 0), out(2, 0))

	snap, err := s.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { _ = snap.Close() })

	_, err = s.Apply(
		point(11, 2),
		[]badgerutxo.Output{out(3, 0)},
		[]badgerutxo.Ref{ref(1, 0)},
	)
	require.NoError(t, err)

	requireLive(t, snap, true, ref(1, 0))
	requireLive(t, snap, false, ref(3, 0))
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, true, ref(3, 0))
	n, err := snap.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(2), n, "the snapshot's count is as of the call")
	tip, _, err := snap.Tip()
	require.NoError(t, err)
	require.Equal(t, uint64(10), tip.Slot)

	later, err := s.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { _ = later.Close() })
	requireLive(t, later, true, ref(3, 0))
}

// TestSnapshotRefusesWrites requires a write through a snapshot to fail and
// leave the store untouched.
func TestSnapshotRefusesWrites(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	bootstrap(t, s, point(10, 1), out(1, 0))
	snap, err := s.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { _ = snap.Close() })

	_, err = snap.Apply(point(11, 2), []badgerutxo.Output{out(3, 0)}, nil)
	require.ErrorIs(t, err, badgerutxo.ErrSnapshotReadOnly)
	err = snap.LoadNonces(nil)
	require.ErrorIs(t, err, badgerutxo.ErrSnapshotReadOnly)
	_, err = snap.Snapshot()
	require.ErrorIs(t, err, badgerutxo.ErrSnapshotReadOnly)
	requireLive(t, s, false, ref(3, 0))
	tip, _, err := s.Tip()
	require.NoError(t, err)
	require.Equal(t, uint64(10), tip.Slot)
}

// TestSnapshotReleaseKeepsTheStoreOpen requires closing a snapshot to end its
// reads with ErrSnapshotReleased and leave the database serving, and a second
// close to do nothing.
func TestSnapshotReleaseKeepsTheStoreOpen(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	bootstrap(t, s, point(10, 1), out(1, 0))
	snap, err := s.Snapshot()
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	require.NoError(t, snap.Close())

	_, _, err = snap.Get(ref(1, 0))
	require.ErrorIs(t, err, badgerutxo.ErrSnapshotReleased)
	requireLive(t, s, true, ref(1, 0))
}

// TestSnapshotReleaseWaitsForReadsInFlight requires Close to wait for a read
// iterating the snapshot, so a concurrent release cannot discard the
// transaction under it.
func TestSnapshotReleaseWaitsForReadsInFlight(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	bootstrap(t, s, point(10, 1), out(1, 0), out(2, 0), out(3, 0))
	snap, err := s.Snapshot()
	require.NoError(t, err)

	inRead := make(chan struct{})
	finish := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	var readErr error
	var seen int
	go func() {
		defer wg.Done()
		readErr = snap.Each(func(badgerutxo.Output) error {
			if seen == 0 {
				close(inRead)
				<-finish
			}
			seen++
			return nil
		})
	}()
	<-inRead
	closed := make(chan struct{})
	go func() {
		_ = snap.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a read was iterating the snapshot")
	case <-time.After(100 * time.Millisecond):
	}
	close(finish)
	<-closed
	wg.Wait()
	require.NoError(t, readErr)
	require.Equal(t, 3, seen)
}

func TestSnapshotHistoricalViewPreservesItsParent(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { _ = s.Close() })
	bootstrap(t, s, point(10, 1), out(1, 0))
	_, err := s.Apply(point(11, 2), []badgerutxo.Output{out(2, 0)}, nil)
	require.NoError(t, err)
	snap, err := s.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { _ = snap.Close() })
	_, err = s.Apply(point(12, 3), nil, []badgerutxo.Ref{ref(1, 0)})
	require.NoError(t, err)

	view, err := snap.AsOf(point(10, 1))
	require.NoError(t, err)
	t.Cleanup(view.Close)
	_, live, err := view.Get(ref(1, 0))
	require.NoError(t, err)
	require.True(t, live)
	_, live, err = view.Get(ref(2, 0))
	require.NoError(t, err)
	require.False(t, live)
	view.Close()
	view.Close()
	requireLive(t, snap, true, ref(1, 0))
	requireLive(t, snap, true, ref(2, 0))
	requireLive(t, s, false, ref(1, 0))

	_, err = snap.AsOf(point(11, 99))
	require.ErrorIs(t, err, badgerutxo.ErrPointNotInJournal)
	held, err := snap.AsOf(point(10, 1))
	require.NoError(t, err)
	t.Cleanup(held.Close)
	closed := make(chan error, 1)
	go func() { closed <- snap.Close() }()
	select {
	case <-closed:
		t.Fatal("parent closed while a historical view still held its transaction")
	case <-time.After(100 * time.Millisecond):
	}
	_, live, err = held.Get(ref(1, 0))
	require.NoError(t, err)
	require.True(t, live)
	held.Close()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("historical view retained its parent after release or failure")
	}
	_, err = snap.AsOf(point(10, 1))
	require.ErrorIs(t, err, badgerutxo.ErrSnapshotReleased)
}
