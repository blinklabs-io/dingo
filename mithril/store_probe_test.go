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

package mithril

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// concurrencyStore records the peak number of Opens in flight. Each Open
// waits until storeProbeWorkers of them are in flight, so a bounded pool is
// observed at exactly its bound. A caller that never reaches the bound gives
// up waiting once, after which no Open waits, so a serial caller is observed
// at 1 rather than hanging.
type concurrencyStore struct {
	ArtifactStore
	mu       sync.Mutex
	inflight int
	peak     int
	release  chan struct{}
	once     sync.Once
}

func newConcurrencyStore(inner ArtifactStore) *concurrencyStore {
	return &concurrencyStore{
		ArtifactStore: inner,
		release:       make(chan struct{}),
	}
}

func (s *concurrencyStore) Open(
	ctx context.Context,
	key string,
) (io.ReadSeekCloser, error) {
	s.mu.Lock()
	s.inflight++
	s.peak = max(s.peak, s.inflight)
	if s.inflight >= storeProbeWorkers {
		s.once.Do(func() { close(s.release) })
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}()
	select {
	case <-s.release:
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		s.once.Do(func() { close(s.release) })
	}
	return s.ArtifactStore.Open(ctx, key)
}

func (s *concurrencyStore) peakOpens() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

func testHash(n int) string {
	return fmt.Sprintf("%064x", n+1)
}

// storeWithArchives returns a store holding every archive of a snapshot with
// immutables immutable files, and that snapshot.
func storeWithArchives(
	t *testing.T,
	immutables uint64,
) (ArtifactStore, *CardanoDatabaseSnapshot) {
	t.Helper()
	store, err := OpenArtifactStore(context.Background(), t.TempDir())
	require.NoError(t, err)
	snapshot := &CardanoDatabaseSnapshot{
		Hash:   testHash(0),
		Beacon: Beacon{ImmutableFileNumber: immutables - 1},
	}
	for _, key := range snapshotArchiveKeys(snapshot) {
		require.NoError(
			t, store.Put(context.Background(), key, bytes.NewReader(nil)),
		)
	}
	return store, snapshot
}

// The aggregator confirms archives while holding its mutex, so the probes
// must not take one store round trip after another.
func TestSnapshotArchivesPresentOpensConcurrently(t *testing.T) {
	t.Parallel()
	inner, snapshot := storeWithArchives(t, 40)
	store := newConcurrencyStore(inner)
	present, err := snapshotArchivesPresent(
		context.Background(), store, snapshot,
	)
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, storeProbeWorkers, store.peakOpens())
}

func TestSnapshotArchivesPresentReportsMissingArchive(t *testing.T) {
	t.Parallel()
	store, snapshot := storeWithArchives(t, 40)
	require.NoError(t, store.DeletePrefix(
		context.Background(),
		path.Join(snapshot.Hash, immutableArchiveName(37)),
	))
	present, err := snapshotArchivesPresent(
		context.Background(), store, snapshot,
	)
	require.NoError(t, err)
	assert.False(t, present)
}

// A cancelled confirmation must fail rather than report the archives present
// because no probe ran to find one missing.
func TestSnapshotArchivesPresentFailsWhenCancelled(t *testing.T) {
	t.Parallel()
	store, snapshot := storeWithArchives(t, 40)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	present, err := snapshotArchivesPresent(ctx, store, snapshot)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, present)
}

// The aggregator lists snapshots while holding its mutex, so the metadata
// reads must not take one store round trip after another.
func TestListSnapshotsReadsConcurrently(t *testing.T) {
	t.Parallel()
	inner, err := OpenArtifactStore(context.Background(), t.TempDir())
	require.NoError(t, err)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const count = 40
	for i := range count {
		require.NoError(t, putJSON(
			context.Background(), inner,
			path.Join(testHash(i), artifactMetadataName),
			&CardanoDatabaseSnapshot{
				Hash: testHash(i),
				CreatedAt: base.Add(time.Duration(i) * time.Minute).
					Format(time.RFC3339Nano),
			},
		))
	}
	store := newConcurrencyStore(inner)
	list, err := ListSnapshots(context.Background(), store)
	require.NoError(t, err)
	require.Len(t, list, count)
	for i, snapshot := range list {
		assert.Equal(t, testHash(count-1-i), snapshot.Hash)
	}
	assert.Equal(t, storeProbeWorkers, store.peakOpens())
}
