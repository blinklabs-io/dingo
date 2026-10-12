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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSnapshots produces one snapshot per entry of trios, each created an
// hour after the previous, and returns their hashes oldest first.
func createSnapshots(
	t *testing.T,
	store ArtifactStore,
	trios ...int,
) []string {
	t.Helper()
	_, key := newSigningKey(t)
	var hashes []string
	for i, n := range trios {
		cfg := newSnapshotConfig(t, newCardanoDB(t, n), store, key)
		cfg.CreatedAt = snapshotCreatedAt.Add(time.Duration(i) * time.Hour)
		artifact, err := CreateSnapshot(context.Background(), cfg)
		require.NoError(t, err)
		hashes = append(hashes, artifact.Hash)
	}
	return hashes
}

func snapshotHashes(t *testing.T, store ArtifactStore) []string {
	t.Helper()
	snapshots, err := ListSnapshots(context.Background(), store)
	require.NoError(t, err)
	var out []string
	for _, s := range snapshots {
		out = append(out, s.Hash)
	}
	return out
}

func TestPruneSnapshotsKeepsNewest(t *testing.T) {
	t.Parallel()

	store, dir := newLocalStore(t)
	hashes := createSnapshots(t, store, 1, 2, 3, 4)

	removed, err := PruneSnapshots(context.Background(), store, 2)
	require.NoError(t, err)

	assert.ElementsMatch(t, hashes[:2], removed)
	assert.Equal(t, []string{hashes[3], hashes[2]}, snapshotHashes(t, store))
	for _, hash := range hashes[:2] {
		assert.NoDirExists(t, dir+"/"+hash)
	}
}

func TestPruneSnapshotsRetainsEverythingBelowOne(t *testing.T) {
	t.Parallel()

	store, _ := newLocalStore(t)
	hashes := createSnapshots(t, store, 1, 2)
	for _, keep := range []int{0, -1, 2, 5} {
		removed, err := PruneSnapshots(context.Background(), store, keep)
		require.NoError(t, err)
		assert.Empty(t, removed, "keep=%d", keep)
	}
	assert.Len(t, snapshotHashes(t, store), len(hashes))
}

func TestPruneSnapshotsLeavesSnapshotInProgress(t *testing.T) {
	t.Parallel()

	store, dir := newLocalStore(t)
	createSnapshots(t, store, 1, 2)
	partial := strings.Repeat("a", 64)
	require.NoError(t, store.Put(
		context.Background(),
		partial+"/00000.tar.zst",
		strings.NewReader("partial"),
	))

	removed, err := PruneSnapshots(context.Background(), store, 1)
	require.NoError(t, err)

	assert.Len(t, removed, 1)
	assert.NotContains(t, removed, partial)
	assert.DirExists(t, dir+"/"+partial)
}

// failingDeleteStore refuses to delete one prefix, the way a removal
// interrupted part way through a snapshot would.
type failingDeleteStore struct {
	ArtifactStore
	failOn string
}

func (s failingDeleteStore) DeletePrefix(
	ctx context.Context,
	prefix string,
) error {
	if prefix == s.failOn {
		return errors.New("delete failed")
	}
	return s.ArtifactStore.DeletePrefix(ctx, prefix)
}

// TestPruneSnapshotsInterruptedRemovalIsUnlisted pins the order of removal:
// the metadata object goes first, so a removal that fails part way leaves a
// remainder that no longer lists, not a listed snapshot missing its archives.
func TestPruneSnapshotsInterruptedRemovalIsUnlisted(t *testing.T) {
	t.Parallel()

	inner, _ := newLocalStore(t)
	hashes := createSnapshots(t, inner, 1, 2)
	store := failingDeleteStore{ArtifactStore: inner, failOn: hashes[0]}

	_, err := PruneSnapshots(context.Background(), store, 1)
	require.Error(t, err)

	assert.Equal(t, []string{hashes[1]}, snapshotHashes(t, store))
}
