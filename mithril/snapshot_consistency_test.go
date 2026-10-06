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
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hookStore runs a hook once, on the first Put or DeletePrefix whose key
// matches, to model another process acting on the store at that moment.
type hookStore struct {
	ArtifactStore
	mu sync.Mutex
	// beforePut runs before the matching Put.
	beforePut func(key string) bool
	// afterDelete runs after the matching DeletePrefix.
	afterDelete func(prefix string) bool
}

func (s *hookStore) Put(ctx context.Context, key string, r io.Reader) error {
	s.mu.Lock()
	hook := s.beforePut
	if hook != nil && hook(key) {
		s.beforePut = nil
	}
	s.mu.Unlock()
	return s.ArtifactStore.Put(ctx, key, r)
}

func (s *hookStore) DeletePrefix(ctx context.Context, prefix string) error {
	if err := s.ArtifactStore.DeletePrefix(ctx, prefix); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.afterDelete != nil && s.afterDelete(prefix) {
		s.afterDelete = nil
	}
	return nil
}

func requireArchivesPresent(
	t *testing.T,
	store ArtifactStore,
	snapshot *CardanoDatabaseSnapshot,
) {
	t.Helper()
	ok, err := snapshotArchivesPresent(context.Background(), store, snapshot)
	require.NoError(t, err)
	require.True(t, ok, "snapshot %s is missing archives", snapshot.Hash)
}

// A retention run that removes the open snapshot after the aggregator checked
// it, but before the certified metadata is written, must not leave a listed
// snapshot whose archives are gone.
func TestAggregatorDoesNotListSnapshotPrunedDuringCertification(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	inner := f.store
	store := &hookStore{ArtifactStore: inner}
	f.store = store
	f.start()
	signers := testSigners(4, 100)
	f.registerAll(signers)
	snap := f.newSnapshot(2)
	_, pc := f.pending()
	require.NotNil(t, pc)

	// The chain head is written after the open snapshot was checked and
	// before its certified metadata; retention lands in between.
	store.beforePut = func(key string) bool {
		if key != aggregatorStateKey {
			return false
		}
		require.NoError(t, inner.DeletePrefix(
			context.Background(), path.Join(snap.Hash, artifactMetadataName),
		))
		require.NoError(t, inner.DeletePrefix(context.Background(), snap.Hash))
		return true
	}
	certified := false
	for _, s := range signers {
		req, ok := f.signature(s, pc)
		if !ok {
			continue
		}
		code, body := f.post("/register-signatures", req)
		if code == http.StatusConflict {
			certified = true
			break
		}
		require.Equal(t, http.StatusCreated, code, body)
	}
	require.True(t, certified, "certification did not reach the removal")
	assert.Empty(t, snapshotHashes(t, f.store))
	status, _ := f.pending()
	assert.Equal(t, http.StatusNoContent, status)
}

// An aggregator that writes certified metadata back while retention removes
// the snapshot's archives must not leave the snapshot listed.
func TestPruneSnapshotsRemovesMetadataRewrittenDuringRemoval(t *testing.T) {
	t.Parallel()
	inner, _ := newLocalStore(t)
	hashes := createSnapshots(t, inner, 1, 2)
	old, err := readSnapshot(context.Background(), inner, hashes[0])
	require.NoError(t, err)
	store := &hookStore{ArtifactStore: inner}
	store.afterDelete = func(prefix string) bool {
		if prefix != hashes[0] {
			return false
		}
		old.CertificateHash = strings.Repeat("c", 64)
		require.NoError(t, putJSON(
			context.Background(), inner,
			path.Join(old.Hash, artifactMetadataName), old,
		))
		return true
	}

	removed, err := PruneSnapshots(context.Background(), store, 1)
	require.NoError(t, err)
	assert.Equal(t, []string{hashes[0]}, removed)
	assert.Equal(t, []string{hashes[1]}, snapshotHashes(t, inner))
}

// A concurrent run's cleanup can remove archives this run already uploaded.
// The run must then fail rather than list a snapshot missing them.
func TestCreateSnapshotFailsWhenArchivesRemovedBeforeMetadata(t *testing.T) {
	t.Parallel()
	db := newCardanoDB(t, 3)
	_, key := newSigningKey(t)
	inner, _ := newLocalStore(t)
	store := &hookStore{ArtifactStore: inner}
	store.beforePut = func(key string) bool {
		if path.Base(key) != artifactMetadataName {
			return false
		}
		require.NoError(t, inner.DeletePrefix(
			context.Background(),
			path.Join(path.Dir(key), immutableArchiveName(1)),
		))
		return true
	}

	_, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.ErrorIs(t, err, ErrSnapshotArchivesMissing)
	assert.Empty(t, snapshotHashes(t, inner))
}

// A listed snapshot with a missing archive is rebuilt by the next run over
// the same database, keeping the certificate it carries.
func TestCreateSnapshotRepairsStoredSnapshotMissingArchives(t *testing.T) {
	t.Parallel()
	db := newCardanoDB(t, 3)
	_, key := newSigningKey(t)
	store, _ := newLocalStore(t)
	first, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.NoError(t, err)
	first.CertificateHash = strings.Repeat("c", 64)
	require.NoError(t, putJSON(
		context.Background(), store,
		path.Join(first.Hash, artifactMetadataName), first,
	))
	require.NoError(t, store.DeletePrefix(
		context.Background(), path.Join(first.Hash, immutableArchiveName(2)),
	))

	again, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.NoError(t, err)
	assert.Equal(t, first.Hash, again.Hash)
	assert.Equal(t, first.CertificateHash, again.CertificateHash)
	requireArchivesPresent(t, store, again)
	stored, err := readSnapshot(context.Background(), store, first.Hash)
	require.NoError(t, err)
	assert.Equal(t, first.CertificateHash, stored.CertificateHash)
}

// A failed run leaves a snapshot that a concurrent run completed intact, but
// removes a listed one whose archives its own removal already took.
func TestRemoveIncompleteSnapshotRespectsCompletedSnapshot(t *testing.T) {
	t.Parallel()
	store, _ := newLocalStore(t)
	hashes := createSnapshots(t, store, 2)
	removeIncompleteSnapshot(context.Background(), store, hashes[0], nil)
	assert.Equal(t, hashes, snapshotHashes(t, store))

	require.NoError(t, store.DeletePrefix(
		context.Background(), path.Join(hashes[0], immutableArchiveName(0)),
	))
	removeIncompleteSnapshot(context.Background(), store, hashes[0], nil)
	assert.Empty(t, snapshotHashes(t, store))
}
