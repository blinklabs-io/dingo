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
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var snapshotCreatedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// newCardanoDB writes a cardano-node style database directory with the given
// number of immutable trios and a ledger state snapshot.
func newCardanoDB(t *testing.T, trios int) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "immutable"), 0o750))
	for num := range trios {
		for _, ext := range immutableFileExtensions {
			name := fmt.Sprintf("%05d.%s", num, ext)
			require.NoError(t, os.WriteFile(
				filepath.Join(dir, "immutable", name),
				fmt.Appendf(nil, "immutable-%s-data", name),
				0o640,
			))
		}
	}
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ledger", "100"), 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "ledger", "100", "state"),
		minimalLedgerState(t, 1000, make([]byte, 32)),
		0o640,
	))
	return dir
}

func newSnapshotConfig(
	t *testing.T,
	dbDir string,
	store ArtifactStore,
	key ed25519.PrivateKey,
) CreateSnapshotConfig {
	t.Helper()
	return CreateSnapshotConfig{
		Network:             "preprod",
		DBDir:               dbDir,
		AncillarySigningKey: key,
		CardanoNodeVersion:  "dingo-test",
		CreatedAt:           snapshotCreatedAt,
		Store:               store,
		Logger:              slog.New(slog.DiscardHandler),
	}
}

func newLocalStore(t *testing.T) (ArtifactStore, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := OpenArtifactStore(context.Background(), dir)
	require.NoError(t, err)
	return store, dir
}

func newSigningKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return pub, priv
}

// readTree returns every file under dir keyed by slash-separated path.
func readTree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	require.NoError(t, filepath.WalkDir(
		dir,
		func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			data, err := os.ReadFile(p) //nolint:gosec // test temp dir
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(dir, p)
			if err != nil {
				return err
			}
			out[filepath.ToSlash(rel)] = data
			return nil
		},
	))
	return out
}

// TestCreateSnapshotIsReproducible pins the deterministic-output contract:
// two runs over the same directory yield identical bytes and the same hash.
func TestCreateSnapshotIsReproducible(t *testing.T) {
	t.Parallel()

	db := newCardanoDB(t, 3)
	_, key := newSigningKey(t)
	storeA, dirA := newLocalStore(t)
	storeB, dirB := newLocalStore(t)

	first, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, storeA, key),
	)
	require.NoError(t, err)
	second, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, storeB, key),
	)
	require.NoError(t, err)

	assert.Equal(t, first.Hash, second.Hash)
	treeA, treeB := readTree(t, dirA), readTree(t, dirB)
	require.Len(t, treeA, 3+3) // trios + digests, ancillary, metadata
	assert.Equal(t, treeA, treeB)
}

// TestCreateSnapshotArtifactIsConsistent checks the produced artifact against
// the verification the client applies to a downloaded one: self-hash, digest
// merkle root, immutable digests and the signed ancillary manifest.
func TestCreateSnapshotArtifactIsConsistent(t *testing.T) {
	t.Parallel()

	db := newCardanoDB(t, 3)
	pub, key := newSigningKey(t)
	store, dir := newLocalStore(t)
	artifact, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.NoError(t, err)

	assert.Equal(t, artifact.ComputeHash(), artifact.Hash)
	assert.Equal(t, "preprod", artifact.Network)
	assert.Equal(t, uint64(2), artifact.Beacon.ImmutableFileNumber)
	assert.Equal(t, "dingo-test", artifact.CardanoNodeVersion)
	assert.Equal(t, "2026-01-02T03:04:05Z", artifact.CreatedAt)

	stored := readTree(t, filepath.Join(dir, artifact.Hash))
	var metadata CardanoDatabaseSnapshot
	require.NoError(t, json.Unmarshal(stored[artifactMetadataName], &metadata))
	assert.Equal(t, *artifact, metadata)

	extract := func(name string) string {
		out := filepath.Join(t.TempDir(), "out")
		_, err := ExtractArchive(
			context.Background(),
			filepath.Join(dir, artifact.Hash, name),
			out, slog.New(slog.DiscardHandler),
		)
		require.NoError(t, err)
		return out
	}

	digestDir := extract(digestsArchiveName)
	var entries []CardanoDatabaseDigestEntry
	require.NoError(t, json.Unmarshal(
		readTree(t, digestDir)[digestsJSONName], &entries,
	))
	require.Len(t, entries, 9)
	require.NoError(t, verifyDigestMerkleRoot(entries, artifact))

	immutableDir := extract("00001.tar.zst")
	chunk, err := os.ReadFile( //nolint:gosec // test temp dir
		filepath.Join(immutableDir, "immutable", "00001.chunk"),
	)
	require.NoError(t, err)
	assert.Equal(t, []byte("immutable-00001.chunk-data"), chunk)

	ancillaryDir := extract(ancillaryArchiveName)
	root, err := os.OpenRoot(ancillaryDir)
	require.NoError(t, err)
	defer root.Close()
	covered, err := verifyAncillaryManifest(root, mithrilJSONHexKey(t, pub))
	require.NoError(t, err)
	assert.Contains(t, covered, "ledger/100/state")
	assert.Equal(t, int64(len(
		minimalLedgerState(t, 1000, make([]byte, 32)),
	)), artifact.Ancillary.SizeUncompressed)
}

func TestCreateSnapshotRejectsIncompleteInput(t *testing.T) {
	t.Parallel()

	_, key := newSigningKey(t)
	cases := map[string]struct {
		mutate  func(t *testing.T, db string)
		wantErr string
	}{
		"missing secondary file": {
			mutate: func(t *testing.T, db string) {
				require.NoError(t, os.Remove(
					filepath.Join(db, "immutable", "00001.secondary"),
				))
			},
			wantErr: "00001.secondary is missing",
		},
		"gap in file numbers": {
			mutate: func(t *testing.T, db string) {
				for _, ext := range immutableFileExtensions {
					require.NoError(t, os.Remove(filepath.Join(
						db, "immutable", "00001."+ext,
					)))
				}
			},
			wantErr: "00001.chunk is missing",
		},
		"no ledger state": {
			mutate: func(t *testing.T, db string) {
				require.NoError(t, os.RemoveAll(filepath.Join(db, "ledger")))
			},
			wantErr: "selecting ledger state",
		},
		"no immutable files": {
			mutate: func(t *testing.T, db string) {
				require.NoError(t, os.RemoveAll(
					filepath.Join(db, "immutable"),
				))
				require.NoError(t, os.Mkdir(
					filepath.Join(db, "immutable"), 0o750,
				))
			},
			wantErr: "holds no immutable files",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newCardanoDB(t, 3)
			tc.mutate(t, db)
			store, dir := newLocalStore(t)
			_, err := CreateSnapshot(
				context.Background(), newSnapshotConfig(t, db, store, key),
			)
			require.ErrorContains(t, err, tc.wantErr)
			assert.Empty(t, readTree(t, dir), "nothing is published on failure")
		})
	}
}

func TestCreateSnapshotRequiresConfig(t *testing.T) {
	t.Parallel()

	db := newCardanoDB(t, 1)
	_, key := newSigningKey(t)
	store, _ := newLocalStore(t)
	good := newSnapshotConfig(t, db, store, key)

	noNetwork := good
	noNetwork.Network = ""
	noKey := good
	noKey.AncillarySigningKey = nil
	noStore := good
	noStore.Store = nil
	for name, cfg := range map[string]CreateSnapshotConfig{
		"network": noNetwork, "signing key": noKey, "store": noStore,
	} {
		_, err := CreateSnapshot(context.Background(), cfg)
		assert.Error(t, err, name)
	}
}

// changeOnPutStore rewrites a source file after the first object is stored,
// the way a node still writing to the directory would.
type changeOnPutStore struct {
	ArtifactStore
	change func()
	done   bool
}

func (s *changeOnPutStore) Put(
	ctx context.Context,
	key string,
	r io.Reader,
) error {
	if !s.done {
		s.done = true
		s.change()
	}
	return s.ArtifactStore.Put(ctx, key, r)
}

// TestCreateSnapshotDetectsFileChangedAfterDigest guards the second pass: an
// archive whose bytes differ from the digest list already computed must fail
// the run, since clients would reject it against the certified list. The
// rewrite keeps the length so the tar size check cannot be what catches it,
// and targets a file the first archive does not read so the outcome does not
// depend on scheduling.
func TestCreateSnapshotDetectsFileChangedAfterDigest(t *testing.T) {
	t.Parallel()

	db := newCardanoDB(t, 2)
	_, key := newSigningKey(t)
	inner, _ := newLocalStore(t)
	store := &changeOnPutStore{
		ArtifactStore: inner,
		change: func() {
			require.NoError(t, os.WriteFile(
				filepath.Join(db, "immutable", "00001.chunk"),
				[]byte("immutable-00001.chunk-DATA"), 0o640,
			))
		},
	}
	_, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.ErrorContains(t, err, "changed while archiving")
}

func TestReadAncillaryKeepsSelectedStateBytes(t *testing.T) {
	t.Parallel()

	db := newCardanoDB(t, 1)
	originalPath := filepath.Join(db, "ledger", "100", "state")
	original, err := os.ReadFile(originalPath)
	require.NoError(t, err)
	root, err := os.OpenRoot(db)
	require.NoError(t, err)
	defer root.Close()
	ancillary, _, err := readAncillary(root)
	require.NoError(t, err)
	defer ancillary.close()

	require.NoError(t, os.Rename(originalPath, originalPath+".old"))
	require.NoError(t, os.WriteFile(
		originalPath, minimalLedgerState(t, 2000, bytes.Repeat([]byte{1}, 32)),
		0o640,
	))
	_, key := newSigningKey(t)
	_, digests, err := ancillary.entries(key)
	require.NoError(t, err)
	want := sha256.Sum256(original)
	require.Equal(t, hex.EncodeToString(want[:]), digests["ledger/100/state"])
}

func TestParseSigningKey(t *testing.T) {
	t.Parallel()

	pub, priv := newSigningKey(t)
	message := []byte("manifest hash")
	for name, encoded := range map[string]string{
		"seed json-hex":        mithrilJSONHexKey(t, priv.Seed()),
		"private key json-hex": mithrilJSONHexKey(t, priv),
		"seed raw hex":         hex.EncodeToString(priv.Seed()),
	} {
		key, err := ParseSigningKey(encoded)
		require.NoError(t, err, name)
		assert.True(
			t, ed25519.Verify(pub, message, ed25519.Sign(key, message)), name,
		)
	}
	for name, encoded := range map[string]string{
		"empty":      "",
		"short":      mithrilJSONHexKey(t, []byte("short")),
		"not hex":    "zz",
		"wrong size": hex.EncodeToString(make([]byte, 48)),
	} {
		_, err := ParseSigningKey(encoded)
		assert.Error(t, err, name)
	}
}
