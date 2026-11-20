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

package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/immutable/objectfile"
	datatypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// dirObjects stands in for an object store: one file per object.
type dirObjects struct {
	dir string

	mu     sync.Mutex
	writes []string
}

func (d *dirObjects) ReadImmutableFile(
	_ context.Context, name string, dst io.Writer,
) (int64, error) {
	f, err := os.Open(filepath.Join(d.dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, fs.ErrNotExist
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(dst, f)
}

func (d *dirObjects) WriteImmutableFile(
	_ context.Context, name string, src io.Reader, _ int64,
) error {
	d.mu.Lock()
	d.writes = append(d.writes, name)
	d.mu.Unlock()
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(d.dir, name), data, 0o600)
}

func (d *dirObjects) ListImmutableFiles(
	context.Context,
) ([]blob.ImmutableObject, error) {
	files, err := immutable.ListFiles(d.dir)
	if err != nil {
		return nil, err
	}
	out := make([]blob.ImmutableObject, 0, len(files))
	for _, f := range files {
		out = append(out, blob.ImmutableObject{Name: f.Name, Size: f.Size})
	}
	return out, nil
}

func (d *dirObjects) DeleteImmutableFile(_ context.Context, name string) error {
	err := os.Remove(filepath.Join(d.dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (d *dirObjects) ResetImmutableFiles(ctx context.Context) error {
	files, err := d.ListImmutableFiles(ctx)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := d.DeleteImmutableFile(ctx, f.Name); err != nil {
			return err
		}
	}
	return nil
}

// written returns the names ever passed to WriteImmutableFile.
func (d *dirObjects) written() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.writes)
}

// requireRemoteChunk checks whether chunk num's three objects are published
// with their certified contents, or that none of them is.
func requireRemoteChunk(
	t *testing.T,
	remote *dirObjects,
	digests map[string]string,
	num uint64,
	present bool,
) {
	t.Helper()
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		name := immutable.ChunkName(num) + ext
		data, err := os.ReadFile(filepath.Join(remote.dir, name))
		if !present {
			require.ErrorIs(t, err, fs.ErrNotExist, name)
			continue
		}
		require.NoError(t, err, name)
		sum := sha256.Sum256(data)
		require.Equal(t, digests[name], hex.EncodeToString(sum[:]), name)
	}
}

// newObjectAdoptDB opens a database whose ImmutableDB publishes to remote.
func newObjectAdoptDB(t *testing.T, remote *dirObjects) *database.Database {
	t.Helper()
	dataDir := t.TempDir()
	host := hostplugin.NewHost()
	require.NoError(
		t, objectfile.RegisterProvider(host, immplugin.S3ProviderName),
	)
	svc, err := hostplugin.Resolve[immplugin.ImmutableDB](
		context.Background(), host, hostplugin.CapabilityStorageImmutable,
		immplugin.S3ProviderName, nil,
		immplugin.ProviderDependencies{DataDir: dataDir, ObjectFiles: remote},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir:     dataDir,
			Logger:      slog.New(slog.DiscardHandler),
			StorageMode: datatypes.StorageModeCore,
			Immutable:   adoptGeometry(t),
		},
		WrapImmutable: func(immplugin.ImmutableDB) immplugin.ImmutableDB {
			return svc
		},
	})
	require.NoError(t, err)
	return db
}

// A certified import into an object-storage ImmutableDB must publish every
// chunk before its markers commit: once the local staging copy is released,
// the remote objects are the only copy of those bodies.
func TestAdoptImmutableChunksIntoObjectStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const chunks = 3
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	remote := &dirObjects{dir: t.TempDir()}
	db := newObjectAdoptDB(t, remote)

	res, err := adopt(t, ctx, db, dir, digests, nil)
	require.NoError(t, err)
	require.Equal(t, len(blocks), res.BlocksCopied)

	for name, want := range digests {
		data, err := os.ReadFile(filepath.Join(remote.dir, name))
		require.NoError(t, err, name)
		sum := sha256.Sum256(data)
		require.Equal(t, want, hex.EncodeToString(sum[:]), name)
	}
	staged, err := immutable.ListFiles(db.ImmutableDir())
	require.NoError(t, err)
	require.Empty(t, staged, "published chunks must not stay staged")
	for i, b := range blocks {
		got, err := database.BlockByPoint(
			ctx, db, ocommon.NewPoint(b.Slot, b.Hash),
		)
		require.NoError(t, err, "block %d", i)
		require.Equal(t, b.Cbor, got.Cbor, "block %d", i)
	}
	require.NoError(t, db.ReconcileImmutable())
}

// A file substituted while its chunk moves into staging is refused before
// publication: the object store would keep the uploaded bytes whether or not
// the adoption went on to refuse them.
func TestAdoptIntoObjectStorageVerifiesBeforePublishing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 3)
	remote := &dirObjects{dir: t.TempDir()}
	db := newObjectAdoptDB(t, remote)

	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.MoveFile = func(src, dst string) error {
			if err := moveChunkFile(src, dst); err != nil {
				return err
			}
			if filepath.Base(dst) != "00001.chunk" {
				return nil
			}
			data, err := os.ReadFile(dst)
			if err != nil {
				return err
			}
			data[len(data)-40] ^= 0xFF
			return os.WriteFile(dst, data, 0o600)
		}
	})
	require.ErrorIs(t, err, immutable.ErrDigestMismatch)
	for _, name := range remote.written() {
		num, ok := immutable.ParseFileName(name)
		require.True(t, ok, name)
		require.Zero(t, num, "%s was published before it was verified", name)
	}
	requireRemoteChunk(t, remote, digests, 0, true)
	requireRemoteChunk(t, remote, digests, 1, false)
	staged, err := immutable.ListFiles(db.ImmutableDir())
	require.NoError(t, err)
	require.Empty(t, staged, "the refused chunk must not stay staged")
}

// A chunk that fails after publication and before its markers commit leaves
// no objects behind: nothing in the database names them.
func TestAdoptIntoObjectStorageUnpublishesUncommittedChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, digests := adoptSource(t, 3)
	remote := &dirObjects{dir: t.TempDir()}
	db := newObjectAdoptDB(t, remote)
	failure := errors.New("injected failure before commit")

	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.BeforeCommit = func(chunk uint64) error {
			if chunk == 1 {
				return failure
			}
			return nil
		}
	})
	require.ErrorIs(t, err, failure)
	require.Contains(t, remote.written(), "00001.chunk")
	requireRemoteChunk(t, remote, digests, 0, true)
	requireRemoteChunk(t, remote, digests, 1, false)

	res, err := adopt(t, ctx, db, dir, digests, nil)
	require.NoError(t, err)
	require.Positive(t, res.BlocksCopied)
	for num := range uint64(3) {
		requireRemoteChunk(t, remote, digests, num, true)
	}
	require.NoError(t, db.ReconcileImmutable())
}

// The chunk the recorded tip ends in can hold committed markers from a run
// that stopped inside it. A resumed run that then finds its staged copy
// damaged must leave the published objects those markers read from, and must
// not replace them with the damaged bytes.
func TestAdoptIntoObjectStorageKeepsTipChunkObjects(t *testing.T) {
	t.Parallel()
	dir, digests := adoptSource(t, 3)
	blocks := fixtureBlocks(t, dir)
	chunk0 := blocksBelow(blocks, 4320)
	remote := &dirObjects{dir: t.TempDir()}
	db := newObjectAdoptDB(t, remote)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := adopt(t, ctx, db, dir, digests, func(cfg *AdoptChunksConfig) {
		cfg.OnProgress = func(p LoadBlobsProgress) {
			if p.BlocksCopied >= len(chunk0)+loadBlockBatchSize {
				cancel()
			}
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	slot, _ := chainTipOf(t, db)
	require.Greater(t, slot, chunk0[len(chunk0)-1].Slot,
		"blocks of chunk 1 already committed")
	requireRemoteChunk(t, remote, digests, 1, true)

	chunkPath := filepath.Join(db.ImmutableDir(), "00001.chunk")
	data, err := os.ReadFile(chunkPath)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(chunkPath, data, 0o644))

	_, err = adopt(t, context.Background(), db, dir, digests, nil)
	require.ErrorIs(t, err, immutable.ErrDigestMismatch)
	requireRemoteChunk(t, remote, digests, 1, true)
	first := blocksBelow(blocks, 8640)[len(chunk0)]
	got, err := database.BlockByPoint(
		context.Background(), db, ocommon.NewPoint(first.Slot, first.Hash),
	)
	require.NoError(t, err)
	require.Equal(t, first.Cbor, got.Cbor)
}
