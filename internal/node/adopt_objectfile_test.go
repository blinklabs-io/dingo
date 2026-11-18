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
type dirObjects struct{ dir string }

func (d dirObjects) ReadImmutableFile(
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

func (d dirObjects) WriteImmutableFile(
	_ context.Context, name string, src io.Reader, _ int64,
) error {
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(d.dir, name), data, 0o600)
}

func (d dirObjects) ListImmutableFiles(
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

func (d dirObjects) DeleteImmutableFile(_ context.Context, name string) error {
	err := os.Remove(filepath.Join(d.dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (d dirObjects) ResetImmutableFiles(ctx context.Context) error {
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

// A certified import into an object-storage ImmutableDB must publish every
// chunk before its markers commit: once the local staging copy is released,
// the remote objects are the only copy of those bodies.
func TestAdoptImmutableChunksIntoObjectStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const chunks = 3
	dir, digests := adoptSource(t, chunks)
	blocks := fixtureBlocks(t, dir)
	remote := dirObjects{dir: t.TempDir()}
	dataDir := t.TempDir()
	host := hostplugin.NewHost()
	require.NoError(t, objectfile.RegisterProvider(host, immplugin.S3ProviderName))
	svc, err := hostplugin.Resolve[immplugin.ImmutableDB](
		ctx, host, hostplugin.CapabilityStorageImmutable,
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
