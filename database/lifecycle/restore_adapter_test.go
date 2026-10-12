//go:build dingo_extra_plugins

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

package lifecycle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/fakecloud"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

var adapterTrustKey = []byte("operator-trust-root-0123456789ab")

// adapterRestoreFixture holds a signed snapshot uploaded through the real S3
// and GCS destinations into an in-memory bucket, and the registry that
// resolves "fakecloud-s3://" and "fakecloud-gcs://" to them.
type adapterRestoreFixture struct {
	fc       *fakecloud.Store
	registry *DestinationRegistry
	manifest Manifest
}

func newAdapterRestoreFixture(t *testing.T) *adapterRestoreFixture {
	t.Helper()
	fc, dests := downloadDestinations(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(models.Block{
		ID: 1, Slot: 10, Number: 1, Type: 1,
		Hash: bytes.Repeat([]byte{0x01}, 32), Cbor: []byte{0x80},
	}, nil))
	snapshotDir := filepath.Join(t.TempDir(), "snap")
	manifest, err := Snapshot(
		t.Context(), db, snapshotDir, TriggerManual, "test", "badger", "sqlite",
		WithManifestKey(adapterTrustKey),
	)
	require.NoError(t, err)

	fixture := &adapterRestoreFixture{
		fc: fc, registry: NewDestinationRegistry(), manifest: manifest,
	}
	for name, dest := range dests {
		require.NoError(t, dest.UploadDir(t.Context(), snapshotDir))
		fixture.registry.Register(
			"fakecloud-"+name,
			func(*url.URL) (CloudDestination, error) { return dest, nil },
		)
	}
	return fixture
}

func (f *adapterRestoreFixture) restore(
	t *testing.T,
	scheme string,
	key []byte,
) (string, error) {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	var opts []ManifestOption
	if key != nil {
		opts = append(opts, WithManifestKey(key))
	}
	target := filepath.Join(t.TempDir(), "restored")
	_, err := Restore(
		t.Context(), host, f.registry,
		"fakecloud-"+scheme+"://"+downloadTestBucket+"/snap", target,
		RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()}, opts...,
	)
	return target, err
}

// A restore through the S3 and GCS destinations refuses a prefix an attacker
// has written to, and never reads what the manifest does not declare.
func TestRestoreThroughCloudAdaptersRefusesForgedSnapshots(t *testing.T) {
	t.Parallel()
	for _, scheme := range []string{"s3", "gcs"} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()

			t.Run("genuine snapshot with an extra object", func(t *testing.T) {
				t.Parallel()
				f := newAdapterRestoreFixture(t)
				f.fc.Put(
					downloadTestBucket,
					"snap/extra.bin",
					make([]byte, 1<<20),
				)
				gets := f.fc.Requests("GET object")
				_, err := f.restore(t, scheme, adapterTrustKey)
				require.NoError(t, err)
				require.Zero(
					t,
					f.fc.Requests("LIST"),
					"the prefix must not be listed",
				)
				// manifest.json, then the two declared payloads.
				require.Equal(t, 3, f.fc.Requests("GET object")-gets)
			})

			t.Run("payload larger than declared", func(t *testing.T) {
				t.Parallel()
				f := newAdapterRestoreFixture(t)
				blob, ok := f.fc.Get(downloadTestBucket, "snap/blob.bak")
				require.True(t, ok)
				f.fc.Put(downloadTestBucket, "snap/blob.bak", append(blob, 0))
				target, err := f.restore(t, scheme, adapterTrustKey)
				require.ErrorIs(t, err, ErrDownloadTooLarge)
				require.NoDirExists(t, target)
			})

			t.Run("tampered payload of the declared size", func(t *testing.T) {
				t.Parallel()
				f := newAdapterRestoreFixture(t)
				sql, ok := f.fc.Get(downloadTestBucket, "snap/metadata.sqlite")
				require.True(t, ok)
				sql[len(sql)/2] ^= 0xFF
				f.fc.Put(downloadTestBucket, "snap/metadata.sqlite", sql)
				target, err := f.restore(t, scheme, adapterTrustKey)
				require.ErrorIs(t, err, ErrSnapshotPayloadMismatch)
				require.NoDirExists(t, target)
			})

			t.Run("payload and manifest replaced together", func(t *testing.T) {
				t.Parallel()
				f := newAdapterRestoreFixture(t)
				forged := []byte("forged sql payload")
				f.fc.Put(downloadTestBucket, "snap/metadata.sqlite", forged)
				m := f.manifest
				m.MetadataBytes = int64(len(forged))
				m.MetadataSHA256 = sha256Hex(forged)
				// Written without the key, which is all an attacker has.
				dir := t.TempDir()
				require.NoError(t, WriteManifest(dir, m))
				raw, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
				require.NoError(t, err)
				f.fc.Put(downloadTestBucket, "snap/manifest.json", raw)

				target, err := f.restore(t, scheme, adapterTrustKey)
				require.ErrorIs(t, err, ErrManifestUnauthenticated)
				require.NoDirExists(t, target)
			})
		})
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
