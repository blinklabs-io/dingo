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

package lifecycle_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// TestRestoreResolvesBlobStoreWithLoadRunMode verifies that Restore's
// blob-store restore step resolves the badger provider with
// RunMode: "load". Badger's own docs require db.Load to be the only
// thing operating on the store -- no concurrent reads, writes, or GC --
// and the badger provider (see database/plugin/blob/badger/provider.go)
// only skips starting its periodic background value-log GC ticker when
// RunMode == "load". A long-running restore that resolved the store any
// other way would risk that ticker firing mid-Load and violating Load's
// exclusivity requirement.
//
// This registers its own capturing "badger" provider (rather than using
// badger.RegisterProvider directly) so the exact ProviderDependencies
// Restore passes at each resolve can be observed, while still
// constructing a real, working *badger.BlobStoreBadger so the rest of
// Restore's flow (metadata restore, blob restore, post-restore
// validation) runs exactly as it would in production.
func TestRestoreResolvesBlobStoreWithLoadRunMode(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	var gotRunModes []string
	host := plugin.NewHost()
	require.NoError(t, plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityStorageBlob,
			Name:        "badger",
			Description: "badger provider wrapped to capture ProviderDependencies.RunMode",
		},
		func() struct{} { return struct{}{} },
		func(_ context.Context, _ struct{}, deps blob.ProviderDependencies) (*badger.BlobStoreBadger, plugin.Instance, error) {
			gotRunModes = append(gotRunModes, deps.RunMode)
			store, err := badger.New(
				badger.WithDataDir(deps.DataDir),
				badger.WithGc(deps.RunMode != "load"),
				badger.WithDeferOpen(),
			)
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{
				StartFunc: func(context.Context) error { return store.Start() },
				StopFunc:  func(context.Context) error { return store.Stop() },
			}, nil
		},
	))
	require.NoError(t, sqlite.RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(), host, testDestinationRegistry, snapshotDir, targetDir,
		lifecycle.RestoreStorageConfig{},
	)
	require.NoError(t, err)

	// restoreBlobStore's resolve (the one that actually calls db.Load) must
	// be the first one, and must carry RunMode "load".
	require.NotEmpty(t, gotRunModes)
	require.Equal(
		t,
		"load",
		gotRunModes[0],
		"restoreBlobStore must resolve the blob plugin with RunMode \"load\" so its background GC ticker does not run concurrently with db.Load",
	)
}
