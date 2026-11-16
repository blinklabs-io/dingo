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

package plugins_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/immutable/chunkfile"
	"github.com/blinklabs-io/dingo/internal/plugins"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func TestNewHostRegistersBuiltInImmutableProvider(t *testing.T) {
	t.Parallel()
	host, err := plugins.NewHost()
	require.NoError(t, err)
	var found []hostplugin.Descriptor
	for _, d := range host.Providers() {
		if d.Capability == hostplugin.CapabilityStorageImmutable {
			found = append(found, d)
		}
	}
	require.Len(t, found, 1)
	require.Equal(t, chunkfile.ProviderName, found[0].Name)
	require.True(t, found[0].DurableClose)
}

func TestResolveStorageProvidesImmutableDB(t *testing.T) {
	t.Parallel()
	host, err := plugins.NewHost()
	require.NoError(t, err)
	dir := t.TempDir()
	stores, err := plugins.ResolveStorage(
		context.Background(), host,
		plugins.StorageSelections{
			Blob:     hostplugin.Selection{Provider: "badger"},
			Metadata: hostplugin.Selection{Provider: "sqlite"},
		},
		plugins.StorageDependencies{DataDir: dir, StorageMode: "core"},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	require.NotNil(t, stores.Immutable)
	require.Equal(t, filepath.Join(dir, "immutable"), stores.Immutable.Dir())
}
