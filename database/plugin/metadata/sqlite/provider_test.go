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

package sqlite

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func TestProviderMaxConnectionsPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]any
		injected int
		want     int
	}{
		{name: "injected worker count", injected: 11, want: 11},
		{name: "provider override", config: map[string]any{"maxConnections": 7}, injected: 11, want: 7},
		{name: "provider default", want: DefaultMaxConnections},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host := plugin.NewHost()
			require.NoError(t, RegisterProvider(host))
			t.Cleanup(func() {
				require.NoError(t, host.Stop(context.Background()))
			})

			store, err := plugin.Resolve[*MetadataStoreSqlite](
				context.Background(),
				host,
				plugin.CapabilityStorageMetadata,
				"sqlite",
				tt.config,
				metadata.ProviderDependencies{MaxConnections: tt.injected},
			)
			require.NoError(t, err)
			require.Equal(t, tt.want, store.maxConnections)
		})
	}
}

func TestProviderDataDirPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		override bool
	}{
		{name: "database path shortcut"},
		{name: "provider override", override: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			injected := t.TempDir()
			want := injected
			config := map[string]any(nil)
			if tt.override {
				want = t.TempDir()
				config = map[string]any{"dataDir": want}
			}
			host := plugin.NewHost()
			require.NoError(t, RegisterProvider(host))
			t.Cleanup(func() {
				require.NoError(t, host.Stop(context.Background()))
			})

			store, err := plugin.Resolve[*MetadataStoreSqlite](
				context.Background(), host,
				plugin.CapabilityStorageMetadata, "sqlite", config,
				metadata.ProviderDependencies{DataDir: injected},
			)
			require.NoError(t, err)
			require.Equal(t, want, store.dataDir)
		})
	}
}
