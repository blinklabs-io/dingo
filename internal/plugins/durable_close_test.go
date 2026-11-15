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

package plugins

import (
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// TestPersistentStoreProvidersAreDurableClose pins that the on-disk blob store
// and the SQLite metadata store are closed to completion at shutdown.
func TestPersistentStoreProvidersAreDurableClose(t *testing.T) {
	t.Parallel()

	host, err := NewHost()
	require.NoError(t, err)
	want := map[string]bool{
		string(plugin.CapabilityStorageBlob) + "/badger":     false,
		string(plugin.CapabilityStorageMetadata) + "/sqlite": false,
	}
	for _, d := range host.Providers() {
		key := string(d.Capability) + "/" + d.Name
		if _, ok := want[key]; ok {
			require.True(t, d.DurableClose, "%s must set DurableClose", key)
			want[key] = true
		}
	}
	for key, seen := range want {
		require.True(t, seen, "provider %s not registered", key)
	}
}
