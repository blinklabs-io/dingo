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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// newTestStorageHost builds a plugin host with just the badger/sqlite
// providers registered, mirroring internal/test/dbtest's identical
// registration -- lifecycle.Restore/RestoreValidated take a host as an
// explicit parameter rather than building one themselves (composition
// code's job in production; a test's job here), so every test calling
// them directly needs one of its own.
func newTestStorageHost(t *testing.T) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host
}
