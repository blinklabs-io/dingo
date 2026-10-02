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

package dblifecycle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

func TestManifestOptionsReadsTrustKeyFile(t *testing.T) {
	t.Parallel()

	t.Run("unset selects no key", func(t *testing.T) {
		t.Parallel()
		opts, err := ManifestOptions(config.DatabaseLifecycleConfig{})
		require.NoError(t, err)
		require.Empty(t, opts)
	})
	t.Run("key authenticates a manifest", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "trust.key")
		require.NoError(
			t,
			os.WriteFile(path, []byte("0123456789abcdef0123\n"), 0o600),
		)
		opts, err := ManifestOptions(config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: path,
		})
		require.NoError(t, err)

		dir := t.TempDir()
		require.NoError(
			t,
			lifecycle.WriteManifest(dir, lifecycle.Manifest{}, opts...),
		)
		m, err := lifecycle.ReadManifest(dir)
		require.NoError(t, err)
		require.NoError(t, m.Authenticate(opts...))
		require.ErrorIs(t, m.Authenticate(
			lifecycle.WithManifestKey([]byte("0123456789abcdef0124")),
		), lifecycle.ErrManifestUnauthenticated)
	})
	t.Run("missing file is an error", func(t *testing.T) {
		t.Parallel()
		_, err := ManifestOptions(config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: filepath.Join(t.TempDir(), "absent"),
		})
		require.Error(t, err)
	})
	t.Run("short secret is an error", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "trust.key")
		require.NoError(t, os.WriteFile(path, []byte("short"), 0o600))
		_, err := ManifestOptions(config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: path,
		})
		require.ErrorContains(t, err, "at least")
	})
}
