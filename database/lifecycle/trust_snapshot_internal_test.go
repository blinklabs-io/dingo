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
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// trustSnapshots holds one unkeyed and one keyed snapshot of a database with a
// single block, each built once per test binary. The trust tests only need a
// valid snapshot in a particular place with particular permissions, so each
// gets a copy instead of opening a database and taking a snapshot of its own.
var trustSnapshots struct {
	mu   sync.Mutex
	dirs map[bool]string // keyed -> parent of the "snap" directory
}

func init() {
	RegisterSharedFixtureCleanup(func() {
		for _, dir := range trustSnapshots.dirs {
			_ = os.RemoveAll(dir)
		}
	})
}

func sharedTrustSnapshot(t *testing.T, keyed bool) string {
	t.Helper()
	trustSnapshots.mu.Lock()
	defer trustSnapshots.mu.Unlock()
	if dir, ok := trustSnapshots.dirs[keyed]; ok {
		return filepath.Join(dir, "snap")
	}
	if trustSnapshots.dirs == nil {
		trustSnapshots.dirs = map[bool]string{}
	}
	parent, err := os.MkdirTemp("", "dingo-lifecycle-trust-")
	require.NoError(t, err)
	trustSnapshots.dirs[keyed] = parent
	var opts []ManifestOption
	if keyed {
		opts = append(opts, WithManifestKey(catalogTestTrustKey))
	}
	dir := filepath.Join(parent, "snap")
	buildTrustSnapshot(t, dir, opts...)
	return dir
}

// buildTrustSnapshot takes a real snapshot of a one-block database at dir.
func buildTrustSnapshot(t *testing.T, dir string, opts ...ManifestOption) {
	t.Helper()
	db := newRestoreInternalTestDB(t)
	require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))
	_, err := Snapshot(
		context.Background(), db, dir,
		TriggerManual, "test", "badger", "sqlite",
		opts...,
	)
	require.NoError(t, err)
}

// snapshotForTrustTest places a snapshot named "snap" at dir, signed when a
// manifest key option is supplied. The snapshot ID is bound to the directory
// name, so dir must end in "snap".
func snapshotForTrustTest(
	t *testing.T,
	dir string,
	opts ...ManifestOption,
) {
	t.Helper()
	require.Equal(t, "snap", filepath.Base(dir))
	src := sharedTrustSnapshot(t, len(opts) > 0)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(src, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, entry.Name()), data, 0o600,
		))
	}
}
