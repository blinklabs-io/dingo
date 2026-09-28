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

package fsyncdir_test

import (
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	"github.com/stretchr/testify/require"
)

// TestSyncSucceedsOnExistingDirectory verifies the common case: syncing a
// real, existing directory (with or without entries in it) succeeds on
// every platform this builds for.
func TestSyncSucceedsOnExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, fsyncdir.Sync(dir))
}

// TestSyncErrorsOnMissingDirectory verifies Sync surfaces a real error
// for a path that doesn't exist, rather than silently succeeding --
// distinguishing "nothing to sync because the caller made a mistake"
// from the platform-specific best-effort behavior Sync applies once a
// directory handle is actually obtained.
func TestSyncErrorsOnMissingDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	require.Error(t, fsyncdir.Sync(missing))
}
