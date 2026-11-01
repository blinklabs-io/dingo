//go:build !windows

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

package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// OwnerOnlyTempDir returns a test temporary directory with permissions that
// do not depend on the process umask.
func OwnerOnlyTempDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	return dir
}

// MakeDirectoryUnwritable removes write permission from a test directory and
// restores it before the test's temporary-directory cleanup runs.
//
// Both modes are derived from what the directory already has rather than
// written literally, so restoring a fixed mode could hand the caller a more
// permissive directory than it passed in.
func MakeDirectoryUnwritable(t testing.TB, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	original := info.Mode().Perm()
	require.NoError(t, os.Chmod(path, original&^0o222))
	t.Cleanup(func() {
		if err := os.Chmod(path, original); err != nil {
			t.Logf("restoring mode on %s: %v", path, err)
		}
	})
}
