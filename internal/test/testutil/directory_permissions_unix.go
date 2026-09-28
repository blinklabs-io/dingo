//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// MakeDirectoryUnwritable removes write permission from a test directory and
// restores it before the test's temporary-directory cleanup runs.
//
// Both modes are derived from what the directory already has rather than
// written literally: t.TempDir() creates 0o700, so restoring a fixed 0o755
// would hand the caller a more permissive directory than it passed in, and
// clearing to a fixed 0o555 would do the same for anything created stricter.
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
