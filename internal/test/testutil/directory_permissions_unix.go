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
func MakeDirectoryUnwritable(t testing.TB, path string) {
	t.Helper()
	require.NoError(t, os.Chmod(path, 0o555))
	t.Cleanup(func() { _ = os.Chmod(path, 0o755) })
}
