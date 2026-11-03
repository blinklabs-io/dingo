//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package lifecycle

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func replaceOpenRestorePath(t *testing.T, oldPath, newPath string) bool {
	t.Helper()
	require.NoError(t, os.Rename(oldPath, newPath))
	return true
}

func removeOpenRestorePath(t *testing.T, path string) bool {
	t.Helper()
	require.NoError(t, os.Remove(path))
	return true
}
