//go:build windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package mithril

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapDirectoryLocksParentForLifetime(t *testing.T) {
	base := t.TempDir()
	ancestor := filepath.Join(base, "ancestor")
	parent := filepath.Join(ancestor, "parent")
	require.NoError(t, os.MkdirAll(parent, 0o700))
	_, owned, err := createBootstrapDir(parent, "owned-*")
	require.NoError(t, err)

	require.Error(t, os.Rename(owned.path, owned.path+"-moved"))
	moved := ancestor + "-moved"
	require.Error(t, os.Rename(ancestor, moved))
	require.NoError(t, owned.cleanup())
	require.NoError(t, os.Rename(ancestor, moved))
	require.NoError(t, os.Rename(moved, ancestor))
}

func TestBootstrapDirectoryLocksComposeForNestedWork(t *testing.T) {
	base := t.TempDir()
	_, tempOwned, err := createBootstrapDir(base, "dingo-mithril-*")
	require.NoError(t, err)

	_, digestOwned, err := createBootstrapDir(tempOwned.path, "digests-*")
	require.NoError(t, err)
	_, feedOwned, err := createBootstrapDir(tempOwned.path, "chunkfeed-1-*")
	require.NoError(t, err)

	require.NoError(t, digestOwned.cleanup())
	require.NoError(t, feedOwned.cleanup())
	require.NoError(t, tempOwned.cleanup())
}
