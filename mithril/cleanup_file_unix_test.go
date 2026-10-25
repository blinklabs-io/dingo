//go:build unix

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package mithril

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAbsentRetainedArchiveCleanup(t *testing.T, archive *bootstrapFile) {
	t.Helper()
	require.NoError(t, os.Remove(archive.path))
	require.NoError(t, archive.cleanup())
}

func testRetainedArchiveSubstitution(t *testing.T, archive *bootstrapFile) {
	t.Helper()
	moved := archive.path + ".moved"
	require.NoError(t, os.Rename(archive.path, moved))
	require.NoError(t, os.WriteFile(archive.path, []byte("keep"), 0o600))
	err := archive.cleanup()
	require.ErrorIs(t, err, errBootstrapCleanupIdentity)
	data, err := os.ReadFile(archive.path)
	require.NoError(t, err)
	assert.Equal(t, []byte("keep"), data)
}

func testBootstrapResultArchiveProtection(t *testing.T) {
	t.Helper()
	parent := t.TempDir()
	archivePath := filepath.Join(parent, "ancillary.tar.zst")
	require.NoError(t, os.WriteFile(archivePath, []byte("ours"), 0o600))
	archiveRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	archive, err := retainBootstrapFile(archivePath, archiveRoot)
	require.NoError(t, err)

	movedArchive := filepath.Join(parent, "moved.tar.zst")
	require.NoError(t, os.Rename(archivePath, movedArchive))
	require.NoError(t, os.WriteFile(archivePath, []byte("keep"), 0o600))

	result := &BootstrapResult{
		AncillaryArchivePath:  archivePath,
		ancillaryArchiveRoot:  archive.root,
		ancillaryArchiveInfo:  archive.info,
		ancillaryArchiveGuard: archive.guard,
	}
	archive.root = nil
	archive.guard = nil
	result.Cleanup(slog.New(slog.DiscardHandler))

	data, err := os.ReadFile(archivePath)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	data, err = os.ReadFile(movedArchive)
	require.NoError(t, err)
	require.Equal(t, []byte("ours"), data)
}

func TestRetainBootstrapFileRejectsWritableParent(t *testing.T) {
	t.Parallel()
	grandparent := t.TempDir()
	parent := filepath.Join(grandparent, "shared")
	require.NoError(t, os.Mkdir(parent, 0o777))
	require.NoError(t, os.Chmod(parent, 0o777))
	path := filepath.Join(parent, "archive")
	require.NoError(t, os.WriteFile(path, []byte("ours"), 0o600))
	root, err := os.OpenRoot(parent)
	require.NoError(t, err)

	_, err = retainBootstrapFile(path, root)
	require.ErrorContains(t, err, "writable by group or other")
}
