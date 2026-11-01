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
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testAbsentRetainedArchiveCleanup(t *testing.T, archive *bootstrapFile) {
	t.Helper()
	require.Error(t, os.Remove(archive.path), "retained archive must be delete locked")
	require.NoError(t, archive.cleanup())
	require.NoFileExists(t, archive.path)
}

func testRetainedArchiveSubstitution(t *testing.T, archive *bootstrapFile) {
	t.Helper()
	require.Error(
		t,
		os.Rename(archive.path, archive.path+".moved"),
		"retained archive must be rename locked",
	)
	require.NoError(t, archive.cleanup())
	require.NoFileExists(t, archive.path)
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

	require.Error(t, os.Rename(archivePath, archivePath+".moved"))
	result := &BootstrapResult{
		AncillaryArchivePath:  archivePath,
		ancillaryArchiveRoot:  archive.root,
		ancillaryArchiveInfo:  archive.info,
		ancillaryArchiveGuard: archive.guard,
	}
	archive.root = nil
	archive.guard = nil
	result.Cleanup(slog.New(slog.DiscardHandler))
	require.NoFileExists(t, archivePath)
}
