//go:build windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package securefs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestWindowsPathLockBlocksAncestorRename(t *testing.T) {
	base := t.TempDir()
	ancestor := filepath.Join(base, "ancestor")
	leaf := filepath.Join(ancestor, "leaf")
	require.NoError(t, os.MkdirAll(leaf, 0o700))
	locked, err := LockWindowsPath(leaf)
	require.NoError(t, err)

	moved := ancestor + "-moved"
	require.Error(t, os.Rename(ancestor, moved))
	require.NoError(t, locked.Close())
	require.NoError(t, os.Rename(ancestor, moved))
	require.NoError(t, os.Rename(moved, ancestor))
}

func TestWindowsPathLockBlocksAncestorWriteHandle(t *testing.T) {
	base := t.TempDir()
	ancestor := filepath.Join(base, "ancestor")
	leaf := filepath.Join(ancestor, "leaf")
	require.NoError(t, os.MkdirAll(leaf, 0o700))
	locked, err := LockWindowsPath(leaf)
	require.NoError(t, err)

	pathPtr, err := windows.UTF16PtrFromString(ancestor)
	require.NoError(t, err)
	handle, err := windows.CreateFile(
		pathPtr,
		windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	require.Error(t, err, "ancestor lock must reject handles that can mutate metadata")
	if handle != windows.InvalidHandle {
		require.NoError(t, windows.CloseHandle(handle))
	}
	require.NoError(t, locked.Close())

	handle, err = windows.CreateFile(
		pathPtr,
		windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	require.NoError(t, err)
	require.NoError(t, windows.CloseHandle(handle))
}

func TestWindowsPathLockRejectsReparseAncestor(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	require.NoError(t, os.MkdirAll(filepath.Join(target, "leaf"), 0o700))
	link := filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("directory reparse creation unsupported: %v", err)
	}

	locked, err := LockWindowsPath(filepath.Join(link, "leaf"))
	require.Error(t, err)
	require.Nil(t, locked)
}
