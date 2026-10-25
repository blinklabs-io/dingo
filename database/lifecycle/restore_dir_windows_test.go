//go:build windows

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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestCreatedRestoreDirectoryHasProtectedDACL(t *testing.T) {
	t.Parallel()
	owned, err := createOwnedRestoreDir(t.TempDir(), ".dingo-owned-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owned.cleanup()) })

	sd, err := windows.GetNamedSecurityInfo(
		owned.dir.path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
	)
	require.NoError(t, err)
	control, _, err := sd.Control()
	require.NoError(t, err)
	require.NotZero(t, control&windows.SE_DACL_PROTECTED)

	require.NoError(t, owned.dir.root.Mkdir("child", 0o700))
	child := filepath.Join(owned.dir.path, "child")
	childSD, err := windows.GetNamedSecurityInfo(
		child,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
	)
	require.NoError(t, err)
	childACL := childSD.String()
	for _, broadSID := range []string{
		"S-1-1-0", "S-1-5-11", "S-1-5-32-545",
		";;;WD)", ";;;AU)", ";;;BU)",
	} {
		require.False(t, strings.Contains(childACL, broadSID), childACL)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	require.NoError(t, err)
	require.Contains(t, childACL, user.User.Sid.String())
}

func TestCreatedRestoreDirectoryLocksParentChain(t *testing.T) {
	base := t.TempDir()
	ancestor := filepath.Join(base, "ancestor")
	parent := filepath.Join(ancestor, "parent")
	require.NoError(t, os.MkdirAll(parent, 0o700))
	owned, err := createOwnedRestoreDir(parent, ".dingo-owned-")
	require.NoError(t, err)

	require.Error(t, os.Rename(owned.dir.path, owned.dir.path+"-moved"))
	moved := ancestor + "-moved"
	require.Error(t, os.Rename(ancestor, moved))
	require.NoError(t, owned.cleanup())
	require.NoError(t, os.Rename(ancestor, moved))
	require.NoError(t, os.Rename(moved, ancestor))
}

func replaceOpenRestorePath(t *testing.T, oldPath, newPath string) bool {
	t.Helper()
	require.Error(t, os.Rename(oldPath, newPath))
	require.DirExists(t, oldPath)
	return false
}

func removeOpenRestorePath(t *testing.T, path string) bool {
	t.Helper()
	require.Error(t, os.Remove(path))
	require.DirExists(t, path)
	return false
}
