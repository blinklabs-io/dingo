//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dingo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// untrustedDataDirParent makes dataDir's parent writable by other users
// without the sticky bit. Any of them could then rename the live data
// directory onto the staging or backup name before a pathname removal runs,
// so every removal and rename in the live restore must refuse to start.
func untrustedDataDirParent(t *testing.T, dataDir string) {
	t.Helper()
	if os.Getuid() == 0 {
		t.Skip("root bypasses the directory permissions this exercises")
	}
	parent := filepath.Dir(dataDir)
	info, err := os.Stat(parent)
	require.NoError(t, err)
	//nolint:gosec // the untrusted mode under test
	require.NoError(t, os.Chmod(parent, 0o777))
	t.Cleanup(func() { _ = os.Chmod(parent, info.Mode().Perm()) })
}

func TestLiveRestoreRefusesUntrustedDataDirParent(t *testing.T) {
	t.Parallel()
	n, _ := newLiveLifecycleTestNode(t, 1)
	stagingDir := n.config.dataDir + restoreStagingSuffix
	writeMarkerFile(t, stagingDir, "earlier-attempt")
	untrustedDataDirParent(t, n.config.dataDir)

	_, err := n.Restore(
		context.Background(), filepath.Join(t.TempDir(), "missing-snapshot"),
	)
	require.Error(t, err)
	requireMarkerFile(t, stagingDir, "earlier-attempt")
	require.NoError(t, n.ctx.Err())
}

func TestSwapInRestoredDataDirRefusesUntrustedParent(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	stagingDir := dataDir + restoreStagingSuffix
	backupDir := dataDir + preRestoreBackupSuffix
	writeMarkerFile(t, dataDir, "original")
	writeMarkerFile(t, stagingDir, "restored")
	writeMarkerFile(t, backupDir, "earlier-backup")
	untrustedDataDirParent(t, dataDir)

	_, err := newSwapTestNode(t, dataDir).swapInRestoredDataDir(stagingDir)
	require.Error(t, err)
	require.NotErrorIs(t, err, errRestoreSwapUnrecoverable)
	requireMarkerFile(t, dataDir, "original")
	requireMarkerFile(t, stagingDir, "restored")
	requireMarkerFile(t, backupDir, "earlier-backup")
}

func TestFinalizeSuccessfulRestoreRefusesUntrustedParent(t *testing.T) {
	t.Parallel()
	dataDir := filepath.Join(t.TempDir(), "data")
	backupDir := dataDir + preRestoreBackupSuffix
	writeMarkerFile(t, dataDir, "restored")
	writeMarkerFile(t, backupDir, "original")
	untrustedDataDirParent(t, dataDir)

	err := finalizeSuccessfulRestore(func() error { return nil }, backupDir)
	require.Error(t, err)
	requireMarkerFile(t, backupDir, "original")
}

func TestReconcileInterruptedLiveRestoreSwapRefusesUntrustedParent(
	t *testing.T,
) {
	t.Parallel()
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	backupDir := dataDir + preRestoreBackupSuffix
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, dataDir, "restored")
	writeMarkerFile(t, backupDir, "original")
	writeMarkerFile(t, stagingDir, "leftover")
	untrustedDataDirParent(t, dataDir)

	n := newSwapTestNode(t, dataDir)
	require.Error(t, n.reconcileInterruptedLiveRestoreSwap())
	requireMarkerFile(t, dataDir, "restored")
	requireMarkerFile(t, backupDir, "original")
	requireMarkerFile(t, stagingDir, "leftover")
}

func TestReconcileInterruptedLiveRestoreSwapRollbackRefusesUntrustedParent(
	t *testing.T,
) {
	t.Parallel()
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	backupDir := dataDir + preRestoreBackupSuffix
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, backupDir, "original")
	writeMarkerFile(t, stagingDir, "restored")
	untrustedDataDirParent(t, dataDir)

	n := newSwapTestNode(t, dataDir)
	require.Error(t, n.reconcileInterruptedLiveRestoreSwap())
	requireNoDir(t, dataDir)
	requireMarkerFile(t, backupDir, "original")
	requireMarkerFile(t, stagingDir, "restored")
}

func TestRemoveConfirmedRestoreBackupRefusesUntrustedParent(t *testing.T) {
	t.Parallel()
	dataDir := filepath.Join(t.TempDir(), "data")
	backupDir := dataDir + preRestoreBackupSuffix
	writeMarkerFile(t, dataDir, "restored")
	writeMarkerFile(t, backupDir, "original")
	untrustedDataDirParent(t, dataDir)

	newSwapTestNode(t, dataDir).removeConfirmedRestoreBackup()
	requireMarkerFile(t, backupDir, "original")
}

func TestSwapInRestoredDataDirRefusesUntrustedParentWithoutBackup(
	t *testing.T,
) {
	t.Parallel()
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, dataDir, "original")
	writeMarkerFile(t, stagingDir, "restored")
	untrustedDataDirParent(t, dataDir)

	_, err := newSwapTestNode(t, dataDir).swapInRestoredDataDir(stagingDir)
	require.Error(t, err)
	require.NotErrorIs(t, err, errRestoreSwapUnrecoverable)
	requireMarkerFile(t, dataDir, "original")
	requireMarkerFile(t, stagingDir, "restored")
	requireNoDir(t, dataDir+preRestoreBackupSuffix)
}
