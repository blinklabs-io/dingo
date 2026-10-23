package lifecycle

import (
	"context"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// A marker whose recovery directory is already gone is never cleaned:
// RecoverInterruptedRestore only removes markers when the directory still
// exists (restore.go:1547). prepareRestore then writes the next marker with
// O_EXCL (restore.go:212) onto the same path and every later durable restore
// fails with "file exists".
func TestStaleMarkerWithoutRecoveryDirWedgesNextRestore(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dataDir := parent
	stagingDir := dataDir + ".restore-staging"
	recoveryDir := RestoreRecoveryDir(stagingDir)

	cfg := RestoreStorageConfig{
		ActiveDataDir:      dataDir,
		RecoveryMarkerDirs: []string{dataDir, dataDir + ".pre-restore"},
	}
	host := plugin.NewHost()
	defer host.Stop(context.Background()) //nolint:errcheck

	require.NoError(t, writeRestoreRecoveryMarker(dataDir, strings.Repeat("ab", restoreRecoveryIDBytes)))
	require.NoError(t, RecoverInterruptedRestore(
		context.Background(), host, recoveryDir, stagingDir, cfg, false,
	))
	// A fresh marker for a new restore must succeed; it fails on the stale
	// marker left behind above.
	require.NoError(t, writeRestoreRecoveryMarker(dataDir, strings.Repeat("cd", restoreRecoveryIDBytes)))
}
