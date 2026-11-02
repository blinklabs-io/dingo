package lifecycle

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// A lower-trust local writer that can create the recovery-directory sibling
// can also create the staging-directory sibling. RecoverInterruptedRemoteRestore
// passes that staging directory as targetDataDir, which restoreRecoveryMarkerDirs
// treats as a trusted marker location, so a marker planted there authenticates a
// forged record.
func TestAuthenticateRejectsMarkerInStagingSibling(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dataDir := filepath.Join(parent, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o700))
	stagingDir := dataDir + ".restore-staging"
	require.NoError(t, os.MkdirAll(stagingDir, 0o755))
	recoveryDir := RestoreRecoveryDir(stagingDir)
	require.NoError(t, os.MkdirAll(recoveryDir, 0o700))

	opID := strings.Repeat("ab", restoreRecoveryIDBytes)
	record := restoreRecoveryRecord{
		OperationID:     opID,
		BlobPlugin:      "s3",
		MetadataPlugin:  "postgres",
		StorageMode:     "core",
		MetadataMutated: true,
	}
	data, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(recoveryDir, restoreRecoveryFileName),
		append(data, '\n'), 0o600,
	))
	// Marker only in the untrusted staging sibling, never in dataDir or the
	// pre-restore backup.
	require.NoError(t, os.WriteFile(
		filepath.Join(stagingDir, restoreRecoveryMarkerName),
		[]byte(opID+"\n"), 0o600,
	))

	rollback, _, err := readRestoreRecovery(recoveryDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rollback.dirHandle.close()) })
	cfg := RestoreStorageConfig{
		ActiveDataDir:      dataDir,
		RecoveryMarkerDirs: []string{dataDir, dataDir + ".pre-restore"},
	}
	rollback.markerDirs = restoreRecoveryMarkerDirs(cfg, stagingDir)
	require.Error(t, rollback.authenticate(),
		"marker in the sibling staging directory must not authenticate a record")
}

// removeDir deletes the recovery directory before it removes the operation
// marker. A crash (or marker-removal failure) in that window leaves a marker
// with no recovery directory; RecoverInterruptedRestore's incomplete-record
// branch only cleans markers when the directory still exists, and the next
// marker write is O_EXCL, so every later Restore fails until an operator
// removes the file by hand.
func TestOrphanMarkerWithoutRecoveryDirIsCleanedUp(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dataDir := filepath.Join(parent, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o700))
	stagingDir := dataDir + ".restore-staging"
	recoveryDir := RestoreRecoveryDir(stagingDir)
	marker := filepath.Join(dataDir, restoreRecoveryMarkerName)
	require.NoError(t, os.WriteFile(
		marker, []byte(strings.Repeat("cd", restoreRecoveryIDBytes)+"\n"), 0o600,
	))

	cfg := RestoreStorageConfig{
		ActiveDataDir:      dataDir,
		RecoveryMarkerDirs: []string{dataDir, dataDir + ".pre-restore"},
	}
	host := plugin.NewHost()
	defer host.Stop(context.Background()) //nolint:errcheck
	require.NoError(t, RecoverInterruptedRestore(
		context.Background(), host, recoveryDir, stagingDir, cfg, false,
	))

	_, statErr := os.Stat(marker)
	require.True(t, os.IsNotExist(statErr),
		"orphaned marker survived a recovery scan with no recovery directory")
	require.NoError(t, writeRestoreRecoveryMarker(dataDir, strings.Repeat("ef", restoreRecoveryIDBytes)),
		"a fresh restore must be able to write its marker")
}
