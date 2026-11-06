// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bark

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestDeleteSnapshotLeavesNonSnapshotEntriesAlone(t *testing.T) {
	snapshotDir := t.TempDir()
	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, t.TempDir())
	h.bark.config.SnapshotDir = snapshotDir

	catalog := filepath.Join(snapshotDir, ".dingo-snapshot-catalog-v1.sqlite")
	require.NoError(t, os.WriteFile(catalog, []byte("catalog"), 0o600))
	verifyDir := filepath.Join(snapshotDir, ".dingo-verify-snapshot-123")
	require.NoError(t, os.Mkdir(verifyDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(verifyDir, lifecycle.ManifestFileName),
		[]byte("{}"), 0o600,
	))
	plainDir := filepath.Join(snapshotDir, "not-a-snapshot")
	require.NoError(t, os.Mkdir(plainDir, 0o755))
	plainFile := filepath.Join(snapshotDir, "some-file")
	require.NoError(t, os.WriteFile(plainFile, []byte("data"), 0o600))

	for _, id := range []string{
		".dingo-snapshot-catalog-v1.sqlite",
		".dingo-verify-snapshot-123",
		"not-a-snapshot",
		"some-file",
	} {
		_, err := h.DeleteSnapshot(
			context.Background(),
			connect.NewRequest(
				&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: id},
			),
		)
		require.Error(t, err, "snapshot id %q", id)
		require.Contains(t, []connect.Code{
			connect.CodeInvalidArgument,
			connect.CodeNotFound,
			connect.CodeFailedPrecondition,
		}, connect.CodeOf(err), "snapshot id %q", id)
	}

	require.FileExists(t, catalog)
	require.DirExists(t, verifyDir)
	require.DirExists(t, plainDir)
	require.FileExists(t, plainFile)
}

func TestDeleteSnapshotRemovesSnapshotWithCorruptManifest(t *testing.T) {
	snapshotDir := t.TempDir()
	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, t.TempDir())
	h.bark.config.SnapshotDir = snapshotDir

	corruptDir := filepath.Join(snapshotDir, "corrupt-snapshot")
	require.NoError(t, os.Mkdir(corruptDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(corruptDir, lifecycle.ManifestFileName),
		[]byte("not json"), 0o600,
	))

	_, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.DeleteSnapshotRequest{
				SnapshotId: "corrupt-snapshot",
			},
		),
	)
	require.NoError(t, err)
	require.NoDirExists(t, corruptDir)
}
