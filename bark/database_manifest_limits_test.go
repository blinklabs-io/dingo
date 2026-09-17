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
	"bytes"
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRPCManifestByteLimit(t *testing.T) {
	for _, source := range []string{"local", "cloud"} {
		for _, operation := range []string{"verify", "restore"} {
			t.Run(source+"/"+operation, func(t *testing.T) {
				dataDir := t.TempDir()
				db := newDiskTestDB(t, dataDir)
				require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
				dbtest.CloseDatabase(db) //nolint:errcheck
				creator := newTestDatabaseServiceHandler(t, nil, dataDir)
				created := createAndAwaitSnapshot(t, creator, &databasev1alpha1.CreateSnapshotRequest{})
				id := created.GetSnapshotId()
				manifestPath := filepath.Join(creator.bark.config.SnapshotDir, id, lifecycle.ManifestFileName)
				original, err := os.ReadFile(manifestPath)
				require.NoError(t, err)
				targetDir := filepath.Join(t.TempDir(), "restore")
				h := newTestDatabaseServiceHandler(t, nil, targetDir)
				if source == "local" {
					h.bark.config.SnapshotDir = creator.bark.config.SnapshotDir
				} else {
					registry := lifecycle.NewDestinationRegistry()
					registry.Register("limits", func(uri *url.URL) (lifecycle.CloudDestination, error) {
						return &barkFakeCloudDestination{dir: filepath.Join(creator.bark.config.SnapshotDir, filepath.Base(uri.Path))}, nil
					})
					h.bark.config.DestinationRegistry = registry
					h.bark.config.SnapshotCloudDestination = "limits://bucket"
					// Restore's lifecycle service must use the same registry.
					h.bark.config.Lifecycle = dblifecycle.NewService(&config.Config{
						DatabasePath: targetDir,
						Plugins: config.PluginsConfig{Storage: config.StoragePluginsConfig{
							Blob:     plugin.Selection{Provider: "badger"},
							Metadata: plugin.Selection{Provider: "sqlite"},
						}},
					}, registry, nil)
				}
				invoke := func(snapshotID string) (string, error) {
					if operation == "verify" {
						resp, err := h.VerifySnapshot(context.Background(), connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{SnapshotId: snapshotID}))
						if err != nil {
							return "", err
						}
						return resp.Msg.GetOperationId(), nil
					}
					resp, err := h.Restore(context.Background(), connect.NewRequest(&databasev1alpha1.RestoreRequest{SnapshotId: snapshotID}))
					if err != nil {
						return "", err
					}
					return resp.Msg.GetOperationId(), nil
				}
				require.NoError(t, os.WriteFile(manifestPath, bytes.Repeat([]byte{'x'}, lifecycle.MaxManifestBytes+1), 0o600))
				_, err = invoke(id)
				require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
				require.ErrorIs(t, err, lifecycle.ErrManifestTooLarge)
				h.mu.Lock()
				busy := h.busy
				h.mu.Unlock()
				require.False(t, busy, "resource rejection must release operation ownership")
				_, err = invoke("missing-snapshot")
				require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
				require.NoError(t, os.WriteFile(manifestPath, original, 0o600))
				opID, err := invoke(id)
				require.NoError(t, err, "a valid manifest must pass the same RPC boundary")
				progress := waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
					op, err := h.lookupOperation(opID)
					require.NoError(t, err)
					return op.progress()
				})
				require.Equal(t, databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED, progress.GetStatus(), progress.GetMessage())
			})
		}
	}
}
