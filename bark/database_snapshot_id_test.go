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

// Not t.Parallel: bark's snapshot RPC tests share the sequential database
// handler fixtures.
func TestSnapshotRPCRejectsSnapshotCopiedUnderNewerID(t *testing.T) {
	for _, source := range []string{"local", "cloud"} {
		for _, operation := range []string{"verify", "restore"} {
			t.Run(source+"/"+operation, func(t *testing.T) {
				dataDir := t.TempDir()
				db := newDiskTestDB(t, dataDir)
				require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
				dbtest.CloseDatabase(db) //nolint:errcheck
				creator := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
				created := createAndAwaitSnapshot(
					t, creator, &databasev1alpha1.CreateSnapshotRequest{},
				)
				olderID := created.GetSnapshotId()
				snapshotRoot := creator.bark.config.SnapshotDir
				newerID := "newer-" + olderID
				olderDir := filepath.Join(snapshotRoot, olderID)
				newerDir := filepath.Join(snapshotRoot, newerID)
				require.NoError(t, os.Mkdir(newerDir, 0o700))
				entries, err := os.ReadDir(olderDir)
				require.NoError(t, err)
				for _, entry := range entries {
					data, err := os.ReadFile(filepath.Join(olderDir, entry.Name()))
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(
						filepath.Join(newerDir, entry.Name()), data, 0o600,
					))
				}

				targetDir := filepath.Join(t.TempDir(), "restore")
				h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, targetDir)
				if source == "local" {
					h.bark.config.SnapshotDir = snapshotRoot
				} else {
					registry := lifecycle.NewDestinationRegistry()
					registry.Register("replay", func(uri *url.URL) (lifecycle.CloudDestination, error) {
						return &barkFakeCloudDestination{
							dir: filepath.Join(snapshotRoot, filepath.Base(uri.Path)),
						}, nil
					})
					h.bark.config.DestinationRegistry = registry
					h.bark.config.SnapshotCloudDestination = "replay://bucket"
					h.bark.config.Lifecycle = dblifecycle.NewService(&config.Config{
						DatabasePath: targetDir,
						DatabaseLifecycle: config.DatabaseLifecycleConfig{
							SnapshotTrustKeyFile: testSnapshotTrustKeyFile(t),
						},
						Plugins: config.PluginsConfig{Storage: config.StoragePluginsConfig{
							Blob:     plugin.Selection{Provider: "badger"},
							Metadata: plugin.Selection{Provider: "sqlite"},
						}},
					}, registry, nil)
				}
				invoke := func(snapshotID string) (string, error) {
					if operation == "verify" {
						resp, err := h.VerifySnapshot(
							context.Background(),
							connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{SnapshotId: snapshotID}),
						)
						if err != nil {
							return "", err
						}
						return resp.Msg.GetOperationId(), nil
					}
					resp, err := h.Restore(
						context.Background(),
						connect.NewRequest(&databasev1alpha1.RestoreRequest{SnapshotId: snapshotID}),
					)
					if err != nil {
						return "", err
					}
					return resp.Msg.GetOperationId(), nil
				}
				operationStatus := func(opID string) *databasev1alpha1.OperationProgress {
					return waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
						op, err := h.lookupOperation(opID)
						require.NoError(t, err)
						return op.progress()
					})
				}

				if source == "local" {
					// Resolution reads the local manifest with the trust
					// key, so the foreign-ID copy is refused up front.
					_, err := invoke(newerID)
					require.Error(t, err)
					require.Equal(t, connect.CodeDataLoss, connect.CodeOf(err))
					require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
				} else {
					// Remove the local copies so resolution reaches the cloud.
					h.bark.config.SnapshotDir = t.TempDir()
					_, err := invoke(newerID)
					require.Error(t, err)
					require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
				}

				opID, err := invoke(olderID)
				require.NoError(t, err, "the snapshot under its own ID must still pass")
				progress := operationStatus(opID)
				require.Equal(
					t, databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
					progress.GetStatus(), progress.GetMessage(),
				)
			})
		}
	}
}
