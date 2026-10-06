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
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// A verification restore is made under the snapshot directory, never in the
// system temp directory, and leaves nothing behind there.
func TestVerifySnapshotRestoresUnderSnapshotDir(t *testing.T) {
	// Not t.Parallel: t.Setenv.
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t, h, &databasev1alpha1.CreateSnapshotRequest{},
	)
	missing := filepath.Join(t.TempDir(), "no-such-tmp")
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(name, missing)
	}

	verifyResp, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: created.GetSnapshotId(),
		}),
	)
	require.NoError(t, err)
	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			resp, err := h.GetOperationHistory(
				context.Background(),
				connect.NewRequest(
					&databasev1alpha1.GetOperationHistoryRequest{},
				),
			)
			require.NoError(t, err)
			for _, rec := range resp.Msg.GetRecords() {
				if rec.GetOperationId() == verifyResp.Msg.GetOperationId() {
					return &databasev1alpha1.OperationProgress{
						OperationId: rec.GetOperationId(),
						Status:      rec.GetStatus(),
						Message:     rec.GetMessage(),
					}
				}
			}
			return &databasev1alpha1.OperationProgress{}
		},
	)
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"verify message: %s", progress.GetMessage(),
	)

	entries, err := os.ReadDir(h.bark.config.SnapshotDir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Equal(t, []string{created.GetSnapshotId()}, names)
}
