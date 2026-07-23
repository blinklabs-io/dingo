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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newDiskTestDB builds a real on-disk database (badger + sqlite), unlike
// blob_test.go's in-memory newTestDB: Snapshot/Restore need real files to
// back up (VACUUM INTO refuses an in-memory sqlite database).
func newDiskTestDB(t *testing.T, dataDir string) *database.Database {
	t.Helper()
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", dataDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", dataDir,
	))
	db, err := database.New(&database.Config{
		DataDir:        dataDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// newTestDatabaseServiceHandler wires a databaseServiceHandler with a real
// offline dblifecycle.Service (no SetLiveNode — this package cannot import
// dingo.Node, which already imports bark) whose configured data directory
// is dbDataDir. bark's own DB field only backs GetDatabaseInfo/the Archive
// service, not Snapshot/Restore/Truncate; when barkDB is nil (tests that
// don't exercise GetDatabaseInfo), an unrelated in-memory database fills
// the constructor's required-DB slot.
func newTestDatabaseServiceHandler(
	t *testing.T,
	barkDB *database.Database,
	dbDataDir string,
) *databaseServiceHandler {
	t.Helper()
	if barkDB == nil {
		barkDB = newTestDB(t)
	}
	svc := dblifecycle.NewService(&config.Config{
		DatabasePath:   dbDataDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	}, nil)
	b, err := NewBark(BarkConfig{
		DB:          barkDB,
		Lifecycle:   svc,
		SnapshotDir: t.TempDir(),
		Port:        1,
	})
	require.NoError(t, err)
	return newDatabaseServiceHandler(b)
}

func testBlock(id uint64, hashByte byte) models.Block {
	return models.Block{
		ID:     id,
		Slot:   id * 10,
		Hash:   bytes.Repeat([]byte{hashByte}, 32),
		Cbor:   []byte{0x80},
		Number: id,
		Type:   1,
	}
}

func waitForOperationStatus(
	t *testing.T,
	get func() *databasev1alpha1.OperationProgress,
) *databasev1alpha1.OperationProgress {
	t.Helper()
	var progress *databasev1alpha1.OperationProgress
	require.Eventually(t, func() bool {
		progress = get()
		switch progress.GetStatus() {
		case databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
			databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED,
			databasev1alpha1.OperationStatus_OPERATION_STATUS_CANCELLED:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "operation must reach a terminal state")
	return progress
}

// createAndAwaitSnapshot drives CreateSnapshot to completion and returns
// the response, failing the test if the snapshot doesn't complete.
func createAndAwaitSnapshot(
	t *testing.T,
	h *databaseServiceHandler,
	req *databasev1alpha1.CreateSnapshotRequest,
) *databasev1alpha1.CreateSnapshotResponse {
	t.Helper()
	createResp, err := h.CreateSnapshot(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	progress := waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
		statusResp, err := h.GetSnapshotStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetSnapshotStatusRequest{
				OperationId: createResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		return statusResp.Msg.GetProgress()
	})
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"snapshot message: %s", progress.GetMessage(),
	)
	return createResp.Msg
}

// TestCreateSnapshotAndGetSnapshotStatus verifies that CreateSnapshot
// completes and GetSnapshotStatus reports the same snapshot ID and a real manifest.
func TestCreateSnapshotAndGetSnapshotStatus(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)

	createResp, err := h.CreateSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CreateSnapshotRequest{Name: "test"}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, createResp.Msg.GetOperationId())
	require.NotEmpty(t, createResp.Msg.GetSnapshotId())

	progress := waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
		statusResp, err := h.GetSnapshotStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetSnapshotStatusRequest{
				OperationId: createResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		require.Equal(t, createResp.Msg.GetSnapshotId(), statusResp.Msg.GetSnapshotId())
		return statusResp.Msg.GetProgress()
	})
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
	)
	require.FileExists(t, filepath.Join(
		h.bark.config.SnapshotDir,
		createResp.Msg.GetSnapshotId(),
		"manifest.json",
	))
}

// TestGetSnapshotStatusUnknownOperationReturnsNotFound verifies that an
// unrecognized operation ID returns CodeNotFound.
func TestGetSnapshotStatusUnknownOperationReturnsNotFound(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	_, err := h.GetSnapshotStatus(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetSnapshotStatusRequest{
			OperationId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestCreateSnapshotRejectsConcurrentOperation verifies that
// CreateSnapshot refuses to start while another operation is already in flight.
func TestCreateSnapshotRejectsConcurrentOperation(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	// Deterministically simulate an in-flight operation rather than
	// racing a real one, which a tiny test database could complete
	// before a concurrent call ever lands.
	h.busy = true

	_, err := h.CreateSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CreateSnapshotRequest{}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// TestTruncateRejectsInvalidTarget verifies that Truncate rejects both a
// nil target and a request with more than one target field set.
func TestTruncateRejectsInvalidTarget(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())

	t.Run("nil target", func(t *testing.T) {
		_, err := h.Truncate(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.TruncateRequest{}),
		)
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("multiple fields set", func(t *testing.T) {
		slot := uint64(10)
		blockNumber := uint64(1)
		_, err := h.Truncate(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.TruncateRequest{
				Target: &databasev1alpha1.BlockRef{
					Slot:        &slot,
					BlockNumber: &blockNumber,
				},
			}),
		)
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

// TestTruncateAndGetTruncateStatus verifies that Truncate completes and
// reports the correct number of blocks removed via GetTruncateStatus.
func TestTruncateAndGetTruncateStatus(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	var last models.Block
	for id := uint64(1); id <= 3; id++ {
		last = testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(last, nil))
	}
	// BlockCreate only writes blob/metadata rows; the tip is a separate
	// record lifecycle.Truncate's target resolution reads from, so it
	// must be set explicitly to match the last block created.
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)

	blockNumber := uint64(1)
	truncResp, err := h.Truncate(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.TruncateRequest{
			Target: &databasev1alpha1.BlockRef{BlockNumber: &blockNumber},
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, truncResp.Msg.GetOperationId())

	progress := waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
		statusResp, err := h.GetTruncateStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetTruncateStatusRequest{
				OperationId: truncResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		return statusResp.Msg.GetProgress()
	})
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"truncate message: %s", progress.GetMessage(),
	)

	statusResp, err := h.GetTruncateStatus(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetTruncateStatusRequest{
			OperationId: truncResp.Msg.GetOperationId(),
		}),
	)
	require.NoError(t, err)
	// Target was block 1 of a 3-block chain (tip at block 3): blocks 2
	// and 3 removed.
	require.Equal(t, uint64(2), statusResp.Msg.GetBlocksRemoved())
}

// TestGetDatabaseInfoReturnsTipSizeBytesAndBlockCount verifies that
// GetDatabaseInfo reports the real tip, on-disk size, block count, and oldest slot.
func TestGetDatabaseInfoReturnsTipSizeBytesAndBlockCount(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	for id := uint64(1); id <= 3; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 30},
		BlockNumber: 3,
	}, nil))

	h := newTestDatabaseServiceHandler(t, db, t.TempDir())

	resp, err := h.GetDatabaseInfo(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(30), resp.Msg.GetTip().GetSlot())
	require.Equal(t, uint64(3), resp.Msg.GetTip().GetBlockNumber())
	require.False(t, resp.Msg.GetOperationInProgress())
	// Real badger+sqlite stores with a block written must report a
	// nonzero combined on-disk size — this is the actual regression
	// check for GetDatabaseInfo summing db.Blob().DiskSize() and
	// db.Metadata().DiskSize() instead of leaving SizeBytes at 0.
	require.NotZero(t, resp.Msg.GetSizeBytes())
	// testBlock(id, ...) sets Slot: id*10, so the 3 blocks created above
	// land at slots 10/20/30 — block_count=3, oldest_slot=10.
	require.Equal(t, uint64(3), resp.Msg.GetBlockCount())
	require.Equal(t, uint64(10), resp.Msg.GetOldestSlot())
}

// TestListSnapshotsReturnsCreatedSnapshotWithLabel verifies that
// ListSnapshots surfaces a created snapshot's name, description, size, and checksum.
func TestListSnapshotsReturnsCreatedSnapshotWithLabel(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{
		Name:        "nightly",
		Description: "pre-hardfork backup",
	})

	listResp, err := h.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, listResp.Msg.GetSnapshots(), 1)
	info := listResp.Msg.GetSnapshots()[0]
	require.Equal(t, created.GetSnapshotId(), info.GetSnapshotId())
	require.Equal(t, "nightly", info.GetName())
	require.Equal(t, "pre-hardfork backup", info.GetDescription())
	require.NotZero(t, info.GetSizeBytes())
	require.NotEmpty(t, info.GetChecksum())
}

// TestListSnapshotsEmptyWhenNoneTaken verifies that ListSnapshots returns
// an empty list and no page token when no snapshot has been taken.
func TestListSnapshotsEmptyWhenNoneTaken(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	listResp, err := h.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Empty(t, listResp.Msg.GetSnapshots())
	require.Empty(t, listResp.Msg.GetNextPageToken())
}

// TestListSnapshotsPaginates verifies that ListSnapshots splits results
// across pages and the second page's token/results are correct.
func TestListSnapshotsPaginates(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	for range 3 {
		createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})
	}

	page1, err := h.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{PageSize: 2}),
	)
	require.NoError(t, err)
	require.Len(t, page1.Msg.GetSnapshots(), 2)
	require.NotEmpty(t, page1.Msg.GetNextPageToken())

	page2, err := h.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{
			PageSize:  2,
			PageToken: page1.Msg.GetNextPageToken(),
		}),
	)
	require.NoError(t, err)
	require.Len(t, page2.Msg.GetSnapshots(), 1)
	require.Empty(t, page2.Msg.GetNextPageToken())
}

// TestListAvailableSnapshotsMirrorsListSnapshots covers the no-cloud-
// destination-configured case: see database_cloud_test.go for the actual
// local+cloud merge behavior this RPC exists for.
func TestListAvailableSnapshotsMirrorsListSnapshots(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	resp, err := h.ListAvailableSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetSnapshots(), 1)
	require.Equal(t, created.GetSnapshotId(), resp.Msg.GetSnapshots()[0].GetSnapshotId())
}

// TestDeleteSnapshotRemovesItFromTheCatalog verifies that a deleted
// snapshot no longer appears in a subsequent ListSnapshots call.
func TestDeleteSnapshotRemovesItFromTheCatalog(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	deleteResp, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{
			SnapshotId: created.GetSnapshotId(),
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, deleteResp.Msg.GetDeletedAt())

	listResp, err := h.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Empty(t, listResp.Msg.GetSnapshots())
}

// TestDeleteSnapshotUnknownIDReturnsNotFound verifies that deleting a
// nonexistent snapshot ID returns CodeNotFound.
func TestDeleteSnapshotUnknownIDReturnsNotFound(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	_, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{
			SnapshotId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestDeleteSnapshotRejectsPathTraversal verifies that snapshot IDs
// containing path-traversal or non-leaf segments are rejected as invalid.
func TestDeleteSnapshotRejectsPathTraversal(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	for _, id := range []string{"../../etc", "..", ".", "a/b", ""} {
		_, err := h.DeleteSnapshot(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: id}),
		)
		require.Error(t, err, "snapshot_id %q must be rejected", id)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}
}

// TestVerifySnapshotSucceedsForValidSnapshot verifies that verifying a
// freshly created, uncorrupted snapshot completes successfully.
func TestVerifySnapshotSucceedsForValidSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	verifyResp, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: created.GetSnapshotId(),
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, verifyResp.Msg.GetOperationId())

	progress := waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
		statusResp, err := h.GetOperationHistory(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{}),
		)
		require.NoError(t, err)
		for _, rec := range statusResp.Msg.GetRecords() {
			if rec.GetOperationId() == verifyResp.Msg.GetOperationId() {
				return &databasev1alpha1.OperationProgress{
					OperationId: rec.GetOperationId(),
					Status:      rec.GetStatus(),
					Message:     rec.GetMessage(),
				}
			}
		}
		return &databasev1alpha1.OperationProgress{}
	})
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"verify message: %s", progress.GetMessage(),
	)
}

// TestVerifySnapshotFailsForCorruptedSnapshot verifies that verifying a
// snapshot with a corrupted blob backup reports a FAILED operation.
func TestVerifySnapshotFailsForCorruptedSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	// Corrupt the blob backup so a real restore-based verify fails.
	// Truncating to empty isn't enough: Badger's Load treats an empty
	// stream as "nothing to load" and succeeds trivially. Garbage bytes
	// fail Badger's internal length-prefix parsing instead.
	blobPath := filepath.Join(h.bark.config.SnapshotDir, created.GetSnapshotId(), "blob.bak")
	require.NoError(t, os.WriteFile(blobPath, []byte("not a valid badger backup stream"), 0o644))

	verifyResp, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: created.GetSnapshotId(),
		}),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		histResp, err := h.GetOperationHistory(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{}),
		)
		require.NoError(t, err)
		for _, rec := range histResp.Msg.GetRecords() {
			if rec.GetOperationId() == verifyResp.Msg.GetOperationId() {
				return rec.GetStatus() == databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "verify of a corrupted snapshot must fail")
}

// tamperManifestChecksum rewrites the manifest at manifestPath so its
// content no longer matches its own recorded checksum (simulating
// corruption/hand-editing), without changing the manifest's format enough
// to make it unparseable — bumping tipSlot by 1 is enough on its own.
func tamperManifestChecksum(t *testing.T, manifestPath string) {
	t.Helper()
	data, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	raw := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &raw))
	tipSlot, ok := raw["tipSlot"].(float64)
	require.True(t, ok, "manifest missing tipSlot field")
	raw["tipSlot"] = tipSlot + 1
	tampered, err := json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, tampered, 0o644))
}

// TestVerifySnapshotOfTamperedManifestReturnsDataLoss guards against a
// misleading-error finding (dingo#1651 follow-up): a snapshot whose
// manifest.json was corrupted/hand-edited (so it fails checksum
// validation) used to be indistinguishable from a snapshot ID that never
// existed at all — both surfaced as CodeNotFound. resolveSnapshotSource
// now checks errors.Is against lifecycle.ErrManifestCorrupted first, so a
// snapshot that IS there but unusable is reported as such.
func TestVerifySnapshotOfTamperedManifestReturnsDataLoss(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	manifestPath := filepath.Join(
		h.bark.config.SnapshotDir, created.GetSnapshotId(), "manifest.json",
	)
	tamperManifestChecksum(t, manifestPath)

	_, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: created.GetSnapshotId(),
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeDataLoss, connect.CodeOf(err))
	require.Contains(t, err.Error(), "corrupted")
}

// TestDeleteSnapshotRemovesLocalSnapshotWithCorruptedManifest guards
// against the other half of the same finding: before this fix,
// DeleteSnapshot gated local existence on a readable manifest too, so an
// operator could never clean up a corrupted local snapshot directory
// through this API even though it was still sitting on disk.
func TestDeleteSnapshotRemovesLocalSnapshotWithCorruptedManifest(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	snapshotDir := filepath.Join(h.bark.config.SnapshotDir, created.GetSnapshotId())
	tamperManifestChecksum(t, filepath.Join(snapshotDir, "manifest.json"))

	deleteResp, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{
			SnapshotId: created.GetSnapshotId(),
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, deleteResp.Msg.GetDeletedAt())

	_, statErr := os.Stat(snapshotDir)
	require.True(t, os.IsNotExist(statErr))
}

// TestVerifySnapshotUnknownIDReturnsNotFound verifies that verifying a
// nonexistent snapshot ID returns CodeNotFound.
func TestVerifySnapshotUnknownIDReturnsNotFound(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	_, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestGetOperationHistoryReturnsPastOperations verifies that a completed
// snapshot operation appears in the history with the right type and status.
func TestGetOperationHistoryReturnsPastOperations(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	histResp, err := h.GetOperationHistory(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, histResp.Msg.GetRecords(), 1)
	rec := histResp.Msg.GetRecords()[0]
	require.Equal(t, created.GetOperationId(), rec.GetOperationId())
	require.Equal(
		t,
		databasev1alpha1.OperationType_OPERATION_TYPE_SNAPSHOT,
		rec.GetType(),
	)
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		rec.GetStatus(),
	)
}

// TestGetOperationHistoryFiltersByTypeAndStatus verifies that filtering
// by a type or status that doesn't match the recorded operation returns nothing.
func TestGetOperationHistoryFiltersByTypeAndStatus(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	restoreType := databasev1alpha1.OperationType_OPERATION_TYPE_RESTORE
	byType, err := h.GetOperationHistory(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{
			TypeFilter: &restoreType,
		}),
	)
	require.NoError(t, err)
	require.Empty(t, byType.Msg.GetRecords())

	failedStatus := databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED
	byStatus, err := h.GetOperationHistory(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{
			StatusFilter: &failedStatus,
		}),
	)
	require.NoError(t, err)
	require.Empty(t, byStatus.Msg.GetRecords())
}

// TestGetOperationHistoryPaginates verifies that operation-history
// records split across pages the same way ListSnapshots does.
func TestGetOperationHistoryPaginates(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	for range 3 {
		createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})
	}

	page1, err := h.GetOperationHistory(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{PageSize: 2}),
	)
	require.NoError(t, err)
	require.Len(t, page1.Msg.GetRecords(), 2)
	require.NotEmpty(t, page1.Msg.GetNextPageToken())

	page2, err := h.GetOperationHistory(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{
			PageSize:  2,
			PageToken: page1.Msg.GetNextPageToken(),
		}),
	)
	require.NoError(t, err)
	require.Len(t, page2.Msg.GetRecords(), 1)
	require.Empty(t, page2.Msg.GetNextPageToken())
}

// TestCancelOperationUnknownIDReturnsNotFound verifies that cancelling a
// nonexistent operation ID returns CodeNotFound.
func TestCancelOperationUnknownIDReturnsNotFound(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	_, err := h.CancelOperation(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
			OperationId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestCancelOperationCancelsContextAndMarksCancelled exercises
// requestCancel/complete's contract directly against a manually started
// operation, rather than racing a real (and, in tests, near-instant)
// Snapshot/Restore/Truncate call: startOperation is called without
// spawning the usual background goroutine, so cancellation can be
// observed deterministically instead of depending on catching the
// operation mid-flight.
func TestCancelOperationCancelsContextAndMarksCancelled(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	op, ctx, err := h.startOperation(databasev1alpha1.OperationType_OPERATION_TYPE_SNAPSHOT)
	require.NoError(t, err)

	cancelResp, err := h.CancelOperation(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
			OperationId: op.id,
		}),
	)
	require.NoError(t, err)
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_PENDING,
		cancelResp.Msg.GetStatus(),
		"response reports status at the moment cancellation was accepted, not the terminal state",
	)
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	// Simulate the operation's own goroutine noticing ctx.Err() and
	// completing, the same way CreateSnapshot/Restore/Truncate's
	// goroutines do.
	op.complete(ctx.Err(), 0)

	progress := op.progress()
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_CANCELLED,
		progress.GetStatus(),
	)
}

// TestCancelOperationOnAlreadyCompletedOperationIsANoOp verifies that
// cancelling an already-completed operation just reports its COMPLETED status.
func TestCancelOperationOnAlreadyCompletedOperationIsANoOp(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	db.Close()

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})

	cancelResp, err := h.CancelOperation(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
			OperationId: created.GetOperationId(),
		}),
	)
	require.NoError(t, err)
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		cancelResp.Msg.GetStatus(),
	)
}
