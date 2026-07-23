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
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// barkFakeCloudDestination is a minimal stand-in for a real cloud
// destination (S3/GCS), backed by an ordinary local directory — the same
// pattern database/lifecycle/destination_test.go uses, redeclared here
// because Go test binaries are per-package: that file's "faketest" scheme
// registration only exists inside database/lifecycle's own test binary,
// not bark's.
type barkFakeCloudDestination struct {
	dir string
}

func (d *barkFakeCloudDestination) UploadDir(_ context.Context, localDir string) error {
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(localDir, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(d.dir, entry.Name()), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (d *barkFakeCloudDestination) DownloadDir(_ context.Context, localDir string) error {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(d.dir, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(localDir, entry.Name()), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (d *barkFakeCloudDestination) ListSnapshots(_ context.Context) ([]lifecycle.SnapshotEntry, error) {
	return lifecycle.ListSnapshots(d.dir)
}

func (d *barkFakeCloudDestination) FetchManifest(_ context.Context) (lifecycle.Manifest, error) {
	return lifecycle.ReadManifest(d.dir)
}

func (d *barkFakeCloudDestination) Delete(_ context.Context) error {
	return os.RemoveAll(d.dir)
}

var (
	_ lifecycle.SnapshotLister       = &barkFakeCloudDestination{}
	_ lifecycle.CloudManifestFetcher = &barkFakeCloudDestination{}
	_ lifecycle.CloudDeleter         = &barkFakeCloudDestination{}
)

var (
	barkFakeCloudMu  sync.Mutex
	barkFakeCloudDir string
)

func init() {
	lifecycle.RegisterCloudDestinationScheme(
		"barkfaketest",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			barkFakeCloudMu.Lock()
			base := barkFakeCloudDir
			barkFakeCloudMu.Unlock()
			return &barkFakeCloudDestination{
				dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
			}, nil
		},
	)
}

func setBarkFakeCloudBackingDir(t *testing.T, dir string) {
	t.Helper()
	barkFakeCloudMu.Lock()
	barkFakeCloudDir = dir
	barkFakeCloudMu.Unlock()
}

// barkFakeCloudDestinationNoDelete is identical to barkFakeCloudDestination
// (backed by the same kind of local directory) except it deliberately
// does not implement lifecycle.CloudDeleter — used to test
// DeleteSnapshot's CodeUnimplemented path for a cloud copy that exists
// but whose destination type doesn't support deletion, distinct from
// "doesn't exist at all" (CodeNotFound) or "delete failed" (CodeInternal).
type barkFakeCloudDestinationNoDelete struct {
	dir string
}

func (d *barkFakeCloudDestinationNoDelete) UploadDir(ctx context.Context, localDir string) error {
	return (&barkFakeCloudDestination{dir: d.dir}).UploadDir(ctx, localDir)
}

func (d *barkFakeCloudDestinationNoDelete) DownloadDir(ctx context.Context, localDir string) error {
	return (&barkFakeCloudDestination{dir: d.dir}).DownloadDir(ctx, localDir)
}

func (d *barkFakeCloudDestinationNoDelete) FetchManifest(
	ctx context.Context,
) (lifecycle.Manifest, error) {
	return (&barkFakeCloudDestination{dir: d.dir}).FetchManifest(ctx)
}

var _ lifecycle.CloudManifestFetcher = &barkFakeCloudDestinationNoDelete{}

var (
	barkFakeCloudNoDeleteMu  sync.Mutex
	barkFakeCloudNoDeleteDir string
)

func init() {
	lifecycle.RegisterCloudDestinationScheme(
		"barkfaketest-nodelete",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			barkFakeCloudNoDeleteMu.Lock()
			base := barkFakeCloudNoDeleteDir
			barkFakeCloudNoDeleteMu.Unlock()
			return &barkFakeCloudDestinationNoDelete{
				dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
			}, nil
		},
	)
}

func setBarkFakeCloudNoDeleteBackingDir(t *testing.T, dir string) {
	t.Helper()
	barkFakeCloudNoDeleteMu.Lock()
	barkFakeCloudNoDeleteDir = dir
	barkFakeCloudNoDeleteMu.Unlock()
}

// TestListAvailableSnapshotsMergesLocalAndCloud seeds three snapshots
// directly via database/lifecycle (bypassing bark's CreateSnapshot RPC,
// whose test harness Service doesn't thread a cloud destination through)
// to exercise mergedSnapshotCatalogPage's actual merge/dedup logic:
//   - one present both locally and in the cloud (dedup must keep the
//     local Location, not the cloud one)
//   - one present ONLY in the cloud (local directory removed after
//     upload, simulating DeleteSnapshot or manual pruning) — this is the
//     entry ListSnapshots could never surface but ListAvailableSnapshots
//     must
//   - one present ONLY locally (never uploaded)
func TestListAvailableSnapshotsMergesLocalAndCloud(t *testing.T) {
	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest://bucket/prefix"

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	localAndCloudDir := filepath.Join(snapshotDir, "local-and-cloud")
	_, err := lifecycle.SnapshotToCloud(
		context.Background(), db, localAndCloudDir,
		lifecycle.TriggerManual, "test-version", cloudDest,
	)
	require.NoError(t, err)

	cloudOnlyDir := filepath.Join(snapshotDir, "cloud-only")
	_, err = lifecycle.SnapshotToCloud(
		context.Background(), db, cloudOnlyDir,
		lifecycle.TriggerManual, "test-version", cloudDest,
	)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(cloudOnlyDir))

	localOnlyDir := filepath.Join(snapshotDir, "local-only")
	_, err = lifecycle.Snapshot(
		context.Background(), db, localOnlyDir, lifecycle.TriggerManual, "test-version",
	)
	require.NoError(t, err)

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	resp, err := h.ListAvailableSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{}),
	)
	require.NoError(t, err)

	byID := make(map[string]*databasev1alpha1.SnapshotInfo, len(resp.Msg.GetSnapshots()))
	for _, s := range resp.Msg.GetSnapshots() {
		byID[s.GetSnapshotId()] = s
	}
	require.Len(t, byID, 3)
	require.Contains(t, byID, "local-and-cloud")
	require.Contains(t, byID, "cloud-only")
	require.Contains(t, byID, "local-only")

	// Deduped entry must report its real local path, not a reconstructed
	// cloud URI.
	require.Equal(
		t,
		filepath.Join(snapshotDir, "local-and-cloud"),
		byID["local-and-cloud"].GetLocation(),
	)
	// The cloud-only entry has no local directory anymore, so its
	// Location must be the cloud URI.
	require.Equal(t, cloudDest+"/cloud-only", byID["cloud-only"].GetLocation())
	require.Equal(
		t,
		filepath.Join(snapshotDir, "local-only"),
		byID["local-only"].GetLocation(),
	)
}

func TestListAvailableSnapshotsWithoutCloudDestIsLocalOnly(t *testing.T) {
	snapshotDir := t.TempDir()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	_, err := lifecycle.Snapshot(
		context.Background(),
		db,
		filepath.Join(snapshotDir, "only-snapshot"),
		lifecycle.TriggerManual,
		"test-version",
	)
	require.NoError(t, err)

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	// SnapshotCloudDestination left empty.

	resp, err := h.ListAvailableSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetSnapshots(), 1)
	require.Equal(t, "only-snapshot", resp.Msg.GetSnapshots()[0].GetSnapshotId())
}

// ── Restore/VerifySnapshot/DeleteSnapshot cloud-fallback ──────────────────
//
// None of these three RPCs previously had a direct in-process test at
// all (only exercised indirectly, if ever, via the real-network wire
// test) — restoring requires a target data directory distinct from the
// one used to create the source snapshot, which newTestDatabaseServiceHandler's
// shared dbDataDir doesn't provide by default. These tests fix that gap
// while also covering the new cloud-fallback behavior.

func TestRestoreFromLocalSnapshot(t *testing.T) {
	sourceDataDir := t.TempDir()
	sourceDB := newDiskTestDB(t, sourceDataDir)
	block1 := testBlock(1, 0x01)
	require.NoError(t, sourceDB.BlockCreate(block1, nil))
	require.NoError(t, sourceDB.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))

	snapshotDir := t.TempDir()
	_, err := lifecycle.Snapshot(
		context.Background(), sourceDB, filepath.Join(snapshotDir, "snap1"),
		lifecycle.TriggerManual, "test-version",
	)
	require.NoError(t, err)
	sourceDB.Close()

	targetDataDir := filepath.Join(t.TempDir(), "target")
	h := newTestDatabaseServiceHandler(t, nil, targetDataDir)
	h.bark.config.SnapshotDir = snapshotDir

	restoreResp, err := h.Restore(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.RestoreRequest{SnapshotId: "snap1"}),
	)
	require.NoError(t, err)

	progress := waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
		statusResp, err := h.GetRestoreStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetRestoreStatusRequest{
				OperationId: restoreResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		return statusResp.Msg.GetProgress()
	})
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"restore message: %s", progress.GetMessage(),
	)
}

func TestRestoreFromCloudOnlySnapshot(t *testing.T) {
	sourceDataDir := t.TempDir()
	sourceDB := newDiskTestDB(t, sourceDataDir)
	block1 := testBlock(1, 0x01)
	require.NoError(t, sourceDB.BlockCreate(block1, nil))
	require.NoError(t, sourceDB.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest://bucket/prefix"

	_, err := lifecycle.SnapshotToCloud(
		context.Background(), sourceDB, filepath.Join(snapshotDir, "cloud-snap"),
		lifecycle.TriggerManual, "test-version", cloudDest,
	)
	require.NoError(t, err)
	sourceDB.Close()

	// Remove the local copy so only the cloud mirror remains — simulating
	// a snapshot ListAvailableSnapshots would surface as cloud-only.
	require.NoError(t, os.RemoveAll(filepath.Join(snapshotDir, "cloud-snap")))

	targetDataDir := filepath.Join(t.TempDir(), "target")
	h := newTestDatabaseServiceHandler(t, nil, targetDataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	restoreResp, err := h.Restore(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.RestoreRequest{SnapshotId: "cloud-snap"}),
	)
	require.NoError(t, err)

	progress := waitForOperationStatus(t, func() *databasev1alpha1.OperationProgress {
		statusResp, err := h.GetRestoreStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetRestoreStatusRequest{
				OperationId: restoreResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		return statusResp.Msg.GetProgress()
	})
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"restore message: %s", progress.GetMessage(),
	)
}

func TestRestoreUnknownIDReturnsNotFound(t *testing.T) {
	h := newTestDatabaseServiceHandler(t, nil, filepath.Join(t.TempDir(), "target"))
	_, err := h.Restore(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.RestoreRequest{SnapshotId: "does-not-exist"}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestVerifySnapshotSucceedsForCloudOnlySnapshot(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest://bucket/prefix"

	_, err := lifecycle.SnapshotToCloud(
		context.Background(), db, filepath.Join(snapshotDir, "cloud-verify"),
		lifecycle.TriggerManual, "test-version", cloudDest,
	)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(filepath.Join(snapshotDir, "cloud-verify")))

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	verifyResp, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{SnapshotId: "cloud-verify"}),
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
				return rec.GetStatus() == databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED
			}
		}
		return false
	}, databaseOperationTimeout, 10*time.Millisecond,
		"verify of a cloud-only snapshot must succeed",
	)
}

func TestDeleteSnapshotRemovesCloudOnlyCopy(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest://bucket/prefix"

	_, err := lifecycle.SnapshotToCloud(
		context.Background(), db, filepath.Join(snapshotDir, "cloud-delete"),
		lifecycle.TriggerManual, "test-version", cloudDest,
	)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(filepath.Join(snapshotDir, "cloud-delete")))

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	_, err = h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: "cloud-delete"}),
	)
	require.NoError(t, err)

	_, ok, err := lifecycle.FetchCloudManifest(
		context.Background(), lifecycle.JoinCloudURI(cloudDest, "cloud-delete"),
	)
	require.True(t, ok)
	require.Error(t, err, "cloud copy must actually be gone after delete")
}

func TestDeleteSnapshotRemovesBothLocalAndCloudCopies(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest://bucket/prefix"

	_, err := lifecycle.SnapshotToCloud(
		context.Background(), db, filepath.Join(snapshotDir, "both"),
		lifecycle.TriggerManual, "test-version", cloudDest,
	)
	require.NoError(t, err)

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	_, err = h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: "both"}),
	)
	require.NoError(t, err)

	require.NoDirExists(t, filepath.Join(snapshotDir, "both"))
	_, ok, err := lifecycle.FetchCloudManifest(
		context.Background(), lifecycle.JoinCloudURI(cloudDest, "both"),
	)
	require.True(t, ok)
	require.Error(t, err, "cloud copy must actually be gone after delete")
}

func TestDeleteSnapshotNeitherLocalNorCloudReturnsNotFound(t *testing.T) {
	setBarkFakeCloudBackingDir(t, t.TempDir())
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	h.bark.config.SnapshotCloudDestination = "barkfaketest://bucket/prefix"

	_, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: "does-not-exist"}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestDeleteSnapshotCloudDestinationWithoutDeleteSupportReturnsUnimplemented
// covers the third DeleteSnapshot outcome distinct from "not found
// anywhere" (CodeNotFound) and "delete itself failed" (CodeInternal): a
// cloud copy genuinely exists, but its destination type doesn't implement
// CloudDeleter — S3/GCS always do, but a future destination type might
// not, and this must be reported clearly rather than silently no-op'ing
// and reporting success.
func TestDeleteSnapshotCloudDestinationWithoutDeleteSupportReturnsUnimplemented(t *testing.T) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudNoDeleteBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest-nodelete://bucket/prefix"

	localDir := filepath.Join(snapshotDir, "no-delete-support")
	_, err := lifecycle.SnapshotToCloud(
		context.Background(), db, localDir,
		lifecycle.TriggerManual, "test-version", cloudDest,
	)
	require.NoError(t, err)
	// Remove the local copy so DeleteSnapshot must act on the cloud-only
	// entry rather than succeeding via the local delete alone.
	require.NoError(t, os.RemoveAll(localDir))

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	_, err = h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{
			SnapshotId: "no-delete-support",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

// TestListAvailableSnapshotsPaginatesAcrossMixedLocalAndCloud seeds two
// local-only and two cloud-only snapshots and pages through
// ListAvailableSnapshots with a page size smaller than the total count,
// proving pagination is correct over the actual merged (not just
// same-source) catalog: every ID appears exactly once across all pages,
// and the last page's next_page_token is empty.
func TestListAvailableSnapshotsPaginatesAcrossMixedLocalAndCloud(t *testing.T) {
	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest://bucket/prefix"

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	for _, name := range []string{"local-a", "local-b"} {
		_, err := lifecycle.Snapshot(
			context.Background(), db, filepath.Join(snapshotDir, name),
			lifecycle.TriggerManual, "test-version",
		)
		require.NoError(t, err)
	}
	for _, name := range []string{"cloud-a", "cloud-b"} {
		dir := filepath.Join(snapshotDir, name)
		_, err := lifecycle.SnapshotToCloud(
			context.Background(), db, dir,
			lifecycle.TriggerManual, "test-version", cloudDest,
		)
		require.NoError(t, err)
		require.NoError(t, os.RemoveAll(dir))
	}

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	seen := make(map[string]bool)
	pageToken := ""
	pages := 0
	for {
		pages++
		require.LessOrEqual(t, pages, 10, "pagination did not terminate")

		resp, err := h.ListAvailableSnapshots(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{
				PageSize:  2,
				PageToken: pageToken,
			}),
		)
		require.NoError(t, err)
		for _, s := range resp.Msg.GetSnapshots() {
			require.False(
				t, seen[s.GetSnapshotId()],
				"snapshot %q returned on more than one page", s.GetSnapshotId(),
			)
			seen[s.GetSnapshotId()] = true
		}

		pageToken = resp.Msg.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, 2, pages, "4 snapshots at page size 2 must take exactly 2 pages")

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	require.ElementsMatch(t, []string{"local-a", "local-b", "cloud-a", "cloud-b"}, ids)
}
