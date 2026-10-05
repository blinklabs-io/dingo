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
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
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

func (d *barkFakeCloudDestination) UploadDir(
	_ context.Context,
	localDir string,
) error {
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

func (d *barkFakeCloudDestination) DownloadFiles(
	_ context.Context,
	localDir string,
	files []lifecycle.DownloadFile,
) error {
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(d.dir, file.Name))
		if err != nil {
			return err
		}
		if int64(len(data)) > file.MaxBytes {
			return lifecycle.ErrDownloadTooLarge
		}
		if err := os.WriteFile(filepath.Join(localDir, file.Name), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (d *barkFakeCloudDestination) ListSnapshots(
	_ context.Context,
	opts ...lifecycle.ManifestOption,
) ([]lifecycle.SnapshotEntry, error) {
	return lifecycle.ListSnapshots(d.dir, opts...)
}

// FetchManifest mirrors the real S3/GCS destinations' contract: a missing
// manifest is wrapped in lifecycle.ErrCloudSnapshotNotFound (confirmed
// absent), distinct from any other error (a real fake-backing-dir I/O
// problem, which this fake has no way to simulate distinctly but is
// preserved unwrapped regardless).
func (d *barkFakeCloudDestination) FetchManifest(
	_ context.Context,
) (lifecycle.Manifest, error) {
	return d.fetchManifest()
}

func (d *barkFakeCloudDestination) FetchManifestWithOptions(
	_ context.Context,
	opts ...lifecycle.ManifestOption,
) (lifecycle.Manifest, error) {
	m, err := d.fetchManifest()
	if err != nil {
		return lifecycle.Manifest{}, err
	}
	if err := m.Authenticate(opts...); err != nil {
		return lifecycle.Manifest{}, err
	}
	return m, nil
}

func (d *barkFakeCloudDestination) fetchManifest() (lifecycle.Manifest, error) {
	m, err := lifecycle.ReadManifest(d.dir)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		return lifecycle.Manifest{}, fmt.Errorf(
			"%w: %w", lifecycle.ErrCloudSnapshotNotFound, err,
		)
	}
	return m, err
}

func (d *barkFakeCloudDestination) Delete(_ context.Context) error {
	return os.RemoveAll(d.dir)
}

var (
	_ lifecycle.SnapshotLister                   = &barkFakeCloudDestination{}
	_ lifecycle.CloudManifestFetcher             = &barkFakeCloudDestination{}
	_ lifecycle.ConfigurableCloudManifestFetcher = &barkFakeCloudDestination{}
	_ lifecycle.CloudDeleter                     = &barkFakeCloudDestination{}
)

// barkFakeCloudDir is process-global and only momentarily locked, so the
// scheme registered below resolves against whichever test wrote it last.
// Every test in this file therefore runs sequentially -- none of them
// calls t.Parallel. Giving the fixture a per-test identity (or the
// serializing gate database/lifecycle's equivalent uses) is what would
// let them run in parallel.
var (
	barkFakeCloudMu  sync.Mutex
	barkFakeCloudDir string
)

// testDestinationRegistry is this test package's own instance-owned
// registry (mirroring what composition code builds at startup — see
// lifecycle.DestinationRegistry's doc comment), shared by every fake cloud
// scheme this file registers below and threaded into
// newTestDatabaseServiceHandler's Service/BarkConfig, instead of the
// removed package-global process registry.
var testDestinationRegistry = lifecycle.NewDestinationRegistry()

var testSnapshotTrustKey = []byte("bark-test-snapshot-trust-key")

func testSnapshotTrustKeyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot-trust-key")
	require.NoError(t, os.WriteFile(path, testSnapshotTrustKey, 0o600))
	return path
}

// fakeCloudBackingDir reads a fake scheme's backing directory under mu and
// refuses to resolve when none is set.
//
// A resolution with no backing directory would filepath.Join against "" and
// write the URI path relative to the package directory. The only way to get
// there is a resolution that outlived the test that set the fixture -- bark
// runs snapshot and restore work on background goroutines -- and that should
// error rather than leave files in the checkout.
//
// Shared by both fake schemes so the two registrations cannot drift.
func fakeCloudBackingDir(
	mu *sync.Mutex,
	dir *string,
	scheme string,
) (string, error) {
	mu.Lock()
	base := *dir
	mu.Unlock()
	if base == "" {
		return "", fmt.Errorf(
			"%s: no backing directory set for this test",
			scheme,
		)
	}
	return base, nil
}

func init() {
	testDestinationRegistry.Register(
		"barkfaketest",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			base, err := fakeCloudBackingDir(
				&barkFakeCloudMu, &barkFakeCloudDir, "barkfaketest",
			)
			if err != nil {
				return nil, err
			}
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
	t.Cleanup(func() {
		barkFakeCloudMu.Lock()
		barkFakeCloudDir = ""
		barkFakeCloudMu.Unlock()
	})
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

func (d *barkFakeCloudDestinationNoDelete) UploadDir(
	ctx context.Context,
	localDir string,
) error {
	return (&barkFakeCloudDestination{dir: d.dir}).UploadDir(ctx, localDir)
}

func (d *barkFakeCloudDestinationNoDelete) DownloadFiles(
	ctx context.Context,
	localDir string,
	files []lifecycle.DownloadFile,
) error {
	return (&barkFakeCloudDestination{dir: d.dir}).DownloadFiles(
		ctx,
		localDir,
		files,
	)
}

func (d *barkFakeCloudDestinationNoDelete) FetchManifest(
	ctx context.Context,
) (lifecycle.Manifest, error) {
	return (&barkFakeCloudDestination{dir: d.dir}).FetchManifest(ctx)
}

func (d *barkFakeCloudDestinationNoDelete) FetchManifestWithOptions(
	ctx context.Context,
	opts ...lifecycle.ManifestOption,
) (lifecycle.Manifest, error) {
	return (&barkFakeCloudDestination{dir: d.dir}).FetchManifestWithOptions(
		ctx,
		opts...,
	)
}

var (
	_ lifecycle.CloudManifestFetcher             = &barkFakeCloudDestinationNoDelete{}
	_ lifecycle.ConfigurableCloudManifestFetcher = &barkFakeCloudDestinationNoDelete{}
)

var (
	barkFakeCloudNoDeleteMu  sync.Mutex
	barkFakeCloudNoDeleteDir string
)

func init() {
	testDestinationRegistry.Register(
		"barkfaketest-nodelete",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			base, err := fakeCloudBackingDir(
				&barkFakeCloudNoDeleteMu,
				&barkFakeCloudNoDeleteDir,
				"barkfaketest-nodelete",
			)
			if err != nil {
				return nil, err
			}
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
	t.Cleanup(func() {
		barkFakeCloudNoDeleteMu.Lock()
		barkFakeCloudNoDeleteDir = ""
		barkFakeCloudNoDeleteMu.Unlock()
	})
}

// barkFakeCloudDestinationCommError simulates a real cloud communication
// failure (auth, network, timeout) rather than a confirmed-absent
// manifest: FetchManifest always returns a plain error, never wrapped in
// lifecycle.ErrCloudSnapshotNotFound. Used to prove that a probe failure
// distinct from "confirmed not there" is surfaced to the caller rather
// than silently folded into "not found".
type barkFakeCloudDestinationCommError struct{}

func (d *barkFakeCloudDestinationCommError) UploadDir(
	context.Context,
	string,
) error {
	return errors.New("simulated cloud communication failure")
}

func (d *barkFakeCloudDestinationCommError) DownloadFiles(
	context.Context,
	string,
	[]lifecycle.DownloadFile,
) error {
	return errors.New("simulated cloud communication failure")
}

func (d *barkFakeCloudDestinationCommError) FetchManifest(
	context.Context,
) (lifecycle.Manifest, error) {
	return lifecycle.Manifest{}, errors.New(
		"simulated cloud communication failure",
	)
}

func (d *barkFakeCloudDestinationCommError) FetchManifestWithOptions(
	context.Context,
	...lifecycle.ManifestOption,
) (lifecycle.Manifest, error) {
	return lifecycle.Manifest{}, errors.New(
		"simulated cloud communication failure",
	)
}

func (d *barkFakeCloudDestinationCommError) ListSnapshots(
	context.Context,
	...lifecycle.ManifestOption,
) ([]lifecycle.SnapshotEntry, error) {
	return nil, errors.New("simulated cloud communication failure")
}

var (
	_ lifecycle.CloudManifestFetcher             = &barkFakeCloudDestinationCommError{}
	_ lifecycle.ConfigurableCloudManifestFetcher = &barkFakeCloudDestinationCommError{}
	_ lifecycle.SnapshotLister                   = &barkFakeCloudDestinationCommError{}
)

func init() {
	testDestinationRegistry.Register(
		"barkfaketest-commerror",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return &barkFakeCloudDestinationCommError{}, nil
		},
	)
}

// TestBarkFakeCloudBackingDirsResetBetweenTests guards against a leaked
// global: setBarkFakeCloudBackingDir/
// setBarkFakeCloudNoDeleteBackingDir used to set their package-level
// backing-dir globals with no corresponding reset, so whichever test
// happened to set one last left that directory in place for every
// subsequent test in this package — a test that forgot to call the
// setter (or was reordered/shuffled ahead of the one that used to set it
// up) could silently resolve "barkfaketest://"/"barkfaketest-nodelete://"
// against a leftover directory from an unrelated test instead of failing
// loudly. Each subtest's t.Cleanup (registered by the respective setter)
// runs synchronously before t.Run returns, so both globals must already
// be reset by the time this checks them.
// TestBarkFakeCloudSchemeRequiresBackingDir proves the fake schemes refuse to
// resolve once no test owns the fixture. bark performs snapshot and restore
// work on background goroutines (database.go), so a resolution can outlive the
// t.Cleanup that reset the backing directory; joining the URI path onto an
// empty base then wrote "prefix/cloud-a/..." into the package directory, which
// a full `go test ./...` run reproduced.
func TestBarkFakeCloudSchemeRequiresBackingDir(t *testing.T) {
	for _, scheme := range []string{"barkfaketest", "barkfaketest-nodelete"} {
		_, err := lifecycle.ParseCloudDestination(
			testDestinationRegistry,
			scheme+"://bucket/prefix",
		)
		require.Error(
			t, err,
			"%s must not resolve with no backing directory set", scheme,
		)
	}
}

func TestBarkFakeCloudBackingDirsResetBetweenTests(t *testing.T) {
	t.Run("sets them", func(t *testing.T) {
		setBarkFakeCloudBackingDir(t, t.TempDir())
		setBarkFakeCloudNoDeleteBackingDir(t, t.TempDir())
	})

	barkFakeCloudMu.Lock()
	gotCloud := barkFakeCloudDir
	barkFakeCloudMu.Unlock()
	require.Empty(
		t,
		gotCloud,
		"barkFakeCloudDir must be reset via t.Cleanup once the test that set it finishes",
	)

	barkFakeCloudNoDeleteMu.Lock()
	gotNoDelete := barkFakeCloudNoDeleteDir
	barkFakeCloudNoDeleteMu.Unlock()
	require.Empty(
		t,
		gotNoDelete,
		"barkFakeCloudNoDeleteDir must be reset via t.Cleanup once the test that set it finishes",
	)
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
		context.Background(), testDestinationRegistry, db, localAndCloudDir,
		lifecycle.TriggerManual, "test-version", "badger", "sqlite", cloudDest,
		"", "", lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)

	cloudOnlyDir := filepath.Join(snapshotDir, "cloud-only")
	_, err = lifecycle.SnapshotToCloud(
		context.Background(), testDestinationRegistry, db, cloudOnlyDir,
		lifecycle.TriggerManual, "test-version", "badger", "sqlite", cloudDest,
		"", "", lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(cloudOnlyDir))

	localOnlyDir := filepath.Join(snapshotDir, "local-only")
	_, err = lifecycle.Snapshot(
		context.Background(),
		db,
		localOnlyDir,
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	resp, err := h.ListAvailableSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{}),
	)
	require.NoError(t, err)

	byID := make(
		map[string]*databasev1alpha1.SnapshotInfo,
		len(resp.Msg.GetSnapshots()),
	)
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

// TestListAvailableSnapshotsSurvivesCloudListingFailure guards a real bug:
// a cloud listing communication failure used to discard the local entries
// mergedSnapshotCatalogPage had already built and fail the whole call with
// CodeInternal, hiding known-good local snapshots from an operator over
// what is often just a transient cloud outage -- exactly the class of
// failure cloudSnapshotExists/resolveSnapshotSource/DeleteSnapshot
// elsewhere in this file already report as CodeUnavailable rather than
// CodeInternal. This proves the fix: a local-only snapshot still comes
// back successfully even when the configured cloud destination's listing
// call errors out.
func TestListAvailableSnapshotsSurvivesCloudListingFailure(t *testing.T) {
	snapshotDir := t.TempDir()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	_, err := lifecycle.Snapshot(
		context.Background(),
		db,
		filepath.Join(snapshotDir, "local-only"),
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = "barkfaketest-commerror://bucket/prefix"

	resp, err := h.ListAvailableSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetSnapshots(), 1)
	require.Equal(t, "local-only", resp.Msg.GetSnapshots()[0].GetSnapshotId())
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
		"badger",
		"sqlite",
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
	require.Equal(
		t,
		"only-snapshot",
		resp.Msg.GetSnapshots()[0].GetSnapshotId(),
	)
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
		lifecycle.TriggerManual, "test-version", "badger", "sqlite",
	)
	require.NoError(t, err)
	dbtest.CloseDatabase(sourceDB) //nolint:errcheck

	targetDataDir := filepath.Join(t.TempDir(), "target")
	h := newTestDatabaseServiceHandler(t, nil, targetDataDir)
	h.bark.config.SnapshotDir = snapshotDir

	restoreResp, err := h.Restore(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.RestoreRequest{SnapshotId: "snap1"},
		),
	)
	require.NoError(t, err)

	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetRestoreStatus(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetRestoreStatusRequest{
					OperationId: restoreResp.Msg.GetOperationId(),
				}),
			)
			require.NoError(t, err)
			return statusResp.Msg.GetProgress()
		},
	)
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
		context.Background(),
		testDestinationRegistry,
		sourceDB,
		filepath.Join(snapshotDir, "cloud-snap"),
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
		cloudDest,
		"",
		"",
		lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)
	dbtest.CloseDatabase(sourceDB) //nolint:errcheck

	// Remove the local copy so only the cloud mirror remains — simulating
	// a snapshot ListAvailableSnapshots would surface as cloud-only.
	require.NoError(t, os.RemoveAll(filepath.Join(snapshotDir, "cloud-snap")))

	targetDataDir := filepath.Join(t.TempDir(), "target")
	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, targetDataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	restoreResp, err := h.Restore(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.RestoreRequest{SnapshotId: "cloud-snap"},
		),
	)
	require.NoError(t, err)

	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetRestoreStatus(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetRestoreStatusRequest{
					OperationId: restoreResp.Msg.GetOperationId(),
				}),
			)
			require.NoError(t, err)
			return statusResp.Msg.GetProgress()
		},
	)
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"restore message: %s", progress.GetMessage(),
	)
}

func TestRestoreUnknownIDReturnsNotFound(t *testing.T) {
	h := newTestDatabaseServiceHandler(
		t,
		nil,
		filepath.Join(t.TempDir(), "target"),
	)
	_, err := h.Restore(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.RestoreRequest{SnapshotId: "does-not-exist"},
		),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestRestoreReturnsUnavailableOnCloudCommunicationFailure guards against
// a real bug: a real cloud communication failure (auth,
// network, timeout — anything other than a confirmed-absent manifest)
// while probing the configured cloud destination used to be silently
// folded into "doesn't exist," so an operator restoring a snapshot whose
// local copy had already been pruned (the exact case cloud fallback
// exists for) could be told "not found" for a snapshot that may well
// still be sitting in the cloud, indistinguishable from actual data loss.
// It must instead be reported as CodeUnavailable, distinct from a
// genuine CodeNotFound.
func TestRestoreReturnsUnavailableOnCloudCommunicationFailure(t *testing.T) {
	h := newTestDatabaseServiceHandlerWithTrustKey(
		t,
		nil,
		filepath.Join(t.TempDir(), "target"),
	)
	h.bark.config.SnapshotCloudDestination = "barkfaketest-commerror://bucket/prefix"

	_, err := h.Restore(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.RestoreRequest{SnapshotId: "some-snapshot"},
		),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
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
		context.Background(),
		testDestinationRegistry,
		db,
		filepath.Join(snapshotDir, "cloud-verify"),
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
		cloudDest,
		"",
		"",
		lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(filepath.Join(snapshotDir, "cloud-verify")))

	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	verifyResp, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.VerifySnapshotRequest{SnapshotId: "cloud-verify"},
		),
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
		context.Background(),
		testDestinationRegistry,
		db,
		filepath.Join(snapshotDir, "cloud-delete"),
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
		cloudDest,
		"",
		"",
		lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(filepath.Join(snapshotDir, "cloud-delete")))

	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	_, err = h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: "cloud-delete"},
		),
	)
	require.NoError(t, err)

	_, ok, err := lifecycle.FetchCloudManifest(
		context.Background(),
		testDestinationRegistry,
		lifecycle.JoinCloudURI(cloudDest, "cloud-delete"),
		lifecycle.WithManifestKey(testSnapshotTrustKey),
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
		context.Background(),
		testDestinationRegistry,
		db,
		filepath.Join(snapshotDir, "both"),
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
		cloudDest,
		"",
		"",
		lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)

	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest

	_, err = h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: "both"},
		),
	)
	require.NoError(t, err)

	require.NoDirExists(t, filepath.Join(snapshotDir, "both"))
	_, ok, err := lifecycle.FetchCloudManifest(
		context.Background(),
		testDestinationRegistry,
		lifecycle.JoinCloudURI(cloudDest, "both"),
		lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.True(t, ok)
	require.Error(t, err, "cloud copy must actually be gone after delete")
}

func TestDeleteSnapshotNeitherLocalNorCloudReturnsNotFound(t *testing.T) {
	setBarkFakeCloudBackingDir(t, t.TempDir())
	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, t.TempDir())
	h.bark.config.SnapshotCloudDestination = "barkfaketest://bucket/prefix"

	_, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.DeleteSnapshotRequest{
				SnapshotId: "does-not-exist",
			},
		),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestDeleteSnapshotReturnsUnavailableOnCloudCommunicationFailure is
// DeleteSnapshot's half of the same regression guard above: a snapshot
// with no local copy must not be reported (or treated) as "not found"
// when the cloud probe itself failed to communicate — that could delete
// nothing while telling the operator there was nothing to delete, when
// the cloud copy may still be there.
func TestDeleteSnapshotReturnsUnavailableOnCloudCommunicationFailure(
	t *testing.T,
) {
	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, t.TempDir())
	h.bark.config.SnapshotCloudDestination = "barkfaketest-commerror://bucket/prefix"

	_, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.DeleteSnapshotRequest{
				SnapshotId: "some-snapshot",
			},
		),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
}

// TestDeleteSnapshotCloudDestinationWithoutDeleteSupportReturnsUnimplemented
// covers the third DeleteSnapshot outcome distinct from "not found
// anywhere" (CodeNotFound) and "delete itself failed" (CodeInternal): a
// cloud copy genuinely exists, but its destination type doesn't implement
// CloudDeleter — S3/GCS always do, but a future destination type might
// not, and this must be reported clearly rather than silently no-op'ing
// and reporting success.
func TestDeleteSnapshotCloudDestinationWithoutDeleteSupportReturnsUnimplemented(
	t *testing.T,
) {
	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setBarkFakeCloudNoDeleteBackingDir(t, cloudBackingDir)
	const cloudDest = "barkfaketest-nodelete://bucket/prefix"

	localDir := filepath.Join(snapshotDir, "no-delete-support")
	_, err := lifecycle.SnapshotToCloud(
		context.Background(), testDestinationRegistry, db, localDir,
		lifecycle.TriggerManual, "test-version", "badger", "sqlite", cloudDest,
		"", "", lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)
	// Remove the local copy so DeleteSnapshot must act on the cloud-only
	// entry rather than succeeding via the local delete alone.
	require.NoError(t, os.RemoveAll(localDir))

	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
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
			lifecycle.TriggerManual, "test-version", "badger", "sqlite",
		)
		require.NoError(t, err)
	}
	for _, name := range []string{"cloud-a", "cloud-b"} {
		dir := filepath.Join(snapshotDir, name)
		_, err := lifecycle.SnapshotToCloud(
			context.Background(),
			testDestinationRegistry,
			db,
			dir,
			lifecycle.TriggerManual,
			"test-version",
			"badger",
			"sqlite",
			cloudDest,
			"",
			"",
			lifecycle.WithManifestKey(testSnapshotTrustKey),
		)
		require.NoError(t, err)
		require.NoError(t, os.RemoveAll(dir))
	}

	h := newTestDatabaseServiceHandlerWithTrustKey(t, nil, dataDir)
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
	require.Equal(
		t,
		2,
		pages,
		"4 snapshots at page size 2 must take exactly 2 pages",
	)

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	require.ElementsMatch(
		t,
		[]string{"local-a", "local-b", "cloud-a", "cloud-b"},
		ids,
	)
}

func TestSnapshotRPCManifestByteLimit(t *testing.T) {
	for _, source := range []string{"local", "cloud"} {
		for _, operation := range []string{"verify", "restore"} {
			t.Run(source+"/"+operation, func(t *testing.T) {
				newHandler := newTestDatabaseServiceHandler
				if source == "cloud" {
					newHandler = newTestDatabaseServiceHandlerWithTrustKey
				}
				dataDir := t.TempDir()
				db := newDiskTestDB(t, dataDir)
				require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
				dbtest.CloseDatabase(db) //nolint:errcheck
				creator := newHandler(t, nil, dataDir)
				created := createAndAwaitSnapshot(t, creator, &databasev1alpha1.CreateSnapshotRequest{})
				id := created.GetSnapshotId()
				manifestPath := filepath.Join(creator.bark.config.SnapshotDir, id, lifecycle.ManifestFileName)
				original, err := os.ReadFile(manifestPath)
				require.NoError(t, err)
				targetDir := filepath.Join(t.TempDir(), "restore")
				h := newHandler(t, nil, targetDir)
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

// databaseOperationTimeout allows for real Badger and SQLite file operations
// running under the race detector alongside every other package in CI. Linux
// arm64 can spend more than 30 seconds restoring even this small fixture when
// the full package matrix is contending for CPU and disk.
const databaseOperationTimeout = 2 * time.Minute

// newDiskTestDB builds a real on-disk database (badger + sqlite), unlike
// blob_test.go's in-memory newTestDB: Snapshot/Restore need real files to
// back up (VACUUM INTO refuses an in-memory sqlite database).
func newDiskTestDB(t *testing.T, dataDir string) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
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
	return newTestDatabaseServiceHandlerWithConfig(t, barkDB, dbDataDir, "")
}

func newTestDatabaseServiceHandlerWithTrustKey(
	t *testing.T,
	barkDB *database.Database,
	dbDataDir string,
) *databaseServiceHandler {
	return newTestDatabaseServiceHandlerWithConfig(
		t,
		barkDB,
		dbDataDir,
		testSnapshotTrustKeyFile(t),
	)
}

func newTestDatabaseServiceHandlerWithConfig(
	t *testing.T,
	barkDB *database.Database,
	dbDataDir string,
	trustKeyFile string,
) *databaseServiceHandler {
	t.Helper()
	if barkDB == nil {
		barkDB = newTestDB(t)
	}
	svc := dblifecycle.NewService(&config.Config{
		DatabasePath: dbDataDir,
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob:     plugin.Selection{Provider: "badger"},
				Metadata: plugin.Selection{Provider: "sqlite"},
			},
		},
		DatabaseLifecycle: config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: trustKeyFile,
		},
	}, testDestinationRegistry, nil)
	b, err := NewBark(BarkConfig{
		DB:                  barkDB,
		Lifecycle:           svc,
		SnapshotDir:         t.TempDir(),
		Port:                1,
		DestinationRegistry: testDestinationRegistry,
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
	}, databaseOperationTimeout, 10*time.Millisecond,
		"operation must reach a terminal state",
	)
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
	createResp, err := h.CreateSnapshot(
		context.Background(),
		connect.NewRequest(req),
	)
	require.NoError(t, err)
	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetSnapshotStatus(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetSnapshotStatusRequest{
					OperationId: createResp.Msg.GetOperationId(),
				}),
			)
			require.NoError(t, err)
			return statusResp.Msg.GetProgress()
		},
	)
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
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)

	createResp, err := h.CreateSnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.CreateSnapshotRequest{Name: "test"},
		),
	)
	require.NoError(t, err)
	require.NotEmpty(t, createResp.Msg.GetOperationId())
	require.NotEmpty(t, createResp.Msg.GetSnapshotId())

	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetSnapshotStatus(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetSnapshotStatusRequest{
					OperationId: createResp.Msg.GetOperationId(),
				}),
			)
			require.NoError(t, err)
			require.Equal(
				t,
				createResp.Msg.GetSnapshotId(),
				statusResp.Msg.GetSnapshotId(),
			)
			return statusResp.Msg.GetProgress()
		},
	)
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
	t.Parallel()

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
	t.Parallel()

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

// TestDeleteSnapshotRejectsConcurrentOperation guards a real gap:
// DeleteSnapshot used to never check the handler's busy flag at all, so it
// could run concurrently with an in-flight CreateSnapshot/Restore/
// VerifySnapshot and remove a snapshot directory while it was still being
// written to or read from. Simulates the in-flight operation
// deterministically (same approach as
// TestCreateSnapshotRejectsConcurrentOperation) rather than racing a real
// one.
func TestDeleteSnapshotRejectsConcurrentOperation(t *testing.T) {
	t.Parallel()

	snapshotDir := t.TempDir()
	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	h.bark.config.SnapshotDir = snapshotDir
	require.NoError(
		t,
		os.Mkdir(filepath.Join(snapshotDir, "some-snapshot"), 0o755),
	)

	h.busy = true

	_, err := h.DeleteSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.DeleteSnapshotRequest{
			SnapshotId: "some-snapshot",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.DirExists(
		t, filepath.Join(snapshotDir, "some-snapshot"),
		"the snapshot must be left untouched when the delete is rejected",
	)
}

// TestRestoreClaimsBusyBeforeResolvingSource guards the reordering fix
// for the finding that Restore used to resolve its snapshot
// source before claiming the busy flag DeleteSnapshot also uses, leaving
// a window where a concurrent DeleteSnapshot could remove the very
// snapshot Restore was about to read. If Restore still resolved the
// source first, this would return CodeNotFound (the snapshot ID doesn't
// exist) instead of CodeFailedPrecondition (another operation already
// holds the busy flag), since resolution would run before ever noticing
// h.busy. Simulates the in-flight operation deterministically, the same
// way TestDeleteSnapshotRejectsConcurrentOperation does.
func TestRestoreClaimsBusyBeforeResolvingSource(t *testing.T) {
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	h.busy = true

	_, err := h.Restore(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.RestoreRequest{
			SnapshotId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// TestVerifySnapshotClaimsBusyBeforeResolvingSource is
// TestRestoreClaimsBusyBeforeResolvingSource's counterpart for
// VerifySnapshot, which shares the same reordering fix.
func TestVerifySnapshotClaimsBusyBeforeResolvingSource(t *testing.T) {
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	h.busy = true

	_, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// TestRestoreReleasesBusyWhenSourceResolutionFails verifies that a failed
// source resolution (unknown snapshot ID) releases the busy flag it
// claimed, rather than leaking it and permanently blocking every later
// operation.
func TestRestoreReleasesBusyWhenSourceResolutionFails(t *testing.T) {
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())

	_, err := h.Restore(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.RestoreRequest{
			SnapshotId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	h.mu.Lock()
	busy := h.busy
	h.mu.Unlock()
	require.False(
		t,
		busy,
		"a failed source resolution must release the busy flag",
	)
}

// TestVerifySnapshotReleasesBusyWhenSourceResolutionFails is
// TestRestoreReleasesBusyWhenSourceResolutionFails's counterpart for
// VerifySnapshot, which shares the same reordering fix.
func TestVerifySnapshotReleasesBusyWhenSourceResolutionFails(t *testing.T) {
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())

	_, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: "does-not-exist",
		}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	h.mu.Lock()
	busy := h.busy
	h.mu.Unlock()
	require.False(
		t,
		busy,
		"a failed source resolution must release the busy flag",
	)
}

// TestTruncateRejectsInvalidTarget verifies that Truncate rejects a nil
// target. See TestTruncateAcceptsConsistentCombinedFields/
// TestTruncateRejectsInconsistentCombinedFieldsAsFailedOperation below for
// the "more than one field set" cases this test used to also cover as a
// synchronous RPC rejection, before blockRefToTarget started accepting any
// combination of fields at the RPC level and deferring agreement-checking
// to dblifecycle.ResolveTarget.
func TestTruncateRejectsInvalidTarget(t *testing.T) {
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())

	_, err := h.Truncate(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.TruncateRequest{}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// TestTruncateAcceptsConsistentCombinedFields verifies that Truncate
// accepts (and successfully completes) a target with more than one of
// slot/hash/block_number set, as long as they all identify the same
// block — per the proto's documented BlockRef contract ("When multiple
// fields are set, all must agree").
func TestTruncateAcceptsConsistentCombinedFields(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	var last models.Block
	for id := uint64(1); id <= 3; id++ {
		last = testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(last, nil))
	}
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)

	target := testBlock(1, 1)
	slot := target.Slot
	blockNumber := target.Number
	hash := hex.EncodeToString(target.Hash)
	truncResp, err := h.Truncate(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.TruncateRequest{
			Target: &databasev1alpha1.BlockRef{
				Slot:        &slot,
				BlockNumber: &blockNumber,
				Hash:        &hash,
			},
		}),
	)
	require.NoError(t, err)

	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetTruncateStatus(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetTruncateStatusRequest{
					OperationId: truncResp.Msg.GetOperationId(),
				}),
			)
			require.NoError(t, err)
			return statusResp.Msg.GetProgress()
		},
	)
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		progress.GetStatus(),
		"truncate message: %s", progress.GetMessage(),
	)
}

// TestTruncateRejectsInconsistentCombinedFieldsAsFailedOperation verifies
// that a target whose slot/hash/block_number fields disagree about which
// block is meant is accepted by the Truncate RPC itself (bark's own
// blockRefToTarget only requires at least one field) but fails
// asynchronously once dblifecycle.ResolveTarget notices the mismatch,
// rather than silently trusting whichever field is used for the actual
// lookup.
func TestTruncateRejectsInconsistentCombinedFieldsAsFailedOperation(
	t *testing.T,
) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	var last models.Block
	for id := uint64(1); id <= 3; id++ {
		last = testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(last, nil))
	}
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)

	slot := testBlock(1, 1).Slot
	mismatchedBlockNumber := uint64(2) // block 2's number, not block 1's
	truncResp, err := h.Truncate(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.TruncateRequest{
			Target: &databasev1alpha1.BlockRef{
				Slot:        &slot,
				BlockNumber: &mismatchedBlockNumber,
			},
		}),
	)
	require.NoError(t, err)

	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetTruncateStatus(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetTruncateStatusRequest{
					OperationId: truncResp.Msg.GetOperationId(),
				}),
			)
			require.NoError(t, err)
			return statusResp.Msg.GetProgress()
		},
	)
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED,
		progress.GetStatus(),
	)
	require.Contains(t, progress.GetMessage(), "does not match")
}

// TestTruncateAndGetTruncateStatus verifies that Truncate completes and
// reports the correct number of blocks removed via GetTruncateStatus.
func TestTruncateAndGetTruncateStatus(t *testing.T) {
	t.Parallel()

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
	dbtest.CloseDatabase(db) //nolint:errcheck

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

	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetTruncateStatus(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetTruncateStatusRequest{
					OperationId: truncResp.Msg.GetOperationId(),
				}),
			)
			require.NoError(t, err)
			return statusResp.Msg.GetProgress()
		},
	)
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
	t.Parallel()

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
	// Tier reports the database's configured storage mode, not a placeholder.
	require.NotEmpty(t, db.StorageMode())
	require.Equal(t, db.StorageMode(), resp.Msg.GetTier())
}

// TestListSnapshotsReturnsCreatedSnapshotWithLabel verifies that
// ListSnapshots surfaces a created snapshot's name, description, size, and checksum.
func TestListSnapshotsReturnsCreatedSnapshotWithLabel(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{
			Name:        "nightly",
			Description: "pre-hardfork backup",
		},
	)

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
	t.Parallel()

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
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

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

// TestListSnapshotsSkipsCorruptedEntryButReturnsOthers verifies that one
// snapshot's corrupted manifest.json doesn't hide every other, otherwise-
// valid snapshot from ListSnapshots: this RPC used
// to fail the whole call whenever lifecycle.ListSnapshots reported ANY
// per-entry problem, discarding the valid entries it had already
// collected instead of using them.
func TestListSnapshotsSkipsCorruptedEntryButReturnsOthers(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	good := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{Name: "good"},
	)

	// A second snapshot directory whose manifest.json fails checksum
	// validation -- present on disk, but unusable.
	corruptDir := filepath.Join(h.bark.config.SnapshotDir, "corrupt-snapshot")
	require.NoError(t, os.Mkdir(corruptDir, 0o755))
	goodManifest := filepath.Join(
		h.bark.config.SnapshotDir,
		good.GetSnapshotId(),
		"manifest.json",
	)
	data, err := os.ReadFile(goodManifest)
	require.NoError(t, err)
	corruptManifest := filepath.Join(corruptDir, "manifest.json")
	require.NoError(t, os.WriteFile(corruptManifest, data, 0o644))
	tamperManifestChecksum(t, corruptManifest)

	listResp, err := h.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, listResp.Msg.GetSnapshots(), 1)
	require.Equal(
		t,
		good.GetSnapshotId(),
		listResp.Msg.GetSnapshots()[0].GetSnapshotId(),
	)
}

// TestListAvailableSnapshotsSkipsCorruptedEntryButReturnsOthers is
// TestListSnapshotsSkipsCorruptedEntryButReturnsOthers's counterpart for
// ListAvailableSnapshots, which scans the same local directory via its own
// call to lifecycle.ListSnapshots.
func TestListAvailableSnapshotsSkipsCorruptedEntryButReturnsOthers(
	t *testing.T,
) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	good := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{Name: "good"},
	)

	corruptDir := filepath.Join(h.bark.config.SnapshotDir, "corrupt-snapshot")
	require.NoError(t, os.Mkdir(corruptDir, 0o755))
	goodManifest := filepath.Join(
		h.bark.config.SnapshotDir,
		good.GetSnapshotId(),
		"manifest.json",
	)
	data, err := os.ReadFile(goodManifest)
	require.NoError(t, err)
	corruptManifest := filepath.Join(corruptDir, "manifest.json")
	require.NoError(t, os.WriteFile(corruptManifest, data, 0o644))
	tamperManifestChecksum(t, corruptManifest)

	resp, err := h.ListAvailableSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetSnapshots(), 1)
	require.Equal(
		t,
		good.GetSnapshotId(),
		resp.Msg.GetSnapshots()[0].GetSnapshotId(),
	)
}

// TestListAvailableSnapshotsMirrorsListSnapshots covers the no-cloud-
// destination-configured case: see database_test.go for the actual
// local+cloud merge behavior this RPC exists for.
func TestListAvailableSnapshotsMirrorsListSnapshots(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

	resp, err := h.ListAvailableSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListAvailableSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetSnapshots(), 1)
	require.Equal(
		t,
		created.GetSnapshotId(),
		resp.Msg.GetSnapshots()[0].GetSnapshotId(),
	)
}

// TestDeleteSnapshotRemovesItFromTheCatalog verifies that a deleted
// snapshot no longer appears in a subsequent ListSnapshots call.
func TestDeleteSnapshotRemovesItFromTheCatalog(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

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
	t.Parallel()

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
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	for _, id := range []string{"../../etc", "..", ".", "a/b", ""} {
		_, err := h.DeleteSnapshot(
			context.Background(),
			connect.NewRequest(
				&databasev1alpha1.DeleteSnapshotRequest{SnapshotId: id},
			),
		)
		require.Error(t, err, "snapshot_id %q must be rejected", id)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}
}

// TestVerifySnapshotSucceedsForValidSnapshot verifies that verifying a
// freshly created, uncorrupted snapshot completes successfully.
func TestVerifySnapshotSucceedsForValidSnapshot(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

	verifyResp, err := h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.VerifySnapshotRequest{
			SnapshotId: created.GetSnapshotId(),
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, verifyResp.Msg.GetOperationId())

	progress := waitForOperationStatus(
		t,
		func() *databasev1alpha1.OperationProgress {
			statusResp, err := h.GetOperationHistory(
				context.Background(),
				connect.NewRequest(
					&databasev1alpha1.GetOperationHistoryRequest{},
				),
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
		},
	)
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
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

	// Corrupt the blob backup so a real restore-based verify fails.
	// Truncating to empty isn't enough: Badger's Load treats an empty
	// stream as "nothing to load" and succeeds trivially. Garbage bytes
	// fail Badger's internal length-prefix parsing instead.
	blobPath := filepath.Join(
		h.bark.config.SnapshotDir,
		created.GetSnapshotId(),
		"blob.bak",
	)
	require.NoError(
		t,
		os.WriteFile(
			blobPath,
			[]byte("not a valid badger backup stream"),
			0o644,
		),
	)

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
	}, databaseOperationTimeout, 10*time.Millisecond,
		"verify of a corrupted snapshot must fail",
	)
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
// misleading-error finding: a snapshot whose
// manifest.json was corrupted/hand-edited (so it fails checksum
// validation) used to be indistinguishable from a snapshot ID that never
// existed at all — both surfaced as CodeNotFound. resolveSnapshotSource
// now checks errors.Is against lifecycle.ErrManifestCorrupted first, so a
// snapshot that IS there but unusable is reported as such.
func TestVerifySnapshotOfTamperedManifestReturnsDataLoss(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

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
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

	snapshotDir := filepath.Join(
		h.bark.config.SnapshotDir,
		created.GetSnapshotId(),
	)
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
	t.Parallel()

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
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

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

// TestOperationsArePrunedOnceOverCap verifies that h.operations doesn't
// grow without bound: once more than
// maxRetainedOperations terminal operations have been registered, the
// oldest ones are pruned so both the map's memory footprint and
// GetOperationHistory's per-call sort over it stay bounded, rather than
// growing for the entire life of the bark process. Drives
// registerOperation/complete directly (no real Snapshot/Restore/Truncate
// work) so the cap can be exercised without hundreds of real disk
// operations.
func TestOperationsArePrunedOnceOverCap(t *testing.T) {
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())

	const total = maxRetainedOperations + 50
	var lastID string
	for range total {
		op, _ := h.registerOperation(
			databasev1alpha1.OperationType_OPERATION_TYPE_SNAPSHOT,
		)
		op.complete(nil, 0)
		lastID = op.id
	}

	h.mu.Lock()
	count := len(h.operations)
	_, lastStillPresent := h.operations[lastID]
	h.mu.Unlock()

	require.LessOrEqual(t, count, maxRetainedOperations)
	require.True(
		t, lastStillPresent,
		"the most recently completed operation must not be pruned",
	)
}

// TestGetOperationHistoryFiltersByTypeAndStatus verifies that filtering
// by a type or status that doesn't match the recorded operation returns nothing.
func TestGetOperationHistoryFiltersByTypeAndStatus(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

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
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	for range 3 {
		createAndAwaitSnapshot(t, h, &databasev1alpha1.CreateSnapshotRequest{})
	}

	page1, err := h.GetOperationHistory(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.GetOperationHistoryRequest{PageSize: 2},
		),
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
	t.Parallel()

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
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	op, ctx, err := h.startOperation(
		databasev1alpha1.OperationType_OPERATION_TYPE_SNAPSHOT,
	)
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

// TestCancelOperationMarksDriverCancellationErrorCancelled verifies that
// storage-driver errors which do not wrap context.Canceled still reflect an
// accepted cancellation request instead of being mislabeled as failures.
func TestCancelOperationMarksDriverCancellationErrorCancelled(t *testing.T) {
	t.Parallel()

	h := newTestDatabaseServiceHandler(t, nil, t.TempDir())
	op, _, err := h.startOperation(
		databasev1alpha1.OperationType_OPERATION_TYPE_SNAPSHOT,
	)
	require.NoError(t, err)

	_, err = h.CancelOperation(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
			OperationId: op.id,
		}),
	)
	require.NoError(t, err)

	op.complete(errors.New("sqlite interrupted (9)"), 0)

	progress := op.progress()
	require.Equal(
		t,
		databasev1alpha1.OperationStatus_OPERATION_STATUS_CANCELLED,
		progress.GetStatus(),
	)
	require.Equal(t, "cancelled", progress.GetMessage())
}

// TestCancelOperationOnAlreadyCompletedOperationIsANoOp verifies that
// cancelling an already-completed operation just reports its COMPLETED status.
func TestCancelOperationOnAlreadyCompletedOperationIsANoOp(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db := newDiskTestDB(t, dataDir)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dbtest.CloseDatabase(db) //nolint:errcheck

	h := newTestDatabaseServiceHandler(t, nil, dataDir)
	created := createAndAwaitSnapshot(
		t,
		h,
		&databasev1alpha1.CreateSnapshotRequest{},
	)

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

// mtlsHTTPClient builds an HTTP/2-over-TLS client suitable for talking to a
// bark.Bark server started with TlsCertFilePath/TlsKeyFilePath: it skips
// verifying the server's certificate (these tests always use
// writeTestTLSCertKey's throwaway self-signed one, which no client would
// otherwise trust) and, when certPath/keyPath are non-empty, presents that
// keypair as its own client certificate for mTLS. Passing "", "" builds an
// anonymous client — no certificate presented at all.
func mtlsHTTPClient(t *testing.T, certPath, keyPath string) *http.Client {
	t.Helper()
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test-only, throwaway self-signed server cert
	}
	if certPath != "" || keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		require.NoError(t, err)
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   tlsCfg,
			ForceAttemptHTTP2: true,
		},
	}
}

// TestDatabaseServiceOverRealHTTP is the wire-level companion to
// database_test.go's in-process handler tests: those call
// databaseServiceHandler's methods directly, proving the job-tracking and
// Service-wiring logic but not that a real Connect client, talking to a
// really-listening bark.Bark server over real HTTP, gets back something
// the generated client can decode. This starts a real server and drives
// it with the generated databaseconnect.DatabaseServiceClient.
func TestDatabaseServiceOverRealHTTP(t *testing.T) {
	t.Parallel()

	// bark's own DB (kept open for GetDatabaseInfo) and the Service's
	// target (opened and closed per call by Service.Snapshot/Truncate)
	// must be separate directories: Badger's exclusive file lock refuses
	// a second concurrent open of the same one, so a single shared
	// directory would fail CreateSnapshot/Truncate with "another process
	// is using this Badger database" the instant they ran.
	block1 := testBlock(1, 0x01)

	barkDataDir := t.TempDir()
	db := newDiskTestDB(t, barkDataDir)
	require.NoError(t, db.BlockCreate(block1, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))

	svcDataDir := t.TempDir()
	svcDB := newDiskTestDB(t, svcDataDir)
	require.NoError(t, svcDB.BlockCreate(block1, nil))
	require.NoError(t, svcDB.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))
	dbtest.CloseDatabase(svcDB) //nolint:errcheck

	svc := dblifecycle.NewService(&config.Config{
		DatabasePath: svcDataDir,
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob:     plugin.Selection{Provider: "badger"},
				Metadata: plugin.Selection{Provider: "sqlite"},
			},
		},
	}, nil, nil)

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	caCert, caKey, caCertPath := writeTestCA(t)
	clientCertPath, clientKeyPath := writeTestClientCert(
		t,
		caCert,
		caKey,
		"wire-test-operator",
	)

	b, err := NewBark(BarkConfig{
		DB:                  db,
		Lifecycle:           svc,
		SnapshotDir:         t.TempDir(),
		Host:                "127.0.0.1",
		TlsCertFilePath:     serverCertPath,
		TlsKeyFilePath:      serverKeyPath,
		TlsClientCAFilePath: caCertPath,
		OperatorCertificateFingerprints: []string{
			testCertificateFingerprint(t, clientCertPath),
		},
	})
	require.NoError(t, err)

	ctx := t.Context()
	require.NoError(t, b.Start(ctx))
	defer func() { _ = b.Stop(context.Background()) }()
	require.NotEmpty(t, b.Addr())

	client := databaseconnect.NewDatabaseServiceClient(
		mtlsHTTPClient(t, clientCertPath, clientKeyPath),
		"https://"+b.Addr(),
	)

	infoResp, err := client.GetDatabaseInfo(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(10), infoResp.Msg.GetTip().GetSlot())
	require.Equal(t, uint64(1), infoResp.Msg.GetTip().GetBlockNumber())
	require.False(t, infoResp.Msg.GetOperationInProgress())
	require.NotZero(t, infoResp.Msg.GetSizeBytes())

	createResp, err := client.CreateSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CreateSnapshotRequest{
			Name: "wire-test",
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, createResp.Msg.GetOperationId())
	require.NotEmpty(t, createResp.Msg.GetSnapshotId())

	var finalProgress *databasev1alpha1.OperationProgress
	require.Eventually(t, func() bool {
		statusResp, err := client.GetSnapshotStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetSnapshotStatusRequest{
				OperationId: createResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		finalProgress = statusResp.Msg.GetProgress()
		return finalProgress.GetStatus() ==
			databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED
	}, databaseOperationTimeout, 20*time.Millisecond,
		"snapshot must complete over the wire",
	)
	require.Equal(t, "completed", finalProgress.GetMessage())
	require.Equal(
		t,
		createResp.Msg.GetOperationId(),
		finalProgress.GetOperationId(),
	)

	// Truncate over the same real connection, sequenced after the
	// snapshot completes so the handler's single-operation gate doesn't
	// reject it.
	blockNumber := uint64(1)
	truncResp, err := client.Truncate(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.TruncateRequest{
			Target: &databasev1alpha1.BlockRef{BlockNumber: &blockNumber},
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, truncResp.Msg.GetOperationId())

	require.Eventually(t, func() bool {
		statusResp, err := client.GetTruncateStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetTruncateStatusRequest{
				OperationId: truncResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		return statusResp.Msg.GetProgress().GetStatus() ==
			databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED
	}, databaseOperationTimeout, 20*time.Millisecond,
		"truncate must complete over the wire",
	)

	histResp, err := client.GetOperationHistory(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, histResp.Msg.GetRecords(), 2)

	// StreamOperationProgress is the one server-streaming RPC, which needs
	// a real transport to exercise (a unit test can't easily construct a
	// *connect.ServerStream by hand) — the truncate above already reached
	// COMPLETED, so the very first message the stream sends should report
	// that and the stream should then close.
	stream, err := client.StreamOperationProgress(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.StreamOperationProgressRequest{
			OperationId: truncResp.Msg.GetOperationId(),
		}),
	)
	require.NoError(t, err)
	var sawCompleted bool
	for stream.Receive() {
		if stream.Msg().GetProgress().GetStatus() ==
			databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED {
			sawCompleted = true
		}
	}
	require.NoError(t, stream.Err())
	require.True(
		t,
		sawCompleted,
		"stream must report the truncate's completed status before closing",
	)

	listResp, err := client.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, listResp.Msg.GetSnapshots(), 1)
	require.Equal(
		t,
		createResp.Msg.GetSnapshotId(),
		listResp.Msg.GetSnapshots()[0].GetSnapshotId(),
	)
}
