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

//go:build dingo_extra_plugins

// This file exercises the real S3/GCS CloudDestination implementations
// (destination_s3.go, destination_gcs.go) against a real bucket, unlike
// destination_test.go's fakeCloudDestination, which only covers the
// generic orchestration logic (SnapshotToCloud/ListCloudSnapshots/
// FetchCloudManifest/DeleteCloudSnapshot) without ever touching an actual
// cloud SDK client. It was previously possible for the real S3/GCS client
// code to have zero test coverage of its own; these tests close that gap
// the same way internal/integration/cloud_test.go's TestCloudPluginS3/GCS
// do for the blob-store plugins, following the exact same
// credentials-detection and DINGO_TEST_S3_BUCKET/DINGO_TEST_GCS_BUCKET
// convention so both suites behave identically in CI and locally.
//
// Both tests skip outright when no credentials are available, so `go test
// -tags dingo_extra_plugins ./...` still passes in an environment with no
// cloud access configured.
package lifecycle_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// hasS3Credentials mirrors internal/integration/cloud_test.go's helper of
// the same name exactly, since that package's unexported helper can't be
// imported from here.
func hasS3Credentials() bool {
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		if _, err := os.Stat(filepath.Join(home, ".aws", "credentials")); err == nil {
			return true
		}
	}
	return false
}

// hasGCSCredentials mirrors internal/integration/cloud_test.go's helper of
// the same name exactly.
func hasGCSCredentials() bool {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		adcPath := filepath.Join(
			home, ".config", "gcloud", "application_default_credentials.json",
		)
		if _, err := os.Stat(adcPath); err == nil {
			return true
		}
	}
	return false
}

// TestCloudDestinationS3RoundTrip exercises s3Destination end to end
// against a real bucket: upload (via SnapshotToCloud), list (via
// ListCloudSnapshots), fetch-manifest (via FetchCloudManifest), download +
// full restore validation (via Restore accepting the s3:// URI directly),
// and delete (via DeleteCloudSnapshot) — every CloudDestination/
// SnapshotLister/CloudManifestFetcher/CloudDeleter method this package
// actually calls in production.
func TestCloudDestinationS3RoundTrip(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	runCloudDestinationRoundTrip(t, "s3", bucket)
}

// TestCloudDestinationGCSRoundTrip is TestCloudDestinationS3RoundTrip's
// GCS counterpart, against gcsDestination.
func TestCloudDestinationGCSRoundTrip(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	runCloudDestinationRoundTrip(t, "gcs", bucket)
}

// runCloudDestinationRoundTrip drives the shared round-trip logic for
// whichever scheme ("s3" or "gcs") the caller already confirmed has
// credentials for. A unique per-run snapshot ID keeps concurrent/repeated
// test runs from colliding in a shared test bucket, and the cloud copy is
// always cleaned up via t.Cleanup regardless of where the test fails.
func runCloudDestinationRoundTrip(t *testing.T, scheme string, bucket string) {
	t.Helper()
	ctx := context.Background()

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, db.BlockCreate(testBlock(2, 0x02), nil))

	snapshotID := fmt.Sprintf("dingo-lifecycle-cloud-test-%d", time.Now().UnixNano())
	baseURI := fmt.Sprintf("%s://%s/dingo-lifecycle-cloud-test", scheme, bucket)
	snapshotURI := lifecycle.JoinCloudURI(baseURI, snapshotID)
	localDir := filepath.Join(t.TempDir(), snapshotID)

	t.Cleanup(func() {
		_, _ = lifecycle.DeleteCloudSnapshot(context.Background(), snapshotURI)
	})

	manifest, err := lifecycle.SnapshotToCloud(
		ctx, db, localDir, lifecycle.TriggerManual, "test", baseURI,
	)
	require.NoError(t, err, "SnapshotToCloud (local write + cloud upload)")

	entries, ok, err := lifecycle.ListCloudSnapshots(ctx, baseURI)
	require.NoError(t, err, "ListCloudSnapshots")
	require.True(t, ok, "cloud destination must report listing support")
	found := false
	for _, e := range entries {
		if e.ID == snapshotID {
			found = true
			require.Equal(t, manifest.TipSlot, e.Manifest.TipSlot)
			break
		}
	}
	require.True(t, found, "uploaded snapshot %q not found in cloud listing", snapshotID)

	fetched, ok, err := lifecycle.FetchCloudManifest(ctx, snapshotURI)
	require.NoError(t, err, "FetchCloudManifest")
	require.True(t, ok, "cloud destination must report manifest-fetch support")
	require.Equal(t, manifest.Checksum, fetched.Checksum)

	// Restore accepts the cloud URI directly (downloads, then runs the same
	// validation as a local restore) — this exercises DownloadDir plus the
	// full manifest/tip/commit-timestamp consistency checks against real
	// downloaded data, not just a local round-trip.
	restoredDir := filepath.Join(t.TempDir(), "restored")
	restoreMan, err := lifecycle.Restore(ctx, snapshotURI, restoredDir)
	require.NoError(t, err, "Restore from cloud URI")
	require.Equal(t, manifest.CommitTimestamp, restoreMan.CommitTimestamp)

	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", restoredDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", restoredDir,
	))
	restored, err := database.New(&database.Config{
		DataDir:        restoredDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	defer restored.Close()
	block1, err := restored.BlockByIndex(1, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), block1.ID)
	block2, err := restored.BlockByIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block2.ID)

	deleted, err := lifecycle.DeleteCloudSnapshot(ctx, snapshotURI)
	require.NoError(t, err, "DeleteCloudSnapshot")
	require.True(t, deleted, "cloud destination must report delete support")

	_, _, err = lifecycle.FetchCloudManifest(ctx, snapshotURI)
	require.Error(t, err, "manifest must no longer be fetchable after delete")
}
