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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const localAuthSnapshotID = "mirrored-snap"

// rewriteManifestUnkeyed rewrites dir's manifest with a valid checksum and
// no authentication tag: it passes the unkeyed read but fails a keyed one.
func rewriteManifestUnkeyed(t *testing.T, dir string) {
	t.Helper()
	m, err := lifecycle.ReadManifest(
		dir, lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)
	m.Description = "attacker-controlled"
	require.NoError(t, lifecycle.WriteManifest(dir, m))
	_, err = lifecycle.ReadManifest(dir)
	require.NoError(t, err, "tampered manifest must pass the unkeyed read")
	_, err = lifecycle.ReadManifest(
		dir, lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
}

// localAuthFixture mirrors one keyed snapshot to local and cloud storage.
type localAuthFixture struct {
	h         *databaseServiceHandler
	localDir  string
	cloudDir  string
	cloudDest string
	logs      *lockedLogBuffer
}

func newLocalAuthFixture(t *testing.T) *localAuthFixture {
	t.Helper()
	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	bucket := setBarkFakeCloudBackingDir(t, cloudBackingDir)
	cloudDest := "barkfaketest://" + bucket + "/prefix"

	localDir := filepath.Join(snapshotDir, localAuthSnapshotID)
	installTemplateSnapshotInCloud(
		t, snapshotDir, localAuthSnapshotID, cloudDest, true,
	)

	h := newTestDatabaseServiceHandlerWithTrustKey(
		t, nil, filepath.Join(t.TempDir(), "target"),
	)
	h.bark.config.SnapshotDir = snapshotDir
	h.bark.config.SnapshotCloudDestination = cloudDest
	logs := &lockedLogBuffer{}
	h.bark.config.Logger = slog.New(slog.NewTextHandler(logs, nil))
	return &localAuthFixture{
		h:         h,
		localDir:  localDir,
		cloudDir:  filepath.Join(cloudBackingDir, "prefix", localAuthSnapshotID),
		cloudDest: cloudDest,
		logs:      logs,
	}
}

func (f *localAuthFixture) restore() error {
	_, err := f.h.Restore(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.RestoreRequest{SnapshotId: localAuthSnapshotID},
		),
	)
	return err
}

func (f *localAuthFixture) verify() error {
	_, err := f.h.VerifySnapshot(
		context.Background(),
		connect.NewRequest(
			&databasev1alpha1.VerifySnapshotRequest{
				SnapshotId: localAuthSnapshotID,
			},
		),
	)
	return err
}

// awaitCompleted waits for the single operation the test started to reach a
// terminal state and requires it to have completed, so a failed operation
// reports its status instead of waiting out the timeout.
func (f *localAuthFixture) awaitCompleted(t *testing.T) {
	t.Helper()
	var status databasev1alpha1.OperationStatus
	var message string
	require.Eventually(t, func() bool {
		resp, err := f.h.GetOperationHistory(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{}),
		)
		require.NoError(t, err)
		recs := resp.Msg.GetRecords()
		if len(recs) != 1 {
			return false
		}
		status = recs[0].GetStatus()
		message = recs[0].GetMessage()
		switch status {
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
	require.Equal(
		t, databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
		status, "operation message: %s", message,
	)
}

func TestRestoreUsesCloudMirrorWhenLocalManifestFailsAuthentication(
	t *testing.T,
) {
	t.Parallel()
	f := newLocalAuthFixture(t)
	rewriteManifestUnkeyed(t, f.localDir)

	require.NoError(t, f.restore())
	f.awaitCompleted(t)

	logs := f.logs.String()
	require.Contains(t, logs, "local snapshot failed manifest authentication")
	require.Contains(t, logs, "snapshot_id="+localAuthSnapshotID)
	require.Contains(t, logs, "using cloud mirror")
}

func TestVerifySnapshotUsesCloudMirrorWhenLocalManifestFailsAuthentication(
	t *testing.T,
) {
	t.Parallel()
	f := newLocalAuthFixture(t)
	rewriteManifestUnkeyed(t, f.localDir)

	require.NoError(t, f.verify())
	f.awaitCompleted(t)

	logs := f.logs.String()
	require.Contains(t, logs, "local snapshot failed manifest authentication")
	require.Contains(t, logs, "using cloud mirror")
}

func TestLocalAuthFailureWithUnusableCloudMirrorReturnsError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(t *testing.T, f *localAuthFixture)
		cloud string
	}{
		{
			name: "tampered cloud",
			setup: func(t *testing.T, f *localAuthFixture) {
				rewriteManifestUnkeyed(t, f.cloudDir)
			},
			cloud: "failed authentication",
		},
		{
			name: "absent cloud",
			setup: func(t *testing.T, f *localAuthFixture) {
				require.NoError(t, os.RemoveAll(f.cloudDir))
			},
			cloud: "no copy in the configured cloud destination",
		},
	}
	for _, tc := range cases {
		for _, op := range []string{"restore", "verify"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				t.Parallel()
				f := newLocalAuthFixture(t)
				rewriteManifestUnkeyed(t, f.localDir)
				tc.setup(t, f)

				var err error
				if op == "restore" {
					err = f.restore()
				} else {
					err = f.verify()
				}
				require.Error(t, err)
				require.Equal(t, connect.CodeDataLoss, connect.CodeOf(err))
				require.Contains(t, err.Error(), "local copy failed authentication")
				require.Contains(t, err.Error(), tc.cloud)
				require.NotContains(t, f.logs.String(), "using cloud mirror")
			})
		}
	}
}

func TestValidLocalManifestIsChosenWithoutConsultingCloud(t *testing.T) {
	t.Parallel()
	f := newLocalAuthFixture(t)
	// The cloud copy is made unusable so a lookup would surface in the log
	// or the result.
	rewriteManifestUnkeyed(t, f.cloudDir)

	source, err := f.h.resolveSnapshotSource(
		context.Background(), localAuthSnapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, f.localDir, source)
	require.NotContains(t, f.logs.String(), "authentication")
}

func TestUnkeyedLocalProbeUnchangedWithoutTrustKey(t *testing.T) {
	t.Parallel()
	f := newLocalAuthFixture(t)
	rewriteManifestUnkeyed(t, f.localDir)
	h := newTestDatabaseServiceHandler(
		t, nil, filepath.Join(t.TempDir(), "target"),
	)
	h.bark.config.SnapshotDir = f.h.bark.config.SnapshotDir
	h.bark.config.SnapshotCloudDestination = f.cloudDest

	source, err := h.resolveSnapshotSource(
		context.Background(), localAuthSnapshotID,
	)
	require.NoError(t, err)
	require.Equal(t, f.localDir, source)
}

// signManifestForOtherID replaces dir's manifest with one the trust key signs
// for a different snapshot ID, as a complete snapshot copied under this ID
// presents.
func signManifestForOtherID(t *testing.T, dir string) {
	t.Helper()
	m, err := lifecycle.ReadManifest(
		dir, lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.NoError(t, err)
	otherDir := filepath.Join(t.TempDir(), "other-snap")
	require.NoError(t, os.MkdirAll(otherDir, 0o700))
	require.NoError(t, lifecycle.WriteManifest(
		otherDir, m, lifecycle.WithManifestKey(testSnapshotTrustKey),
	))
	data, err := os.ReadFile(filepath.Join(otherDir, lifecycle.ManifestFileName))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, lifecycle.ManifestFileName), data, 0o600,
	))
	_, err = lifecycle.ReadManifest(
		dir, lifecycle.WithManifestKey(testSnapshotTrustKey),
	)
	require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
}

func oversizeManifest(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, lifecycle.ManifestFileName),
		[]byte(strings.Repeat("x", lifecycle.MaxManifestBytes+1)), 0o600,
	))
}

// Under a trust key the unkeyed checksum and the size bound are no stronger
// than the tag: a writer to the snapshot directory can fail any of them, so
// none may make the authenticated cloud mirror of the same ID unavailable.
func TestKeyedLocalManifestFailureUsesAuthenticatedCloudMirror(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		damage func(t *testing.T, dir string)
		logMsg string
	}{
		{
			name: "corrupted checksum",
			damage: func(t *testing.T, dir string) {
				tamperManifestChecksum(
					t, filepath.Join(dir, lifecycle.ManifestFileName),
				)
			},
			logMsg: "local snapshot failed manifest checksum validation",
		},
		{
			name:   "oversized",
			damage: oversizeManifest,
			logMsg: "local snapshot failed manifest size check",
		},
		{
			name: "unparseable",
			damage: func(t *testing.T, dir string) {
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, lifecycle.ManifestFileName),
					[]byte("{"), 0o600,
				))
			},
			logMsg: "local snapshot manifest is unreadable",
		},
		{
			name:   "signed for another ID",
			damage: signManifestForOtherID,
			logMsg: "local snapshot failed manifest authentication",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newLocalAuthFixture(t)
			tc.damage(t, f.localDir)

			source, err := f.h.resolveSnapshotSource(
				context.Background(), localAuthSnapshotID,
			)
			require.NoError(t, err)
			require.NotEqual(t, f.localDir, source)

			require.NoError(t, f.verify())
			f.awaitCompleted(t)
			logs := f.logs.String()
			require.Contains(t, logs, tc.logMsg)
			require.Contains(t, logs, "snapshot_id="+localAuthSnapshotID)
			if tc.name != "unparseable" {
				require.Contains(t, logs, "using cloud mirror")
			}
		})
	}
}

// Without a usable cloud mirror a keyed local failure keeps the code the
// local failure alone would produce.
func TestKeyedLocalManifestFailureWithoutCloudKeepsLocalCode(t *testing.T) {
	t.Parallel()
	t.Run("corrupted", func(t *testing.T) {
		t.Parallel()
		f := newLocalAuthFixture(t)
		require.NoError(t, os.RemoveAll(f.cloudDir))
		tamperManifestChecksum(
			t, filepath.Join(f.localDir, lifecycle.ManifestFileName),
		)
		err := f.verify()
		require.Equal(t, connect.CodeDataLoss, connect.CodeOf(err))
		require.ErrorIs(t, err, lifecycle.ErrManifestCorrupted)
		require.Contains(t, err.Error(), "no copy in the configured cloud destination")
	})
	t.Run("oversized", func(t *testing.T) {
		t.Parallel()
		f := newLocalAuthFixture(t)
		require.NoError(t, os.RemoveAll(f.cloudDir))
		oversizeManifest(t, f.localDir)
		err := f.restore()
		require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
		require.ErrorIs(t, err, lifecycle.ErrManifestTooLarge)
		require.Contains(t, err.Error(), "no copy in the configured cloud destination")
	})
}

// A configured trust key that cannot be loaded fails the request before any
// snapshot is read, rather than probing the local copy unkeyed.
func TestUnloadableTrustKeyFailsResolutionClosed(t *testing.T) {
	t.Parallel()
	f := newLocalAuthFixture(t)
	h := newTestDatabaseServiceHandlerWithConfig(
		t, nil, filepath.Join(t.TempDir(), "target"),
		filepath.Join(t.TempDir(), "missing-trust-key"),
	)
	h.bark.config.SnapshotDir = f.h.bark.config.SnapshotDir
	h.bark.config.SnapshotCloudDestination = f.cloudDest
	for _, op := range []func() error{
		func() error {
			_, err := h.Restore(context.Background(), connect.NewRequest(
				&databasev1alpha1.RestoreRequest{SnapshotId: localAuthSnapshotID},
			))
			return err
		},
		func() error {
			_, err := h.VerifySnapshot(context.Background(), connect.NewRequest(
				&databasev1alpha1.VerifySnapshotRequest{
					SnapshotId: localAuthSnapshotID,
				},
			))
			return err
		},
	} {
		err := op()
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		require.Contains(t, err.Error(), "trust key")
		h.mu.Lock()
		busy := h.busy
		h.mu.Unlock()
		require.False(t, busy, "a refused request must release ownership")
	}
}
