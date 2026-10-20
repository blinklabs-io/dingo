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

package lifecycle

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCloudReconciliationRejectsEntryUnderAnotherID(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(
		t,
		EnsureSnapshotCatalog(base, WithManifestKey(catalogTestTrustKey)),
	)
	dir := filepath.Join(t.TempDir(), "older")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, WriteManifest(dir, Manifest{
		CreatedAt: time.Unix(1, 0).UTC(), Trigger: TriggerManual,
	}, WithManifestKey(catalogTestTrustKey)))
	older, err := ReadManifest(dir, WithManifestKey(catalogTestTrustKey))
	require.NoError(t, err)
	registry := NewDestinationRegistry()
	registry.Register("replayedscan", func(*url.URL) (CloudDestination, error) {
		return &catalogScriptedLister{entries: []SnapshotEntry{
			{ID: "older", Manifest: older},
			{ID: "newer", Manifest: older},
		}}, nil
	})

	current, err := ReconcileCloudSnapshotCatalogContext(
		t.Context(), base, registry, "replayedscan://bucket/snapshots",
		WithManifestKey(catalogTestTrustKey),
	)
	require.ErrorIs(t, err, ErrSnapshotCatalogIncomplete)
	require.ErrorIs(t, err, ErrManifestIDMismatch)
	require.False(t, current)
}
