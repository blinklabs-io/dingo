// Copyright 2024 Blink Labs Software
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

package dblifecycle_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestDBLifecycleOperationSpans(t *testing.T) {
	spans := testutil.RecordSpans(t)
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()
	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 1,
		SnapshotRetention:    1,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	for _, epoch := range []uint64{1, 2} {
		publishEpochTransition(eb, epoch)
		dir := filepath.Join(
			snapshotDir,
			"epoch-"+strconv.FormatUint(epoch, 10),
		)
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(dir, "manifest.json"))
			return err == nil
		}, snapshotWait, 10*time.Millisecond)
	}
	require.Eventually(t, func() bool {
		count := map[string]int{}
		for _, span := range spans.Ended() {
			count[span.Name()]++
		}
		return count["dblifecycle.epoch_snapshot"] == 2 &&
			count["dblifecycle.prune"] >= 1
	}, snapshotWait, 10*time.Millisecond)

	roots := map[string]bool{}
	for _, span := range spans.Ended() {
		if span.Name() == "dblifecycle.epoch_snapshot" {
			roots[span.SpanContext().SpanID().String()] = true
		}
	}
	retriesUnderEpoch := 0
	for _, span := range spans.Ended() {
		under := roots[span.Parent().SpanID().String()]
		switch span.Name() {
		case "dblifecycle.prune":
			require.True(t, under, "prune must run under an epoch snapshot")
		case "dblifecycle.mirror_retry":
			// Start also retries once, outside any epoch.
			if under {
				retriesUnderEpoch++
			}
		}
	}
	require.NotZero(t, retriesUnderEpoch)
}
