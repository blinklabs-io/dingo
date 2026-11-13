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

package mithril

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/stretchr/testify/require"
)

// TestSyncCatchUpRefusesLiveUtxoSetBeforeDownloading requires a catch-up
// with a live UTxO set to fail at dispatch, before any artifact is fetched.
func TestSyncCatchUpRefusesLiveUtxoSetBeforeDownloading(t *testing.T) {
	t.Parallel()
	fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
	dataDir := t.TempDir()
	// A marker behind the artifact's immutable tip makes the run a catch-up.
	seedCompleteDB(t, dataDir, "core", 0, true)

	_, err := Sync(context.Background(), SyncConfig{
		Network:           "preview",
		DataDir:           dataDir,
		StorageMode:       "core",
		Backend:           BackendV2,
		AggregatorURL:     fix.server.URL,
		AllowInsecureHTTP: true,
		StoragePlugins:    testStoragePlugins(),
		DatabaseWorkers:   1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		UtxoSet:           struct{ ledgerstate.UtxoSetLoader }{},
	})
	require.ErrorContains(t, err, "cannot catch up a database that keeps")
}
