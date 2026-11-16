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

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestMithrilSyncRefusesUtxoSetOnlyWithoutDatabasePath requires the sync to
// refuse before opening the live UTxO set, which would otherwise land in the
// working directory.
// Not t.Parallel: it changes the working directory to catch a stray set.
func TestMithrilSyncRefusesUtxoSetOnlyWithoutDatabasePath(t *testing.T) {
	t.Chdir(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runMithrilSyncWithRepair(
		ctx,
		&config.Config{LedgerUtxoBadgerOnly: true},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"preview",
		nil,
		false,
	)
	require.ErrorContains(t, err, "ledgerUtxoBadgerOnly needs databasePath")
	_, statErr := os.Stat("ledger")
	require.ErrorIs(t, statErr, os.ErrNotExist, "no set was opened")
}
