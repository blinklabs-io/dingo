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

package dingo

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// failingStateSource fails the restore-state check, which ends the startup
// restore with an error.
type failingStateSource struct {
	*fakeSnapshotSource
}

func (failingStateSource) SnapshotState(
	context.Context, *database.Database,
) (MithrilSnapshotState, error) {
	return MithrilSnapshotFresh, errors.New("state unavailable")
}

// TestMithrilSnapshotStartupClosesUtxoSetOnFailure requires a startup
// restore that fails to leave no live UTxO set open: Start returns the
// error without registering the set's cleanup.
func TestMithrilSnapshotStartupClosesUtxoSetOnFailure(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	n := &Node{
		db:            db,
		mithrilSource: failingStateSource{&fakeSnapshotSource{}},
	}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	n.config.dataDir = t.TempDir()
	n.config.cardanoNodeConfig = &cardano.CardanoNodeConfig{}
	n.config.cfg = &internalconfig.Config{}
	n.config.ledgerUtxoBadger = true
	n.config.ledgerUtxoBadgerOnly = true

	require.Error(t, n.mithrilSnapshotStartup(t.Context()))
	if n.utxoSet != nil {
		_ = n.closeUtxoSet()
		t.Fatal("live UTxO set left open after a failed restore")
	}
}
