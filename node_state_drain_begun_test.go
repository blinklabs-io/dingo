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
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A drain interrupted after its marker was written has deleted some rows but
// set no latch bit; a start without ledgerStateSetOnly must refuse it.
func TestBegunDrainRefusesAStartWithoutSetOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newDrainDatabase(t, t.TempDir())
	require.NoError(t, db.SetSyncState(
		statedrain.MarkerPrefix+nodesettings.FamilyAccounts.String(), "1", nil,
	))
	n := &Node{db: db}
	n.config.ledgerUtxoBadgerOnly = true

	err := n.checkLedgerStateSetOnly(ctx)
	require.ErrorContains(t, err, "start with ledgerStateSetOnly")
	require.ErrorContains(t, err, "accounts")
	require.ErrorContains(
		t, n.reconcileUtxoSet(ctx), "start with ledgerStateSetOnly",
	)

	n.config.ledgerStateSetOnly = true
	require.NoError(t, n.checkLedgerStateSetOnly(ctx))
}

// The startup reconcile runs before the drain resumes, so the family whose
// drain was interrupted must already be published as drained to the ledger.
func TestBegunDrainIsPublishedBeforeTheReconcile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newDrainDatabase(t, t.TempDir())
	require.NoError(t, db.SetSyncState(
		statedrain.MarkerPrefix+nodesettings.FamilyPoolsHistory.String(),
		"1", nil,
	))
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	n := &Node{db: db, utxoSet: set, ledgerState: &ledger.LedgerState{}}
	n.config.ledgerUtxoBadgerOnly = true
	n.config.ledgerStateSetOnly = true

	require.NoError(t, n.publishDrainedFamilies(ctx))
	want := nodesettings.FamilyMask(0).With(nodesettings.FamilyPoolsHistory)
	require.Equal(t, want, n.ledgerState.DrainedFamilies())
	require.True(t, db.FamilyDrained(nodesettings.FamilyPoolsHistory))
}
