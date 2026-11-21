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

// Package drainfix drains a test database's ledger-state families into its
// live UTxO set, the way the node does at startup.
package drainfix

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// Store adapts a database to statedrain.Store.
type Store struct{ *database.Database }

// SQLTip implements statedrain.Store.
func (s Store) SQLTip(context.Context) (ocommon.Point, error) {
	tip, err := s.GetTip(nil)
	return tip.Point, err
}

// GetSyncState implements statedrain.Store.
func (s Store) GetSyncState(_ context.Context, key string) (string, error) {
	return s.Database.GetSyncState(key, nil)
}

// SetSyncState implements statedrain.Store.
func (s Store) SetSyncState(_ context.Context, key, value string) error {
	return s.Database.SetSyncState(key, value, nil)
}

// DeleteSyncState implements statedrain.Store.
func (s Store) DeleteSyncState(_ context.Context, key string) error {
	return s.DeleteFamilySyncState(key)
}

// ListSyncStateKeys implements statedrain.SyncKeyLister.
func (s Store) ListSyncStateKeys(
	_ context.Context,
	prefix string,
) ([]string, error) {
	return s.ListSyncStateKeysByPrefix(prefix, nil)
}

// Source adapts the set to statedrain.Source.
type Source struct {
	*utxoset.Set
}

// Loaded implements statedrain.Source.
func (s Source) Loaded(f nodesettings.Family) (bool, error) {
	return ledger.FamilyLoaded(f, s.Set)
}

// Drain loads set from db's SQL rows and drains every graduated family,
// leaving the set the only copy. It returns the latch.
func Drain(
	t *testing.T,
	db *database.Database,
	set *utxoset.Set,
) nodesettings.FamilyMask {
	t.Helper()
	ctx := t.Context()
	ldb := dbadapter.New(db)
	require.NoError(t, set.LoadLedgerState(ctx, ldb))
	require.NoError(t, statedrain.Run(
		ctx,
		statedrain.Env{Store: Store{db}, Set: set},
		statedrain.Graduated(Source{Set: set}),
	))
	mask, err := db.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.NotZero(t, mask, "the fixture must have drained families")
	set.SetDrainedFamilies(mask)
	return mask
}
