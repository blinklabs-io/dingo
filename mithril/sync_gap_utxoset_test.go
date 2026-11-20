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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// The gap cleanup deletes governance rows straight from the metadata store,
// which a live UTxO set that copied them cannot see, so the cleanup must drop
// the set's governance marker. The invalidation also runs when the cleanup
// itself failed part way.
func TestGapCleanupInvalidatesTheLiveUtxoSetGovernance(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	for _, slot := range []uint64{100, 200} {
		require.NoError(t, db.BlockCreate(models.Block{
			Slot: slot,
			Hash: testGapHash32("blk"),
			Cbor: []byte{0x82, byte(slot)},
			Type: 1,
		}, nil))
	}
	store, err := badgerutxo.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.LoadGovernance(
		func(func(k, v []byte) error) error { return nil },
	))
	loaded, err := store.GovernanceLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	cfg := SyncConfig{InvalidateUtxoSetGovernance: store.InvalidateGovernance}
	require.NoError(
		t, cfg.deleteGapBlocksAbove(context.Background(), db, 150, nil),
	)
	loaded, err = store.GovernanceLoaded()
	require.NoError(t, err)
	require.False(t, loaded, "the set kept governance rows the store deleted")

	// A failing invalidation is reported, not dropped.
	boom := errors.New("invalidation failed")
	cfg.InvalidateUtxoSetGovernance = func() error { return boom }
	require.ErrorIs(
		t,
		cfg.deleteGapBlocksAbove(context.Background(), db, 150, nil),
		boom,
	)
}
