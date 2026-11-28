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

package sqlstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// seedBatchAssets stores holdings for five assets: live-only, live plus
// spent, spent-only, one with an empty name, and one nobody holds.
func seedBatchAssets(t *testing.T, store *Store) []models.MintedAsset {
	t.Helper()
	policyA := bytes.Repeat([]byte{0x61}, 28)
	policyB := bytes.Repeat([]byte{0x62}, 28)
	assets := []models.MintedAsset{
		{PolicyId: policyA, Name: []byte("live")},
		{PolicyId: policyA, Name: []byte("mixed")},
		{PolicyId: policyB, Name: []byte("spent")},
		{PolicyId: policyB, Name: []byte{}},
		{PolicyId: policyB, Name: []byte("unheld")},
	}
	insertAssetHolding(t, store, 70, policyA, []byte("live"), 3, 0)
	insertAssetHolding(t, store, 71, policyA, []byte("live"), 4, 0)
	insertAssetHolding(t, store, 72, policyA, []byte("mixed"), 10, 0)
	insertAssetHolding(t, store, 73, policyA, []byte("mixed"), 99, 5)
	insertAssetHolding(t, store, 74, policyB, []byte("spent"), 8, 6)
	insertAssetHolding(t, store, 75, policyB, []byte{}, 1<<62, 0)
	return assets
}

// TestGetAssetQuantitiesByPolicyAndNameMatchesSingleLookups covers the batch
// totalling each asset exactly as the single-asset lookup does, in input
// order, including assets with no live holding and a repeated asset.
func TestGetAssetQuantitiesByPolicyAndNameMatchesSingleLookups(t *testing.T) {
	t.Parallel()
	store := withAPIStorage(newMigratedSQLiteStore(t))
	assets := seedBatchAssets(t, store)
	assets = append(assets, assets[1])

	got, err := store.GetAssetQuantitiesByPolicyAndName(assets, nil)
	require.NoError(t, err)
	require.Len(t, got, len(assets))
	for i, asset := range assets {
		want, err := store.GetAssetQuantityByPolicyAndName(
			lcommon.NewBlake2b224(asset.PolicyId), asset.Name, nil,
		)
		require.NoError(t, err)
		require.Equal(t, want, got[i], "asset %d", i)
	}
	require.Equal(
		t,
		[]uint64{7, 10, 0, 1 << 62, 0, 10},
		got,
	)

	empty, err := store.GetAssetQuantitiesByPolicyAndName(nil, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

// TestGetAssetQuantitiesByPolicyAndNameIssuesOneQuery covers the batch
// totalling a page of assets in a single statement rather than one per asset.
func TestGetAssetQuantitiesByPolicyAndNameIssuesOneQuery(t *testing.T) {
	t.Parallel()
	store, counter := newRowCountingStore(t)
	store = withAPIStorage(store)
	assets := seedBatchAssets(t, store)

	counter.armed.Store(true)
	got, err := store.GetAssetQuantitiesByPolicyAndName(assets, nil)
	counter.armed.Store(false)
	require.NoError(t, err)
	require.Equal(t, []uint64{7, 10, 0, 1 << 62, 0}, got)
	require.Equal(t, int64(1), counter.queries.Load())
}

// TestGetAssetQuantitiesByPolicyAndNameChunksAtParameterLimit covers a batch
// wider than the dialect's bind-parameter limit: it is split into chunks and
// every asset is still totalled.
func TestGetAssetQuantitiesByPolicyAndNameChunksAtParameterLimit(
	t *testing.T,
) {
	t.Parallel()
	store, counter := newRowCountingStore(t)
	store = withAPIStorage(store)
	// Two assets per statement.
	d := store.dialect.(dialect)
	d.parameterLimit = 4
	store.dialect = d
	assets := seedBatchAssets(t, store)

	counter.armed.Store(true)
	got, err := store.GetAssetQuantitiesByPolicyAndName(assets, nil)
	counter.armed.Store(false)
	require.NoError(t, err)
	require.Equal(t, []uint64{7, 10, 0, 1 << 62, 0}, got)
	require.Equal(t, int64(3), counter.queries.Load())
}

// TestGetAssetQuantitiesByPolicyAndNameRejectsOverflow covers one asset's
// total passing 64 bits, which fails the batch rather than wrapping.
func TestGetAssetQuantitiesByPolicyAndNameRejectsOverflow(t *testing.T) {
	t.Parallel()
	store := withAPIStorage(newMigratedSQLiteStore(t))
	policyID := bytes.Repeat([]byte{0x63}, 28)
	insertAssetHolding(t, store, 80, policyID, []byte("big"), 1<<63, 0)
	insertAssetHolding(t, store, 81, policyID, []byte("big"), 1<<63, 0)

	_, err := store.GetAssetQuantitiesByPolicyAndName(
		[]models.MintedAsset{{PolicyId: policyID, Name: []byte("big")}},
		nil,
	)
	require.ErrorContains(t, err, "overflow")
}

// TestGetAssetQuantitiesByPolicyAndNameStopsWithItsContext covers the batch
// running under the caller's transaction context.
func TestGetAssetQuantitiesByPolicyAndNameStopsWithItsContext(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	policyID := bytes.Repeat([]byte{0x64}, 28)
	insertAssetHolding(t, store, 90, policyID, []byte("canceled"), 5, 0)

	ctx, cancel := context.WithCancel(t.Context())
	txn := store.ReadTransaction(ctx)
	defer txn.Rollback() //nolint:errcheck
	cancel()
	_, err := store.GetAssetQuantitiesByPolicyAndName(
		[]models.MintedAsset{{PolicyId: policyID, Name: []byte("canceled")}},
		txn,
	)
	require.ErrorIs(t, err, context.Canceled)
}
