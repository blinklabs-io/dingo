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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestInsertUtxoModelCachesAssetIDLookup proves getAssetIDQuery -- the other
// query insertUtxoModel now routes through the hot-statement cache -- is
// populated, reused across independent write transactions, and still
// resolves each asset's id correctly.
func TestInsertUtxoModelCachesAssetIDLookup(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	utxo := utxoForInsertCacheTest(10, 0, 2_000_000)
	utxo.Assets = []models.Asset{
		{
			Name:        []byte("token"),
			PolicyId:    bytes.Repeat([]byte{0xAA}, 28),
			Fingerprint: []byte("asset1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			Amount:      types.Uint64(42),
		},
	}
	insertUtxoInTxn(t, store, utxo, true)
	require.NotZero(t, utxo.Assets[0].ID)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[getAssetIDQuery]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected getAssetIDQuery to be cached on SQLite",
	)

	utxo2 := utxoForInsertCacheTest(11, 0, 3_000_000)
	utxo2.Assets = []models.Asset{
		{
			Name:        []byte("token2"),
			PolicyId:    bytes.Repeat([]byte{0xBB}, 28),
			Fingerprint: []byte("asset1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			Amount:      types.Uint64(7),
		},
	}
	insertUtxoInTxn(t, store, utxo2, true)
	require.NotZero(t, utxo2.Assets[0].ID)
	require.NotEqual(t, utxo.Assets[0].ID, utxo2.Assets[0].ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[getAssetIDQuery]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)
}
