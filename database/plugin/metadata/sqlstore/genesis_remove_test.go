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
	"github.com/stretchr/testify/require"
)

// TestDeleteUnspentGenesisUtxosKeepsAssetHolder removes a zero-coin output
// with no assets and keeps one that holds an asset, so the asset holding is
// never cascaded away with its output.
func TestDeleteUnspentGenesisUtxosKeepsAssetHolder(t *testing.T) {
	t.Parallel()
	store := withAPIStorage(newForeignKeySQLiteStore(t))
	bare := utxoForInsertCacheTest(41, 0, 0)
	bare.Address = []byte("test-address")
	holder := utxoForInsertCacheTest(42, 0, 0)
	holder.Address = []byte("test-address")
	holder.Assets = []models.Asset{{
		PolicyId: bytes.Repeat([]byte{0x63}, 28),
		Name:     []byte("held"),
		Amount:   7,
	}}
	insertUtxoInTxn(t, store, bare, true)
	insertUtxoInTxn(t, store, holder, true)
	refs := []models.UtxoId{
		{Hash: bare.TxId, Idx: bare.OutputIdx},
		{Hash: holder.TxId, Idx: holder.OutputIdx},
	}

	removed, _, err := store.DeleteUnspentGenesisUtxos(refs, nil, nil)
	require.NoError(t, err)
	require.Equal(t, refs[:1], removed)
	require.Equal(t, 1, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM utxo`))
	require.Equal(t, 1, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_holding`))
}
