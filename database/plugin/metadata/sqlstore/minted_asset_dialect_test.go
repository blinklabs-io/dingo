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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMintedAssetIndexLifecycle(t *testing.T) {
	t.Parallel()
	testMintedAssetIndexLifecycle(t, newAPIModeSQLiteStore(t, nil))
}

// testMintedAssetIndexLifecycle drives the minted_asset maintenance
// statements through a store's own dialect: the row-value IN insert, the
// rollback delete and the correlated repair update.
func testMintedAssetIndexLifecycle(t *testing.T, store *Store) {
	t.Helper()
	policy := assetPolicy(0x5a)
	flush := func(events ...struct {
		tx   byte
		name string
		slot uint64
	}) {
		txn := store.Transaction(context.Background())
		db, ctx, err := store.dbFromTxn(txn)
		require.NoError(t, err)
		rows := rowBatch{}
		for _, event := range events {
			rows.add(
				assetMintBurnShape, credentialHash(event.tx), policy,
				[]byte(event.name), []byte("fp"), event.slot, "1",
				uint32(0),
			)
		}
		require.NoError(t, rows.flush(ctx, db, store.dialect.ParameterLimit()))
		require.NoError(t, txn.Commit())
	}
	type event = struct {
		tx   byte
		name string
		slot uint64
	}
	names := func() []string {
		assets, err := store.ListMintedAssets(policy, 0, 0, false, nil)
		require.NoError(t, err)
		ret := make([]string, 0, len(assets))
		for _, asset := range assets {
			ret = append(ret, string(asset.Name))
		}
		return ret
	}
	count := func() int {
		total, err := store.CountMintedAssets(policy, nil)
		require.NoError(t, err)
		return total
	}

	flush(event{0x71, "D", 50}, event{0x71, "", 50}, event{0x72, "E", 20})
	flush(event{0x73, "D", 15}, event{0x73, "E", 15}, event{0x74, "F", 60})
	require.Equal(t, []string{"D", "", "E", "F"}, names())
	require.Equal(t, 4, count())

	require.NoError(t, store.DeleteTransactionsAfterSlot(30, nil))
	require.Equal(t, []string{"E", "D"}, names())
	require.Equal(t, 2, count())

	require.NoError(t, store.DeleteTransactionsAfterSlot(10, nil))
	require.Empty(t, names())
	require.Zero(t, count())
}
