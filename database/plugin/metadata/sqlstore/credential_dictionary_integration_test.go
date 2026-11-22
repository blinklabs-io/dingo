//go:build dingo_db_integration

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
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func engineRows(t *testing.T, store *Store, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, store.writeDB.QueryRow(
		store.dialect.Rebind(query), args...,
	).Scan(&n))
	return n
}

// TestCredentialDictionariesOnEngines runs the dictionary writers and readers
// on PostgreSQL and MySQL: shared credentials and datum hashes are held once,
// an unrevealed datum row is filled in by the reveal's conditional upsert and
// then left alone, a rolled back write leaves no dictionary row, the live
// stake key source batches by (tag, hash) however the ids were issued, and a
// pool's op-cert counters round trip through the clustered table.
func TestCredentialDictionariesOnEngines(t *testing.T) {
	t.Parallel()
	for _, engine := range []string{"postgres", "mysql"} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			store := newAssetIntegrationStore(t, engine)
			// The outputs carry no address, which API mode requires.
			store.storageMode = types.StorageModeCore
			ctx := context.Background()
			payment, stake := dictionaryHash(0xa1), dictionaryHash(0xb1)
			hash := bytes.Repeat([]byte{0xd1}, 32)
			var datumHash lcommon.Blake2b256
			copy(datumHash[:], hash)

			importCredentialUtxos(t, store,
				credentialUtxo(1, payment, stake, hash, 0),
				credentialUtxo(2, payment, stake, hash, 1),
				credentialUtxo(3, nil, nil, nil, 0),
			)
			require.Equal(t, 2, engineRows(t, store,
				`SELECT COUNT(*) FROM credential`))
			require.Equal(t, 1, engineRows(t, store,
				`SELECT COUNT(*) FROM datum WHERE LENGTH(raw_datum) = 0`))
			got, err := store.GetUtxo(bytes.Repeat([]byte{2}, 32), 0, nil)
			require.NoError(t, err)
			require.Equal(t, payment, got.PaymentKey)
			require.Equal(t, stake, got.StakingKey)
			require.Equal(t, hash, got.DatumHash)
			require.Equal(t, uint8(1), got.CredentialTag)
			missing, err := store.GetDatum(datumHash, nil)
			require.NoError(t, err)
			require.Nil(t, missing)

			raw := []byte{0x82, 0x01, 0x02}
			var rows rowBatch
			rows.add(datumShape, hash, raw, int64(50))
			rows.add(datumShape, hash, []byte{0x01}, int64(51))
			require.NoError(t, rows.flush(
				ctx, newDialectQueryer(store.writeDB, engine),
				store.dialect.ParameterLimit(),
			))
			var again rowBatch
			again.add(datumShape, hash, []byte{0xff}, int64(99))
			require.NoError(t, again.flush(
				ctx, newDialectQueryer(store.writeDB, engine),
				store.dialect.ParameterLimit(),
			))
			revealed, err := store.GetDatum(datumHash, nil)
			require.NoError(t, err)
			require.NotNil(t, revealed)
			require.Equal(t, raw, revealed.RawDatum)
			require.Equal(t, uint64(50), revealed.AddedSlot)
			require.Equal(t, 1, engineRows(t, store,
				`SELECT COUNT(*) FROM datum`))

			txn := store.Transaction(ctx)
			require.NoError(t, store.ImportUtxos([]models.Utxo{
				credentialUtxo(4, dictionaryHash(0xc1), nil, nil, 0),
			}, txn))
			require.NoError(t, txn.Rollback())
			require.Equal(t, 2, engineRows(t, store,
				`SELECT COUNT(*) FROM credential`))

			// Keys are batched by (tag, hash), not by dictionary id.
			keys := []stakeKeyBound{
				{0, dictionaryHash(0x30)}, {0, dictionaryHash(0x10)},
				{1, dictionaryHash(0x20)}, {0, dictionaryHash(0x20)},
			}
			for i, key := range keys {
				importCredentialUtxos(t, store, credentialUtxo(
					byte(0x40+i), nil, key.key, nil, key.tag,
				))
			}
			q := newDialectQueryer(store.writeDB, engine)
			count, err := countRewardLiveStakeKeys(ctx, q)
			require.NoError(t, err)
			// The keys above, and the stake key of outputs 1 and 2 (tag 0
			// and tag 1 share one hash).
			require.Equal(t, int64(len(keys)+2), count)
			end, err := nextRewardLiveStakeBatchEnd(ctx, q, nil, 2)
			require.NoError(t, err)
			require.NotNil(t, end)
			require.Equal(t, uint8(0), end.tag)
			require.Equal(t, dictionaryHash(0x20), end.key)

			pool := lcommon.PoolKeyHash{9}
			require.NoError(t, store.UpdatePoolOpCertSequence(pool, 1, 10, nil))
			require.NoError(t, store.UpdatePoolOpCertSequence(pool, 3, 20, nil))
			require.ErrorContains(t,
				store.UpdatePoolOpCertSequence(pool, 5, 10, nil),
				"opcert counter conflict")
			latest, found, err := store.LatestPoolOpCertSequence(pool, nil)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, uint64(3), latest)
		})
	}
}
