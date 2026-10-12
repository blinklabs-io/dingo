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

package database

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestGetDatumRejectsWrongLengthHash covers the datum lookup key. Padded, a
// 31-byte value becomes a well-formed hash that names a different datum.
func TestGetDatumRejectsWrongLengthHash(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	datum, err := db.GetDatum(context.Background(), bytes.Repeat([]byte{0x01}, 31), nil)
	require.ErrorContains(t, err, "datum hash")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
	require.False(t, errors.Is(err, ErrDatumNotFound))
	require.Nil(t, datum)
}

// TestResolvePoolRewardAccountAutoVotesRejectsMalformedPoolKey covers a stake
// snapshot row whose pool key is shorter than a pool key hash. The array
// conversion it replaced panicked on it.
func TestResolvePoolRewardAccountAutoVotesRejectsMalformedPoolKey(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	var err error
	require.NotPanics(t, func() {
		err = db.ResolvePoolRewardAccountAutoVotes(
			context.Background(),
			[]*models.PoolStakeSnapshot{{
				Epoch: 3,
				PoolKeyHash: bytes.Repeat(
					[]byte{0x02},
					lcommon.Blake2b224Size-1,
				),
			}},
			nil,
		)
	})
	require.ErrorContains(
		t,
		err,
		"resolve reward account auto-vote for epoch 3",
	)
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
}

func TestRecoverUtxoCborRejectsWrongLengthTransactionID(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	txn := db.Transaction(context.Background(), false)
	defer txn.Release()
	cbor, err := recoverUtxoCbor(db, txn, bytes.Repeat([]byte{0x03}, 31), 0)
	require.ErrorContains(t, err, "utxo recovery transaction id")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
	require.Nil(t, cbor)
}

// TestResolveUtxoCborRejectsWrongLengthTransactionID covers the hot-cache key,
// which is the id zero-padded to 32 bytes. A 31-byte id would otherwise read
// the cached CBOR of the transaction whose id is that prefix plus a zero byte.
func TestResolveUtxoCborRejectsWrongLengthTransactionID(t *testing.T) {
	t.Parallel()
	cache := NewTieredCborCache(CborCacheConfig{
		HotUtxoEntries:  10,
		HotTxEntries:    10,
		HotTxMaxBytes:   1024,
		BlockLRUEntries: 10,
	}, nil)
	cached := make([]byte, lcommon.Blake2b256Size)
	cached[0] = 0x04
	cache.hotUtxo.Put(makeUtxoKey(cached, 0), []byte{0x82, 0x01, 0x02})

	cbor, err := cache.ResolveUtxoCbor(cached[:lcommon.Blake2b256Size-1], 0)
	require.ErrorContains(t, err, "resolve utxo cbor transaction id")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
	require.Nil(t, cbor)

	cbor, err = cache.ResolveUtxoCbor(cached, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{0x82, 0x01, 0x02}, cbor)
}
