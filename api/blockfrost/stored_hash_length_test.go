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

package blockfrost

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestPoolsListRejectsMalformedStoredPoolKey covers a pool id rendered from a
// stored key hash. Padded, a 27-byte key becomes the bech32 id of a pool that
// does not exist.
func TestPoolsListRejectsMalformedStoredPoolKey(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	// The active-pool query resolves the current epoch from stored epochs.
	require.NoError(t, db.Metadata().SetEpoch(
		0, 0, nil, nil, nil, nil, 6, 1, 1000, nil,
	))
	short := bytes.Repeat([]byte{0x31}, lcommon.Blake2b224Size-1)
	vrf := bytes.Repeat([]byte{0x32}, lcommon.Blake2b256Size)
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: short, VrfKeyHash: vrf},
		&models.PoolRegistration{
			PoolKeyHash: short,
			VrfKeyHash:  vrf,
		},
		nil,
	))

	ids, total, err := adapter.PoolsList(PaginationParams{Count: 10, Page: 1})
	require.ErrorContains(t, err, "active pool key hash")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, ids)
	require.Zero(t, total)
}

// TestUtxoRefRejectsWrongLengthTransactionID covers the key the adapter
// resolves UTxO CBOR under. Padded, a 31-byte id addresses a different
// transaction's outputs in the CBOR cache.
func TestUtxoRefRejectsWrongLengthTransactionID(t *testing.T) {
	t.Parallel()
	short := bytes.Repeat([]byte{0x41}, lcommon.Blake2b256Size-1)

	_, err := utxoRef(models.Utxo{TxId: short})
	require.ErrorContains(t, err, "utxo transaction id")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")

	_, err = utxoIdRef(models.UtxoId{Hash: short})
	require.ErrorContains(t, err, "utxo id")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
}
