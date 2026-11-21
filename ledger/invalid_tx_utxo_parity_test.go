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

package ledger

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A phase-2-invalid transaction consumes its collateral and creates only its
// collateral return, at the index after its regular outputs. The metadata
// store and the UTxO change staged for the live set must agree on all of it:
// they are the two sides the live-stake aggregate is compared across.
func TestInvalidTransactionUtxoChangeMatchesTheStore(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: t.TempDir()},
	})
	require.NoError(t, err)

	stake := bytes.Repeat([]byte{0xab}, 28)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xaa}, 28), stake,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	seed := func(id byte, amount uint64) []byte {
		hash := bytes.Repeat([]byte{id}, 32)
		require.NoError(t, db.CreateUtxo(t.Context(), nil, &models.Utxo{
			TxId: hash, OutputIdx: 0, Amount: types.Uint64(amount),
			Address: addrBytes, StakingKey: stake, AddedSlot: 5,
		}))
		return hash
	}
	collateral := seed(0xc0, 1_000)
	regular := seed(0xc1, 777)

	out := func(amount uint64) map[uint]any {
		return map[uint]any{0: addrBytes, 1: amount}
	}
	raw, err := cbor.Encode([]any{
		map[uint]any{
			0:  []any{[]any{regular, uint64(0)}},
			1:  []any{out(500), out(600)},
			2:  uint64(7),
			13: []any{[]any{collateral, uint64(0)}},
			16: out(400),
		},
		map[uint]any{}, false, nil,
	})
	require.NoError(t, err)
	tx, err := conway.NewConwayTransactionFromCbor(raw)
	require.NoError(t, err)
	var hash [32]byte
	copy(hash[:], tx.Hash().Bytes())
	point := ocommon.NewPoint(9, bytes.Repeat([]byte{0x09}, 32))

	ldb := dbadapter.New(db)
	txn := ldb.Begin(t.Context(), true)
	defer txn.Release()
	require.NoError(t, ldb.SetTransactionWithOpts(
		t.Context(), tx, point, 0, 0, nil, nil,
		&database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{hash: {}},
			UtxoOffsets: map[database.UtxoRef]database.CborOffset{
				{TxId: hash, OutputIdx: 2}: {BlockSlot: 9, ByteLength: 1},
			},
		},
		txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
	))
	require.NoError(t, txn.Commit())

	evt, err := utxoEventFor(point, 0, tx)
	require.NoError(t, err)

	live := func(hash []byte, idx uint32) *models.Utxo {
		u, err := db.Metadata().GetUtxo(hash, idx, nil)
		require.NoError(t, err)
		return u
	}
	require.Nil(t, live(collateral, 0), "store keeps the collateral")
	require.NotNil(t, live(regular, 0), "store consumed a regular input")
	require.Nil(t, live(hash[:], 0), "store created regular output 0")
	require.Nil(t, live(hash[:], 1), "store created regular output 1")
	ret := live(hash[:], 2)
	require.NotNil(t, ret, "store lacks the collateral return at index 2")
	require.Equal(t, types.Uint64(400), ret.Amount)
	require.Equal(t, stake, ret.StakingKey)

	require.Equal(t,
		[]models.UtxoId{{Hash: collateral, Idx: 0}}, evt.Removed,
		"set event removes exactly the collateral",
	)
	require.Len(t, evt.Added, 1, "set event adds exactly the return")
	require.Equal(t,
		models.UtxoId{Hash: hash[:], Idx: 2}, evt.Added[0].Ref,
	)
}
