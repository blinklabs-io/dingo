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

package sqlite

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/types"
)

var testMintPolicy = lcommon.NewBlake2b224(
	[]byte("0123456789012345678901234567"), // 28 bytes
)

func mintOf(name []byte, amount int64) *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	ma := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			testMintPolicy: {
				cbor.NewByteString(name): big.NewInt(amount),
			},
		},
	)
	return &ma
}

func TestAssetMintBurnTrackingAndRollback(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, types.StorageModeAPI)
	name := []byte("token")

	setTx := func(hash string, slot uint64, amount int64) {
		tx := &mockTransaction{
			hash:    lcommon.NewBlake2b256([]byte(hash)),
			isValid: true,
			mint:    mintOf(name, amount),
		}
		require.NoError(t, store.SetTransaction(
			tx,
			ocommon.Point{Hash: []byte(hash + "_block"), Slot: slot},
			0,
			nil,
			nil,
		))
	}

	// Initial mint, a later mint, then a burn.
	setTx("mint_tx_1", 100, 1000)
	setTx("mint_tx_2", 200, 500)
	setTx("burn_tx_3", 300, -200)

	initialHash, count, err := store.GetAssetMintBurnInfo(
		testMintPolicy,
		name,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
	assert.Equal(
		t,
		lcommon.NewBlake2b256([]byte("mint_tx_1")).Bytes(),
		initialHash,
	)

	// Re-applying the initial mint must be idempotent (no duplicate rows).
	setTx("mint_tx_1", 100, 1000)
	_, count, err = store.GetAssetMintBurnInfo(testMintPolicy, name, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Rollback past slot 150 removes the later mint and the burn.
	require.NoError(t, store.DeleteTransactionsAfterSlot(150, nil))
	initialHash, count, err = store.GetAssetMintBurnInfo(
		testMintPolicy,
		name,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(
		t,
		lcommon.NewBlake2b256([]byte("mint_tx_1")).Bytes(),
		initialHash,
	)
}

func TestAssetMintBurnInfoNoHistory(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, types.StorageModeAPI)

	initialHash, count, err := store.GetAssetMintBurnInfo(
		testMintPolicy,
		[]byte("missing"),
		nil,
	)
	require.NoError(t, err)
	assert.Nil(t, initialHash)
	assert.Equal(t, 0, count)
}

func TestAssetMintBurnNotRecordedInCoreMode(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, types.StorageModeCore)
	name := []byte("token")

	tx := &mockTransaction{
		hash:    lcommon.NewBlake2b256([]byte("core_mint")),
		isValid: true,
		mint:    mintOf(name, 1000),
	}
	require.NoError(t, store.SetTransaction(
		tx,
		ocommon.Point{Hash: []byte("core_block"), Slot: 100},
		0,
		nil,
		nil,
	))

	_, count, err := store.GetAssetMintBurnInfo(testMintPolicy, name, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
