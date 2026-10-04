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
	"encoding/hex"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

func TestBlockByPointPreservesDeepConwayRedeemer(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("testdata/conway-deep-redeemer.cbor")
	require.NoError(t, err)

	const blockHash = "c6ce58758d3634f06056c05a9425cacc07ae7581192f28ef7e15afe47e4b5dbb"
	hash, err := hex.DecodeString(blockHash)
	require.NoError(t, err)
	block := models.Block{
		ID: 1, Slot: 122726746, Number: 4661680, Hash: hash,
		Type: ledger.BlockTypeConway, Cbor: raw,
	}
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(block, nil))

	stored, err := BlockByPoint(db, ocommon.NewPoint(block.Slot, hash))
	require.NoError(t, err)
	require.Equal(t, raw, stored.Cbor)
	decoded, err := stored.Decode()
	require.NoError(t, err)
	require.Equal(t, blockHash, decoded.Hash().String())
	require.Equal(t, block.Slot, decoded.SlotNumber())
	require.Equal(t, block.Number, decoded.BlockNumber())
	require.Len(t, decoded.Transactions(), 2)

	redeemers := decoded.Transactions()[1].Witnesses().Redeemers()
	require.NotNil(t, redeemers)
	count := 0
	for _, redeemer := range redeemers.Iter() {
		count++
		current := redeemer.Data.Data
		depth := 0
		for {
			list, ok := current.(*data.List)
			if !ok {
				break
			}
			depth++
			require.Len(t, list.Items, 1)
			current = list.Items[0]
		}
		require.Greater(t, depth, 256)
	}
	require.Equal(t, 1, count)
}
