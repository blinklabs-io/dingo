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

//go:build dingo_extra_plugins

package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/database/plugin/blob/internal/blockverify"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// TestGetBlockVerifiesContent proves GetBlock re-derives the block's hash
// from the returned bytes rather than trusting the (slot, hash) key alone:
// a genuine block round-trips, but content stored under a hash it does not
// actually hash to -- as a corrupted object, an eventual-consistency stale
// read, or a misdirected request could produce -- is rejected instead of
// being handed back to the caller as though it were the requested block.
func TestGetBlockVerifiesContent(t *testing.T) {
	blocks, err := fixtures.GenerateConwayChain(
		1, lcommon.Blake2b256{}, 1000, 10, 2,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	realBlock, otherBlock := blocks[0], blocks[1]

	store, err := NewWithOptions()
	require.NoError(t, err)
	store.client = new(s3.Client)

	t.Run("genuine content round-trips", func(t *testing.T) {
		hash := realBlock.Hash()
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			txn, 1000, hash[:], realBlock.Cbor(),
			1, uint(gledger.BlockTypeConway), 1, nil,
		))

		gotCbor, meta, err := store.GetBlock(txn, 1000, hash[:])
		require.NoError(t, err)
		require.Equal(t, realBlock.Cbor(), gotCbor)
		require.Equal(t, uint(gledger.BlockTypeConway), meta.Type)
	})

	t.Run("content for the wrong block is rejected", func(t *testing.T) {
		requestedHash := realBlock.Hash()
		txn := store.NewTransaction(true)
		// Stage otherBlock's bytes under realBlock's key, simulating a
		// remote store handing back the wrong object for the key.
		require.NoError(t, store.SetBlock(
			txn, 1010, requestedHash[:], otherBlock.Cbor(),
			2, uint(gledger.BlockTypeConway), 1, nil,
		))

		_, _, err := store.GetBlock(txn, 1010, requestedHash[:])
		require.ErrorIs(t, err, blockverify.ErrHashMismatch)
	})
}
