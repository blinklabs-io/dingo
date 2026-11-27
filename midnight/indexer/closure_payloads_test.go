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

package indexer

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// A certifying Leios ranking block carries no transactions of its own: the
// datums of the closure's transactions are recorded as offsets into the
// endorser block's transaction list, which BlockPayloads names, not into the
// ranking block.
func TestRegistrationOfAClosureTransactionNamesTheEndorserBlock(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	datumCbor := simpleDatumCbor(t)
	// Each raw transaction is a CBOR item; the datum is inside the second.
	first, err := cbor.Encode([]any{"a closure transaction"})
	require.NoError(t, err)
	second, err := cbor.Encode([]any{cbor.RawMessage(datumCbor), 7})
	require.NoError(t, err)
	txs := []cbor.RawMessage{first, second}
	backing, err := database.LeiosEndorserBacking(txs)
	require.NoError(t, err)
	ebHash := pad32("eb000001")
	ebHashBytes := validHashBytes(t, ebHash)

	cfg := registrationRebuildConfig(store)
	cfg.BlockPayloads = func(
		context.Context,
		models.Block,
	) (types.PayloadLocator, error) {
		return types.NewEndorserPayloadLocator(400, ebHashBytes, backing)
	}
	idx, err := New(cfg)
	require.NoError(t, err)

	regTx := buildTx(t, pad32("cc000001"),
		[]lcommon.TransactionInput{buildInput(t, pad32("cc000002"), 0)},
		[]lcommon.TransactionOutput{
			buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datumCbor),
		},
	)
	require.NoError(t, idx.processBlock(
		testBlock(5, 500, 0x05), []lcommon.Transaction{regTx}, 5_000,
	))

	var column []byte
	require.NoError(t, store.raw.QueryRow(
		"SELECT full_datum FROM midnight_registrations",
	).Scan(&column))
	offset, err := types.DecodeEndorserOffset(column)
	require.NoError(t, err, "the datum column names the endorser block")
	require.EqualValues(t, 400, offset.BlockSlot)
	require.Equal(t, ebHashBytes, offset.BlockHash[:])
	require.Equal(
		t, datumCbor,
		backing[offset.ByteOffset:offset.ByteOffset+offset.ByteLength],
	)
}
