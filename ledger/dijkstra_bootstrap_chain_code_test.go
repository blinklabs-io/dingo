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
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

const bootstrapChainCodeError = "bootstrap witness chain code must be 32 bytes"

// withBootstrapWitness returns the CBOR of the fixture's transaction and of a
// block carrying it, with one correctly signed bootstrap witness that no input
// needs. The transaction body is unchanged, so the fixture's UTxOs and vkey
// witness still apply.
func withBootstrapWitness(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
	chainCodeLen int,
) (txCbor, blockCbor []byte) {
	t.Helper()
	privateKey := ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{0x5a}, ed25519.SeedSize),
	)
	tx := *fx.tx
	tx.WitnessSet.BootstrapWitnesses = cbor.NewSetType(
		[]lcommon.BootstrapWitness{{
			PublicKey:  privateKey.Public().(ed25519.PublicKey),
			Signature:  ed25519.Sign(privateKey, tx.Hash().Bytes()),
			ChainCode:  bytes.Repeat([]byte{0x33}, chainCodeLen),
			Attributes: []byte{0xa0},
		}},
		false,
	)
	tx.WitnessSet.SetCbor(nil)
	tx.SetCbor(nil)
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	return txCbor, dijkstraCollateralReturnBlockCbor(t, &tx)
}

// TestDijkstraBootstrapWitnessChainCodeLength drives a signed bootstrap witness
// that no Byron input needs through every path that decodes Dijkstra traffic.
// The reference rejects a chain code that is not 32 bytes when it decodes the
// witness set, so a malformed one must never reach validation or state.
func TestDijkstraBootstrapWitnessChainCodeLength(t *testing.T) {
	t.Parallel()

	for _, chainCodeLen := range []int{31, 33} {
		t.Run(
			fmt.Sprintf("%d byte chain code", chainCodeLen),
			func(t *testing.T) {
				t.Parallel()
				fx := newDijkstraCollateralReturnFixture(t, 0)
				require.NoError(t, fx.db.SetTip(fx.startTip, nil))
				txCbor, blockCbor := withBootstrapWitness(t, fx, chainCodeLen)

				_, err := gledger.NewTransactionFromCbor(
					uint(gdijkstra.TxTypeDijkstra),
					txCbor,
				)
				require.ErrorContains(t, err, bootstrapChainCodeError)

				pool := newDijkstraTestMempool(t, fx.ls)
				require.ErrorContains(
					t,
					pool.AddTransaction(uint(gdijkstra.TxTypeDijkstra), txCbor),
					bootstrapChainCodeError,
				)
				require.Empty(t, pool.Transactions())

				// Block replay decodes a persisted block before applying it.
				_, err = fx.ls.decodeReadChainBatchWithError(
					context.Background(),
					[]models.Block{{
						Slot: fx.block.SlotNumber(),
						Hash: fx.block.Hash().Bytes(),
						Type: uint(gledger.BlockTypeDijkstra),
						Cbor: blockCbor,
					}},
				)
				require.ErrorContains(t, err, bootstrapChainCodeError)

				tip, err := fx.db.GetTip(nil)
				require.NoError(t, err)
				require.Equal(t, fx.startTip, tip)
				for _, inputId := range fx.inputIds {
					_, err := fx.db.UtxoByRef(t.Context(), inputId, 0, nil)
					require.NoError(t, err)
				}
			},
		)
	}

	t.Run("32 byte control", func(t *testing.T) {
		t.Parallel()
		fx := newDijkstraCollateralReturnFixture(t, 0)
		txCbor, blockCbor := withBootstrapWitness(t, fx, 32)

		tx, err := gledger.NewTransactionFromCbor(
			uint(gdijkstra.TxTypeDijkstra),
			txCbor,
		)
		require.NoError(t, err)
		require.NoError(t, fx.ls.ValidateTx(tx))
		pool := newDijkstraTestMempool(t, fx.ls)
		require.NoError(
			t,
			pool.AddTransaction(uint(gdijkstra.TxTypeDijkstra), txCbor),
		)
		require.Len(t, pool.Transactions(), 1)

		decoded, err := fx.ls.decodeReadChainBatchWithError(
			context.Background(),
			[]models.Block{{
				Slot: fx.block.SlotNumber(),
				Hash: fx.block.Hash().Bytes(),
				Type: uint(gledger.BlockTypeDijkstra),
				Cbor: blockCbor,
			}},
		)
		require.NoError(t, err)
		require.Len(t, decoded, 1)
	})
}

// TestConwayBootstrapWitnessChainCodeStaysPermissive pins the reference's
// deliberate leniency before the Dijkstra era: a Conway transaction whose
// bootstrap witness has a short chain code still decodes, so historical replay
// is unaffected.
func TestConwayBootstrapWitnessChainCodeStaysPermissive(t *testing.T) {
	t.Parallel()

	witness := []any{
		bytes.Repeat([]byte{0x01}, 32),
		bytes.Repeat([]byte{0x02}, 64),
		bytes.Repeat([]byte{0x03}, 31),
		[]byte{0xa0},
	}
	txCbor, err := cbor.Encode([]any{
		map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)},
		map[uint]any{2: []any{witness}},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(
		uint(gledger.TxTypeConway),
		txCbor,
	)
	require.NoError(t, err)
	require.Len(t, tx.Witnesses().Bootstrap(), 1)
}
