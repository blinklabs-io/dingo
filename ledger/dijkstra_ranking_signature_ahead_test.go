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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

type dijkstraRankingAheadFixture struct {
	path   *pathFixture
	blocks []gledger.Block
}

func newDijkstraRankingAheadFixture(t *testing.T, ahead, corrupt bool) *dijkstraRankingAheadFixture {
	t.Helper()
	f := newPathFixture(t, nil, []pathTx{{}, {}})
	f.ls.config.LedgerValidateAheadEnabled = ahead
	f.ls.config.LedgerPrefetchAheadEnabled = true
	f.ls.config.ValidateHistorical = true
	f.ls.utxoPrefetchAheadAwaitReady = true
	if corrupt {
		var parts []cbor.RawMessage
		_, err := cbor.Decode(f.txs[1].Cbor(), &parts)
		require.NoError(t, err)
		encoded, err := cbor.Encode([]any{
			parts[0], pathSignedWitnesses(pathSigner(), lcommon.Blake2b256Hash([]byte("wrong ranking body"))), parts[2],
		})
		require.NoError(t, err)
		tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, encoded)
		require.NoError(t, err)
		f.txs[1] = tx.(*dijkstra.DijkstraTransaction)
	}
	f.storeOriginAndBlock(false)
	env := &dijkstraRankingAheadFixture{path: f}
	previous := lcommon.NewBlake2b256(f.originHash)
	for i, tx := range f.txs {
		block := &dijkstra.DijkstraBlock{
			BlockHeader: dijkstraTestBlockHeader(uint64(i+1), pathBlockSlot+uint64(i), previous, lcommon.Blake2b256{}, 0),
			BlockBody:   dijkstra.DijkstraBlockBody{Transactions: []dijkstra.DijkstraTransaction{*tx}},
		}
		body, err := block.BlockBody.MarshalCBOR()
		require.NoError(t, err)
		block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
		block.BlockHeader.Body.BlockBodySize = uint64(len(body))
		encoded, err := block.MarshalCBOR()
		require.NoError(t, err)
		block.SetCbor(encoded)
		previous = block.Hash()
		require.NoError(t, f.db.BlockCreate(models.Block{
			Slot: block.SlotNumber(), Hash: block.Hash().Bytes(), PrevHash: block.PrevHash().Bytes(),
			Number: block.BlockNumber(), Type: gledger.BlockTypeDijkstra, Cbor: encoded,
		}, nil))
		env.blocks = append(env.blocks, block)
	}
	return env
}

func (f *dijkstraRankingAheadFixture) replay(t *testing.T) error {
	t.Helper()
	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	results <- readChainResult{blocks: f.blocks, done: done}
	close(results)
	err := f.path.ls.ledgerProcessBlocksFromSource(t.Context(), results)
	select {
	case <-done:
	default:
		t.Fatal("ranking replay did not release its reader result")
	}
	return err
}

func (f *dijkstraRankingAheadFixture) state(t *testing.T) aheadReplayState {
	t.Helper()
	tip, err := f.path.db.GetTip(nil)
	require.NoError(t, err)
	state := aheadReplayState{Tip: tip, LiveUtxos: map[string]bool{}}
	for _, tx := range f.path.txs {
		for _, input := range tx.Inputs() {
			utxo, err := f.path.db.Metadata().GetUtxo(input.Id().Bytes(), input.Index(), nil)
			require.NoError(t, err)
			state.LiveUtxos[input.Id().String()] = utxo != nil
		}
		utxo, err := f.path.db.Metadata().GetUtxo(tx.Hash().Bytes(), 0, nil)
		require.NoError(t, err)
		state.LiveUtxos[tx.Hash().String()] = utxo != nil
	}
	return state
}

func TestDijkstraRankingValidateAheadMatchesSerial(t *testing.T) {
	t.Parallel()
	for _, corrupt := range []bool{false, true} {
		name := "valid"
		if corrupt {
			name = "invalid second signature"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			serial := newDijkstraRankingAheadFixture(t, false, corrupt)
			ahead := newDijkstraRankingAheadFixture(t, true, corrupt)
			serialErr := serial.replay(t)
			aheadErr := ahead.replay(t)
			require.Equal(t, serial.state(t), ahead.state(t))
			require.Zero(t, serial.path.ls.signaturePreverifyAheadServed.Load())
			require.Equal(t, uint64(2), ahead.path.ls.signaturePreverifyAheadServed.Load(), "Dijkstra ranking validation must consume both worker results")
			require.Zero(t, ahead.path.ls.utxoPrefetchAheadServed.Load(), "Dijkstra must retain its UTxO prefetch exclusion")
			if corrupt {
				require.Error(t, serialErr)
				require.ErrorContains(t, serialErr, "signature")
				require.EqualError(t, aheadErr, serialErr.Error())
				require.Equal(t, ahead.path.originHash, ahead.state(t).Tip.Point.Hash)
				for i, tx := range ahead.path.txs {
					require.True(t, ahead.path.inputUnspent(i), "the whole chunk must roll back")
					stored, err := ahead.path.db.GetTransactionByHash(t.Context(), tx.Hash().Bytes(), nil)
					require.NoError(t, err)
					require.Nil(t, stored)
				}
			} else {
				require.NoError(t, serialErr)
				require.NoError(t, aheadErr)
				require.Equal(t, ahead.blocks[1].Hash().Bytes(), ahead.state(t).Tip.Point.Hash)
				for i, tx := range ahead.path.txs {
					require.False(t, ahead.path.inputUnspent(i))
					stored, err := ahead.path.db.GetTransactionByHash(t.Context(), tx.Hash().Bytes(), nil)
					require.NoError(t, err)
					require.NotNil(t, stored)
				}
			}
		})
	}
}
