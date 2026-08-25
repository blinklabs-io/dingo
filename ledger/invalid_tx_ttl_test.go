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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestLedgerProcessBlockRejectsExpiredPhase2InvalidTransaction(
	t *testing.T,
) {
	const (
		invalidHereafter = uint64(9)
		blockSlot        = uint64(10)
	)
	db := newTestDB(t)
	txCbor, err := cbor.Encode([]any{
		map[uint]any{2: uint64(0), 3: invalidHereafter},
		map[uint]any{},
		nil,
	})
	require.NoError(t, err)
	tx, err := dijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	tx.TxIsValid = false

	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  blockSlot,
				ByteLength: uint32(len(txCbor)), // #nosec G115 -- fixture is bounded
			},
		},
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        blockSlot,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: 12,
					},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			InvalidTransactions: []uint{0},
			Transactions:        []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)

	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err = db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: blockSlot, Hash: []byte("expired-invalid-tx")},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{},
			offsets,
			eras.DijkstraEraDesc,
			pparams,
			nil,
		)
		return err
	})
	var invalidHereafterErr eras.InvalidHereafterError
	require.ErrorAs(t, err, &invalidHereafterErr)
}
