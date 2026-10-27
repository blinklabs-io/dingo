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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidatedBlockTransactionWriteIsObservedAsApply drives a validating
// block through ledgerProcessBlock. A validating block writes each
// transaction's state as soon as it validates, so the batch flush has nothing
// left; the apply stage must still account for that write.
func TestValidatedBlockTransactionWriteIsObservedAsApply(t *testing.T) {
	t.Parallel()

	ls := processValidatedTestBlock(t)
	count, sum := blockStageSampleCount(t, &ls.metrics, blockStageApply)
	assert.Equal(
		t, uint64(1), count,
		"the validated transaction's state write must be observed as apply",
	)
	assert.Positive(t, sum)
}

// TestValidatedBlockInputResolutionIsObservedAsValidate requires the
// block-level part of validating a block, resolving its inputs and checking
// its reference scripts, to be a validate sample beside the one per
// transaction.
func TestValidatedBlockInputResolutionIsObservedAsValidate(t *testing.T) {
	t.Parallel()

	ls := processValidatedTestBlock(t)
	count, _ := blockStageSampleCount(t, &ls.metrics, blockStageValidate)
	assert.Equal(
		t, uint64(2), count,
		"one block-level and one per-transaction validate sample",
	)
}

// TestDeferredHeaderCheckIsObservedPerBlock requires the in-transaction
// check of whether admission deferred a block's header verification, and
// that verification when it was deferred, to be a sample of its own stage.
func TestDeferredHeaderCheckIsObservedPerBlock(t *testing.T) {
	t.Parallel()

	ls := processValidatedTestBlock(t)
	count, sum := blockStageSampleCount(
		t, &ls.metrics, blockStageDeferredHeaderVerify,
	)
	assert.Equal(t, uint64(1), count)
	assert.Positive(t, sum)
}

// processValidatedTestBlock applies one validating Dijkstra block holding a
// single transaction through ledgerProcessBlock and returns its ledger state.
func processValidatedTestBlock(t *testing.T) *LedgerState {
	t.Helper()

	db := newTestDB(t)
	inputID := bytes.Repeat([]byte{0x41}, 32)
	input, err := omockledger.NewSimpleTransactionInput(inputID, 0)
	require.NoError(t, err)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x44}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	output, err := omockledger.NewSimpleTransactionOutput(address.String(), 10)
	require.NoError(t, err)
	mockTx := omockledger.NewTransactionBuilder()
	mockTx.WithId(bytes.Repeat([]byte{0x43}, 32))
	mockTx.WithType(gdijkstra.TxTypeDijkstra)
	mockTx.WithInputs(input)
	mockTx.WithOutputs(output)
	mockTx.WithValid(true)
	tx := &validityOutcomeStateTx{MockTransaction: mockTx}
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {BlockSlot: 10, ByteLength: 1},
		},
		UtxoOffsets: map[database.UtxoRef]database.CborOffset{
			{TxId: txHash, OutputIdx: 0}: {BlockSlot: 10, ByteLength: 1},
		},
	}
	require.NoError(
		t,
		db.CreateUtxo(context.Background(), nil, &models.Utxo{
			TxId: inputID, OutputIdx: 0, AddedSlot: 1,
		}),
	)

	validated := 0
	testEra := eras.DijkstraEraDesc
	testEra.ValidateTxFunc = func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error {
		validated++
		return nil
	}
	pparams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MaxBlockBodySize:   100_000,
			MaxBlockHeaderSize: 100_000,
		},
	}
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{testEra},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
		currentEra: testEra,
	}
	ls.metrics.init(prometheus.NewRegistry())
	block := &validityOutcomeTestBlock{
		header: &gdijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        10,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
		txs: []lcommon.Transaction{tx},
		era: gdijkstra.EraDijkstra,
	}
	require.NoError(t, db.Transaction(context.Background(), true).
		Do(func(txn *database.Txn) error {
			delta, err := ls.ledgerProcessBlock(
				context.Background(),
				txn,
				ocommon.NewPoint(10, block.Hash().Bytes()),
				block,
				true,
				false,
				false,
				nil,
				envelopeParent{origin: true},
				offsets,
				testEra,
				pparams,
				nil,
				0,
				0,
				false,
			)
			if delta != nil {
				delta.Release()
			}
			return err
		}))

	require.Equal(t, 1, validated, "the block must take the validating path")
	return ls
}
