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

package forging

import (
	"errors"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// newDijkstraFitBuilder builds a Dijkstra-era builder whose block body
// budget is maxBlockBody bytes, so a test can decide exactly which
// candidates fit.
func newDijkstraFitBuilder(
	t *testing.T,
	mempool *mockMempool,
	validator *sessionMockTxValidator,
	maxTxSize int,
	maxBlockBody int,
) *DefaultBlockBuilder {
	t.Helper()
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(maxTxSize),
			MaxBlockBodySize: uint(maxBlockBody),
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: setupTestCredentials(t),
		TxValidator: validator,
	})
	require.NoError(t, err)
	return builder
}

// makeWideTxCbor builds a valid Conway transaction carrying outputCount
// outputs, which is how a test grows a transaction past a block-body budget
// without producing an address the decoder rejects.
func makeWideTxCbor(t *testing.T, txID byte, outputCount int) []byte {
	t.Helper()
	txHash := make([]byte, 32)
	txHash[0] = txID
	outputs := make([]any, 0, outputCount)
	for range outputCount {
		// 29 bytes: a Shelley enterprise address, header plus key hash.
		addr := make([]byte, 29)
		addr[0] = 0x61
		outputs = append(outputs, []any{addr, uint64(1000000)})
	}
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{txHash, uint64(0)}},
		},
		1: outputs,
		2: uint64(200000),
	}
	txCbor, err := cbor.Encode([]any{bodyMap, map[uint]any{}, true, nil})
	require.NoError(t, err)
	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err, "generated CBOR must decode as a valid Conway tx")
	return txCbor
}

// TestSelectionContinuesPastACandidateThatCannotFit pins the stop rule.
// Running the exact block-body check before re-validation made the size
// break reachable from a candidate that would also have failed validation,
// so a transaction whose inputs were consumed since mempool admission --
// which used to be skipped -- could end the whole selection pass. On a
// mempool re-validating badly that shortens blocks for a reason that has
// nothing to do with fullness.
//
// A candidate that does not fit is now skipped, exactly as the MaxTxSize
// and MaxExUnits checks above it skip theirs, and only a re-validated
// candidate that the block cannot hold ends the pass.
func TestSelectionContinuesPastACandidateThatCannotFit(t *testing.T) {
	// first and last are small; middle is far too large for what remains
	// of the body once first has been selected, and would fail
	// re-validation as well.
	firstCbor := makeMinimalTxCbor(t, 0x01, 0)
	middleCbor := makeWideTxCbor(t, 0x02, 40)
	lastCbor := makeMinimalTxCbor(t, 0x03, 0)
	require.Greater(t, len(middleCbor), len(firstCbor)+len(lastCbor))

	validator := &sessionMockTxValidator{}
	// Enough body for the two small transactions and their wrapper, and
	// nowhere near enough for the large one.
	maxBlockBody := len(firstCbor) + len(lastCbor) + 64
	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: firstCbor,
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx2",
				Cbor: middleCbor,
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx3",
				Cbor: lastCbor,
				Type: dijkstra.TxTypeDijkstra,
			},
		},
	}
	// MaxTxSize admits the large transaction, so the body budget is the
	// only thing that can reject it.
	builder := newDijkstraFitBuilder(
		t,
		mempool,
		validator,
		len(middleCbor)+64,
		maxBlockBody,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(
		t,
		block.Transactions(),
		2,
		"a candidate that cannot fit must not end selection for the ones after it",
	)
	require.Len(
		t,
		validator.validatedHashes,
		2,
		"a candidate that cannot fit must not pay for re-validation",
	)
}

// TestSelectionContinuesPastACandidateThatFailsRevalidation is the plain
// case the hoist must not have disturbed: a transaction the block could
// have carried, rejected by re-validation, is skipped and the ones after it
// are still selected.
func TestSelectionContinuesPastACandidateThatFailsRevalidation(
	t *testing.T,
) {
	offered := 0
	validator := &sessionMockTxValidator{
		validateErr: func(string) error {
			offered++
			if offered == 2 {
				return errors.New("inputs already consumed")
			}
			return nil
		},
	}
	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: makeMinimalTxCbor(t, 0x01, 0),
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx2",
				Cbor: makeMinimalTxCbor(t, 0x02, 0),
				Type: dijkstra.TxTypeDijkstra,
			},
			{
				Hash: "tx3",
				Cbor: makeMinimalTxCbor(t, 0x03, 0),
				Type: dijkstra.TxTypeDijkstra,
			},
		},
	}
	builder := newDijkstraFitBuilder(t, mempool, validator, 4096, 16384)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 2)
	require.Len(
		t,
		validator.validatedHashes,
		3,
		"every candidate the block could hold is offered to re-validation",
	)
}
