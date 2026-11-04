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

package eras

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// BenchmarkEvaluateTxConwayPreprodFixtures evaluates every redeemer of the
// real preprod Plutus transactions in preprodPlutusFixtures, the work phase-2
// validation does per transaction, against their resolved inputs.
func BenchmarkEvaluateTxConwayPreprodFixtures(b *testing.B) {
	pp := preprodFixtureProtocolParams(b)
	for _, fixture := range preprodPlutusFixtures {
		blk, err := gledger.NewBlockFromCbor(
			gledger.BlockTypeConway,
			readErasFixture(b, fixture.blockFile),
		)
		require.NoError(b, err)
		var tx lcommon.Transaction
		for _, candidate := range blk.Transactions() {
			if candidate.Hash().String() == fixture.txId {
				tx = candidate
			}
		}
		require.NotNil(b, tx)
		var inputTxBytes [][]byte
		_, err = cbor.Decode(readErasFixture(b, fixture.inputsFile), &inputTxBytes)
		require.NoError(b, err)
		ls := preprodLedgerState{mockLedgerState: newMockLedgerState()}
		ls.networkId = uint(lcommon.AddressNetworkTestnet)
		// A node's LedgerState caches evaluation contexts across
		// transactions; without one every redeemer would rebuild it.
		ls.plutusEvalContextCache = NewPlutusEvalContextCache()
		for _, raw := range inputTxBytes {
			inputTx, err := conway.NewConwayTransactionFromCbor(raw)
			require.NoError(b, err)
			for idx, output := range inputTx.Outputs() {
				ls.addUtxo(
					shelley.MustNewShelleyTransactionInput(inputTx.Hash().String(), idx),
					output,
				)
			}
		}
		b.Run(fixture.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := EvaluateTxConway(tx, ls, pp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
