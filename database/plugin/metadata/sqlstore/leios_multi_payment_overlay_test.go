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

package sqlstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func retainedMultiPaymentChain(
	t testing.TB,
	n int,
) (lcommon.Transaction, []lcommon.Transaction) {
	t.Helper()
	templateSeed, template := retainedStakeChain(t, 2)
	templates := []lcommon.TransactionOutput{
		templateSeed.Outputs()[0],
		template[0].Outputs()[0],
		template[1].Outputs()[0],
	}
	outputs := func(count, offset int) []lcommon.TransactionOutput {
		result := make([]lcommon.TransactionOutput, count)
		for i := range count {
			out, err := mockledger.NewTransactionOutputBuilder().
				WithAddress(templates[(i+offset)%len(templates)].Address().String()).
				WithLovelace(uint64(12_000_000 / count)).
				Build()
			require.NoError(t, err)
			result[i] = out
		}
		return result
	}
	seed := mockledger.NewTransactionBuilder()
	seed.
		WithId(overlayTxID(0x71, 0)).
		WithOutputs(outputs(2, 0)...).
		WithValid(true)
	prior, priorCount := seed.Hash().Bytes(), 2
	counts := []int{1, 3, 2, 4, 1, 2}
	txs := make([]lcommon.Transaction, n)
	for i := range n {
		inputs := make([]lcommon.TransactionInput, priorCount)
		for j := range priorCount {
			inputs[j] = overlayTxInput(t, prior, uint32(j))
		}
		count := counts[i%len(counts)]
		tx := mockledger.NewTransactionBuilder()
		tx.
			WithId(overlayTxID(0x72, i)).
			WithInputs(inputs...).
			WithOutputs(outputs(count, i+1)...).
			WithValid(true).
			WithMetadata([]byte{0xa1, 0x01, 0x02})
		txs[i] = tx
		prior, priorCount = tx.Hash().Bytes(), count
	}
	return seed, txs
}

func TestLeiosRetainedMultiPaymentMatchesPerTransactionState(t *testing.T) {
	t.Parallel()
	for _, barriers := range []bool{false, true} {
		for _, commit := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("barriers=%t/commit=%t", barriers, commit),
				func(t *testing.T) {
					seed, txs := retainedMultiPaymentChain(t, 12)
					var states [2]string
					var views [2][]string
					for i, enabled := range []bool{false, true} {
						s := retainedStakeStore(t, seed, false)
						before := retainedStakeHistoryState(
							t,
							s,
						) + retainedStakeState(
							t,
							s.writeDB,
						)
						views[i] = writeRetainedStakeChain(
							t,
							s,
							txs,
							enabled,
							barriers,
							commit,
						)
						states[i] = retainedStakeHistoryState(
							t,
							s,
						) + retainedStakeState(
							t,
							s.writeDB,
						)
						if !commit {
							require.Equal(t, before, states[i])
						}
					}
					require.Equal(t, views[0], views[1])
					require.Equal(t, states[0], states[1])
				},
			)
		}
	}
}

type multiPaymentOverlayTransaction struct {
	lcommon.Transaction
	inputs   []lcommon.TransactionInput
	produced []lcommon.Utxo
}

func (tx multiPaymentOverlayTransaction) Inputs() []lcommon.TransactionInput { return tx.inputs }

func (tx multiPaymentOverlayTransaction) Consumed() []lcommon.TransactionInput { return tx.inputs }
func (tx multiPaymentOverlayTransaction) Outputs() []lcommon.TransactionOutput {
	result := make([]lcommon.TransactionOutput, len(tx.produced))
	for i, out := range tx.produced {
		result[i] = out.Output
	}
	return result
}

func (tx multiPaymentOverlayTransaction) Produced() []lcommon.Utxo { return tx.produced }

func TestLeiosRetainedMultiPaymentRejectsMalformedReferences(t *testing.T) {
	t.Parallel()
	seed, txs := retainedMultiPaymentChain(t, 2)
	for _, name := range []string{"duplicate-input", "nil-input", "duplicate-output", "nil-output", "wrong-output-index", "excess-inputs", "excess-outputs"} {
		t.Run(name, func(t *testing.T) {
			tx := multiPaymentOverlayTransaction{
				Transaction: txs[0],
				inputs: append(
					[]lcommon.TransactionInput(nil),
					txs[0].Inputs()...),
				produced: append([]lcommon.Utxo(nil), txs[0].Produced()...),
			}
			switch name {
			case "duplicate-input":
				tx.inputs = append(tx.inputs, tx.inputs[0])
			case "nil-input":
				tx.inputs[1] = nil
			case "duplicate-output":
				tx.produced = append(tx.produced, tx.produced[0])
			case "nil-output":
				tx.produced[0].Output = nil
			case "wrong-output-index":
				tx.produced[0].Id = overlayTxInput(t, tx.Hash().Bytes(), 99)
			case "excess-inputs":
				tx.inputs = make(
					[]lcommon.TransactionInput,
					maxLeiosUtxoOverlayReferences+1,
				)
			case "excess-outputs":
				tx.produced = make(
					[]lcommon.Utxo,
					maxLeiosUtxoOverlayReferences+1,
				)
			}
			s := retainedStakeStore(t, seed, false)
			txn := s.Transaction(t.Context())
			defer func() { _ = txn.Rollback() }()
			acc := s.NewLeiosClosureBatchAccumulator()
			defer acc.Reset()
			enabled, err := s.EnableLeiosClosureUtxoOverlay(
				acc,
				txn,
				[]lcommon.Transaction{tx, txs[1]},
			)
			require.NoError(t, err)
			require.False(t, enabled)
			require.Nil(t, acc.(*transactionBatchAccumulator).overlay)
		})
	}
}

func BenchmarkLeiosRetainedMultiPayment(b *testing.B) {
	seed, txs := retainedMultiPaymentChain(b, 1000)
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprint(enabled), func(b *testing.B) {
			s := retainedStakeStore(b, seed, false)
			s.storageMode = types.StorageModeAPI
			b.ResetTimer()
			for range b.N {
				txn := s.Transaction(context.Background())
				acc := s.NewLeiosClosureBatchAccumulator()
				if enabled {
					ok, err := s.EnableLeiosClosureUtxoOverlay(acc, txn, txs)
					require.NoError(b, err)
					require.True(b, ok)
				}
				contextSlot := uint64(99)
				for index, tx := range txs {
					require.NoError(
						b,
						s.SetTransactionLeiosClosureBatched(
							tx,
							ocommon.Point{
								Slot: 100,
								Hash: overlayTxID(0x73, 0),
							},
							uint32(index),
							nil,
							true,
							&contextSlot,
							acc,
							txn,
							0,
						),
					)
				}
				require.NoError(b, s.FlushBatch(acc, txn))
				require.NoError(b, txn.Rollback())
				acc.Reset()
			}
		})
	}
}
