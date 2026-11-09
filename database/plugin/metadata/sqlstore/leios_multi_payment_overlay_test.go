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
	"github.com/prometheus/client_golang/prometheus"
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

func TestLeiosMultiPaymentInteriorWindowMatchesImmediate(t *testing.T) {
	t.Parallel()
	for _, count := range []int{31, 32} {
		for _, commit := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("payments=%d/commit=%t", count, commit),
				func(t *testing.T) {
					seed, txs := retainedMultiPaymentChain(t, count+1)
					last, ok := txs[count].(*mockledger.MockTransaction)
					require.True(t, ok)
					last.WithReferenceInputs(
						overlayTxInput(t, seed.Hash().Bytes(), 0),
					)
					var states [2]string
					for i, staged := range []bool{false, true} {
						s := retainedStakeStore(t, seed, false)
						before := retainedStakeHistoryState(
							t,
							s,
						) + retainedStakeState(
							t,
							s.writeDB,
						)
						txn := s.Transaction(t.Context())
						defer func() { _ = txn.Rollback() }()
						acc := s.NewLeiosClosureBatchAccumulator()
						defer acc.Reset()
						for offset := 0; offset < len(txs); {
							n := len(txs) - offset
							if staged {
								var err error
								n, err = s.PrepareLeiosClosureBatch(
									acc,
									txn,
									txs[offset:],
								)
								require.NoError(t, err)
								if offset == 0 {
									require.Equal(t, count, n)
									require.Equal(
										t,
										count >= minLeiosClosureOverlayRun,
										acc.(*transactionBatchAccumulator).overlay != nil,
									)
								} else {
									require.Nil(t, acc.(*transactionBatchAccumulator).overlay)
									prior, err := s.GetUtxo(txs[offset-1].Hash().Bytes(), 0, txn)
									require.NoError(t, err)
									require.NotNil(t, prior)
								}
							}
							for index := offset; index < offset+n; index++ {
								require.NoError(
									t,
									s.SetTransactionLeiosClosureBatched(
										txs[index],
										ocommon.Point{
											Slot: 100,
											Hash: overlayTxID(0x73, 0),
										},
										uint32(index),
										nil,
										true,
										nil,
										acc,
										txn,
										0,
									),
								)
							}
							offset += n
						}
						require.NoError(t, s.FlushBatch(acc, txn))
						if commit {
							require.NoError(t, txn.Commit())
						} else {
							require.NoError(t, txn.Rollback())
						}
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
					require.Equal(t, states[0], states[1])
				},
			)
		}
	}
}

func retainedWidePaymentChain(
	t testing.TB,
	count, inputs, outputs int,
) (lcommon.Transaction, []lcommon.Transaction) {
	t.Helper()
	template, _ := retainedMultiPaymentChain(t, 2)
	makeOutputs := func(n int) []lcommon.TransactionOutput {
		result := make([]lcommon.TransactionOutput, n)
		for i := range n {
			result[i] = template.Outputs()[0]
		}
		return result
	}
	seed := mockledger.NewTransactionBuilder()
	seed.WithId(overlayTxID(0x74, 0)).
		WithOutputs(makeOutputs(inputs)...).
		WithValid(true)
	txs := make([]lcommon.Transaction, count)
	prior := seed.Hash().Bytes()
	for i := range count {
		refs := make([]lcommon.TransactionInput, inputs)
		for j := range inputs {
			refs[j] = overlayTxInput(t, prior, uint32(j))
		}
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(overlayTxID(0x75, i)).
			WithInputs(refs...).
			WithOutputs(makeOutputs(outputs)...).
			WithValid(true)
		txs[i] = tx
		prior = tx.Hash().Bytes()
	}
	return seed, txs
}

func TestLeiosMultiPaymentAggregateBoundsBeforePreflight(t *testing.T) {
	t.Parallel()
	for _, inputs := range []bool{false, true} {
		t.Run(fmt.Sprintf("inputs=%t", inputs), func(t *testing.T) {
			in, out := 1, 256
			if inputs {
				in, out = out, in
			}
			_, txs := retainedWidePaymentChain(t, 33, in, out)
			for _, tx := range txs {
				require.True(t, plainLeiosOverlayTransaction(tx))
			}
			reg := prometheus.NewRegistry()
			s := newMigratedSQLiteStoreWithRegistry(t, reg)
			s.storageMode = types.StorageModeAPI
			txn := s.Transaction(t.Context())
			defer func() { _ = txn.Rollback() }()
			acc := s.NewLeiosClosureBatchAccumulator()
			defer acc.Reset()
			before := counterValue(t, reg, "select")
			enabled, err := s.EnableLeiosClosureUtxoOverlay(acc, txn, txs)
			require.NoError(t, err)
			require.False(t, enabled)
			require.Equal(
				t,
				before,
				counterValue(t, reg, "select"),
				"aggregate bounds must reject before SQL preflight",
			)
			require.Nil(t, acc.(*transactionBatchAccumulator).overlay)
			// The unseeded store declines staging, but selection must still split
			// independently at either aggregate reference bound.
			n, err := s.PrepareLeiosClosureBatch(acc, txn, txs)
			require.NoError(t, err)
			require.Equal(t, 32, n, "selection must bound each reference total")
		})
	}
}

func TestLeiosMultiPaymentSelectsPrefixAtReferenceBound(t *testing.T) {
	t.Parallel()
	seed, txs := retainedWidePaymentChain(t, 33, 256, 256)
	s := retainedStakeStore(t, seed, false)
	txn := s.Transaction(t.Context())
	defer func() { _ = txn.Rollback() }()
	acc := s.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	n, err := s.PrepareLeiosClosureBatch(acc, txn, txs)
	require.NoError(t, err)
	require.Equal(t, 32, n, "retain the largest admissible payment prefix")
	require.NotNil(t, acc.(*transactionBatchAccumulator).overlay)
	for index, tx := range txs[:n] {
		require.NoError(t, s.SetTransactionLeiosClosureBatched(
			tx, ocommon.Point{Slot: 100, Hash: overlayTxID(0x76, 0)},
			uint32(index), nil, true, nil, acc, txn, 0,
		))
	}
	next, err := s.PrepareLeiosClosureBatch(acc, txn, txs[n:])
	require.NoError(t, err)
	require.Equal(t, 1, next)
	require.Nil(t, acc.(*transactionBatchAccumulator).overlay)
	last, err := s.GetUtxo(txs[n-1].Hash().Bytes(), 255, txn)
	require.NoError(t, err)
	require.NotNil(t, last, "the next window must see the retained prefix")
	require.NoError(t, txn.Rollback())
	missing, err := s.GetUtxo(txs[n-1].Hash().Bytes(), 255, nil)
	require.NoError(t, err)
	require.Nil(t, missing, "flushing a window must preserve outer rollback")
}
