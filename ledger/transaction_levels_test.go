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

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/stretchr/testify/require"
)

func TestValidateTransactionOutputIndexes(t *testing.T) {
	t.Parallel()
	newOutputs := func(count int) []dijkstra.DijkstraTransactionOutput {
		outputs := make([]dijkstra.DijkstraTransactionOutput, count)
		for i := range outputs {
			outputs[i].Output = &mary.MaryTransactionOutput{}
		}
		return outputs
	}

	t.Run("valid final output index", func(t *testing.T) {
		t.Parallel()
		tx := &dijkstra.DijkstraTransaction{
			Body: dijkstra.DijkstraTransactionBody{
				TxOutputs: newOutputs(1 << 16),
			},
			TxIsValid: true,
		}
		require.NoError(t, validateTransactionOutputIndexes(
			[]common.Transaction{tx},
		))
	})

	t.Run("rejects transaction output index overflow", func(t *testing.T) {
		t.Parallel()
		tx := &dijkstra.DijkstraTransaction{
			Body: dijkstra.DijkstraTransactionBody{
				TxOutputs: newOutputs((1 << 16) + 1),
			},
			TxIsValid: true,
		}
		err := validateTransactionOutputIndexes([]common.Transaction{tx})
		require.Error(t, err)
		require.Contains(t, err.Error(), "output index 65536 exceeds maximum 65535")
	})

	t.Run("rejects sub-transaction output index overflow", func(t *testing.T) {
		t.Parallel()
		tx := &dijkstra.DijkstraTransaction{
			Body: dijkstra.DijkstraTransactionBody{
				TxSubTransactions: cbor.NewSetType(
					[]dijkstra.DijkstraSubTransaction{{
						Body: dijkstra.DijkstraSubTransactionBody{
							TxOutputs: newOutputs((1 << 16) + 1),
						},
					}},
					false,
				),
			},
			TxIsValid: true,
		}
		err := validateTransactionOutputIndexes([]common.Transaction{tx})
		require.Error(t, err)
		require.Contains(t, err.Error(), "output index 65536 exceeds maximum 65535")
	})

	t.Run("collateral return output index overflow", func(t *testing.T) {
		t.Parallel()
		tx := &dijkstra.DijkstraTransaction{
			Body: dijkstra.DijkstraTransactionBody{
				TxOutputs: newOutputs(1 << 16),
				TxCollateralReturn: &dijkstra.DijkstraTransactionOutput{
					Output: &mary.MaryTransactionOutput{},
				},
			},
		}
		err := validateTransactionOutputIndexes([]common.Transaction{tx})
		require.Error(t, err)
		require.Contains(t, err.Error(), "collateral return index 65536 exceeds maximum 65535")
	})
}
