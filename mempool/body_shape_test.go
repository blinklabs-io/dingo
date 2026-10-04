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

package mempool

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// requireTxShapeAdmission submits one transaction through AddTransaction, the
// admission path for every network and API submission. A body that violates a
// wire-level constraint must be refused with an error naming that constraint
// before the ledger validator or the pool sees it; a legal neighbour must be
// admitted.
func requireTxShapeAdmission(
	t *testing.T,
	txType uint,
	txBytes []byte,
	wantErr string,
) {
	t.Helper()
	validator := &countingValidator{}
	m := newTestMempoolWithValidator(t, validator)
	err := m.AddTransaction(txType, txBytes)
	if wantErr == "" {
		require.NoError(t, err)
		require.EqualValues(t, 1, validator.calls.Load())
		require.Len(t, m.Transactions(), 1)
		return
	}
	require.Error(t, err)
	require.ErrorContains(t, err, "decode transaction")
	require.ErrorContains(t, err, wantErr)
	require.Zero(t, validator.calls.Load(), "ledger validation must not run")
	require.Empty(t, m.Transactions())
}

func TestAddTransactionConwayBodyShapes(t *testing.T) {
	t.Parallel()
	for _, tc := range testutil.ConwayBodyShapeCases(t) {
		for _, isValid := range []bool{true, false} {
			name := tc.Name + "/isValid"
			if !isValid {
				name = tc.Name + "/isInvalid"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				requireTxShapeAdmission(
					t,
					gledger.TxTypeConway,
					testutil.ConwayShapeTxBytes(
						t, testutil.ShapeTxBody(tc.Extra), isValid,
					),
					tc.WantErr,
				)
			})
		}
	}
}

func TestAddTransactionDijkstraBodyShapes(t *testing.T) {
	t.Parallel()
	for _, tc := range testutil.DijkstraBodyShapeCases(t) {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			requireTxShapeAdmission(
				t,
				gledger.TxTypeDijkstra,
				testutil.DijkstraShapeMempoolTxBytes(
					t, testutil.ShapeTxBody(tc.Extra),
				),
				tc.WantErr,
			)
		})
	}
}
