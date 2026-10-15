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
	"context"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func preverifyTestTransaction(t *testing.T, fee uint64) lcommon.Transaction {
	t.Helper()
	tx := &conway.ConwayTransaction{}
	tx.Body.TxFee = fee
	bodyCbor, err := cbor.Encode(&tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	return tx
}

func TestPreverifyTransactionSignaturesBindsEveryResult(t *testing.T) {
	t.Parallel()

	transactions := make([]lcommon.Transaction, 64)
	for idx := range transactions {
		transactions[idx] = preverifyTestTransaction(t, uint64(idx+1))
	}

	results := preverifyTransactionSignatures(transactions)
	require.Len(t, results, len(transactions))
	ctx := withPreverifiedSignatures(context.Background(), results)
	for _, tx := range transactions {
		require.Same(t, results[tx.Hash()], preverifiedSignaturesFrom(ctx, tx))
	}
	require.Nil(
		t,
		preverifiedSignaturesFrom(ctx, preverifyTestTransaction(t, 1_000)),
	)
}

func TestPreverifyTransactionSignaturesEmptyInput(t *testing.T) {
	t.Parallel()

	require.Nil(t, preverifyTransactionSignatures(nil))
	ctx := context.Background()
	require.Equal(t, ctx, withPreverifiedSignatures(ctx, nil))
}
