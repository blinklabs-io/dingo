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
	"crypto/ed25519"
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

func preverifyTestTransactionWithWitness(
	t testing.TB,
	fee uint64,
	seedByte byte,
) *conway.ConwayTransaction {
	t.Helper()
	tx := &conway.ConwayTransaction{}
	tx.Body.TxFee = fee
	bodyCbor, err := cbor.Encode(&tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)

	seed := make([]byte, ed25519.SeedSize)
	seed[len(seed)-1] = seedByte
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	txHash := tx.Hash()
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, txHash[:]),
		}},
		false,
	)
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
		preverified := preverifiedSignaturesFrom(ctx, tx)
		require.NotNil(t, preverified)
		require.Same(t, results[tx.LeiosHash()], preverified)
	}
	require.Nil(
		t,
		preverifiedSignaturesFrom(ctx, preverifyTestTransaction(t, 1_000)),
	)
}

func TestPreverifyTransactionSignaturesSeparatesWitnessSets(t *testing.T) {
	t.Parallel()

	first := preverifyTestTransactionWithWitness(t, 42, 1)
	second := preverifyTestTransactionWithWitness(t, 42, 2)
	require.Equal(t, first.Hash(), second.Hash())
	require.NotEqual(t, first.LeiosHash(), second.LeiosHash())

	results := preverifyTransactionSignatures(
		[]lcommon.Transaction{first, second},
	)
	require.Len(
		t,
		results,
		2,
		"same-body transactions with different witnesses need separate results",
	)
	ctx := withPreverifiedSignatures(context.Background(), results)
	require.Same(
		t,
		results[first.LeiosHash()],
		preverifiedSignaturesFrom(ctx, first),
	)
	require.Same(
		t,
		results[second.LeiosHash()],
		preverifiedSignaturesFrom(ctx, second),
	)
}

func TestPreverifyTransactionSignaturesEmptyInput(t *testing.T) {
	t.Parallel()

	require.Nil(t, preverifyTransactionSignatures(nil))
	ctx := context.Background()
	require.Equal(t, ctx, withPreverifiedSignatures(ctx, nil))
}

func BenchmarkEndorserBlockSignaturePreverification(b *testing.B) {
	const txCount = 1035
	txs := make([]lcommon.Transaction, txCount)
	for i := range txs {
		txs[i] = preverifyTestTransactionWithWitness(b, uint64(i+1), byte(i))
	}
	b.Run("serial", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, tx := range txs {
				_ = lcommon.PreverifySignatures(tx)
			}
		}
	})
	b.Run("bounded_workers", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = preverifyTransactionSignatures(txs)
		}
	})
}
