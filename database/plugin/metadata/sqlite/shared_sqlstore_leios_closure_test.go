// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sqlite

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// consumeTx builds a transaction with the given hash byte that consumes the one
// producer input and produces a single output, mirroring a Leios endorser-block
// transaction.
func consumeTx(hashByte byte, input mockTransactionInput) *mockTransaction {
	h := lcommon.Blake2b256{}
	h[0] = hashByte
	return &mockTransaction{
		hash:     h,
		isValid:  true,
		consumed: []lcommon.TransactionInput{input},
		produced: []lcommon.Utxo{{
			Id:     mockTransactionInput{hash: h, index: 0},
			Output: &mockTransactionOutput{amount: big.NewInt(600)},
		}},
	}
}

// TestSharedSQLStoreLeiosClosureTolerateDoubleConsume covers the cross-EB
// double-consume that wedged the Musashi ledger pipeline: two certified
// endorser-block transactions name the same input across blocks. The reference
// ledger's applyLeiosClosure (ValidateNone) folds the closure without
// re-validation, so the second consume of an already-spent input is a no-op.
// SetTransaction (ranking-block path) must still reject it as a double-spend,
// while SetTransactionLeiosClosure must tolerate it and still write the second
// transaction's produced output.
func TestSharedSQLStoreLeiosClosureTolerateDoubleConsume(t *testing.T) {
	t.Parallel()

	producerHash := lcommon.Blake2b256{}
	producerHash[0] = 0xa1
	input := mockTransactionInput{hash: producerHash, index: 0}

	// newStoreWithSpentInput returns a store whose producer input has already
	// been spent by an earlier certified transaction (txA).
	newStoreWithSpentInput := func(t *testing.T) (*sqlstore.Store, *mockTransaction) {
		t.Helper()
		store, _ := newSharedSQLStore(t)
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId: producerHash.Bytes(), OutputIdx: 0, Amount: 700, AddedSlot: 5,
		}))
		txA := consumeTx(0xa2, input)
		pointA := ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{0xc1}, 32)}
		require.NoError(t, store.SetTransaction(txA, pointA, 0, nil, true, nil))
		return store, txA
	}

	// Ranking-block path: the second consume of the already-spent input must
	// fail with ErrUtxoConflict.
	t.Run("ranking block rejects double consume", func(t *testing.T) {
		t.Parallel()
		store, _ := newStoreWithSpentInput(t)
		txB := consumeTx(0xb2, input)
		pointB := ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0xc2}, 32)}
		err := store.SetTransaction(txB, pointB, 0, nil, true, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, types.ErrUtxoConflict)
	})

	// Leios closure path: the second consume is a no-op, the call succeeds, the
	// producer input stays spent by the first transaction, and the second
	// transaction's produced output is written.
	t.Run("leios closure tolerates double consume", func(t *testing.T) {
		t.Parallel()
		store, txA := newStoreWithSpentInput(t)
		txB := consumeTx(0xb3, input)
		pointB := ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0xc3}, 32)}
		require.NoError(
			t,
			store.SetTransactionLeiosClosure(txB, pointB, 0, nil, true, nil),
		)

		// Producer input remains spent by the first (earlier certified) tx.
		spent, err := store.GetUtxoIncludingSpent(producerHash.Bytes(), 0, nil)
		require.NoError(t, err)
		require.NotNil(t, spent)
		require.True(
			t,
			bytes.Equal(txA.hash.Bytes(), spent.SpentAtTxId[:]),
			"producer input must remain spent by the first certified tx",
		)

		// The second transaction and its produced output are recorded.
		stored, err := store.GetTransactionByHash(txB.hash.Bytes(), nil)
		require.NoError(t, err)
		require.NotNil(t, stored)
		producedB, err := store.GetUtxo(txB.hash.Bytes(), 0, nil)
		require.NoError(t, err)
		require.NotNil(t, producedB)
		require.Equal(t, uint64(600), uint64(producedB.Amount))
	})
}
