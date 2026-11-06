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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestLeiosRetainedUtxoOverlayDirectDepositFallback(t *testing.T) {
	t.Parallel()
	seed, _ := plainOverlayChain(t, 2)
	address, err := seed.Outputs()[0].Address().Bytes()
	require.NoError(t, err)
	stakeKey := bytes.Repeat([]byte{0x42}, 28)
	rewardAddress := append([]byte{0xe0}, stakeKey...)
	makeTx := func(input []byte, amount uint64, deposits bool) lcommon.Transaction {
		body := map[uint]any{
			0: []any{[]any{input, uint64(0)}},
			1: []any{map[uint]any{0: address, 1: amount}},
			2: uint64(200_000),
		}
		if deposits {
			body[25] = map[cbor.ByteString]uint64{cbor.NewByteString(rewardAddress): 20}
		}
		raw, err := cbor.Encode([]any{body, map[uint]any{}, nil})
		require.NoError(t, err)
		tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, raw)
		require.NoError(t, err)
		return tx
	}
	first := makeTx(seed.Hash().Bytes(), 799_980, true)
	second := makeTx(first.Hash().Bytes(), 599_980, false)
	require.True(t, plainLeiosOverlayTransaction(second))
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	require.NoError(
		t,
		store.SetTransaction(
			seed,
			ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
			0,
			nil,
			true,
			nil,
			0,
		),
	)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey, CredentialTag: 0, AddedSlot: 1, CreatedSlot: 1, Active: true, Reward: 5,
	}))
	txn := store.Transaction(t.Context())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := store.EnableLeiosClosureUtxoOverlay(
		acc,
		txn,
		[]lcommon.Transaction{first, second},
	)
	require.NoError(t, err)
	t.Logf("overlay enabled for direct-deposit closure: %t", enabled)
	contextSlot := uint64(99)
	for index, tx := range []lcommon.Transaction{first, second} {
		require.NoError(
			t,
			store.SetTransactionLeiosClosureBatched(
				tx,
				ocommon.Point{Slot: 100, Hash: overlayTxID(0x53, 0)},
				uint32(index),
				nil,
				true,
				&contextSlot,
				acc,
				txn,
				0,
			),
		)
		if index == 0 {
			require.NoError(
				t,
				store.AddAccountRewardByCredential(0, stakeKey, 20, 100, first.Hash().Bytes(), txn),
			)
		}
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	account, err := store.GetAccountByCredential(0, stakeKey, false, txn)
	require.NoError(t, err)
	require.EqualValues(t, 25, account.Reward)
	require.False(t, enabled)
}

type directDepositOverlayTransaction struct {
	lcommon.Transaction
	deposits dijkstra.DijkstraDirectDeposits
}

func (tx directDepositOverlayTransaction) DijkstraDirectDeposits() dijkstra.DijkstraDirectDeposits {
	return tx.deposits
}

func TestPlainLeiosOverlayDirectDepositCapability(t *testing.T) {
	t.Parallel()
	_, txs := plainOverlayChain(t, 2)
	wrapped := directDepositOverlayTransaction{Transaction: txs[0]}
	require.True(t, plainLeiosOverlayTransaction(wrapped))
	wrapped.deposits = dijkstra.DijkstraDirectDeposits{}
	require.True(t, plainLeiosOverlayTransaction(wrapped))
	wrapped.deposits[cbor.NewByteString(append([]byte{0xe0}, bytes.Repeat([]byte{0x42}, 28)...))] = 20
	require.False(t, plainLeiosOverlayTransaction(wrapped))
}
