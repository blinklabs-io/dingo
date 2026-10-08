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
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var revalidationTestAddress = append(
	[]byte{0x60},
	bytes.Repeat([]byte{0x42}, 28)...,
)

// spendTxCbor encodes a Conway transaction spending input and paying amount
// to one output.
func spendTxCbor(
	t *testing.T,
	inputID []byte,
	inputIndex uint32,
	amount uint64,
) ([]byte, string) {
	t.Helper()
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{inputID, uint64(inputIndex)}},
		},
		1: []any{[]any{revalidationTestAddress, amount}},
		2: uint64(200_000),
	}
	txCbor, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(uint(conway.EraIdConway), txCbor)
	require.NoError(t, err)
	return txCbor, tx.Hash().String()
}

func seedLedgerUtxo(
	t *testing.T,
	db *database.Database,
	txID []byte,
	index uint32,
	amount uint64,
) {
	t.Helper()
	outputCbor, err := cbor.Encode([]any{revalidationTestAddress, amount})
	require.NoError(t, err)
	require.NoError(t, db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		if err := db.CreateUtxo(context.Background(), txn, &models.Utxo{
			TxId:      txID,
			OutputIdx: index,
			AddedSlot: 1,
		}); err != nil {
			return err
		}
		return db.Blob().SetUtxo(txn.Blob(), txID, index, outputCbor)
	}))
}

// confirmSpend records in the ledger a block whose transaction txHash spent
// the given output and created output 0 worth amount, then republishes the
// ledger snapshots as a block application does.
func confirmSpend(
	t *testing.T,
	f *pendingAccountsFixture,
	spentID []byte,
	txHash string,
	amount uint64,
) {
	t.Helper()
	require.NoError(t, f.ls.db.MarkUtxosDeletedAtSlot(
		context.Background(),
		nil,
		[]types.UtxoKey{{TxId: spentID, OutputIdx: 0}},
		2,
	))
	id, err := hex.DecodeString(txHash)
	require.NoError(t, err)
	seedLedgerUtxo(t, f.ls.db, id, 0, amount)
	f.ls.Lock()
	f.ls.publishSnapshotsLocked()
	f.ls.Unlock()
}

// TestMempoolRevalidationKeepsDescendantOfConfirmedParent drives revalidation
// through a real LedgerState. Once a block confirms a pending parent, the
// parent fails revalidation because its input is spent, while its child now
// spends an output the ledger holds and must stay in the pool. When a
// conflicting transaction is confirmed instead, the parent's output exists
// nowhere and the child must go too.
func TestMempoolRevalidationKeepsDescendantOfConfirmedParent(t *testing.T) {
	t.Parallel()
	newPool := map[string]func(dingomempool.MempoolConfig) (*dingomempool.Mempool, error){
		"fifo": func(cfg dingomempool.MempoolConfig) (*dingomempool.Mempool, error) {
			pool, err := dingomempool.NewFIFO(cfg)
			if err != nil {
				return nil, err
			}
			return pool.Mempool, nil
		},
		"dag": func(cfg dingomempool.MempoolConfig) (*dingomempool.Mempool, error) {
			pool, err := dingomempool.NewDAG(cfg)
			if err != nil {
				return nil, err
			}
			return pool.Mempool, nil
		},
	}
	for backend, construct := range newPool {
		for _, confirmParent := range []bool{true, false} {
			name := backend + "/conflicting transaction confirmed"
			if confirmParent {
				name = backend + "/parent confirmed"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				f := newPendingAccountsFixtureWithRule(
					t, 0, shelley.UtxoValidateBadInputsUtxo,
					&shelley.ShelleyProtocolParameters{},
				)
				rootID := bytes.Repeat([]byte{0x5a}, 32)
				seedLedgerUtxo(t, f.ls.db, rootID, 0, 10_000_000)
				parentCbor, parentHash := spendTxCbor(t, rootID, 0, 9_000_000)
				parentID, err := hex.DecodeString(parentHash)
				require.NoError(t, err)
				childCbor, childHash := spendTxCbor(t, parentID, 0, 8_000_000)
				_, conflictHash := spendTxCbor(t, rootID, 0, 7_000_000)

				bus := event.NewEventBus(nil, nil)
				t.Cleanup(bus.Close)
				pool, err := construct(dingomempool.MempoolConfig{
					Validator: f.ls,
					EventBus:  bus,
					Logger: slog.New(
						slog.NewTextHandler(io.Discard, nil),
					),
					PromRegistry:    prometheus.NewRegistry(),
					MempoolCapacity: 1 << 20,
				})
				require.NoError(t, err)
				require.NoError(t, pool.Start(context.Background()))
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(
						context.Background(), 5*time.Second,
					)
					defer cancel()
					require.NoError(t, pool.Stop(ctx))
				})
				txType := uint(conway.EraIdConway)
				require.NoError(t, pool.AddTransaction(txType, parentCbor))
				require.NoError(t, pool.AddTransaction(txType, childCbor))
				require.Len(t, pool.Transactions(), 2)

				if confirmParent {
					confirmSpend(t, f, rootID, parentHash, 9_000_000)
				} else {
					confirmSpend(t, f, rootID, conflictHash, 7_000_000)
				}
				bus.Publish(
					chain.ChainUpdateEventType,
					event.NewEvent(chain.ChainUpdateEventType, nil),
				)
				require.Eventually(t, func() bool {
					_, ok := pool.GetTransaction(parentHash)
					return !ok
				}, 5*time.Second, 5*time.Millisecond,
					"the parent's input is spent, so revalidation drops it")

				_, childKept := pool.GetTransaction(childHash)
				require.Equal(
					t,
					confirmParent,
					childKept,
					"the child is valid exactly when the ledger holds its input",
				)
			})
		}
	}
}
