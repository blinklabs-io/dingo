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
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

type commitFailingBlobStore struct {
	blob.BlobStore
	err error
}

func (s commitFailingBlobStore) NewTransaction(readWrite bool) dbtypes.Txn {
	txn := s.BlobStore.NewTransaction(readWrite)
	if !readWrite {
		return txn
	}
	return &commitFailingBlobTxn{Txn: txn, err: s.err}
}

func (s commitFailingBlobStore) SetTx(
	txn dbtypes.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	return s.BlobStore.SetTx(
		unwrapCommitFailingBlobTxn(txn),
		txHash,
		offsetData,
	)
}

func (s commitFailingBlobStore) SetCommitTimestamp(
	timestamp int64,
	txn dbtypes.Txn,
) error {
	return s.BlobStore.SetCommitTimestamp(
		timestamp,
		unwrapCommitFailingBlobTxn(txn),
	)
}

type commitFailingBlobTxn struct {
	dbtypes.Txn
	err error
}

func (t *commitFailingBlobTxn) Commit() error {
	_ = t.Txn.Rollback()
	return t.err
}

func unwrapCommitFailingBlobTxn(txn dbtypes.Txn) dbtypes.Txn {
	if wrapped, ok := txn.(*commitFailingBlobTxn); ok {
		return wrapped.Txn
	}
	return txn
}

func newTransactionEventTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, <-chan event.Event) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	subID, events := bus.SubscribeWithBuffer(TransactionEventType, 16)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	return &LedgerState{
		db: db,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}, db, events
}

func newTransactionEventTestDelta(
	t *testing.T,
	seed byte,
	index int,
) *LedgerDelta {
	t.Helper()

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{seed}, 32))
	tx.WithType(gledger.TxTypeDijkstra)
	tx.WithValid(true)

	point := ocommon.Point{
		Slot: uint64(seed),
		Hash: bytes.Repeat([]byte{seed + 1}, 32),
	}
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	var blockHash [32]byte
	copy(blockHash[:], point.Hash)

	delta := NewLedgerDelta(
		point,
		uint(dijkstra.EraIdDijkstra),
		uint64(seed),
	)
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	delta.Offsets.TxOffsets[txHash] = database.CborOffset{
		BlockSlot:  point.Slot,
		BlockHash:  blockHash,
		ByteLength: 1,
	}
	delta.addTransaction(tx, index)
	return delta
}

func addTransactionEventTestTransaction(
	t *testing.T,
	delta *LedgerDelta,
	seed byte,
	index int,
) {
	t.Helper()

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{seed}, 32))
	tx.WithType(gledger.TxTypeDijkstra)
	tx.WithValid(true)
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	var blockHash [32]byte
	copy(blockHash[:], delta.Point.Hash)
	delta.Offsets.TxOffsets[txHash] = database.CborOffset{
		BlockSlot:  delta.Point.Slot,
		BlockHash:  blockHash,
		ByteLength: 1,
	}
	delta.addTransaction(tx, index)
}

func requireTransactionEvent(
	t *testing.T,
	events <-chan event.Event,
	wantIndex uint32,
) TransactionEvent {
	t.Helper()
	evt := testutil.RequireReceive(
		t,
		events,
		2*time.Second,
		"post-commit transaction event",
	)
	txEvt, ok := evt.Data.(TransactionEvent)
	require.True(t, ok, "unexpected payload %T", evt.Data)
	require.Equal(t, wantIndex, txEvt.TxIndex)
	require.False(t, txEvt.Rollback)
	return txEvt
}

func TestLedgerDeltaPublishesApplyEventsOnlyAfterCommit(t *testing.T) {
	t.Run("commit publishes in transaction order", func(t *testing.T) {
		ls, db, events := newTransactionEventTestLedger(t)
		delta := newTransactionEventTestDelta(t, 1, 0)
		defer delta.Release()
		addTransactionEventTestTransaction(t, delta, 2, 1)

		txn := db.Transaction(true)
		require.NoError(t, delta.apply(ls, txn))
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event before commit",
		)
		require.NoError(t, txn.Commit())

		firstEvt := requireTransactionEvent(t, events, 0)
		secondEvt := requireTransactionEvent(t, events, 1)
		require.Equal(
			t,
			delta.Transactions[0].Tx.Hash(),
			firstEvt.Transaction.Hash(),
		)
		require.Equal(
			t,
			delta.Transactions[1].Tx.Hash(),
			secondEvt.Transaction.Hash(),
		)
	})

	t.Run("rollback publishes nothing", func(t *testing.T) {
		ls, db, events := newTransactionEventTestLedger(t)
		delta := newTransactionEventTestDelta(t, 3, 0)
		defer delta.Release()

		txn := db.Transaction(true)
		require.NoError(t, delta.apply(ls, txn))
		require.NoError(t, txn.Rollback())
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event after rollback",
		)
	})

	t.Run("later delta failure publishes nothing", func(t *testing.T) {
		ls, db, events := newTransactionEventTestLedger(t)
		first := newTransactionEventTestDelta(t, 4, 0)
		second := newTransactionEventTestDelta(t, 5, -1)
		batch := NewLedgerDeltaBatch()
		batch.addDelta(first)
		batch.addDelta(second)
		defer batch.Release()

		err := db.Transaction(true).Do(func(txn *database.Txn) error {
			return batch.apply(ls, txn)
		})
		require.ErrorContains(t, err, "transaction index out of range")
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event after later delta failure",
		)
	})

	t.Run("commit failure publishes nothing", func(t *testing.T) {
		ls, baseDB, events := newTransactionEventTestLedger(t)
		commitErr := errors.New("injected blob commit failure")
		failingDB, err := database.New(
			baseDB.Config(),
			database.Stores{
				Blob: commitFailingBlobStore{
					BlobStore: baseDB.Blob(),
					err:       commitErr,
				},
				Metadata: baseDB.Metadata(),
			},
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, failingDB.Close()) })
		ls.db = failingDB
		delta := newTransactionEventTestDelta(t, 6, 0)
		defer delta.Release()

		err = failingDB.Transaction(true).Do(func(txn *database.Txn) error {
			return delta.apply(ls, txn)
		})
		require.ErrorIs(t, err, commitErr)
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event after commit failure",
		)
	})
}
