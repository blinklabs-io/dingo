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
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestRollbackUndoesCertifiedEndorserTransactions rolls back a ranking block
// that applied its parent's certified endorser block, and requires the undo
// for the endorser transaction even though the rollback truncates the rows
// that recorded which block applied it before the undo is published.
func TestRollbackUndoesCertifiedEndorserTransactions(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	parent := f.announce()
	certifier, offsets, envParent := f.certifyingChild(parent)
	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	require.NoError(t, f.process(certifier, offsets, envParent))
	f.requireEndorserEffects(true)
	require.Empty(t, certifier.Transactions(),
		"the only transaction to undo is the endorser block's")

	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.EventBus = bus
	subID, txCh := bus.Subscribe(TransactionEventType)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	certPoint := ocommon.NewPoint(
		certifier.SlotNumber(),
		certifier.Hash().Bytes(),
	)
	parentPoint := ocommon.NewPoint(parent.SlotNumber(), parent.Hash().Bytes())
	blocks := []models.Block{{
		Slot:     certPoint.Slot,
		Hash:     certPoint.Hash,
		PrevHash: certifier.PrevHash().Bytes(),
		Number:   certifier.BlockNumber(),
		Type:     gledger.BlockTypeDijkstra,
		Cbor:     certifier.Cbor(),
	}}
	// The rollback paths write the intent before truncating and publish the
	// undo after; between them the rows that say which block applied the
	// endorser transaction are deleted.
	f.ls.transactionEventMutex.Lock()
	require.NoError(
		t,
		f.ls.ensureRollbackIntent(t.Context(), parentPoint, blocks),
	)
	truncate := f.db.Transaction(context.Background(), true)
	require.NoError(t, truncate.Do(func(txn *database.Txn) error {
		if err := f.db.UtxosDeleteRolledback(
			context.Background(), parentPoint.Slot, txn,
		); err != nil {
			return err
		}
		return f.db.TransactionsDeleteRolledback(
			context.Background(), parentPoint.Slot, txn,
		)
	}))
	f.ls.emitRollbackTransactionEvents(t.Context(), blocks)
	f.ls.transactionEventMutex.Unlock()
	rows, err := f.db.GetTransactionsByHashes(
		context.Background(),
		[][]byte{f.ebTx.Hash().Bytes()},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, rows, "the truncation removed the endorser transaction")

	evt := testutil.RequireReceive(t, txCh, testutil.AsyncWait,
		"undo for the endorser transaction")
	got, ok := evt.Data.(TransactionEvent)
	require.True(t, ok)
	require.True(t, got.Rollback)
	require.Equal(t, f.ebTx.Hash(), got.Transaction.Hash())
	require.Equal(t, certPoint, got.Point)
	require.Equal(t, uint32(0), got.TxIndex)
	testutil.RequireNoReceive(t, txCh, 100*time.Millisecond,
		"one undo per applied transaction")
}
