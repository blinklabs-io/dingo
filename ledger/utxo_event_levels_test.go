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
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// TestUtxoEventCoversSubtransactionLevels applies a Dijkstra transaction
// with a subtransaction and requires its ledger.utxo event to carry the
// block's whole change to the live set, which the store reports
// independently: the outputs it created that are live and the outputs it
// spent. The store applies each level as its own transaction while the event
// is derived from the enclosing one, whose Consumed and Produced must cover
// every level.
func TestUtxoEventCoversSubtransactionLevels(t *testing.T) {
	t.Parallel()
	deposit := func(key byte, amount uint64) map[uint]any {
		return pathDeposits(map[cbor.ByteString]uint64{
			pathKeyAccount(key): amount,
		})
	}
	f := newPathFixture(
		t,
		[]pathAccount{{key: 0x42, reward: 5}},
		[]pathTx{{
			top: pathLevel{fields: deposit(0x42, 7), funds: 7},
			children: []pathLevel{{
				fields: deposit(0x42, 20), funds: 20,
			}},
		}},
	)
	require.Len(t, TransactionLevelsForApply(f.txs[0]), 2)
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.EventBus = bus
	subID, ch := bus.SubscribeWithBuffer(UtxoEventType, 16)
	t.Cleanup(func() { bus.Unsubscribe(UtxoEventType, subID) })

	require.NoError(t, f.applyBlock())
	evt := testutil.RequireReceive(t, ch, testutil.AsyncWait,
		"the transaction's UTxO change")
	got, ok := evt.Data.(UtxoEvent)
	require.True(t, ok)
	testutil.RequireNoReceive(t, ch, 100*time.Millisecond,
		"one event for the one transaction")

	created, spent, err := f.ls.ledgerDB().UtxoRollbackChanges(
		t.Context(),
		pathOriginSlot,
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, spent)
	var wantAdded, wantRemoved, gotAdded, gotRemoved []string
	ref := func(hash []byte, idx uint32) string {
		return fmt.Sprintf("%x#%d", hash, idx)
	}
	for _, id := range created {
		wantAdded = append(wantAdded, ref(id.Hash, id.Idx))
	}
	for _, u := range spent {
		wantRemoved = append(wantRemoved, ref(u.TxId, u.OutputIdx))
	}
	for _, out := range got.Added {
		gotAdded = append(gotAdded, ref(out.Ref.Hash, out.Ref.Idx))
	}
	for _, id := range got.Removed {
		gotRemoved = append(gotRemoved, ref(id.Hash, id.Idx))
	}
	require.ElementsMatch(t, wantAdded, gotAdded, "added")
	require.ElementsMatch(t, wantRemoved, gotRemoved, "removed")
}
