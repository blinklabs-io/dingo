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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestEmitTransactionRollbackEvents_decodeFailureEmitsLedgerErrorPerBlock(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	subID, errCh := bus.Subscribe(LedgerErrorEventType)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	require.NotNil(t, errCh)
	t.Cleanup(func() {
		bus.Unsubscribe(LedgerErrorEventType, subID)
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   logger,
		},
	}
	ls.rollbackWG.Add(1)

	rollbackEvt := chain.ChainRollbackEvent{
		RolledBackBlocks: []models.Block{
			{
				Slot:   100,
				Hash:   []byte{0x01, 0x02},
				Number: 10,
				Type:   0,
				Cbor:   []byte{0xff},
			},
			{
				Slot:   200,
				Hash:   []byte{0x03, 0x04},
				Number: 20,
				Type:   0,
				Cbor:   []byte{0xfe},
			},
		},
	}

	ls.emitTransactionRollbackEvents(rollbackEvt)

	for i, wantSlot := range []uint64{100, 200} {
		evt := testutil.RequireReceive(
			t,
			errCh,
			2*time.Second,
			fmt.Sprintf("ledger error event %d (want slot %d)", i, wantSlot),
		)
		le, ok := evt.Data.(LedgerErrorEvent)
		require.True(t, ok, "event %d type", i)
		require.Equal(t, "rollback_tx_undo_decode", le.Operation)
		require.Equal(t, wantSlot, le.Point.Slot)
		require.Error(t, le.Error)
	}
}
