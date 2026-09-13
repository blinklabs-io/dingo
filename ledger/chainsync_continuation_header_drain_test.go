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
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
)

// TestBlockfetchContinuationPublishesHeaderInvalidationOnFailure is the
// regression test for the blockfetch continuation discarding the header queue
// without draining the chain's event sequencer.
//
// The continuation runs on its own worker with its own pendingPublishes, so
// the drain registered by the handler that scheduled it covers nothing this
// worker does. When every attempt fails the worker calls clearQueuedHeaders,
// and Chain.ClearHeaders enqueues the invalidation for the announcements those
// headers carried on the chain-level sequencer rather than publishing it
// inline. With nothing draining that sequencer the invalidation was never
// published, so the vote manager kept votes armed for announcements whose
// ranking blocks had just been thrown away -- and, being keyed by announcing
// ranking block, nothing else would retract them.
func TestBlockfetchContinuationPublishesHeaderInvalidationOnFailure(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := chain.NewManager(nil, bus)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	require.NotNil(t, testChain)

	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("cont-drain")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	primary := testChainsyncConnId(6400, 3001)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return nil
			},
			// Every attempt fails, so the worker reaches the branch that
			// clears the header queue and asks for a chainsync re-sync.
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) error {
				return errors.New("request failed")
			},
		},
	}

	ls.chainsyncBlockfetchMutex.Lock()
	ls.startQueuedBlockfetchFromEventLocked(
		primary,
		primary,
		"test continuation",
	)
	ls.chainsyncBlockfetchMutex.Unlock()

	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case evt := <-headerCh:
			invalidation, ok := evt.Data.(chain.ChainHeaderInvalidationEvent)
			if !ok {
				continue
			}
			require.Equal(
				t,
				chain.HeaderInvalidationQueueCleared,
				invalidation.Reason,
			)
			require.NotEmpty(
				t,
				invalidation.RbHashes,
				"the invalidation must name the discarded headers",
			)
			return
		case <-deadline:
			t.Fatal(
				"the continuation discarded the header queue without " +
					"publishing its invalidation, leaving announcements " +
					"armed in the vote manager",
			)
		}
	}
}
