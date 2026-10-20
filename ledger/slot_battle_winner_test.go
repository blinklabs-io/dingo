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
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/forging"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The winner of a detected slot battle is whichever block the chain holds,
// not whether the rival was refused: chain.AddBlock accepting a block says it
// was stored, not that it displaced ours.
func TestCheckSlotBattleWinnerFollowsChain(t *testing.T) {
	t.Parallel()

	rivalHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}
	for _, tc := range []struct {
		name string
		// localOnChain selects whether the block this node forged for the
		// slot is the one the chain holds.
		localOnChain bool
	}{
		{name: "forged block is on the chain", localOnChain: true},
		{name: "forged block is not on the chain", localOnChain: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newSiblingFixture(t)
			localHash := bytes.Repeat([]byte{0x01}, 32)
			if tc.localOnChain {
				localHash = f.rival.Hash().Bytes()
			}
			bus := newTestEventBus(t, nil)
			f.ls.config.EventBus = bus
			f.ls.config.ForgedBlockChecker = &mockForgedBlockChecker{
				forgedSlots: map[uint64][]byte{siblingSlot: localHash},
			}
			_, evtCh := bus.Subscribe(forging.SlotBattleEventType)

			f.ls.checkSlotBattle(BlockfetchEvent{
				Point: ocommon.Point{Slot: siblingSlot, Hash: rivalHash},
			})

			evt := testutil.RequireReceive(
				t, evtCh, testutil.AsyncWait,
				"timeout waiting for SlotBattleEvent",
			)
			battle, ok := evt.Data.(forging.SlotBattleEvent)
			require.True(t, ok)
			assert.Equal(t, tc.localOnChain, battle.Won,
				"Won must report whether the forged block is on the chain")
		})
	}
}
