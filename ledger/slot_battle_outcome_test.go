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
	"sync"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// outcomeSlotBattleRecorder is a SlotBattleRecorder that also resolves the
// owner of a battle's block, and records what settlement reports to it.
type outcomeSlotBattleRecorder struct {
	countingSlotBattleRecorder
	// owned says whether the recorder owns the block it is asked about.
	owned bool

	mu       sync.Mutex
	asked    [][]byte
	outcomes []bool
}

func (r *outcomeSlotBattleRecorder) SlotBattleOutcome(
	_ uint64,
	hash []byte,
) func(won bool) {
	r.mu.Lock()
	r.asked = append(r.asked, append([]byte(nil), hash...))
	r.mu.Unlock()
	if !r.owned {
		return nil
	}
	return func(won bool) {
		r.mu.Lock()
		r.outcomes = append(r.outcomes, won)
		r.mu.Unlock()
	}
}

func (r *outcomeSlotBattleRecorder) snapshot() (asked [][]byte, outcomes []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.asked...), append([]bool(nil), r.outcomes...)
}

// switchToForged puts the locally forged block back on the chain in place of
// the rival, the way a later chain selection does.
func (h *slotBattleHarness) switchToForged(t *testing.T) {
	t.Helper()
	pc := h.f.ls.chain
	require.NoError(t, pc.Rollback(
		context.Background(),
		ocommon.NewPoint(h.f.parent.SlotNumber(), h.f.parent.Hash().Bytes()),
	))
	require.NoError(t, pc.AddBlock(context.Background(), h.forged, nil))
}

// The recorder that owns the forged block hears the settled outcome, once,
// and not before the battle settles.
func TestSlotBattleSettlementReportsAWonBattleToTheOwner(t *testing.T) {
	t.Parallel()

	h := newSlotBattleHarness(t)
	recorder := &outcomeSlotBattleRecorder{owned: true}
	h.f.ls.SetSlotBattleRecorder(recorder)

	h.deliver(t, h.f.rival)
	asked, outcomes := recorder.snapshot()
	require.Equal(t, [][]byte{h.forged.Hash().Bytes()}, asked,
		"the owner is resolved from the forged block when the battle is detected")
	require.Empty(t, outcomes, "nothing is reported before settlement")

	// Our block takes the chain back, as in
	// TestSlotBattleWinnerFollowsLaterChainSwitch.
	h.switchToForged(t)
	last := h.extend(t, h.forged, 1)
	h.extend(t, last, 1)
	h.requireWon(t, true)

	_, outcomes = recorder.snapshot()
	require.Equal(t, []bool{true}, outcomes)
}

func TestSlotBattleSettlementReportsALostBattleToTheOwner(t *testing.T) {
	t.Parallel()

	h := newSlotBattleHarness(t)
	recorder := &outcomeSlotBattleRecorder{owned: true}
	h.f.ls.SetSlotBattleRecorder(recorder)

	h.deliver(t, h.f.rival)
	last := h.extend(t, h.f.rival, 1)
	h.extend(t, last, 1)
	h.requireWon(t, false)

	_, outcomes := recorder.snapshot()
	require.Equal(t, []bool{false}, outcomes)
}

// A recorder that owns no block of the battle reports nothing, and settlement
// still publishes the event.
func TestSlotBattleSettlementSkipsARecorderThatOwnsNoBlock(t *testing.T) {
	t.Parallel()

	h := newSlotBattleHarness(t)
	recorder := &outcomeSlotBattleRecorder{owned: false}
	h.f.ls.SetSlotBattleRecorder(recorder)

	h.deliver(t, h.f.rival)
	last := h.extend(t, h.f.rival, 1)
	h.extend(t, last, 1)
	h.requireWon(t, false)

	asked, outcomes := recorder.snapshot()
	require.Len(t, asked, 1)
	require.Empty(t, outcomes)
}
