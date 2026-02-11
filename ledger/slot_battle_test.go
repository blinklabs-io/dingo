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

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/forging"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockForgedBlockChecker is a test implementation of ForgedBlockChecker.
type mockForgedBlockChecker struct {
	forgedSlots map[uint64][]byte
}

func (m *mockForgedBlockChecker) WasForgedByUs(
	slot uint64,
) ([]byte, bool) {
	hash, ok := m.forgedSlots[slot]
	return hash, ok
}

func TestCheckSlotBattle_DetectsConflict(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Subscribe to slot battle events
	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Simulate an incoming block at slot 1000 with a different hash.
	// Pass a non-nil error to indicate the remote block was rejected
	// (local block stays on chain, so local won).
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}
	ls.checkSlotBattle(e, errors.New("block rejected"))

	// Verify the event was emitted
	select {
	case evt := <-evtCh:
		battle, ok := evt.Data.(forging.SlotBattleEvent)
		require.True(t, ok, "event data should be SlotBattleEvent")
		assert.Equal(t, uint64(1000), battle.Slot)
		assert.Equal(t, localHash, battle.LocalBlockHash)
		assert.Equal(t, remoteHash, battle.RemoteBlockHash)
		assert.True(t, battle.Won,
			"local should win when remote block is rejected")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SlotBattleEvent")
	}
}

func TestCheckSlotBattle_RemoteWinsWhenAccepted(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Pass nil error to indicate the remote block was accepted
	// (remote won, our block is replaced).
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}
	ls.checkSlotBattle(e, nil)

	select {
	case evt := <-evtCh:
		battle, ok := evt.Data.(forging.SlotBattleEvent)
		require.True(t, ok)
		assert.Equal(t, uint64(1000), battle.Slot)
		assert.False(t, battle.Won,
			"local should lose when remote block is accepted")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SlotBattleEvent")
	}
}

func TestCheckSlotBattle_NoConflictDifferentSlot(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: {0x01, 0x02, 0x03, 0x04},
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Incoming block is at slot 2000, not a slot we forged
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 2000,
			Hash: []byte{0x0A, 0x0B, 0x0C, 0x0D},
		},
	}
	ls.checkSlotBattle(e, nil)

	// Verify no event was emitted
	select {
	case evt := <-evtCh:
		t.Fatalf("unexpected SlotBattleEvent: %+v", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: no event emitted
	}
}

func TestCheckSlotBattle_SameHashIsNotBattle(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	blockHash := []byte{0x01, 0x02, 0x03, 0x04}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: blockHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Incoming block has the same hash (it's our own block echoed back)
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: blockHash,
		},
	}
	ls.checkSlotBattle(e, nil)

	select {
	case evt := <-evtCh:
		t.Fatalf("same-hash block should not trigger slot battle: %+v", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: no event for same-hash block
	}
}

func TestCheckSlotBattle_NilCheckerSkips(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: nil, // No checker configured
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: []byte{0x0A, 0x0B, 0x0C, 0x0D},
		},
	}
	ls.checkSlotBattle(e, nil)

	select {
	case evt := <-evtCh:
		t.Fatalf("nil checker should not emit events: %+v", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: no event when checker is nil
	}
}

func TestCheckSlotBattle_NilEventBus(t *testing.T) {
	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           nil, // No event bus
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Should not panic with nil event bus
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}
	ls.checkSlotBattle(e, errors.New("rejected"))
}

// TestCheckSlotBattle_UnderWriteLock is a regression test for the
// deadlock where checkSlotBattle was called while holding ls.Lock()
// (write lock) but internally attempted ls.RLock() on the same
// non-reentrant sync.RWMutex.
func TestCheckSlotBattle_UnderWriteLock(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}

	// Call checkSlotBattle while holding the write lock, exactly
	// as processBlockEvents does. Before the fix this deadlocked.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.Lock()
		ls.checkSlotBattle(e, errors.New("block rejected"))
		ls.Unlock()
	}()

	select {
	case <-done:
		// OK — no deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("checkSlotBattle deadlocked under write lock")
	}

	select {
	case evt := <-evtCh:
		battle, ok := evt.Data.(forging.SlotBattleEvent)
		require.True(t, ok)
		assert.Equal(t, uint64(1000), battle.Slot)
		assert.True(t, battle.Won)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SlotBattleEvent")
	}
}

func TestSetForgedBlockChecker(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	assert.Nil(t, ls.config.ForgedBlockChecker)

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: {0x01},
		},
	}
	ls.SetForgedBlockChecker(checker)

	assert.NotNil(t, ls.config.ForgedBlockChecker)

	hash, ok := ls.config.ForgedBlockChecker.WasForgedByUs(1000)
	assert.True(t, ok)
	assert.Equal(t, []byte{0x01}, hash)
}
