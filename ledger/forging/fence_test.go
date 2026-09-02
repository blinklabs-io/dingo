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

package forging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fenceTestStore is an in-memory ForgeFenceStore that survives forger
// instances, so a test can model a restart by building a second forger
// over the same store.
type fenceTestStore struct {
	slot     uint64
	present  bool
	loadErr  error
	storeErr error
	loads    int
	stored   []uint64
	onStore  func()
}

func (s *fenceTestStore) LoadLastForgedSlot() (uint64, bool, error) {
	s.loads++
	if s.loadErr != nil {
		return 0, false, s.loadErr
	}
	return s.slot, s.present, nil
}

func (s *fenceTestStore) StoreLastForgedSlot(slot uint64) error {
	if s.onStore != nil {
		s.onStore()
	}
	if s.storeErr != nil {
		return s.storeErr
	}
	s.slot = slot
	s.present = true
	s.stored = append(s.stored, slot)
	return nil
}

// fenceTestBuilder records the order of BuildBlock calls against a shared
// trace so a test can assert that the fence is written before signing.
type fenceTestBuilder struct {
	block  ledger.Block
	cbor   []byte
	calls  int
	onCall func()
}

func (b *fenceTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	if b.onCall != nil {
		b.onCall()
	}
	return b.block, b.cbor, nil
}

func (b *fenceTestBuilder) BuildBlockWithLeios(
	uint64,
	uint64,
	LeiosBlockData,
) (ledger.Block, []byte, error) {
	return b.BuildBlock(0, 0)
}

// newFenceTestForger builds a production forger wired to store, with the
// slot clock reporting currentSlot over a chain tip one slot behind. The
// tip guard therefore never fires, so a refusal to forge can only come
// from the fence.
func newFenceTestForger(
	t *testing.T,
	store ForgeFenceStore,
	currentSlot uint64,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	observed *[]string,
) (*BlockForger, error) {
	t.Helper()
	return NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       store,
		BlockForged: func(ledger.Block, []byte, time.Duration) {
			if observed != nil {
				*observed = append(*observed, "observe")
			}
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       currentSlot,
			chainTipSlot:      currentSlot - 1,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
}

// TestForgeFencePersistsBeforeSigning proves the fence is durable before
// the header is signed. A crash in the window between signing and
// adoption must not leave the slot unrecorded, or a restart could sign a
// second, different block for it.
func TestForgeFencePersistsBeforeSigning(t *testing.T) {
	var callOrder []string
	store := &fenceTestStore{
		onStore: func() { callOrder = append(callOrder, "fence") },
	}
	builder := &fenceTestBuilder{
		block:  newForgerTestBlock(10, 2),
		cbor:   []byte{0x83, 0xaa, 0xbb},
		onCall: func() { callOrder = append(callOrder, "build") },
	}
	forger, err := newFenceTestForger(
		t, store, 10, builder, &forgerTestBroadcaster{}, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, []string{"fence", "build"}, callOrder)
	assert.Equal(t, []uint64{10}, store.stored)
}

// TestForgeFenceRejectsDuplicateSlotAfterRestart covers the restart
// sequence: a fresh forger reloads the persisted fence and refuses a slot
// it has already committed to, even though the chain tip is behind that
// slot and the tip guard would allow forging.
func TestForgeFenceRejectsDuplicateSlotAfterRestart(t *testing.T) {
	store := &fenceTestStore{slot: 10, present: true}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	var observed []string

	forger, err := newFenceTestForger(
		t, store, 10, builder, broadcaster, &observed,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, store.loads)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, 0, builder.calls, "must not sign a fenced slot")
	assert.Equal(t, 0, broadcaster.calls)
	assert.Empty(t, observed)
	assert.Empty(t, store.stored)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeFenceBlocked),
	)
}

// TestForgeFenceBlocksReforgeAfterFailedAddBlock is the replay sequence
// the fence exists for: adoption fails, the node restarts, and the slot
// clock hands the same slot back. The first block may already have
// reached peers, so signing a second one for that slot would equivocate.
func TestForgeFenceBlocksReforgeAfterFailedAddBlock(t *testing.T) {
	store := &fenceTestStore{}
	firstBuilder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	first, err := newFenceTestForger(
		t,
		store,
		10,
		firstBuilder,
		&forgerTestBroadcaster{err: errors.New("not adopted")},
		nil,
	)
	require.NoError(t, err)

	err = first.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "failed to add block")
	require.Equal(t, 1, firstBuilder.calls)
	require.Equal(t, []uint64{10}, store.stored)

	// Restart: a new forger over the same durable fence.
	secondBuilder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xcc, 0xdd},
	}
	secondBroadcaster := &forgerTestBroadcaster{}
	second, err := newFenceTestForger(
		t, store, 10, secondBuilder, secondBroadcaster, nil,
	)
	require.NoError(t, err)

	require.NoError(t, second.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 0, secondBuilder.calls)
	assert.Equal(t, 0, secondBroadcaster.calls)
	assert.Equal(t, []uint64{10}, store.stored)
}

// TestForgeFenceRejectsSlotBelowFence covers a backwards slot-clock jump:
// the fence is keyed on the highest slot used, not just the last one.
func TestForgeFenceRejectsSlotBelowFence(t *testing.T) {
	store := &fenceTestStore{slot: 20, present: true}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	forger, err := newFenceTestForger(
		t, store, 10, builder, &forgerTestBroadcaster{}, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 0, builder.calls)
	assert.Empty(t, store.stored)
}

// TestForgeFenceAllowsSlotAboveFence keeps the fence from blocking normal
// forging: a slot above the recorded fence proceeds and advances it.
func TestForgeFenceAllowsSlotAboveFence(t *testing.T) {
	store := &fenceTestStore{slot: 9, present: true}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := newFenceTestForger(
		t, store, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, []uint64{10}, store.stored)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeFenceBlocked),
	)
}

// TestForgeFenceWriteFailureAbortsForge fails closed: a fence that cannot
// be persisted offers no protection, so the block must not be signed.
func TestForgeFenceWriteFailureAbortsForge(t *testing.T) {
	store := &fenceTestStore{storeErr: errors.New("disk full")}
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := newFenceTestForger(
		t, store, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "disk full")
	assert.Equal(t, 0, builder.calls, "must not sign without a fence")
	assert.Equal(t, 0, broadcaster.calls)
}

// TestNewBlockForgerFailsWhenFenceUnreadable fails closed at wiring time
// rather than starting a producer with no duplicate-slot protection.
func TestNewBlockForgerFailsWhenFenceUnreadable(t *testing.T) {
	store := &fenceTestStore{loadErr: errors.New("metadata unavailable")}
	_, err := newFenceTestForger(
		t,
		store,
		10,
		&fenceTestBuilder{block: newForgerTestBlock(10, 2)},
		&forgerTestBroadcaster{},
		nil,
	)
	require.ErrorContains(t, err, "metadata unavailable")
}

// TestForgeFenceAbsentPreservesForging keeps a nil fence store working
// for embedders and dev-mode wiring that have no metadata store.
func TestForgeFenceAbsentPreservesForging(t *testing.T) {
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := newFenceTestForger(
		t, nil, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
}

// TestForgeFenceBlocksSameSlotRetryWithoutStore covers the in-process
// half of the fence. The slot-aligned loop can re-enter the same slot
// after a failed forge (a clock that has not advanced, or a NextSlotTime
// already in the past), and a second block built from a changed mempool
// would be a different block for a slot already signed for.
func TestForgeFenceBlocksSameSlotRetryWithoutStore(t *testing.T) {
	builder := &fenceTestBuilder{
		block: newForgerTestBlock(10, 2),
		cbor:  []byte{0x83, 0xaa, 0xbb},
	}
	broadcaster := &forgerTestBroadcaster{err: errors.New("not adopted")}
	forger, err := newFenceTestForger(
		t, nil, 10, builder, broadcaster, nil,
	)
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "failed to add block")
	require.Equal(t, 1, builder.calls)

	// Same slot again: refused without building a second block.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeFenceBlocked),
	)
}
