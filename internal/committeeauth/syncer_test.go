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

package committeeauth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// slotSink records every SetImmutableSlot call for assertions.
type slotSink struct {
	mu    sync.Mutex
	slot  uint64
	known bool
	calls int
}

func (s *slotSink) set(slot uint64, known bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slot = slot
	s.known = known
	s.calls++
}

func (s *slotSink) snapshot() (uint64, bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.slot, s.known, s.calls
}

func TestSyncerPushesResolvedImmutableSlot(t *testing.T) {
	t.Parallel()
	sink := &slotSink{}
	syncer := NewSyncer(SyncerConfig{
		PointAtDepth: func(depth uint64) (ocommon.Point, bool, error) {
			require.Equal(t, uint64(2160), depth)
			return ocommon.Point{Slot: 150_000}, true, nil
		},
		SecurityParam:    func() int { return 2160 },
		SetImmutableSlot: sink.set,
	})
	require.NoError(t, syncer.Start(context.Background()))
	t.Cleanup(func() { _ = syncer.Stop(context.Background()) })

	slot, known, calls := sink.snapshot()
	require.True(t, known)
	require.Equal(t, uint64(150_000), slot)
	require.Equal(t, 1, calls, "Start must resolve once immediately")
}

func TestSyncerPushesUnknownWhenSecurityParamUnavailable(t *testing.T) {
	t.Parallel()
	sink := &slotSink{}
	syncer := NewSyncer(SyncerConfig{
		PointAtDepth: func(depth uint64) (ocommon.Point, bool, error) {
			t.Fatal(
				"PointAtDepth must not be called without a security parameter",
			)
			return ocommon.Point{}, false, nil
		},
		SecurityParam:    func() int { return 0 },
		SetImmutableSlot: sink.set,
	})
	require.NoError(t, syncer.Start(context.Background()))
	t.Cleanup(func() { _ = syncer.Stop(context.Background()) })

	_, known, calls := sink.snapshot()
	require.False(t, known)
	require.Equal(t, 1, calls)
}

func TestSyncerPushesUnknownOnPointAtDepthError(t *testing.T) {
	t.Parallel()
	sink := &slotSink{}
	syncer := NewSyncer(SyncerConfig{
		PointAtDepth: func(depth uint64) (ocommon.Point, bool, error) {
			return ocommon.Point{}, false, errors.New("boom")
		},
		SecurityParam:    func() int { return 2160 },
		SetImmutableSlot: sink.set,
	})
	require.NoError(t, syncer.Start(context.Background()))
	t.Cleanup(func() { _ = syncer.Stop(context.Background()) })

	_, known, _ := sink.snapshot()
	require.False(t, known)
}

func TestSyncerPushesUnknownWhenNotFound(t *testing.T) {
	t.Parallel()
	sink := &slotSink{}
	syncer := NewSyncer(SyncerConfig{
		PointAtDepth: func(depth uint64) (ocommon.Point, bool, error) {
			return ocommon.Point{}, false, nil
		},
		SecurityParam:    func() int { return 2160 },
		SetImmutableSlot: sink.set,
	})
	require.NoError(t, syncer.Start(context.Background()))
	t.Cleanup(func() { _ = syncer.Stop(context.Background()) })

	_, known, _ := sink.snapshot()
	require.False(t, known)
}

func TestSyncerRefreshesOnEachTick(t *testing.T) {
	t.Parallel()
	sink := &slotSink{}
	slot := uint64(100)
	var mu sync.Mutex
	syncer := NewSyncer(SyncerConfig{
		PointAtDepth: func(depth uint64) (ocommon.Point, bool, error) {
			mu.Lock()
			defer mu.Unlock()
			slot++
			return ocommon.Point{Slot: slot}, true, nil
		},
		SecurityParam:    func() int { return 2160 },
		SetImmutableSlot: sink.set,
		Frequency:        5 * time.Millisecond,
	})
	require.NoError(t, syncer.Start(context.Background()))
	t.Cleanup(func() { _ = syncer.Stop(context.Background()) })

	require.Eventually(t, func() bool {
		_, _, calls := sink.snapshot()
		return calls >= 3
	}, time.Second, 5*time.Millisecond)
}

func TestSyncerStartRequiresAllDependencies(t *testing.T) {
	t.Parallel()
	sink := &slotSink{}
	base := SyncerConfig{
		PointAtDepth: func(depth uint64) (ocommon.Point, bool, error) {
			return ocommon.Point{}, true, nil
		},
		SecurityParam:    func() int { return 2160 },
		SetImmutableSlot: sink.set,
	}

	missingPointAtDepth := base
	missingPointAtDepth.PointAtDepth = nil
	require.Error(t, NewSyncer(missingPointAtDepth).Start(context.Background()))

	missingSecurityParam := base
	missingSecurityParam.SecurityParam = nil
	require.Error(
		t,
		NewSyncer(missingSecurityParam).Start(context.Background()),
	)

	missingSink := base
	missingSink.SetImmutableSlot = nil
	require.Error(t, NewSyncer(missingSink).Start(context.Background()))
}

func TestSyncerStopIsIdempotentAndSafeBeforeStart(t *testing.T) {
	t.Parallel()
	sink := &slotSink{}
	syncer := NewSyncer(SyncerConfig{
		PointAtDepth: func(depth uint64) (ocommon.Point, bool, error) {
			return ocommon.Point{Slot: 1}, true, nil
		},
		SecurityParam:    func() int { return 2160 },
		SetImmutableSlot: sink.set,
	})
	// Stop before Start must not panic (cancel is nil).
	require.NoError(t, syncer.Stop(context.Background()))

	require.NoError(t, syncer.Start(context.Background()))
	require.NoError(t, syncer.Stop(context.Background()))
}
