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

package blockpromote_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/blockpromote"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestCeilingFromTip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		tip, k uint64
		want   uint64
		ok     bool
	}{
		{tip: 100, k: 10, want: 90, ok: true},
		{tip: 11, k: 10, want: 1, ok: true},
		{tip: 10, k: 10},
		{tip: 5, k: 10},
		{tip: 100, k: 0},
	} {
		got, ok := blockpromote.CeilingFromTip(tc.tip, tc.k)
		require.Equal(t, tc.ok, ok, "tip %d k %d", tc.tip, tc.k)
		require.Equal(t, tc.want, got, "tip %d k %d", tc.tip, tc.k)
	}
}

func seed(t *testing.T, db *database.Database, n uint64) {
	t.Helper()
	for id := uint64(1); id <= n; id++ {
		require.NoError(t, db.BlockCreate(models.Block{
			ID: id, Slot: id * 10, Number: id, Type: 1,
			Hash: bytes.Repeat([]byte{byte(id)}, 32),
			Cbor: []byte{0x82, 0x41, byte(id), 0x41, 0x02},
		}, nil))
	}
}

func TestRunOncePromotesOnlyThroughInjectedCeiling(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 1000},
	})
	require.NoError(t, err)
	seed(t, db, 20)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	_, ch := bus.Subscribe(blockpromote.TipAdvancedEventType)

	ceiling, ok := uint64(0), false
	p := blockpromote.New(blockpromote.Config{
		DB: db, EventBus: bus,
		Ceiling: func(context.Context) (uint64, bool, error) {
			return ceiling, ok, nil
		},
	})
	// No ceiling yet: nothing moves.
	n, err := p.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, n)
	st, _ := db.ImmutableState()
	require.Zero(t, st.TipIndex)

	// The ceiling, not the chain tip (index 20), bounds promotion.
	ceiling, ok = 12, true
	n, err = p.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 12, n)
	st, _ = db.ImmutableState()
	require.Equal(t, uint64(12), st.TipIndex)

	evt := testutil.RequireReceive(t, ch, 5*time.Second, "tip advanced event")
	payload, isEvt := evt.Data.(blockpromote.TipAdvancedEvent)
	require.True(t, isEvt)
	require.Equal(t, uint64(12), payload.TipIndex)
	require.Equal(t, uint64(120), payload.TipSlot)
}

func TestRunOnceReclaimsBlobSpaceOnInterval(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 1000},
	})
	require.NoError(t, err)
	seed(t, db, 30)

	var (
		now      = time.Unix(1_000_000, 0)
		reclaims int
		failNext bool
		ceiling  uint64
	)
	p := blockpromote.New(blockpromote.Config{
		DB: db,
		Ceiling: func(context.Context) (uint64, bool, error) {
			return ceiling, ceiling > 0, nil
		},
		ReclaimInterval: time.Minute,
		Now:             func() time.Time { return now },
		Reclaim: func() error {
			reclaims++
			if failNext {
				failNext = false
				return errors.New("compaction failed")
			}
			return nil
		},
	})
	run := func() {
		t.Helper()
		_, err := p.RunOnce(t.Context())
		require.NoError(t, err)
	}

	// Nothing promoted, nothing to reclaim.
	run()
	require.Zero(t, reclaims)

	ceiling = 10
	run()
	require.Equal(t, 1, reclaims, "promotion must trigger a reclaim")

	// More promotion inside the interval waits for the interval.
	ceiling = 20
	now = now.Add(10 * time.Second)
	run()
	require.Equal(t, 1, reclaims)

	// With no further promotion the pending reclaim still runs once due.
	now = now.Add(time.Minute)
	run()
	require.Equal(t, 2, reclaims, "the last promoted blocks must be reclaimed")
	now = now.Add(time.Hour)
	run()
	require.Equal(t, 2, reclaims, "a finished reclaim must not repeat")

	// A failed reclaim is retried at the next interval, not dropped.
	ceiling = 25
	failNext = true
	run()
	require.Equal(t, 3, reclaims)
	now = now.Add(time.Minute)
	run()
	require.Equal(t, 4, reclaims, "a failed reclaim must be retried")
	now = now.Add(time.Hour)
	run()
	require.Equal(t, 4, reclaims)
}

// Stop must not return while a promotion step is writing the stores, whatever
// its context says: the caller closes the stores next, and a step abandoned
// mid-write would write to them while they close.
func TestStopWaitsForStoreWritePastContextDeadline(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 1000},
	})
	require.NoError(t, err)
	seed(t, db, 5)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseStep := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseStep()
	var enteredOnce sync.Once
	p := blockpromote.New(blockpromote.Config{
		DB:        db,
		Frequency: time.Hour,
		Ceiling: func(context.Context) (uint64, bool, error) {
			return 5, true, nil
		},
		// The reclaim runs inside the store-writing part of the step.
		Reclaim: func() error {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return nil
		},
	})
	require.NoError(t, p.Start(t.Context()))
	testutil.RequireReceive(t, entered, 10*time.Second, "store write started")

	stopCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(stopCtx) }()

	// The deadline has long passed by the time this window closes.
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned (%v) while a store write was running", err)
	case <-time.After(300 * time.Millisecond):
	}
	releaseStep()
	err = testutil.RequireReceive(t, stopped, 10*time.Second, "Stop returned")
	require.NoError(t, err)
}

// A ceiling lookup writes nothing, so Stop does not wait past its deadline for
// one that is stuck, and the step it belongs to never reaches the stores.
func TestStopDoesNotWaitPastDeadlineForCeiling(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 1000},
	})
	require.NoError(t, err)
	seed(t, db, 5)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCeiling := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCeiling()
	var enteredOnce sync.Once
	reclaimed := make(chan struct{}, 1)
	p := blockpromote.New(blockpromote.Config{
		DB:        db,
		Frequency: time.Hour,
		// Ignores its context, as a lookup blocked on a lock would.
		Ceiling: func(context.Context) (uint64, bool, error) {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return 5, true, nil
		},
		Reclaim: func() error {
			reclaimed <- struct{}{}
			return nil
		},
	})
	require.NoError(t, p.Start(t.Context()))
	testutil.RequireReceive(t, entered, 10*time.Second, "ceiling lookup started")

	stopCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(stopCtx) }()
	err = testutil.RequireReceive(t, stopped, 10*time.Second, "Stop returned")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// The lookup returning after Stop must not promote anything.
	releaseCeiling()
	require.NoError(t, p.Stop(context.Background()))
	st, _ := db.ImmutableState()
	require.Zero(t, st.TipIndex)
	select {
	case <-reclaimed:
		t.Fatal("reclaim ran after Stop")
	default:
	}
}
