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

package leader

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func TestComputeGateBoundsElectionsSharingIt(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{}, 1)
	a := NewElection(lcommon.PoolKeyHash{1}, nil, nil, nil, nil, nil)
	b := NewElection(lcommon.PoolKeyHash{2}, nil, nil, nil, nil, nil)
	a.SetComputeGate(gate)
	b.SetComputeGate(gate)

	releaseA, _, err := a.acquireComputeGate(context.Background())
	if err != nil {
		t.Fatalf("first computation: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()
	if _, _, err := b.acquireComputeGate(ctx); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf(
			"second pool while the first computes: err = %v, want the wait to block",
			err,
		)
	}
	releaseA()
	releaseB, _, err := b.acquireComputeGate(context.Background())
	if err != nil {
		t.Fatalf("second pool after the first finished: %v", err)
	}
	releaseB()
}

func TestComputeGateUnsetDoesNotWait(t *testing.T) {
	t.Parallel()
	e := NewElection(lcommon.PoolKeyHash{1}, nil, nil, nil, nil, nil)
	for range 3 {
		release, _, err := e.acquireComputeGate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
}

func TestGatedRefreshSkipsAScheduleStoredWhileItWaited(t *testing.T) {
	t.Parallel()
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	epochProvider.activeSlotCoeff = 1.0
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	e := NewElection(
		lcommon.PoolKeyHash{},
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	e.SetComputeGate(make(chan struct{}, 1))
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Stop() }()
	waitForSchedule(t, e, testutil.AsyncWait)
	if got := stakeProvider.calls.Load(); got != 1 {
		t.Fatalf("startup computations = %d, want 1", got)
	}

	// A second caller for the epoch, past its persisted-schedule check, finds
	// the schedule once it holds the gate and does not compute it again.
	if err := e.RefreshScheduleForEpoch(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if got := stakeProvider.calls.Load(); got != 1 {
		t.Fatalf("computations after a gated refresh = %d, want 1", got)
	}
}
