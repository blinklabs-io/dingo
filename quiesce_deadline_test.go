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

package dingo

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStopWithDeadlineReturnsWhenStopReturns covers the ordinary case: a
// component that stops promptly must return its own error (or nil) unchanged,
// with no drain escalation attached.
func TestStopWithDeadlineReturnsWhenStopReturns(t *testing.T) {
	t.Parallel()

	require.NoError(t, stopWithDeadline(
		time.Minute,
		"prompt component",
		func() error { return nil },
	))

	sentinel := errors.New("stop failed")
	err := stopWithDeadline(
		time.Minute,
		"failing component",
		func() error { return sentinel },
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed,
		"a component that reported a failure did stop; only an unfinished "+
			"wait leaves a goroutine possibly still using storage")
}

// TestStopWithDeadlineEscalatesAnUnfinishedStop is the point of the change.
//
// These Stop calls cancel their context and then wait on a WaitGroup with no
// bound, so a goroutine that does not observe the cancellation blocks the
// whole live restore/truncate indefinitely — past the configured shutdown
// timeout, with no error and no way for the caller to react. Once bounded, an
// unfinished stop has to escalate to errStorageDrainUnconfirmed: the goroutine
// may still be reading or writing n.db, so Restore/Truncate must abandon the
// operation and force a supervised restart rather than reopen storage
// underneath it.
func TestStopWithDeadlineEscalatesAnUnfinishedStop(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	err := stopWithDeadline(
		10*time.Millisecond,
		"wedged component",
		func() error {
			<-release
			return nil
		},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errStorageDrainUnconfirmed,
		"an unfinished stop must force a supervised restart, not a resume")
	assert.ErrorContains(t, err, "wedged component")
}

// TestStopWithDeadlineIgnoresCallerCancellation pins the deliberate choice not
// to consult the caller's context.
//
// Cancelling a restore must not escalate a component that would have stopped
// cleanly into a supervised restart, so a cancelled context neither shortens
// the wait nor changes the result. The deadline alone bounds it.
func TestStopWithDeadlineIgnoresCallerCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stopped := make(chan struct{})
	err := stopWithDeadline(
		time.Minute,
		"prompt component",
		func() error { close(stopped); return nil },
	)
	require.NoError(t, err, "a clean stop under a cancelled caller is clean")
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed)
	select {
	case <-stopped:
	default:
		t.Fatal("stop should have run to completion")
	}
	_ = ctx
}

// TestQuiesceComponentStopsCoverEveryUnboundedStop pins the set of components
// routed through the deadline.
//
// Each of these has a Stop that cancels its own context and then waits on a
// sync.WaitGroup with no deadline of its own, so a call site that went back to
// calling Stop directly would drop out of this list and escape the bound —
// which is exactly what happened to the database lifecycle manager before.
func TestQuiesceComponentStopsCoverEveryUnboundedStop(t *testing.T) {
	n := &Node{
		blockForger:          &forging.BlockForger{},
		leaderElection:       &leader.Election{},
		leiosPipelineManager: &leios.PipelineManager{},
		leiosVoteManager:     &leios.VoteManager{},
		snapshotMgr:          &snapshot.Manager{},
		dbLifecycleMgr:       &dblifecycle.Manager{},
	}

	var names []string
	for _, cs := range n.quiesceComponentStops() {
		names = append(names, cs.name)
	}
	assert.Equal(t, []string{
		"block forger",
		"leader election",
		"leios pipeline manager",
		"leios vote manager",
		"snapshot manager",
		"database lifecycle manager",
	}, names)
}

// TestQuiesceComponentStopsSkipsAbsentComponents covers a node that never
// built the optional components, which is the ordinary case for a
// non-block-producing or non-Leios node.
func TestQuiesceComponentStopsSkipsAbsentComponents(t *testing.T) {
	n := &Node{snapshotMgr: &snapshot.Manager{}}

	stops := n.quiesceComponentStops()
	require.Len(t, stops, 1)
	assert.Equal(t, "snapshot manager", stops[0].name)
}

// TestQuiesceEscalatesAStopThatNeverReturns drives the production quiesce path
// with a component whose Stop blocks until released.
//
// This is what the stopWithDeadline unit tests above cannot show: that
// quiesceForLiveLifecycleOp actually routes its component stops through the
// deadline and surfaces the escalation. Restore and Truncate branch on
// errStorageDrainUnconfirmed to force a supervised restart instead of
// reopening storage, so a quiesce that swallowed or never reached it would let
// them resume on a database a live goroutine may still be using.
func TestQuiesceEscalatesAStopThatNeverReturns(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	previous := componentStopsForQuiesce
	t.Cleanup(func() { componentStopsForQuiesce = previous })
	componentStopsForQuiesce = func(*Node) []namedStop {
		return []namedStop{{
			name: "wedged component",
			stop: func() error {
				<-release
				return nil
			},
		}}
	}

	n := &Node{}
	n.config.cfg = &internalconfig.Config{ShutdownTimeout: "20ms"}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Bounded here too: a quiesce that called the stop directly would block on
	// the wedged component forever, and a hung test reports far worse than a
	// failing one.
	done := make(chan error, 1)
	go func() { done <- n.quiesceForLiveLifecycleOp(context.Background()) }()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("quiesce did not return; its component stops are not bounded")
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, errStorageDrainUnconfirmed,
		"Restore/Truncate branch on this to force a supervised restart")
	assert.ErrorContains(t, err, "wedged component")
}

// TestQuiesceReportsAStopFailureWithoutEscalating is the negative case. A
// component that returns an error has stopped, so the caller may still resume
// on the untouched data directory; only an unfinished wait means a goroutine
// might still be using the database.
func TestQuiesceReportsAStopFailureWithoutEscalating(t *testing.T) {
	previous := componentStopsForQuiesce
	t.Cleanup(func() { componentStopsForQuiesce = previous })
	sentinel := errors.New("stop reported a failure")
	componentStopsForQuiesce = func(*Node) []namedStop {
		return []namedStop{{
			name: "failing component",
			stop: func() error { return sentinel },
		}}
	}

	n := &Node{}
	n.config.cfg = &internalconfig.Config{ShutdownTimeout: "1m"}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	err := n.quiesceForLiveLifecycleOp(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed)
}
