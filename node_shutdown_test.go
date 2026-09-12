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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShutdownPhase1ComponentStopsCoverEveryUnboundedStop pins the set of
// phase-1 components routed through stopWithDeadline. Each of these has a
// Stop that cancels its own context and then waits for a goroutine to exit
// with no deadline of its own -- a call site that went back to calling Stop
// directly would drop out of this list and escape the bound, exactly the gap
// dingo#1649 (case R9) describes.
func TestShutdownPhase1ComponentStopsCoverEveryUnboundedStop(t *testing.T) {
	t.Parallel()

	n := &Node{
		blockForger:    &forging.BlockForger{},
		leaderElection: &leader.Election{},
		snapshotMgr:    &snapshot.Manager{},
		dbLifecycleMgr: &dblifecycle.Manager{},
	}

	var names []string
	for _, cs := range n.shutdownPhase1ComponentStops() {
		names = append(names, cs.name)
	}
	assert.Equal(t, []string{
		"chainsync stall recycler",
		"chain-selected-to-none worker",
		"block forger",
		"leader election",
		"snapshot manager",
		"database lifecycle manager",
	}, names)
}

// TestShutdownPhase1ComponentStopsSkipsAbsentComponents covers a node that
// never built the optional components, the ordinary case for a
// non-block-producing node. The stall recycler and selected-to-none waits
// are unconditional: both no-op internally when their worker was never
// started (see waitChainsyncStallRecycler and waitChainSelectedNoneWorker).
func TestShutdownPhase1ComponentStopsSkipsAbsentComponents(t *testing.T) {
	t.Parallel()

	n := &Node{}
	var names []string
	for _, cs := range n.shutdownPhase1ComponentStops() {
		names = append(names, cs.name)
	}
	assert.Equal(t, []string{
		"chainsync stall recycler",
		"chain-selected-to-none worker",
	}, names)
}

// TestNodeStopEscalatesWhenPhase1ComponentNeverReturns is the point of the
// change (dingo#1649 case R9). Before this fix, node_shutdown.go called each
// phase-1 component's Stop directly; a Stop that cancelled its own context
// and then waited on a WaitGroup with no bound of its own (see
// stopWithDeadline's doc comment in node_lifecycle.go) could wedge Node.Stop
// forever, well past the configured shutdown timeout, with no error the
// caller could act on. #3558 bounded this same style of wait in
// quiesceForLiveLifecycleOp but not on this, the normal process-shutdown
// path.
//
// None of the real phase-1 components can be made to block from outside the
// package (their WaitGroups and done channels are unexported in other
// packages), so this swaps componentStopsForShutdownPhase1 -- the same seam
// componentStopsForQuiesce uses, and for the same reason -- to inject a stop
// that blocks until released, and drives the change through the real
// Node.Stop() entry point.
// Not t.Parallel: swaps the package-level componentStopsForShutdownPhase1
// seam.
func TestNodeStopEscalatesWhenPhase1ComponentNeverReturns(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	previous := componentStopsForShutdownPhase1
	t.Cleanup(func() { componentStopsForShutdownPhase1 = previous })
	componentStopsForShutdownPhase1 = func(*Node) []namedStop {
		return []namedStop{{
			name: "wedged component",
			stop: func() error {
				<-release
				return nil
			},
		}}
	}

	n := &Node{}
	n.config = NewConfig(WithShutdownTimeout(20 * time.Millisecond))
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Bounded here too: a shutdown that called the stop directly would block
	// on the wedged component forever, and a hung test reports far worse than
	// a failing one.
	done := make(chan error, 1)
	go func() { done <- n.Stop() }()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Node.Stop did not return; its phase 1 component stops " +
				"are not bounded",
		)
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, errStorageDrainUnconfirmed,
		"an unfinished phase 1 stop must be reported, not silently dropped")
	assert.ErrorContains(t, err, "wedged component")
}

// TestNodeStopSkipsDatabaseCloseWhenPhase1DrainUnconfirmed proves the second
// half of the fix: an unfinished phase-1 wait must also suppress the phase-3
// database close, matching the existing ledgerState-close-failure case
// (node_shutdown.go's ledgerStateDrainConfirmed), since a component that
// never confirmed stopping may still be using n.db.
// Not t.Parallel: swaps the package-level componentStopsForShutdownPhase1
// seam.
func TestNodeStopSkipsDatabaseCloseWhenPhase1DrainUnconfirmed(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	previous := componentStopsForShutdownPhase1
	t.Cleanup(func() { componentStopsForShutdownPhase1 = previous })
	componentStopsForShutdownPhase1 = func(*Node) []namedStop {
		return []namedStop{{
			name: "wedged component",
			stop: func() error {
				<-release
				return nil
			},
		}}
	}

	db, err := dbtest.NewDatabase(t, nil)
	require.NoError(t, err)

	n := &Node{db: db}
	n.config = NewConfig(WithShutdownTimeout(20 * time.Millisecond))
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	done := make(chan error, 1)
	go func() { done <- n.Stop() }()

	var stopErr error
	select {
	case stopErr = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Node.Stop did not return; its phase 1 component stops " +
				"are not bounded",
		)
	}
	require.Error(t, stopErr)
	// node_shutdown.go's phase 3 emits this message from a branch that is
	// mutually exclusive with calling n.db.Close: its presence is direct
	// proof the close branch did not run.
	assert.ErrorContains(t, stopErr, "database close skipped")
}
