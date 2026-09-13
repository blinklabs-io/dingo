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
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/leios"
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

// shutdownTestResourceLogHandler counts closeWithShutdownTimeout's log records
// for one resource. closeWithShutdownTimeout logs exactly one record carrying
// the resource name on every path before it returns -- closed, failed, or
// timed out -- so a zero count once Node.Stop has returned is synchronous
// proof shutdown never began closing that resource.
type shutdownTestResourceLogHandler struct {
	resource string
	count    *atomic.Int32
}

func (h shutdownTestResourceLogHandler) Enabled(
	context.Context,
	slog.Level,
) bool {
	return true
}

func (h shutdownTestResourceLogHandler) Handle(
	_ context.Context,
	record slog.Record,
) error {
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == "resource" && a.Value.String() == h.resource {
			h.count.Add(1)
			return false
		}
		return true
	})
	return nil
}

func (h shutdownTestResourceLogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h shutdownTestResourceLogHandler) WithGroup(string) slog.Handler {
	return h
}

// TestNodeStopSkipsLedgerStateCloseWhenPhase1DrainUnconfirmed pins that an
// unfinished phase-1 wait suppresses the phase-3 LedgerState.Close, not only
// the database close after it. The block forger, leader election, and both
// Leios managers call into n.ledgerState from their own goroutines, so a
// component whose Stop outlived the shutdown deadline may still be using it.
// Restore and Truncate skip closeStorageForLiveLifecycleOp, and with it
// LedgerState.Close, on the same errStorageDrainUnconfirmed from quiesce.
//
// The stop is wedged past the deadline, so phase 3 runs with the shutdown
// context already expired and closeWithShutdownTimeout returns without
// waiting for a Close it starts. Two observations cover that: the node's
// close log for "ledgerState" (synchronous, see
// shutdownTestResourceLogHandler), and the ledger state's own Close log, which
// a Close started in the background emits shortly after Node.Stop returns.
// Not t.Parallel: swaps the package-level componentStopsForShutdownPhase1
// seam.
func TestNodeStopSkipsLedgerStateCloseWhenPhase1DrainUnconfirmed(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	chainManager, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ledgerCloseStarted := make(chan struct{}, 1)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		Logger: slog.New(nodeTestLogSignalHandler{
			message: "waiting for in-flight header replay goroutines",
			seen:    ledgerCloseStarted,
		}),
	})
	require.NoError(t, err)
	// Runs after release below and before the database cleanup dbtest
	// registered: the fixed shutdown deliberately leaves both open.
	t.Cleanup(func() { _ = ledgerState.Close() })

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

	var ledgerCloseLogs atomic.Int32
	n := &Node{db: db, ledgerState: ledgerState}
	n.config = NewConfig(WithShutdownTimeout(20 * time.Millisecond))
	n.config.logger = slog.New(shutdownTestResourceLogHandler{
		resource: "ledgerState",
		count:    &ledgerCloseLogs,
	})

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
	require.ErrorIs(t, stopErr, errStorageDrainUnconfirmed)
	assert.ErrorContains(t, stopErr, "ledger state close skipped")
	assert.Zero(t, ledgerCloseLogs.Load(),
		"shutdown began closing ledger state although a phase 1 component "+
			"never confirmed stopping")

	select {
	case <-ledgerCloseStarted:
		t.Fatal(
			"LedgerState.Close ran although a phase 1 component never " +
				"confirmed stopping",
		)
	case <-time.After(250 * time.Millisecond):
	}
}

// TestNodeStopClosesLedgerStateWhenPhase1DrainConfirmed is the control for
// TestNodeStopSkipsLedgerStateCloseWhenPhase1DrainUnconfirmed: when every
// phase-1 stop returns within the deadline, phase 3 still closes the ledger
// state. Without it, a guard that skipped the close unconditionally would pass
// the unconfirmed-path test.
//
// Deterministic without a wait: the shutdown context has not expired, so
// closeWithShutdownTimeout returns only after LedgerState.Close has, and Close
// emits its header-replay log before returning.
// Not t.Parallel: swaps the package-level componentStopsForShutdownPhase1
// seam.
func TestNodeStopClosesLedgerStateWhenPhase1DrainConfirmed(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	chainManager, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ledgerCloseStarted := make(chan struct{}, 1)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		Logger: slog.New(nodeTestLogSignalHandler{
			message: "waiting for in-flight header replay goroutines",
			seen:    ledgerCloseStarted,
		}),
	})
	require.NoError(t, err)

	previous := componentStopsForShutdownPhase1
	t.Cleanup(func() { componentStopsForShutdownPhase1 = previous })
	componentStopsForShutdownPhase1 = func(*Node) []namedStop {
		return []namedStop{{
			name: "stopped component",
			stop: func() error { return nil },
		}}
	}

	var ledgerCloseLogs atomic.Int32
	// n.db is left unset: dbtest's cleanup owns the database close, and this
	// test is about the ledger state close alone.
	n := &Node{ledgerState: ledgerState}
	n.config = NewConfig(WithShutdownTimeout(5 * time.Second))
	n.config.logger = slog.New(shutdownTestResourceLogHandler{
		resource: "ledgerState",
		count:    &ledgerCloseLogs,
	})

	assert.NoError(t, n.Stop())
	assert.Equal(t, int32(1), ledgerCloseLogs.Load(),
		"shutdown must close ledger state once phase 1 confirmed stopping")
	select {
	case <-ledgerCloseStarted:
	default:
		t.Fatal("LedgerState.Close did not run after a confirmed phase 1 drain")
	}
}

// TestShutdownPhase1ComponentStopsCoverQuiesceComponentStops pins that every
// component live restore/truncate bounds before tearing storage down is also
// stopped, bounded, before shutdown's phase 3 tears the same storage down.
// The Leios pipeline and vote managers read n.ledgerState from their own
// goroutines and were previously stopped only by quiesce and the startup
// rollback stack, never by shutdown.
func TestShutdownPhase1ComponentStopsCoverQuiesceComponentStops(t *testing.T) {
	t.Parallel()

	n := &Node{
		blockForger:          &forging.BlockForger{},
		leaderElection:       &leader.Election{},
		leiosPipelineManager: &leios.PipelineManager{},
		leiosVoteManager:     &leios.VoteManager{},
		snapshotMgr:          &snapshot.Manager{},
		dbLifecycleMgr:       &dblifecycle.Manager{},
	}

	var quiesce []string
	for _, cs := range n.quiesceComponentStops() {
		quiesce = append(quiesce, cs.name)
	}
	require.Len(t, quiesce, 6, "fixture must populate every quiesce stop")

	var shutdown []string
	for _, cs := range n.shutdownPhase1ComponentStops() {
		shutdown = append(shutdown, cs.name)
	}
	assert.Subset(t, shutdown, quiesce)
}

// TestNodeStopBoundsPhase1StopsByOneShutdownDeadline pins that phase-1 stops
// share the single shutdown deadline rather than each getting a fresh
// shutdown timeout. With a fresh budget per stop, N wedged components hold
// Node.Stop for at least N timeouts; by then the shutdown context every
// later phase uses has long expired, so the extra wait cannot buy a
// confirmed ledger-state or database close.
// Not t.Parallel: swaps the package-level componentStopsForShutdownPhase1
// seam.
func TestNodeStopBoundsPhase1StopsByOneShutdownDeadline(t *testing.T) {
	const (
		shutdownTimeout = 100 * time.Millisecond
		wedgedStops     = 20
	)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	previous := componentStopsForShutdownPhase1
	t.Cleanup(func() { componentStopsForShutdownPhase1 = previous })
	componentStopsForShutdownPhase1 = func(*Node) []namedStop {
		stops := make([]namedStop, wedgedStops)
		for i := range stops {
			stops[i] = namedStop{
				name: fmt.Sprintf("wedged component %d", i),
				stop: func() error {
					<-release
					return nil
				},
			}
		}
		return stops
	}

	n := &Node{}
	n.config = NewConfig(WithShutdownTimeout(shutdownTimeout))
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- n.Stop() }()

	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Node.Stop did not return")
	}
	elapsed := time.Since(start)

	require.ErrorIs(t, err, errStorageDrainUnconfirmed)
	// A fresh budget per stop takes at least wedgedStops*shutdownTimeout
	// (2s); one shared deadline returns shortly after shutdownTimeout.
	assert.Less(t, elapsed, wedgedStops/2*shutdownTimeout,
		"phase 1 stops must share the one shutdown deadline")
}
