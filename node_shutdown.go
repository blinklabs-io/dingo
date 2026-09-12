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
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/plugin"
)

var errShutdownLifecycleGate = errors.New("shutdown lifecycle gate")

func (n *Node) Stop() error {
	n.shutdownMu.Lock()
	if n.shutdownDone {
		err := n.shutdownErr
		n.shutdownMu.Unlock()
		return err
	}
	if n.shutdownRunning {
		wait := n.shutdownWait
		n.shutdownMu.Unlock()
		<-wait
		return n.Stop()
	}
	n.shutdownRunning = true
	n.shutdownWait = make(chan struct{})
	wait := n.shutdownWait
	n.shutdownMu.Unlock()

	err := n.shutdown()

	n.shutdownMu.Lock()
	n.shutdownErr = err
	n.shutdownRunning = false
	if !errors.Is(err, errShutdownLifecycleGate) {
		n.shutdownDone = true
	}
	close(wait)
	n.shutdownMu.Unlock()
	return err
}

func lockMutexContext(ctx context.Context, mutex *sync.Mutex) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if mutex.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n *Node) closeWithShutdownTimeout(
	ctx context.Context,
	resource string,
	shutdownTimeout time.Duration,
	closeFn func() error,
) error {
	closeCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()

	t := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- closeFn()
	}()

	select {
	case closeErr := <-done:
		elapsed := time.Since(t).Round(time.Millisecond)
		if closeErr != nil {
			n.config.logger.Error(
				"shutdown resource close failed",
				"resource", resource,
				"elapsed", elapsed,
				"error", closeErr,
			)
			return closeErr
		}
		n.config.logger.Info(
			"shutdown resource closed",
			"resource", resource,
			"elapsed", elapsed,
		)
		return nil
	case <-closeCtx.Done():
		n.config.logger.Warn(
			"shutdown resource close timed out",
			"resource", resource,
			"timeout", shutdownTimeout,
			"elapsed", time.Since(t).Round(time.Millisecond),
			"error", closeCtx.Err(),
		)
		return closeCtx.Err()
	}
}

func (n *Node) configuredShutdownTimeout() time.Duration {
	if d, err := n.config.ShutdownTimeoutDuration(); err == nil {
		if d > 0 {
			return d
		}
	} else {
		n.config.logger.Warn("invalid shutdown timeout, using default", "value", n.config.ShutdownTimeout(), "error", err)
	}
	return 30 * time.Second
}

// shutdownPhase1ComponentStops is every phase-1 component whose Stop cancels
// its own context and then waits for a goroutine to exit with no deadline of
// its own, mirroring quiesceComponentStops (node_lifecycle.go) for the live
// restore/truncate path. #3558 bounded this same style of wait there but not
// here, on the normal process-shutdown path (dingo#1649 case R9): a goroutine
// that never observes n.cancel() could wedge Node.Stop past shutdownTimeout
// with no error for the caller to act on.
//
// The stall recycler and selected-to-none waits are unconditional -- both
// no-op internally when their worker was never started. Ordering matches the
// direct calls this replaced: the selected-to-none worker must finish before
// n.chainSelector is stopped below, since it reads the selector's state (see
// waitChainSelectedNoneWorker's doc comment), so both context-owned waits run
// first; the block forger stops next, to stop producing new blocks before
// leader election tears down. n.chainSelector.Stop and n.peerGov.Stop(ctx)
// stay direct calls immediately after this list runs (chainSelector.Stop only
// cancels and does not wait; peerGov.Stop already takes and honors ctx) --
// only the two storage-facing managers after them share this list's
// unbounded-wait shape, and neither depends on chainSelector or peerGov
// having already stopped.
func (n *Node) shutdownPhase1ComponentStops() []namedStop {
	stops := []namedStop{
		{
			name: "chainsync stall recycler",
			stop: func() error { n.waitChainsyncStallRecycler(); return nil },
		},
		{
			name: "chain-selected-to-none worker",
			stop: func() error { n.waitChainSelectedNoneWorker(); return nil },
		},
	}
	if n.blockForger != nil {
		stops = append(stops, namedStop{
			name: "block forger",
			stop: func() error { n.blockForger.Stop(); return nil },
		})
	}
	if n.leaderElection != nil {
		stops = append(stops, namedStop{
			name: "leader election",
			stop: n.leaderElection.Stop,
		})
	}
	if n.snapshotMgr != nil {
		stops = append(stops, namedStop{
			name: "snapshot manager",
			stop: n.snapshotMgr.Stop,
		})
	}
	if n.dbLifecycleMgr != nil {
		stops = append(stops, namedStop{
			name: "database lifecycle manager",
			stop: n.dbLifecycleMgr.Stop,
		})
	}
	return stops
}

// componentStopsForShutdownPhase1 is (*Node).shutdownPhase1ComponentStops,
// indirected through a variable so a shutdown-level test can inject a stop
// that blocks until released -- the same seam componentStopsForQuiesce uses,
// and for the same reason: none of these components can be made to block
// from outside the package.
var componentStopsForShutdownPhase1 = (*Node).shutdownPhase1ComponentStops

func (n *Node) shutdown() error {
	shutdownTimeout := n.configuredShutdownTimeout()
	deadline := time.Now().Add(shutdownTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	shutdownStart := time.Now()

	// Signal the node before waiting on lifecycle gates. A live restore,
	// truncate, or snapshot may need the node context to be cancelled before
	// it can release its gate; waiting first would strand cancellation and
	// leave the node running when the shutdown deadline expires.
	if n.cancel != nil {
		n.cancel()
	}

	// Run holds this gate until startup has either completed or rolled back.
	// In particular, a signal can reach Stop while Run is still unwinding a
	// failed startup; waiting here keeps the phase-ordered shutdown from
	// concurrently closing a component the startup stack is stopping.
	if err := lockMutexContext(ctx, &n.startupLifecycleMu); err != nil {
		return fmt.Errorf("shutdown startup lifecycle lock: %w: %w", errShutdownLifecycleGate, err)
	}
	defer n.startupLifecycleMu.Unlock()
	// Restore and Truncate hold these gates while quiescing, closing, and
	// rebuilding storage-dependent components. Shutdown must take the same
	// gates, in the same order, before cancelling those components or closing
	// their storage; otherwise a concurrent live operation can use a resource
	// while shutdown tears it down.
	if err := lockMutexContext(ctx, &n.liveLifecycleMu); err != nil {
		return fmt.Errorf("shutdown live lifecycle lock: %w: %w", errShutdownLifecycleGate, err)
	}
	defer n.liveLifecycleMu.Unlock()
	if err := lockMutexContext(ctx, &n.snapshotMu); err != nil {
		return fmt.Errorf("shutdown snapshot lock: %w: %w", errShutdownLifecycleGate, err)
	}
	defer n.snapshotMu.Unlock()

	var err error

	n.config.logger.Info(
		"starting graceful shutdown",
		"timeout", shutdownTimeout,
	)

	// Phase 1: Stop accepting new work
	n.config.logger.Info("shutdown phase 1: stopping new work")

	// n.cancel() above asks the stall recycler, the selected-to-none worker,
	// the block forger, the leader election, the snapshot manager, and the
	// database lifecycle manager to stop; each cancels its own context and
	// then waits for a goroutine to exit with no deadline of its own, so
	// bound each wait here rather than calling it directly -- see
	// shutdownPhase1ComponentStops's doc comment for the ordering this
	// preserves and stopWithDeadline's (node_lifecycle.go) for why an
	// unfinished wait escalates to errStorageDrainUnconfirmed rather than
	// being reported as an ordinary stop failure.
	phase1DrainConfirmed := true
	for _, cs := range componentStopsForShutdownPhase1(n) {
		if stopErr := stopWithDeadline(
			shutdownTimeout, cs.name, cs.stop,
		); stopErr != nil {
			if errors.Is(stopErr, errStorageDrainUnconfirmed) {
				phase1DrainConfirmed = false
			}
			err = errors.Join(err, stopErr)
		}
	}

	if n.chainSelector != nil {
		n.chainSelector.Stop()
	}

	if n.peerGov != nil {
		if stopErr := n.peerGov.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("peer governor shutdown: %w", stopErr),
			)
		}
	}

	if n.bark != nil {
		if stopErr := n.bark.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("bark shutdown: %w", stopErr))
		}
	}

	if n.midnightServer != nil {
		if stopErr := n.midnightServer.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("midnight gRPC server shutdown: %w", stopErr),
			)
		}
	}

	if n.historyExpiry != nil {
		if stopErr := n.historyExpiry.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("history expiry shutdown: %w", stopErr))
		}
	}

	// API providers are stopped before consumers and stateful dependencies.
	if n.pluginHost != nil {
		for _, capability := range []plugin.Capability{
			plugin.CapabilityAPIUtxorpc,
			plugin.CapabilityAPIMesh,
			plugin.CapabilityAPIBlockfrost,
		} {
			if stopErr := n.pluginHost.StopCapability(ctx, capability); stopErr != nil {
				err = errors.Join(err, stopErr)
			}
		}
	}

	if n.offchainMetadataFetcher != nil {
		if stopErr := n.offchainMetadataFetcher.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("off-chain metadata fetcher shutdown: %w", stopErr),
			)
		}
	}

	// The token registry sync holds a background goroutine and reads n.db
	// through its store, so it has to stop here, before phase 3 tears the
	// store down. Run()'s rollback stack only covers startup failure.
	if n.tokenRegistrySync != nil {
		if stopErr := n.tokenRegistrySync.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("token registry sync shutdown: %w", stopErr),
			)
		}
	}

	// Stop the Koios parity observer before phase 3 tears down n.db/
	// n.pluginHost: the observer's background goroutine reads Dingo's
	// committed reward state through its RewardParitySource, which is backed
	// by n.db, so it must fully stop (Observer.Stop blocks until its
	// goroutine has actually exited, not just been signaled) before that
	// store is closed. This is the only place the observer is stopped on the
	// normal/signal-driven shutdown path — startKoiosParityObserver's Run()
	// registration only covers the startup-failure/panic rollback path.
	if n.koiosParityObserver != nil {
		if stopErr := n.koiosParityObserver.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("koios parity observer shutdown: %w", stopErr),
			)
		}
	}

	n.config.logger.Info(
		"shutdown phase 1 complete",
		"elapsed", time.Since(shutdownStart).Round(time.Millisecond),
	)

	// Phase 2: Drain and close connections
	n.config.logger.Info("shutdown phase 2: draining connections")
	phase2Start := time.Now()

	if n.pluginHost != nil {
		if stopErr := n.pluginHost.StopCapability(ctx, plugin.CapabilityMempool); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
	}

	// Close the EventBus while draining connections. Since v0.68, event
	// delivery applies backpressure instead of dropping events. A chainsync
	// callback can therefore be blocked waiting for a full ledger subscriber,
	// while a blockfetch continuation can wait for a BatchDone event that only
	// connection shutdown releases. Run both operations together so neither
	// side waits indefinitely for the other.
	var connManagerDone chan error
	if n.connManager != nil {
		connManagerDone = make(chan error, 1)
		go func() {
			connManagerDone <- n.connManager.Stop(ctx)
		}()
	}

	if n.eventBus != nil {
		n.config.logger.Info("closing event bus while draining connections")
		n.eventBus.Close()
	}

	if connManagerDone != nil {
		if stopErr := <-connManagerDone; stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("connection manager shutdown: %w", stopErr),
			)
		}
	}

	n.config.logger.Info(
		"shutdown phase 2 complete",
		"elapsed", time.Since(phase2Start).Round(time.Millisecond),
	)

	// Phase 3: Flush state and close database
	n.config.logger.Info("shutdown phase 3: flushing state")
	phase3Start := time.Now()
	// Starts from phase1DrainConfirmed: a phase-1 component that never
	// confirmed stopping may still be using n.db, exactly the same danger an
	// unconfirmed ledgerState close guards against below, so either failure
	// must skip the database close and plugin host shutdown that follow.
	ledgerStateDrainConfirmed := phase1DrainConfirmed

	if n.ledgerState != nil {
		n.config.logger.Info("closing ledger state")
		if closeErr := n.closeWithShutdownTimeout(
			ctx,
			"ledgerState",
			shutdownTimeout,
			n.ledgerState.Close,
		); closeErr != nil {
			ledgerStateDrainConfirmed = false
			err = errors.Join(
				err,
				fmt.Errorf("ledger state close: %w", closeErr),
			)
		}
	}

	if n.deferredIndexMaintenanceDone != nil {
		n.config.logger.Info("waiting for deferred-index maintenance")
		select {
		case <-n.deferredIndexMaintenanceDone:
			n.config.logger.Info("deferred-index maintenance stopped")
		case <-ctx.Done():
			n.config.logger.Warn(
				"timed out waiting for deferred-index maintenance; continuing shutdown",
				"timeout",
				shutdownTimeout,
				"error",
				ctx.Err(),
			)
			err = errors.Join(
				err,
				fmt.Errorf(
					"deferred-index maintenance shutdown: %w",
					ctx.Err(),
				),
			)
		}
	}

	if n.db != nil {
		if !ledgerStateDrainConfirmed {
			n.config.logger.Error(
				"skipping database close because ledger state drain was not confirmed",
			)
			err = errors.Join(
				err,
				errors.New(
					"database close skipped: ledger state drain unconfirmed",
				),
			)
		} else {
			n.config.logger.Info("closing database")
			if closeErr := n.closeWithShutdownTimeout(
				ctx,
				"database",
				shutdownTimeout,
				n.db.Close,
			); closeErr != nil {
				err = errors.Join(
					err,
					fmt.Errorf("database close: %w", closeErr),
				)
			}
		}
	}
	if n.pluginHost != nil {
		if !ledgerStateDrainConfirmed {
			n.config.logger.Error(
				"skipping plugin host shutdown because ledger state drain was not confirmed",
			)
		} else if stopErr := n.pluginHost.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("plugin host shutdown: %w", stopErr),
			)
		}
	}

	n.config.logger.Info(
		"shutdown phase 3 complete",
		"elapsed", time.Since(phase3Start).Round(time.Millisecond),
	)

	// Phase 4: Cleanup resources
	n.config.logger.Info("shutdown phase 4: cleanup resources")

	// Call registered shutdown functions
	for _, fn := range n.shutdownFuncs {
		if fnErr := fn(ctx); fnErr != nil {
			err = errors.Join(err, fmt.Errorf("shutdown function: %w", fnErr))
		}
	}
	n.shutdownFuncs = nil

	n.config.logger.Info(
		"graceful shutdown complete",
		"total_elapsed", time.Since(shutdownStart).Round(time.Millisecond),
	)
	return err
}
