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
	"time"

	"github.com/blinklabs-io/dingo/plugin"
)

func (n *Node) Stop() error {
	n.shutdownOnce.Do(func() {
		n.shutdownErr = n.shutdown()
	})
	return n.shutdownErr
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
	if n.config.shutdownTimeout > 0 {
		return n.config.shutdownTimeout
	}
	return 30 * time.Second
}

func (n *Node) shutdown() error {
	shutdownStart := time.Now()
	shutdownTimeout := n.configuredShutdownTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if n.cancel != nil {
		n.cancel()
	}

	var err error

	n.config.logger.Info(
		"starting graceful shutdown",
		"timeout", shutdownTimeout,
	)

	// Phase 1: Stop accepting new work
	n.config.logger.Info("shutdown phase 1: stopping new work")

	// n.cancel() above asks the stall recycler to stop; wait here so it cannot
	// race later shutdown phases that close connection, ledger, or DB state.
	n.waitChainsyncStallRecycler()

	// Stop block forger first to prevent new blocks
	if n.blockForger != nil {
		n.blockForger.Stop()
	}

	// Stop leader election to clean up resources
	if n.leaderElection != nil {
		if stopErr := n.leaderElection.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("leader election shutdown: %w", stopErr),
			)
		}
	}

	if n.chainSelector != nil {
		n.chainSelector.Stop()
	}

	if n.peerGov != nil {
		n.peerGov.Stop()
	}

	if n.snapshotMgr != nil {
		if stopErr := n.snapshotMgr.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("snapshot manager shutdown: %w", stopErr),
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

	if n.connManager != nil {
		if stopErr := n.connManager.Stop(ctx); stopErr != nil {
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

	if n.ledgerState != nil {
		n.config.logger.Info("closing ledger state")
		if closeErr := n.closeWithShutdownTimeout(
			ctx,
			"ledgerState",
			shutdownTimeout,
			n.ledgerState.Close,
		); closeErr != nil {
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
				"timeout", shutdownTimeout,
				"error", ctx.Err(),
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
	if n.pluginHost != nil {
		if stopErr := n.pluginHost.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("plugin host shutdown: %w", stopErr))
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

	if n.eventBus != nil {
		// Close (not Stop): this is a terminal shutdown, and Stop restarts the
		// async-worker pool, leaking those goroutines past node teardown.
		n.eventBus.Close()
	}

	n.config.logger.Info(
		"graceful shutdown complete",
		"total_elapsed", time.Since(shutdownStart).Round(time.Millisecond),
	)
	return err
}
