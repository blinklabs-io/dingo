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
)

func (n *Node) Stop() error {
	var err error
	n.shutdownOnce.Do(func() {
		err = n.shutdown()
	})
	return err
}

func (n *Node) shutdown() error {
	shutdownStart := time.Now()
	// Create shutdown context with timeout (default 30s if not configured)
	shutdownTimeout := 30 * time.Second
	if n.config.shutdownTimeout > 0 {
		shutdownTimeout = n.config.shutdownTimeout
	}
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

	if n.utxorpc != nil {
		if stopErr := n.utxorpc.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("utxorpc shutdown: %w", stopErr))
		}
	}

	if n.bark != nil {
		if stopErr := n.bark.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("bark shutdown: %w", stopErr))
		}
	}

	if n.barkPruner != nil {
		if stopErr := n.barkPruner.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("bark pruner shutdown: %w", stopErr))
		}
	}

	if n.blockfrostAPI != nil {
		if stopErr := n.blockfrostAPI.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("blockfrost API shutdown: %w", stopErr),
			)
		}
	}

	if n.meshAPI != nil {
		if stopErr := n.meshAPI.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("mesh API shutdown: %w", stopErr),
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

	if n.mempool != nil {
		if stopErr := n.mempool.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("mempool shutdown: %w", stopErr))
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
		t := time.Now()
		if closeErr := n.ledgerState.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("ledger state close: %w", closeErr),
			)
		}
		n.config.logger.Info(
			"ledger state closed",
			"elapsed", time.Since(t).Round(time.Millisecond),
		)
	}

	if n.db != nil {
		n.config.logger.Info("closing database")
		t := time.Now()
		if closeErr := n.db.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("database close: %w", closeErr),
			)
		}
		n.config.logger.Info(
			"database closed",
			"elapsed", time.Since(t).Round(time.Millisecond),
		)
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
		n.eventBus.Stop()
	}

	n.config.logger.Info(
		"graceful shutdown complete",
		"total_elapsed", time.Since(shutdownStart).Round(time.Millisecond),
	)
	return err
}
