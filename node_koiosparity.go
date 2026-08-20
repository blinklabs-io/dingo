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
	"fmt"
	"path/filepath"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/koiosparity"
)

// defaultKoiosParityCacheSubdir mirrors cmd/koios-parity's own
// defaultCacheSubdir default so the in-process observer and the standalone
// CLI agree on where an unconfigured cache.db lives under a data directory.
const defaultKoiosParityCacheSubdir = ".koios/cache.db"

// startKoiosParityObserver wires the optional in-process Koios reward-parity
// observer (dingo #3098) into the running node: it builds the narrow
// reward-parity source adapter directly from n.db (no export, no second
// Dingo sync, no new permanent table — see koiosparity.DatabaseSource's doc
// comment), constructs the observer, subscribes it to
// event.EpochTransitionEventType on n.eventBus, and starts its background
// processing loop.
//
// Called from Run() before n.ledgerState.Start (below in Run()) so the
// subscription exists before the ledger's slot-clock/block-processing
// goroutines — started by that call — can publish the first
// event.EpochTransitionEvent. The observer itself never touches the ledger
// lock or write transaction: HandleEpochTransitionEvent only records the
// closed epoch and wakes a background goroutine; all Koios/database I/O
// happens there, strictly after the epoch-boundary transaction that produced
// the event has already committed.
//
// A strict-mode failure (the default — see KoiosParityConfig) calls
// n.cancel via FatalFunc, matching every other FatalErrorFunc-style
// composition callback in Run() (ledger, Midnight indexer): a Koios/tool
// error or exact parity mismatch stops the node rather than being logged as
// ordinary operation.
func (n *Node) startKoiosParityObserver() error {
	cfg := n.config.koiosParity

	// Accounts defaults to true when unset (see KoiosParityConfig's doc
	// comment); the mirror populated by syncCompatFields always carries a
	// resolved, non-nil pointer, but this stays defensive against a nil
	// value regardless.
	accountsEnabled := true
	if cfg.Accounts != nil {
		accountsEnabled = *cfg.Accounts
	}

	network := cfg.Network
	if network == "" {
		network = n.config.network
	}
	if network != "preview" && network != "preprod" {
		return fmt.Errorf(
			"koios parity observer requires network preview or preprod, got %q "+
				"(set koiosParity.network explicitly if it differs from the node's own network)",
			network,
		)
	}

	cachePath := cfg.CachePath
	if cachePath == "" {
		cachePath = filepath.Join(
			n.config.dataDir,
			defaultKoiosParityCacheSubdir,
		)
	}

	source, err := koiosparity.NewDatabaseSource(n.db)
	if err != nil {
		return fmt.Errorf("create koios parity reward source: %w", err)
	}

	observer, err := koiosparity.NewObserver(koiosparity.ObserverConfig{
		Network:              network,
		CachePath:            cachePath,
		APIKey:               cfg.APIKey,
		Source:               source,
		Strict:               cfg.Strict,
		AccountsEnabled:      accountsEnabled,
		GraceHours:           cfg.GraceHours,
		AccountChunkSize:     cfg.AccountChunkSize,
		AccountChunkMaxBytes: cfg.AccountChunkMaxBytes,
		Logger:               n.config.logger,
		FatalFunc: func(err error) {
			n.config.logger.Error(
				"fatal koios parity validation failure, initiating shutdown",
				"error", err,
			)
			n.cancel()
		},
	})
	if err != nil {
		return fmt.Errorf("create koios parity observer: %w", err)
	}

	if err := observer.Start(n.ctx); err != nil { //nolint:contextcheck
		// observer has not been stored on n yet, so nothing else will ever
		// close the cache.db handle NewObserver already opened above (and
		// the Koios client it created) unless we release them here. Stop is
		// safe to call on a not-yet-(fully)-started Observer: Start only
		// ever returns an error before it sets o.cancel or launches run's
		// goroutine, so Stop's cancel/wg.Wait are no-ops and it falls
		// straight through to closing the cache.
		if stopErr := observer.Stop(n.ctx); stopErr != nil { //nolint:contextcheck
			n.config.logger.Warn(
				"koios parity observer: failed to release cache after failed start",
				"error",
				stopErr,
			)
		}
		return fmt.Errorf("start koios parity observer: %w", err)
	}
	n.koiosParityObserver = observer
	n.koiosParitySubId = n.eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		observer.HandleEpochTransitionEvent,
	)
	n.config.logger.Info(
		"koios parity observer enabled",
		"network", network,
		"strict", cfg.Strict,
		"accounts", accountsEnabled,
		"cache", cachePath,
	)
	return nil
}
