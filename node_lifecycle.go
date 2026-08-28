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

// This file implements live database restore/truncate against an
// already-running Node, in-process, without a full process restart.
//
// Unlike node_shutdown.go's shutdown() (which cancels n.ctx once and lets
// the process exit clean up whatever isn't explicitly stopped), this code
// must leave the process — and n.ctx — running afterward, so it explicitly
// stops every component that either touches storage directly or holds a
// direct (non-live, non-closure) reference to n.db/n.ledgerState, then
// reconstructs all of them, then restarts everything. n.ctx is never
// cancelled or replaced: every Start(ctx) call below reuses the same,
// still-valid n.ctx Run() originally derived from the caller's context, so
// signal-driven shutdown (SIGINT/SIGTERM) keeps working across any number
// of live restore/truncate cycles.
//
// Components intentionally left running throughout (verified to hold no
// stale reference to n.db/n.ledgerState — see the Phase 2 design notes):
// n.eventBus, n.chainSelector (holds only a one-time
// SecurityParam snapshot and closures over n). n.ouroboros has one
// deliberate, narrow exception to "left running": its background Leios
// endorser-block persistence writer is paused (not merely left alone) by
// quiesceForLiveLifecycleOp, since — unlike everything else on
// n.ouroboros — that writer directly calls Database.SetLeiosEB on
// whatever n.ouroboros's ledger state currently resolves to, on its own
// timer, entirely independent of the request/response flow every other
// part of n.ouroboros only reacts to. Left unpaused, a write already queued
// before quiesce began could still be draining when the database closes
// out from under it, or could still be sitting queued when LedgerState is
// reassigned post-reinit, silently writing pre-operation data into the
// freshly restored/truncated store.
//
// Everything else that depends on n.db or n.ledgerState — chainManager,
// ledgerState, mempool, chainsyncState, peerGov, connManager, the
// background managers, the optional API servers, and the block-producer
// path — is stopped, discarded, and rebuilt from scratch, mirroring (by
// necessity duplicating, since Run() itself is intentionally left
// unmodified) the equivalent construction in node.go's Run(). n.ouroboros
// is rebuilt along with them: it takes its dependencies at construction and
// never reassigns them, so reinitializeNetworkingCore closes the outgoing
// instance (releasing its Prometheus collectors and EventBus subscriptions,
// both of which sit on registries that outlive it) and constructs a
// replacement from the retained n.ouroborosConfig. Callbacks the node handed
// to other components resolve n.ouroboros when they fire rather than binding
// an instance, so they follow the replacement automatically.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/api/blockfrost"
	"github.com/blinklabs-io/dingo/api/mesh"
	"github.com/blinklabs-io/dingo/api/utxorpc"
	"github.com/blinklabs-io/dingo/bark"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	"github.com/blinklabs-io/dingo/internal/historyexpiry"
	"github.com/blinklabs-io/dingo/internal/node/ledgerpeers"
	"github.com/blinklabs-io/dingo/internal/offchainmetadata"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	dingoversion "github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/mempool"
	midnightindexer "github.com/blinklabs-io/dingo/midnight/indexer"
	midnightserver "github.com/blinklabs-io/dingo/midnight/server"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// quiesceForLiveLifecycleOp stops every subsystem that touches storage, or
// holds a direct (non-self-healing) reference to n.db/n.ledgerState, in
// preparation for closing the database out from under this running node.
// It does not touch n.eventBus, n.chainSelector, or n.ctx. It does touch
// n.ouroboros exactly once, to pause its Leios persistence writer — see
// this file's top doc comment for why that one piece of n.ouroboros can't
// simply be left running like everything else on it.
// namedStop pairs a component's Stop with the name quiesce reports it under.
type namedStop struct {
	name string
	stop func() error
}

// quiesceComponentStops is every component quiesceForLiveLifecycleOp stops
// whose own Stop cancels a context and then waits on a sync.WaitGroup with no
// deadline of its own. Each is therefore bounded by stopWithDeadline rather
// than called directly.
//
// Ordering is preserved from the inline calls it replaced: the Leios pipeline
// manager stops before the vote manager because it consumes that manager's
// EbQuorumEvent, matching the dependency order Run()'s startup-failure cleanup
// stack already uses (see node.go). The database lifecycle manager stops last,
// as it did inline, after the koios parity observer.
func (n *Node) quiesceComponentStops() []namedStop {
	var stops []namedStop
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
	if n.leiosPipelineManager != nil {
		stops = append(stops, namedStop{
			name: "leios pipeline manager",
			stop: n.leiosPipelineManager.Stop,
		})
	}
	if n.leiosVoteManager != nil {
		stops = append(stops, namedStop{
			name: "leios vote manager",
			stop: n.leiosVoteManager.Stop,
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

// componentStopsForQuiesce is (*Node).quiesceComponentStops, indirected
// through a variable so a quiesce-level test can inject a stop that blocks
// until released -- the same indirection syncDataDirParent uses for fsync
// failures. None of these components can be made to block from outside, so
// without it the escalation path could only be exercised below quiesce.
var componentStopsForQuiesce = (*Node).quiesceComponentStops

// stopWithDeadline runs a component's Stop and waits at most d for it to
// return.
//
// The component Stop calls in quiesceForLiveLifecycleOp cancel their own
// context and then wait on a WaitGroup with no bound of their own, so a
// goroutine that does not observe the cancellation wedges the entire live
// restore or truncate — well past the configured shutdown timeout, with no
// error for the caller to act on. Bounding the wait turns that hang into a
// decision.
//
// An unfinished wait escalates to errStorageDrainUnconfirmed rather than being
// reported as an ordinary failure, matching connManager.Stop, the leios
// persist writer's drain, and the storage providers below. The distinction is
// what the caller does next: a component that reports a failure has stopped,
// while one that never returned may still be reading or writing n.db, so
// Restore/Truncate must abandon the operation and force a supervised restart
// instead of reopening storage underneath it.
//
// The abandoned goroutine is deliberately left running. It cannot be
// interrupted from here, and the escalation brings the process down anyway, so
// leaking it for that window is strictly safer than proceeding to close the
// database it may still be using.
//
// The caller's context deliberately does not shorten this wait. Cancelling a
// restore should not escalate a component that would have stopped cleanly into
// a supervised restart, and a cancelled context is no reason to abandon a stop
// mid-flight: the deadline alone decides, so the wait is bounded either way.
func stopWithDeadline(
	d time.Duration,
	name string,
	stop func() error,
) error {
	done := make(chan error, 1)
	go func() { done <- stop() }()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case stopErr := <-done:
		if stopErr != nil {
			return fmt.Errorf("%s shutdown: %w", name, stopErr)
		}
		return nil
	case <-timer.C:
		return errors.Join(
			errStorageDrainUnconfirmed,
			fmt.Errorf("%s did not stop within %s", name, d),
		)
	}
}

func (n *Node) quiesceForLiveLifecycleOp(ctx context.Context) error {
	var err error

	// Every component whose Stop cancels its own context and then waits on a
	// WaitGroup with no deadline of its own is bounded here -- see
	// stopWithDeadline for why an unfinished wait escalates to
	// errStorageDrainUnconfirmed rather than being reported as an ordinary
	// stop failure. The budget is the configured shutdown timeout, the same
	// one the koios parity observer below uses.
	//
	// They are stopped as one ordered list rather than inline so the set is
	// visible in one place and testable as a set: quiesceComponentStops is
	// what a quiesce-level test asserts against, and a call site that went
	// back to a direct Stop would drop out of it.
	stopTimeout := n.configuredShutdownTimeout()
	for _, cs := range componentStopsForQuiesce(n) {
		if stopErr := stopWithDeadline(
			stopTimeout, cs.name, cs.stop,
		); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
	}
	if n.peerGov != nil {
		if stopErr := n.peerGov.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("peer governor shutdown: %w", stopErr),
			)
		}
	}
	// reinitializeNetworkingCore constructs a fresh PoolRelayProvider on
	// every cycle (it has no long-lived identity of its own, unlike
	// peerGov above) -- without unsubscribing the stale one here first, the
	// EventBus (never recreated across this cycle) accumulates one more
	// permanently-active subscription per live restore/truncate cycle,
	// each pointing at an otherwise-unreachable, abandoned provider.
	if n.poolRelayProvider != nil {
		n.poolRelayProvider.Close()
		n.poolRelayProvider = nil
	}
	// The Koios parity observer (dingo #3098) reads Dingo's committed reward
	// state through a RewardParitySource backed directly by n.db, the same
	// way snapshotMgr above does -- it must be fully stopped (Observer.Stop
	// blocks until its background goroutine has actually exited) and its
	// event.EpochTransitionEventType subscription torn down before n.db is
	// closed below, or it would keep running against a stale, soon-to-be-
	// closed database and its subscription would leak (the EventBus itself
	// is never recreated across a live restore/truncate cycle). It is
	// rebuilt against the new n.db and resubscribed by
	// reinitializeBackgroundManagers, mirroring reinitializeMidnightIndexer's
	// stop-here/rebuild-there split for the same reason.
	if n.koiosParityObserver != nil {
		stopCtx, cancel := context.WithTimeout(
			ctx,
			n.configuredShutdownTimeout(),
		)
		stopErr := n.koiosParityObserver.Stop(stopCtx)
		cancel()
		if stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("koios parity observer shutdown: %w", stopErr),
			)
		}
		n.koiosParityObserver = nil
	}
	if n.koiosParitySubId != 0 {
		n.eventBus.UnsubscribeAndWait(
			event.EpochTransitionEventType,
			n.koiosParitySubId,
		)
		n.koiosParitySubId = 0
	}
	// utxorpc/blockfrost/mesh are API-capability plugin providers with no
	// service kept on Node (see node.go's Run()) -- StopCapability is a
	// no-op if the capability was never resolved (e.g. non-API storage
	// mode or a zero configured port).
	if n.pluginHost != nil {
		if stopErr := n.pluginHost.StopCapability(
			ctx, plugin.CapabilityAPIUtxorpc,
		); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("utxorpc shutdown: %w", stopErr))
		}
	}
	// n.bark is deliberately NOT stopped here: its DatabaseService handler
	// (bark/database.go) is exactly what a remote caller uses to poll a
	// live Restore/Truncate's progress, so the server must stay reachable
	// for the whole operation. Its Archive service's DB reference is
	// updated in place afterward (see reinitializeAPIServers) instead of
	// the server restarting.
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
				fmt.Errorf("history expiry shutdown: %w", stopErr),
			)
		}
	}
	if n.pluginHost != nil {
		if stopErr := n.pluginHost.StopCapability(
			ctx, plugin.CapabilityAPIBlockfrost,
		); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("blockfrost API shutdown: %w", stopErr),
			)
		}
		if stopErr := n.pluginHost.StopCapability(
			ctx, plugin.CapabilityAPIMesh,
		); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("mesh API shutdown: %w", stopErr),
			)
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
	if n.tokenRegistrySync != nil {
		if stopErr := n.tokenRegistrySync.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("token registry sync shutdown: %w", stopErr),
			)
		}
		// Stop does not return until the worker has exited (see its doc
		// comment), so by here nothing can still reach the store being
		// closed. Cleared so reinitializeStorage's gating rebuilds it
		// rather than restarting an instance bound to the closed store.
		n.tokenRegistrySync = nil
	}
	// midnightIndexer and chainManager/chainsyncState have no corresponding
	// stop call in node_shutdown.go's shutdown() — production shutdown
	// relies on process exit to clean them up. That's not available here
	// since the process keeps running, so midnightIndexer is stopped
	// explicitly; chainManager/chainsyncState have no Stop method at all
	// and are simply discarded/replaced in reinitializeStorage.
	if n.midnightIndexer != nil {
		n.midnightIndexer.Stop()
	}
	// mempool is a plugin-capability provider with no Stop method of its
	// own (see node.go's Run()) -- it is stopped/discarded via the host.
	if n.pluginHost != nil {
		if stopErr := n.pluginHost.StopCapability(
			ctx, plugin.CapabilityMempool,
		); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("mempool shutdown: %w", stopErr))
		}
	}
	n.mempool = nil
	if n.connManager != nil {
		if stopErr := n.connManager.Stop(ctx); stopErr != nil {
			// errStorageDrainUnconfirmed, not a bare join: connManager.Stop
			// returning an error means its own bounded wait (connection
			// close, then goroutineWg) gave up before confirming every
			// connection/listener goroutine actually exited -- exactly the
			// precondition PauseLeiosPersistWriterForLiveLifecycleOp below
			// depends on ("no more inbound Leios fetch traffic") to safely
			// reset the persist writer's start-once guard. Escalating here,
			// the same way an unconfirmed leios persist drain itself does,
			// means Restore/Truncate call n.cancel() for a full supervised
			// restart instead of reinitializeAndResume -- so a straggling
			// connection's Leios fetch racing that reset (see
			// PauseLeiosPersistWriterForLiveLifecycleOp's doc comment) can
			// no longer happen: the node never reaches reinitializeAndResume
			// in that case at all.
			err = errors.Join(
				err,
				errStorageDrainUnconfirmed,
				fmt.Errorf("connection manager shutdown: %w", stopErr),
			)
		}
	}

	// Unsubscribe the handlers bound to components being discarded, so the
	// EventBus (which is never stopped/restarted here) doesn't accumulate a
	// stale subscriber pointing at an object this function just tore down.
	// See the Node struct field comments (node.go) for why only these two
	// of Run()'s ~19 direct subscriptions need this.
	//
	// UnsubscribeAndWait, not Unsubscribe: closeStorageForLiveLifecycleOp
	// (called right after this returns) nils out n.chainManager and
	// n.chainsyncState with no synchronization of its own. Plain
	// Unsubscribe only stops future deliveries, so a handler goroutine
	// already dispatched before this loop runs could still be reading
	// those fields concurrently with that teardown.
	if n.chainsyncClientRemoveSubId != 0 {
		n.eventBus.UnsubscribeAndWait(
			chainsync.ClientRemoveRequestedEventType,
			n.chainsyncClientRemoveSubId,
		)
		n.chainsyncClientRemoveSubId = 0
	}
	if n.connManagerRecycleSubId != 0 {
		n.eventBus.UnsubscribeAndWait(
			connmanager.ConnectionRecycleRequestedEventType,
			n.connManagerRecycleSubId,
		)
		n.connManagerRecycleSubId = 0
	}
	// leiosVoteManager is stopped above and rebuilt by initLeiosVoteManager
	// (called again for a Dijkstra/Leios-enabled node during reinit), which
	// re-subscribes a fresh handler -- without unsubscribing the old one
	// here first, the EventBus (never recreated across this cycle) would
	// accumulate one extra permanently-active subscription per live
	// restore/truncate cycle, and a single emitted vote would be enqueued
	// (and diffused to peers) once per accumulated subscription.
	if n.leiosVoteEmittedSubId != 0 {
		n.eventBus.UnsubscribeAndWait(
			leios.VoteEmittedEventType,
			n.leiosVoteEmittedSubId,
		)
		n.leiosVoteEmittedSubId = 0
	}
	if n.leiosVoteReceivedSubId != 0 {
		n.eventBus.UnsubscribeAndWait(
			leios.VoteReceivedEventType,
			n.leiosVoteReceivedSubId,
		)
		n.leiosVoteReceivedSubId = 0
	}

	// Last, now that connManager.Stop above has closed every connection —
	// so no more inbound Leios fetch traffic can call enqueueLeiosPersist
	// concurrently with the reset this performs (see
	// PauseLeiosPersistWriterForLiveLifecycleOp's own doc comment for why
	// that ordering matters, and this file's top doc comment for why
	// n.ouroboros needs this one exception at all).
	//
	// errStorageDrainUnconfirmed, not a bare join: an unconfirmed leios
	// persist drain means that writer goroutine may still be running
	// against the about-to-close database, exactly the same danger
	// errStorageDrainUnconfirmed already makes Restore/Truncate fail
	// closed on rather than attempt reinitializeAndResume.
	if n.ouroboros() != nil {
		if pauseErr := n.ouroboros().PauseLeiosPersistWriterForLiveLifecycleOp(); pauseErr != nil {
			err = errors.Join(
				err,
				errStorageDrainUnconfirmed,
				fmt.Errorf("leios persist writer pause: %w", pauseErr),
			)
		}
	}

	return err
}

// closeStorageForLiveLifecycleOp closes n.ledgerState and n.db, mirroring
// shutdown()'s Phase 3. Must only be called after
// quiesceForLiveLifecycleOp has returned, since every component closed
// here must already have no active caller.
func (n *Node) closeStorageForLiveLifecycleOp(ctx context.Context) error {
	var err error

	if n.ledgerState != nil {
		if closeErr := n.ledgerState.Close(); closeErr != nil {
			// Fail closed: do not nil n.ledgerState, close n.db, or stop
			// the storage plugins below -- a goroutine this Close call
			// could not confirm had exited may still be using them. Return
			// immediately with errStorageDrainUnconfirmed so the caller
			// (Restore/Truncate) skips reinitializeAndResume instead of
			// reopening storage out from under it, and escalates to a
			// supervised restart instead.
			return errors.Join(
				errStorageDrainUnconfirmed,
				fmt.Errorf("ledger state close: %w", closeErr),
			)
		}
		n.ledgerState = nil
	}

	if n.deferredIndexMaintenanceDone != nil {
		select {
		case <-n.deferredIndexMaintenanceDone:
		case <-ctx.Done():
			err = errors.Join(
				err,
				fmt.Errorf(
					"deferred-index maintenance shutdown: %w",
					ctx.Err(),
				),
			)
		}
		n.deferredIndexMaintenanceDone = nil
	}

	if n.db != nil {
		if closeErr := n.db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("database close: %w", closeErr))
		}
		n.db = nil
	}

	// database.Close no longer touches the provider-owned stores it was
	// given (see database.Stores's doc comment) -- they must be stopped
	// separately here, or reinitializeCoreStorage's re-resolve of the same
	// capabilities below would collide with these still-open instances
	// (e.g. badger's exclusive file lock on the same on-disk directory).
	// Stopped in reverse of Run()'s resolve order (blob, then metadata).
	if n.pluginHost != nil {
		if stopErr := n.pluginHost.StopCapability(
			ctx, plugin.CapabilityStorageMetadata,
		); stopErr != nil {
			if errors.Is(stopErr, context.Canceled) ||
				errors.Is(stopErr, context.DeadlineExceeded) {
				stopErr = errors.Join(errStorageDrainUnconfirmed, stopErr)
			}
			err = errors.Join(
				err,
				fmt.Errorf("metadata storage shutdown: %w", stopErr),
			)
		}
		if stopErr := n.pluginHost.StopCapability(
			ctx, plugin.CapabilityStorageBlob,
		); stopErr != nil {
			if errors.Is(stopErr, context.Canceled) ||
				errors.Is(stopErr, context.DeadlineExceeded) {
				stopErr = errors.Join(errStorageDrainUnconfirmed, stopErr)
			}
			err = errors.Join(
				err,
				fmt.Errorf("blob storage shutdown: %w", stopErr),
			)
		}
	}

	// chainManager and chainsyncState have no Stop/Close method (see
	// quiesceForLiveLifecycleOp) — they're simply dropped here so
	// reinitializeStorage starts from a clean slate.
	n.chainManager = nil
	n.chainsyncState = nil

	// Every component reinitialized below (or, for Truncate, opened
	// briefly to resolve the target) re-registers its Prometheus
	// collectors under the same metric names. Clear the previous
	// registration first so that doesn't panic when a real registry is
	// configured. See metrics_registerer.go.
	n.rebuildableMetrics.unregisterAll()

	return err
}

// reinitializeCoreStorage reopens the database (already mutated on disk by
// a database/lifecycle Restore/Truncate call made between
// closeStorageForLiveLifecycleOp and this call) and rebuilds every
// component that held a direct reference to the old db/ledgerState:
// chainManager, ledgerState, mempool, chainsyncState, connManager, and
// peerGov — in that dependency order, matching node.go's Run(). Each is
// also started here (Run() interleaves construct-then-start per
// component; this mirrors that rather than separating build from start).
//
// n.ouroboros and n.chainSelector are never touched by quiesce/close, so
// they still exist; this function reassigns their exported fields
// (LedgerState, Mempool, ChainsyncState, ConnManager, PeerGov) once the new
// objects exist, exactly like Run()'s late-binding setters do.
func (n *Node) reinitializeCoreStorage(ctx context.Context) error {
	deps := n.storageDependencies(n.config.dataDir)
	deps.PromRegistry = n.config.promRegistry
	stores, err := internalplugins.ResolveStorage(
		ctx, n.pluginHost, n.storageSelections(), deps,
	)
	if err != nil {
		return fmt.Errorf("failed to reopen storage: %w", err)
	}
	db, err := database.New(n.databaseConfig(), stores)
	if db == nil {
		if err != nil {
			return fmt.Errorf("failed to reopen database: %w", err)
		}
		return errors.New("reopen database: empty database returned")
	}
	n.db = db
	dbNeedsRecovery := false
	if err != nil {
		if _, ok := errors.AsType[database.CommitTimestampError](err); !ok {
			return fmt.Errorf("failed to reopen database: %w", err)
		}
		n.config.logger.Warn(
			"database reinitialization error, needs recovery",
			"error", err,
		)
		dbNeedsRecovery = true
	}

	cm, err := chain.NewManager(n.db, n.eventBus, n.config.promRegistry)
	if err != nil {
		return fmt.Errorf("failed to reload chain manager: %w", err)
	}
	n.chainManager = cm
	// The contextcheck exemption below covers ledgerStateConfig's
	// EndorserBlockFetcher callback: it is driven by the ledger's own later
	// call, exactly as the method value it replaced was, and only defers
	// resolving n.ouroboros() -- it does not inherit this function's ctx.
	state, err := ledger.NewLedgerState(
		n.ledgerStateConfig(), //nolint:contextcheck
	)
	if err != nil {
		return fmt.Errorf("failed to reload state database: %w", err)
	}
	n.ledgerState = state
	// n.ouroboros is rewired in one place, once every rebuilt dependency
	// exists; see the NewOuroboros call in reinitializeNetworkingCore.
	if err := n.chainManager.SetLedger(n.ledgerState); err != nil {
		return fmt.Errorf(
			"failed to reconfigure chain security parameter: %w",
			err,
		)
	}

	if n.config.barkBaseUrl != "" {
		barkBlobStore, err := bark.NewBarkBlobStore(bark.BlobStoreBarkConfig{
			BaseUrl:                   n.config.barkBaseUrl,
			BlockDownloadAllowedHosts: n.config.barkBlockDownloadHosts,
			HTTPClient: &http.Client{
				Timeout: 30 * time.Second,
			},
		}, n.db.Blob())
		if err != nil {
			return fmt.Errorf("failed to recreate bark blob store: %w", err)
		}
		n.db.SetBlobStore(barkBlobStore)
	}

	// Recovery changes both the ledger tip and blob contents. Complete it
	// before starting background maintenance that reads or prunes either store.
	if dbNeedsRecovery {
		if err := n.ledgerState.RecoverCommitTimestampConflict(); err != nil {
			return fmt.Errorf("failed to recover database: %w", err)
		}
	}

	if n.config.historyExpiry.Enabled {
		prunerFreq := n.config.historyExpiry.Frequency
		if prunerFreq <= 0 {
			prunerFreq = time.Hour
		}
		n.historyExpiry = historyexpiry.NewPruner(historyexpiry.PrunerConfig{
			LedgerState: n.ledgerState,
			DB:          n.db,
			Logger:      n.config.logger,
			Frequency:   prunerFreq,
		})
		if err := n.historyExpiry.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("failed to restart history expiry: %w", err)
		}
	}

	if err := n.backfillRewardLiveStake(); err != nil {
		return err
	}

	return nil
}

// reinitializeMidnightIndexer recreates the Midnight indexer (if API
// storage mode) before n.ledgerState.Start, for the same race-avoidance
// reason Run() creates it before starting the ledger: the synchronous
// backfill must run while no new blocks can arrive, and the EventBus
// subscription must exist before any BlockActionApply event can fire.
func (n *Node) reinitializeMidnightIndexer() error {
	if !n.config.storageMode.IsAPI() {
		return nil
	}
	if err := n.ledgerState.PrepareEpochCacheForStartup(); err != nil {
		return fmt.Errorf(
			"load epoch cache before Midnight indexer restart: %w",
			err,
		)
	}
	midnightIdx, err := midnightindexer.New(midnightindexer.Config{
		EventBus:                    n.eventBus,
		Metadata:                    n.db.Metadata(),
		SlotTimer:                   n.ledgerState,
		Logger:                      n.config.logger,
		PromRegistry:                n.config.promRegistry,
		CNightPolicyID:              n.config.midnight.CNightPolicyID,
		CNightAssetName:             n.config.midnight.CNightAssetName,
		MappingValidatorAddress:     n.config.midnight.MappingValidatorAddress,
		AuthTokenPolicyID:           n.config.midnight.AuthTokenPolicyID,
		AuthTokenAssetName:          n.config.midnight.AuthTokenAssetName,
		TechnicalCommitteeAddress:   n.config.midnight.TechnicalCommitteeAddress,
		TechnicalCommitteePolicyID:  n.config.midnight.TechnicalCommitteePolicyID,
		CouncilAddress:              n.config.midnight.CouncilAddress,
		CouncilPolicyID:             n.config.midnight.CouncilPolicyID,
		PermissionedCandidatePolicy: n.config.midnight.PermissionedCandidatePolicy,
		CommitteeCandidateAddress:   n.config.midnight.CommitteeCandidateAddress,
		SlotToEpoch: func(slot uint64) (uint64, error) {
			epoch, err := n.ledgerState.SlotToEpoch(slot)
			if err != nil {
				return 0, err
			}
			return epoch.EpochId, nil
		},
		BlockIterator: func(startSlot, endSlot uint64, fn func(models.Block) error) error {
			return database.ForEachBlockInRangeDB(n.db, startSlot, endSlot, fn)
		},
		FatalErrorFunc: func(err error) {
			n.config.logger.Error(
				"fatal midnight indexer error, initiating shutdown",
				"error", err,
			)
			n.cancel()
		},
	})
	if err != nil {
		return fmt.Errorf("recreating midnight indexer: %w", err)
	}
	n.midnightIndexer = midnightIdx
	if err := n.midnightIndexer.Start(); err != nil {
		return fmt.Errorf("restarting midnight indexer: %w", err)
	}
	return nil
}

// reinitializeBackgroundManagers rebuilds the stake-snapshot manager and
// wires both its epoch-boundary hooks (the stake hook and the capture
// hook), (re)starts the optional Koios parity observer (dingo #3098) if
// configured, then starts n.ledgerState -- in that order, matching Run()'s
// own "hooks configured → observer subscribed → ledger started" sequencing
// (node.go), so an epoch boundary reached immediately after restart can
// never fire before both snapshot hooks or the parity observer's
// subscription exist. It then restarts the database-lifecycle manager and
// (if enabled) the Leios vote/pipeline managers, matching Run()'s order.
func (n *Node) reinitializeBackgroundManagers(ctx context.Context) error {
	n.snapshotMgr = snapshot.NewManager(n.db, n.eventBus, n.config.logger)
	// Mirror the CIP-0163 reward-account inactivity gate into snapshot
	// capture so it matches the ledger config that drives account expiry
	// stamping (see Run()'s identical call in node.go) -- must happen
	// before CaptureGenesisSnapshot/Start below locks this configuration,
	// or a live restore/truncate would silently run post-CIP-163 stake
	// and reward snapshots with pre-CIP behavior even though the operator
	// has it enabled.
	if err := n.snapshotMgr.SetDelegatorInactivity(
		n.config.delegatorInactivityEnabled,
		n.config.delegatorInactivity,
	); err != nil {
		return fmt.Errorf("configuring snapshot manager: %w", err)
	}
	n.snapshotMgr.SetPromRegistry(n.config.promRegistry)
	// Reinstall both epoch-boundary hooks, in the same order and with the
	// same bodies as Run() (node.go): the stake hook first, so the
	// authoritative SNAP-point stake read (after MIR, before POOLREAP/
	// enactment) is captured via ComputeEpochBoundarySnapshot, then the
	// capture hook, which stages that snapshot atomically as part of the
	// same rollover transaction. A live Restore/Truncate must not leave
	// only the capture hook installed -- without the paired stake hook,
	// the next epoch boundary would fall back to reconstructing stake
	// instead of using the SNAP-point capture, producing snapshot/reward
	// state inconsistent with a normal (non-restored) startup and able to
	// trip false mismatches in the Koios parity observer/check.
	n.ledgerState.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return n.snapshotMgr.ComputeEpochBoundarySnapshot(n.ctx, txn, evt)
		},
	)
	n.ledgerState.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return n.snapshotMgr.CaptureEpochBoundarySnapshot(n.ctx, txn, evt)
		},
	)

	// Rebuild the Koios parity observer (if enabled) against the fresh
	// n.db a live restore/truncate just reinitialized, and resubscribe it to
	// event.EpochTransitionEventType, before n.ledgerState.Start below --
	// quiesceForLiveLifecycleOp already stopped and unsubscribed the old
	// one (bound to the now-closed pre-rebuild n.db) as part of tearing
	// storage down. startKoiosParityObserver sets n.koiosParityObserver/
	// n.koiosParitySubId itself, identical to Run()'s own call.
	if n.config.koiosParity.Enabled {
		if err := n.startKoiosParityObserver(); err != nil {
			return fmt.Errorf("restarting koios parity observer: %w", err)
		}
	}

	if err := n.ledgerState.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to restart ledger: %w", err)
	}

	if err := n.snapshotMgr.CaptureGenesisSnapshot(ctx); err != nil {
		if err := n.handleGenesisSnapshotError(err); err != nil {
			return err
		}
	}
	if err := n.snapshotMgr.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to restart snapshot manager: %w", err)
	}

	n.dbLifecycleMgr = dblifecycle.NewManager(
		n.db,
		n.eventBus,
		n.config.databaseLifecycle,
		n.config.pluginSelections[plugin.CapabilityStorageBlob].Provider,
		n.config.pluginSelections[plugin.CapabilityStorageMetadata].Provider,
		n.destinationRegistry,
		n.config.logger,
	)
	if err := n.dbLifecycleMgr.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf(
			"failed to restart database lifecycle manager: %w",
			err,
		)
	}

	if n.config.experimentalDijkstraEnabled() {
		if err := n.initLeiosVoteManager(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf(
				"failed to reinitialize leios vote manager: %w",
				err,
			)
		}
		if err := n.initLeiosPipelineManager(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf(
				"failed to reinitialize leios pipeline manager: %w",
				err,
			)
		}
	}

	return nil
}

// reinitializeNetworkingCore rebuilds mempool, chainsyncState, connManager,
// and peerGov, in that dependency order, then starts connManager and
// peerGov. Must run after reinitializeBackgroundManagers (mempool/
// chainsyncState come after the background managers in Run()'s order,
// though nothing here actually depends on them).
func (n *Node) reinitializeNetworkingCore(ctx context.Context) error {
	mempoolSelection := n.config.pluginSelections[plugin.CapabilityMempool]
	var err error
	n.mempool, err = plugin.Resolve[mempool.Service](
		ctx,
		n.pluginHost,
		plugin.CapabilityMempool,
		mempoolSelection.Provider,
		mempoolSelection.Config,
		mempool.ProviderDependencies{
			PromRegistry:    n.config.promRegistry,
			Validator:       n.ledgerState,
			Logger:          n.config.logger,
			EventBus:        n.eventBus,
			CurrentSlotFunc: n.ledgerState.CurrentOrTipSlot,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to recreate mempool: %w", err)
	}
	n.ledgerState.SetMempool(&ledgerMempoolAdapter{source: n.mempool})

	chainsyncCfg := chainsync.DefaultConfig()
	if n.config.chainsyncMaxClients > 0 {
		chainsyncCfg.MaxClients = n.config.chainsyncMaxClients
	}
	if n.config.chainsyncStallTimeout > 0 {
		chainsyncCfg.StallTimeout = n.config.chainsyncStallTimeout
	}
	chainsyncCfg.HeaderSyncStrategy = n.config.chainsyncStrategy
	chainsyncCfg.PromRegistry = n.config.promRegistry
	n.chainsyncState = chainsync.NewStateWithConfig(
		n.eventBus,
		n.ledgerState,
		chainsyncCfg,
	)
	n.chainsyncClientRemoveSubId = n.eventBus.SubscribeFunc(
		chainsync.ClientRemoveRequestedEventType,
		n.chainsyncState.HandleClientRemoveRequestedEvent,
	)

	// Providers, not eagerly-computed values, matching Run. Resolving these
	// here would capture the outgoing ouroboros instance that is replaced
	// below, so every listener and outbound dial would install protocol
	// handlers bound to a closed Ouroboros and the node would never resume
	// syncing after the restore.
	n.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:   n.config.logger,
			EventBus: n.eventBus,
			ListenersProvider: func() []connmanager.ListenerConfig {
				return n.ouroboros().ConfigureListeners(n.config.listeners)
			},
			OutboundSourcePort: n.config.outboundSourcePort,
			OutboundConnOptsProvider: func() []ouroboros.ConnectionOptionFunc {
				return n.ouroboros().OutboundConnOpts()
			},
			PromRegistry:        n.config.promRegistry,
			MaxConnectionsPerIP: n.config.maxConnectionsPerIP,
			MaxInboundConns:     n.config.maxInboundConns,
			ConnClosedFunc:      n.handleConnManagerClosed,
		},
	)
	n.connManagerRecycleSubId = n.eventBus.SubscribeFunc(
		connmanager.ConnectionRecycleRequestedEventType,
		n.connManager.HandleConnectionRecycleRequestedEvent,
	)

	n.poolRelayProvider, err = ledger.NewPoolRelayProvider(
		n.ledgerState,
		n.db,
		n.eventBus,
	)
	if err != nil {
		return fmt.Errorf("failed to recreate ledger relay provider: %w", err)
	}
	ledgerPeerProvider := ledgerpeers.NewProvider(n.poolRelayProvider)

	var useLedgerAfterSlot int64 = -1
	if n.config.topologyConfig != nil {
		useLedgerAfterSlot = n.config.topologyConfig.UseLedgerAfterSlot
	}
	peerGovConfig := peergov.PeerGovernorConfig{
		Logger:          n.config.logger,
		EventBus:        n.eventBus,
		ConnManager:     n.connManager,
		DisableOutbound: n.config.isDevMode(),
		PromRegistry:    n.config.promRegistry,
		PeerRequestFunc: func(peer *peergov.Peer) []string {
			return n.ouroboros().RequestPeersFromPeer(peer)
		},
		LedgerPeerProvider:                   ledgerPeerProvider,
		UseLedgerAfterSlot:                   useLedgerAfterSlot,
		LedgerPeerTarget:                     n.config.ledgerPeerTarget,
		ActivePeersTopologyQuota:             n.config.activePeersTopologyQuota,
		ActivePeersGossipQuota:               n.config.activePeersGossipQuota,
		ActivePeersLedgerQuota:               n.config.activePeersLedgerQuota,
		InboundWarmTarget:                    n.config.inboundWarmTarget,
		InboundHotQuota:                      n.config.inboundHotQuota,
		InboundMinTenure:                     n.config.inboundMinTenure,
		InboundHotScoreThreshold:             n.config.inboundHotScoreThreshold,
		InboundPruneAfter:                    n.config.inboundPruneAfter,
		InboundDuplexOnlyForHot:              n.config.inboundDuplexOnlyForHot,
		InboundCooldown:                      n.config.inboundCooldown,
		MinHotPeers:                          n.config.minHotPeers,
		ReconcileInterval:                    n.config.reconcileInterval,
		InactivityTimeout:                    n.config.inactivityTimeout,
		SyncProgressProvider:                 n.ledgerState,
		BootstrapPromotionMinDiversityGroups: n.config.bootstrapPromotionMinDiversityGroups,
	}
	applyPeerTargets(n.config, &peerGovConfig)
	n.peerGov = peergov.NewPeerGovernor(peerGovConfig)
	// Replace ouroboros. It takes its dependencies at construction and never
	// reassigns them, so rebuilding those dependencies means rebuilding it
	// too. Closing the old instance first is required, not merely tidy: it
	// owns Prometheus collectors on the retained registry (a duplicate
	// registration panics) and EventBus subscriptions on the retained bus (a
	// leaked one would be handled once per restore cycle, forever).
	//
	// The settings half of the config is reused verbatim from Run, so the two
	// paths cannot drift, and a dependency added to OuroborosConfig fails to
	// compile here rather than silently leaving a half-wired instance behind
	// a live restore.
	//
	// The connection-event subscriptions registered during Run do not need
	// re-registering: they are closures over n, so they resolve whichever
	// instance n.ouroboros currently holds.
	if err := n.ouroboros().Close(); err != nil {
		return fmt.Errorf("failed to close previous ouroboros: %w", err)
	}
	ouroborosCfg := n.ouroborosConfig
	ouroborosCfg.LedgerState = n.ledgerState
	ouroborosCfg.LeiosAnnouncementLedger = n.ledgerState
	ouroborosCfg.Mempool = n.mempool
	ouroborosCfg.ChainsyncState = n.chainsyncState
	ouroborosCfg.ConnManager = n.connManager
	ouroborosCfg.PeerGov = n.peerGov
	rebuiltOuroboros, err := ouroborosPkg.NewOuroboros(ouroborosCfg)
	if err != nil {
		return fmt.Errorf("failed to reconstruct ouroboros: %w", err)
	}
	// Carry the optional Leios prototype handlers across. Their managers were
	// rebuilt earlier in this restore, by reinitializeBackgroundManagers, so
	// without this the replacement instance would silently lose Leios vote
	// and pipeline handling. Same call Run makes after its construction.
	if err := n.attachLeiosHandlers(rebuiltOuroboros); err != nil {
		return err
	}
	n.ouroborosRef.Store(rebuiltOuroboros)
	// Re-register the chainsync resync handler: Close took the previous
	// instance's subscription off the retained bus.
	//
	// n.ctx, not the restore operation's ctx: this subscription outlives the
	// restore, so binding it to a context cancelled at the end of the
	// operation would leave the node running with resync handling silently
	// disabled. This matches Run's own long-lived subscription.
	n.ouroboros().SubscribeChainsyncResync(n.ctx) //nolint:contextcheck

	genesisSelectionMode := n.config.genesisBootstrap &&
		!n.config.intersectTip &&
		len(n.config.intersectPoints) == 0
	if n.config.topologyConfig != nil {
		topologyConfig := n.config.topologyConfig
		usePeerSnapshot := genesisSelectionMode &&
			topologyConfig.PeerSnapshot != nil &&
			topologyConfig.PeerSnapshot.HasRelays()
		if usePeerSnapshot {
			topologyConfig = topologyConfig.WithoutBootstrapPeers()
		}
		n.peerGov.LoadTopologyConfig(topologyConfig)
		if usePeerSnapshot {
			added := n.peerGov.LoadPeerSnapshot(
				ctx,
				n.config.topologyConfig.PeerSnapshot,
			)
			if added == 0 {
				n.peerGov.LoadTopologyConfig(n.config.topologyConfig)
			}
		}
	}

	if err := n.connManager.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to restart connection manager: %w", err)
	}
	// See node.go's identical call after its own connManager.Start for why:
	// a caller-supplied listener is single-use, so its concrete resolved
	// address (not the now-closed object) is what must carry forward to
	// the NEXT reinit.
	n.config.listeners = n.connManager.ResolvedListeners()
	if err := n.peerGov.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("peer governor restart failed: %w", err)
	}

	return nil
}

// reinitializeAPIServers rebuilds the optional, storage-mode/config-gated API
// servers (utxorpc, midnightServer, blockfrostAPI, meshAPI,
// offchainMetadataFetcher), matching Run()'s gating exactly. The Bark blob-
// store client (n.config.barkBaseUrl) is handled in reinitializeCoreStorage
// since it wires directly onto n.db, not a separate server object.
//
// bark itself is neither stopped nor rebuilt here — see
// quiesceForLiveLifecycleOp's comment on why its server must stay
// reachable — it just gets its Archive service's DB reference updated to
// the freshly rebuilt n.db via ResumeDB, which also releases the pause
// PauseDB put in place before storage was closed (see Restore/Truncate).
//
// Note: the chainsync stall recycler is deliberately NOT restarted here —
// it was never stopped (see quiesceForLiveLifecycleOp's comment) and its
// component provider (nodeRecyclerComponents) reads n.ledgerState/
// n.chainsyncState/n.chainSelector fresh each tick, so it picks up the
// rebuilt objects on its own next tick.
func (n *Node) reinitializeAPIServers() error {
	utxorpcSelection, utxorpcPort, err := n.apiPluginSelection(
		plugin.CapabilityAPIUtxorpc,
	)
	if err != nil {
		return err
	}
	if n.config.storageMode.IsAPI() && utxorpcPort > 0 {
		err = plugin.ResolveProvider(
			n.ctx, n.pluginHost, plugin.CapabilityAPIUtxorpc,
			utxorpcSelection.Provider, utxorpcSelection.Config,
			utxorpc.ProviderDependencies{
				Logger: n.config.logger, EventBus: n.eventBus,
				LedgerState: n.ledgerState, Mempool: n.mempool,
				Host:               n.config.bindAddr,
				CORSAllowedOrigins: n.config.corsAllowedOrigins,
			},
		)
		if err != nil {
			return fmt.Errorf("restarting utxorpc: %w", err)
		}
	}

	if n.bark != nil {
		n.bark.ResumeDB(n.db)
	}

	if midnightServerActive(n.config.storageMode, n.config.midnight) {
		var err error
		n.midnightServer, err = midnightserver.New(
			midnightserver.Config{
				Logger:   n.config.logger,
				Metadata: n.db.Metadata(),
				BlockNumberByHash: func(hash []byte) (uint64, bool, error) {
					block, err := database.BlockByHash(n.db, hash)
					if err != nil {
						if errors.Is(err, models.ErrBlockNotFound) {
							return 0, false, nil
						}
						return 0, false, err
					}
					return block.Number, true, nil
				},
				Host:                n.config.midnight.Host,
				Port:                n.config.midnight.Port,
				TLSCertFilePath:     n.config.tlsCertFilePath,
				TLSKeyFilePath:      n.config.tlsKeyFilePath,
				AllowInsecureRemote: n.config.midnight.AllowInsecureRemote,
				ReflectionEnabled:   n.config.midnight.ReflectionEnabled,
				ShutdownTimeout:     n.config.shutdownTimeout,
				Database:            midnightserver.NewDatabase(n.db),
				SlotTimer:           n.ledgerState,
				PromRegistry:        n.config.promRegistry,
			},
		)
		if err != nil {
			return fmt.Errorf(
				"failed to recreate midnight gRPC server: %w",
				err,
			)
		}
		if err := n.midnightServer.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("restarting midnight gRPC server: %w", err)
		}
	}

	blockfrostSelection, blockfrostPort, err := n.apiPluginSelection(
		plugin.CapabilityAPIBlockfrost,
	)
	if err != nil {
		return err
	}
	if n.config.storageMode.IsAPI() && blockfrostPort > 0 {
		adapter, err := blockfrost.NewNodeAdapter(n.ledgerState, n.mempool)
		if err != nil {
			return fmt.Errorf("recreating blockfrost node adapter: %w", err)
		}
		err = plugin.ResolveProvider(
			n.ctx, n.pluginHost, plugin.CapabilityAPIBlockfrost,
			blockfrostSelection.Provider, blockfrostSelection.Config,
			blockfrost.ProviderDependencies{
				Node: adapter, Logger: n.config.logger, Host: n.config.bindAddr,
				CORSAllowedOrigins: n.config.corsAllowedOrigins,
			},
		)
		if err != nil {
			return fmt.Errorf("restarting blockfrost API: %w", err)
		}
	}

	meshSelection, meshPort, err := n.apiPluginSelection(
		plugin.CapabilityAPIMesh,
	)
	if err != nil {
		return err
	}
	if n.config.storageMode.IsAPI() && meshPort > 0 {
		var genesisHash string
		var genesisStartTimeSec int64
		if nc := n.config.cardanoNodeConfig; nc != nil {
			genesisHash = nc.ByronGenesisHash
			if sg := nc.ShelleyGenesis(); sg != nil {
				genesisStartTimeSec = sg.SystemStart.Unix()
			}
		}
		if genesisHash == "" || genesisStartTimeSec == 0 {
			return errors.New(
				"mesh API requires Cardano node config " +
					"(Byron genesis hash and Shelley genesis)",
			)
		}
		err = plugin.ResolveProvider(
			n.ctx, n.pluginHost, plugin.CapabilityAPIMesh,
			meshSelection.Provider, meshSelection.Config,
			mesh.ProviderDependencies{
				Logger:              n.config.logger,
				LedgerState:         n.ledgerState,
				Database:            mesh.NewMeshDatabase(n.db),
				Chain:               n.ledgerState.Chain(),
				Mempool:             n.mempool,
				Host:                n.config.bindAddr,
				Network:             n.config.network,
				NetworkMagic:        n.config.networkMagic,
				GenesisHash:         genesisHash,
				GenesisStartTimeSec: genesisStartTimeSec,
				CORSAllowedOrigins:  n.config.corsAllowedOrigins,
			},
		)
		if err != nil {
			return fmt.Errorf("recreate mesh API server: %w", err)
		}
	}

	if n.config.storageMode.IsAPI() {
		fetcher, err := offchainmetadata.New(
			offchainmetadata.Config{
				Logger:                n.config.logger,
				Store:                 n.db.Metadata(),
				HTTPClient:            n.config.offchainMetadata.HTTPClient,
				Interval:              n.config.offchainMetadata.Interval,
				RequestTimeout:        n.config.offchainMetadata.RequestTimeout,
				UserAgent:             n.config.offchainMetadata.UserAgent,
				IPFSGatewayURL:        n.config.offchainMetadata.IPFSGatewayURL,
				BatchSize:             n.config.offchainMetadata.BatchSize,
				MaxBytes:              n.config.offchainMetadata.MaxBytes,
				AllowPrivateAddresses: n.config.offchainMetadata.AllowPrivateAddresses,
			},
		)
		if err != nil {
			return fmt.Errorf("recreating off-chain metadata fetcher: %w", err)
		}
		n.offchainMetadataFetcher = fetcher
		if err := n.offchainMetadataFetcher.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("restarting off-chain metadata fetcher: %w", err)
		}
		if n.config.tokenRegistry.Enabled {
			sync, err := n.newTokenRegistrySync()
			if err != nil {
				return fmt.Errorf("recreate token registry sync: %w", err)
			}
			n.tokenRegistrySync = sync
			if err := n.tokenRegistrySync.Start(n.ctx); err != nil { //nolint:contextcheck
				return fmt.Errorf("restarting token registry sync: %w", err)
			}
		}
		// startDeferredIndexMaintenance sets n.deferredIndexMaintenanceDone
		// itself (nil already, since closeStorageForLiveLifecycleOp cleared
		// it) and returns a cleanup closure Run() only uses for its
		// startup-failure rollback stack; closeStorageForLiveLifecycleOp
		// already waits on the channel directly, so the closure is unused
		// here.
		_ = n.startDeferredIndexMaintenance()
	}

	return nil
}

// reinitializeBlockProducer rebuilds the block-forging path (leader
// election, block forger, Leios vote wiring) if block production is
// enabled, reusing the same helper methods Run() calls
// (node_forging.go) rather than duplicating their bodies.
func (n *Node) reinitializeBlockProducer() error {
	if !n.config.blockProducer {
		return nil
	}
	creds, err := n.validateBlockProducerStartup()
	if err != nil {
		return fmt.Errorf("block producer startup validation failed: %w", err)
	}
	if err := n.validateBlockProducerLedger(creds); err != nil {
		return fmt.Errorf(
			"block producer credentials failed ledger check: %w",
			err,
		)
	}
	//nolint:contextcheck // n.ctx is the node's lifecycle context, correct parent for forger
	if err := n.initBlockForger(n.ctx, creds); err != nil {
		return fmt.Errorf("failed to reinitialize block forger: %w", err)
	}
	if err := n.enableLeiosVoting(creds); err != nil {
		return fmt.Errorf("failed to enable leios voting: %w", err)
	}
	if n.blockForger != nil {
		n.ledgerState.SetForgedBlockChecker(n.blockForger.SlotTracker())
		n.ledgerState.SetForgingEnabled(true)
		n.ledgerState.SetSlotBattleRecorder(n.blockForger)
	}
	return nil
}

// databaseConfig builds the database.Config Run() itself builds inline;
// factored out here so reinitializeCoreStorage and Truncate's temporary
// pre-reinitialize database handle don't duplicate the literal a third
// time (Run()'s own copy in node.go is intentionally left untouched).
func (n *Node) databaseConfig() *database.Config {
	return &database.Config{
		DataDir:              n.config.dataDir,
		Logger:               n.config.logger,
		PromRegistry:         n.config.promRegistry,
		StorageMode:          string(n.config.storageMode),
		Network:              n.config.network,
		NetworkMagic:         n.config.networkMagic,
		StartEra:             string(n.config.startEra),
		StrictUtxoValidation: n.config.strictUtxoValidation,
		BlobPlugin: n.config.pluginSelections[plugin.CapabilityStorageBlob].
			Provider,
		MetadataPlugin: n.config.pluginSelections[plugin.CapabilityStorageMetadata].
			Provider,
		CacheConfig: database.CborCacheConfig{
			BlockLRUEntries: n.config.cacheBlockLRUEntries,
			HotUtxoEntries:  n.config.cacheHotUtxoEntries,
			HotTxEntries:    n.config.cacheHotTxEntries,
			HotTxMaxBytes:   n.config.cacheHotTxMaxBytes,
		},
	}
}

// storageSelections returns this node's configured blob/metadata provider
// selections, for the live-lifecycle paths that must resolve storage
// themselves (reinitializeCoreStorage against n.pluginHost; a disposable
// internalplugins.OpenDatabase host elsewhere) rather than through Run()'s
// own one-time resolution.
func (n *Node) storageSelections() internalplugins.StorageSelections {
	return internalplugins.StorageSelections{
		Blob:     n.config.pluginSelections[plugin.CapabilityStorageBlob],
		Metadata: n.config.pluginSelections[plugin.CapabilityStorageMetadata],
	}
}

// storageDependencies returns the shared application settings injected
// into storage providers for dataDir, mirroring Run()'s own construction
// (node.go) but parameterized so a temporary handle (e.g. Truncate's
// tmpDB, or Restore's staging-directory validation) can target a
// different directory than n.config.dataDir.
func (n *Node) storageDependencies(
	dataDir string,
) internalplugins.StorageDependencies {
	return internalplugins.StorageDependencies{
		DataDir:        dataDir,
		RunMode:        n.config.runMode,
		StorageMode:    string(n.config.storageMode),
		MaxConnections: n.config.DatabaseWorkerPoolConfig.WorkerPoolSize,
		Logger:         n.config.logger,
	}
}

// reinitializeAndResume reconstructs and restarts every component
// quiesceForLiveLifecycleOp/closeStorageForLiveLifecycleOp tore down, in
// dependency order. A failure partway through leaves the node with some
// subsystems up and some not — not a state it can safely keep serving
// from — so it brings the whole node down (n.cancel()) for a supervised
// restart, the same way LedgerStateConfig.FatalErrorFunc does elsewhere.
func (n *Node) reinitializeAndResume(ctx context.Context) error {
	steps := []struct {
		name string
		fn   func() error
	}{
		{"core storage", func() error {
			return n.reinitializeCoreStorage(ctx)
		}},
		{"midnight indexer", n.reinitializeMidnightIndexer},
		{"background managers", func() error {
			return n.reinitializeBackgroundManagers(ctx)
		}},
		{"networking core", func() error {
			return n.reinitializeNetworkingCore(ctx)
		}},
		{"API servers", n.reinitializeAPIServers},
		{"block producer", n.reinitializeBlockProducer},
	}
	for _, step := range steps {
		if err := step.fn(); err != nil {
			n.cancel()
			return fmt.Errorf("reinitialize %s: %w", step.name, err)
		}
	}
	return nil
}

// Snapshot captures a point-in-time backup of this running node's own
// database into destDir, which must not already exist. Unlike
// Restore/Truncate, this does not quiesce anything — Database.PauseCommits
// (see database/lifecycle.Snapshot) is enough to keep the blob and
// metadata backups consistent while the node keeps forging/syncing/
// serving normally. It takes snapshotMu, not liveLifecycleMu, both to
// serialize against a concurrent Restore/Truncate closing n.db out from
// under it (Restore/Truncate take snapshotMu too, alongside their own
// liveLifecycleMu) and to match the bark DatabaseService's "only one
// operation at a time" invariant — see snapshotMu's own doc comment
// (node.go) for why a long-running Snapshot must not also hold
// liveLifecycleMu: that would block background readers like the
// chainsync recycler tick, which only need liveLifecycleMu to know
// whether n.ledgerState/n.chainsyncState are mid-rebuild, for a Snapshot
// that never touches either field. name/description label the snapshot
// (pass "" for either to leave it unlabeled) — see
// lifecycle.SnapshotToCloud's doc comment for why labeling must happen
// before any cloud mirroring.
func (n *Node) Snapshot(
	ctx context.Context,
	destDir string,
	name string,
	description string,
) (lifecycle.Manifest, error) {
	n.snapshotMu.Lock()
	defer n.snapshotMu.Unlock()

	if n.db == nil {
		return lifecycle.Manifest{}, errors.New(
			"node database is not open",
		)
	}
	return lifecycle.SnapshotToCloud(
		ctx,
		n.destinationRegistry,
		n.db,
		destDir,
		lifecycle.TriggerManual,
		dingoversion.GetVersionString(),
		n.config.pluginSelections[plugin.CapabilityStorageBlob].Provider,
		n.config.pluginSelections[plugin.CapabilityStorageMetadata].Provider,
		n.config.databaseLifecycle.SnapshotCloudDestination,
		name,
		description,
	)
}

func (n *Node) stopForPendingRestoreRollback(
	err error,
	recovery *lifecycle.RestoreRecovery,
) error {
	if !errors.Is(err, lifecycle.ErrRestoreRollbackPending) {
		return nil
	}
	// Automatic compensation already failed under a non-cancelled context.
	// Reopening these providers could make the node serve an unknown mixture
	// of original and incoming state. Keep it stopped for supervised recovery
	// from the retained backups instead.
	n.cancel()
	if recovery != nil {
		return fmt.Errorf(
			"restore: %w (node stopped with remote rollback pending at %q)",
			err,
			recovery.BackupDir(),
		)
	}
	return fmt.Errorf("restore: %w", err)
}

// Restore replaces this running node's database with the snapshot at
// snapshotDir, quiescing and reinitializing every storage-dependent
// subsystem in-process (see this file's package comment for exactly what
// stays running and what gets rebuilt). The node is quiesced and its storage
// handles are closed first, because external providers cannot be reset while
// the node holds their connections. Restore then checks manifest compatibility,
// validates the complete archives and, for external providers, captures
// rollback copies before replacing either configured target. An incompatible
// snapshot is therefore rejected after the quiesce, not before it.
//
// A bad snapshot is rejected with both original stores intact. A failure after
// a live external reset automatically restores the original metadata and blob
// pair before this method resumes the node. An incomplete automatic rollback,
// unconfirmed storage drain, unrecoverable local directory swap, or
// reinitialization failure brings the node down (n.cancel()) for a supervised
// restart.
func (n *Node) Restore(
	ctx context.Context,
	snapshotDir string,
) (lifecycle.Manifest, error) {
	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()
	// Excludes a concurrent Snapshot too -- see snapshotMu's doc comment
	// (node.go) for why Snapshot itself only takes this lock, not
	// liveLifecycleMu.
	n.snapshotMu.Lock()
	defer n.snapshotMu.Unlock()

	stagingDir := n.config.dataDir + restoreStagingSuffix
	if err := os.RemoveAll(stagingDir); err != nil {
		return lifecycle.Manifest{}, fmt.Errorf(
			"clear restore staging directory: %w", err,
		)
	}

	// A fresh, single-use plugin host built just for staging this restore
	// -- not n.pluginHost, the live node's own app-wide host, which must
	// not be stopped out from under the still-running node the way this
	// host is stopped right below. Mirrors Truncate's identical tmpDB
	// pattern (internalplugins.OpenDatabase builds its own scratch host
	// the same way).
	restoreHost, err := internalplugins.NewHost()
	if err != nil {
		return lifecycle.Manifest{}, fmt.Errorf(
			"build storage plugin host for restore: %w", err,
		)
	}
	defer restoreHost.Stop(context.WithoutCancel(ctx)) //nolint:errcheck

	// Quiesce and close this node's own storage BEFORE any restore work
	// touches either the staging directory or (for a client/server
	// metadata backend) the live remote database, not after: unlike
	// file-based blob/metadata storage, where lifecycle.Restore below only
	// ever writes into the isolated stagingDir and the live data directory
	// is untouched until swapInRestoredDataDir runs, a Resettable provider
	// (postgres/mysql) has no such staging copy -- its connection config
	// always points at the one real, already-configured database
	// regardless of stagingDir, so Reset+RestoreFrom mutate that live
	// database directly. Running that while this node's own,
	// still-open connection pool could concurrently be reading or writing
	// the same database (or, on a later validation failure, leaving the
	// node running against a database a concurrent Reset already emptied
	// or partially reloaded) is exactly the hazard quiescing first closes.
	// This costs a little avoidable downtime on a restore that later turns
	// out to fail validation, a strictly safer trade than the alternative.
	//
	// quiesceForLiveLifecycleOp attempts every one of its stop calls
	// regardless of an earlier one failing (e.g. a Stop(ctx) call hitting
	// ctx's deadline), so a non-nil err here still means the node is
	// already substantially quiesced — leaving it there without
	// attempting to resume would strand it running but silently
	// unresponsive (no forging, no mempool, no APIs) with no indication
	// to the caller that it needs a restart. The original data directory
	// (and, for a client/server metadata backend, the live database) is
	// still untouched at this point in both cases, so
	// reinitializeAndResume can safely bring the node back up on it; only
	// if that resume itself fails do we give up and bring the process
	// down for a supervised restart, mirroring swapInRestoredDataDir's
	// failure handling below.
	if err := n.quiesceForLiveLifecycleOp(ctx); err != nil {
		if errors.Is(err, errStorageDrainUnconfirmed) {
			// See errStorageDrainUnconfirmed's doc comment: reopening
			// storage is not safe here either — reinitializeAndResume
			// would reopen the same data directory a goroutine this
			// quiesce step could not confirm had exited may still be
			// using.
			n.cancel()
			return lifecycle.Manifest{}, fmt.Errorf("quiesce: %w", err)
		}
		if resumeErr := n.reinitializeAndResume(context.WithoutCancel(ctx)); resumeErr != nil {
			n.cancel()
			return lifecycle.Manifest{}, fmt.Errorf(
				"quiesce: %w (resume also failed: %w)", err, resumeErr,
			)
		}
		return lifecycle.Manifest{}, fmt.Errorf("quiesce: %w", err)
	}
	// Pinned Bark request handlers (Archive.FetchBlock,
	// DatabaseService.GetDatabaseInfo — see Bark.Acquire's doc comment) must
	// stop being handed n.db before it's closed below, and must not resume
	// until reinitializeAPIServers's ResumeDB call publishes the rebuilt
	// n.db afterward. From here on, every exit path either reaches that
	// ResumeDB call (success) or calls n.cancel() (process is coming down
	// for a supervised restart, so a gate left paused is moot).
	if n.bark != nil {
		n.bark.PauseDB()
	}
	if err := n.closeStorageForLiveLifecycleOp(ctx); err != nil {
		if errors.Is(err, errStorageDrainUnconfirmed) {
			// See errStorageDrainUnconfirmed's doc comment: unlike every
			// other quiesce/close-storage error, reinitializeAndResume is
			// not safe here — it would reopen the same data directory a
			// goroutine this Close call could not confirm had exited may
			// still be using.
			n.cancel()
			return lifecycle.Manifest{}, fmt.Errorf("close storage: %w", err)
		}
		if resumeErr := n.reinitializeAndResume(context.WithoutCancel(ctx)); resumeErr != nil {
			n.cancel()
			return lifecycle.Manifest{}, fmt.Errorf(
				"close storage: %w (resume also failed: %w)", err, resumeErr,
			)
		}
		return lifecycle.Manifest{}, fmt.Errorf("close storage: %w", err)
	}

	// This node's own storage handle is now closed: a Resettable
	// provider's Reset+RestoreFrom below can only ever affect a database
	// this node itself has stopped using, never one it is concurrently
	// serving from. This always restores into the same database this node
	// is already configured for (Metadata below), not an
	// independently-resolved one a misconfigured DSN could point
	// elsewhere -- see metadata.AllowResetOfPopulatedTarget's doc comment
	// for why that makes it safe to bypass a Resettable provider's own
	// "target already has real data" guard here, unlike the offline
	// `dingo database restore` CLI path, which must keep going through
	// that guard.
	manifest, recovery, err := lifecycle.RestoreRecoverable(
		metadata.AllowResetOfPopulatedTarget(ctx),
		restoreHost, n.destinationRegistry, snapshotDir, stagingDir,
		func(m lifecycle.Manifest) error {
			compatibilityGates := n.nodeSettingsGateValues()
			if n.config.networkMagic != 0 {
				compatibilityGates["network_magic"] = strconv.FormatUint(
					uint64(n.config.networkMagic), 10,
				)
			}
			if n.config.startEra != "" {
				compatibilityGates["start_era"] = string(n.config.startEra)
			} else {
				compatibilityGates["start_era"] = nodesettings.NoStartEra
			}
			if err := m.CheckCompatibility(
				n.config.pluginSelections[plugin.CapabilityStorageBlob].Provider,
				n.config.pluginSelections[plugin.CapabilityStorageMetadata].Provider,
				string(n.config.storageMode),
				n.config.network,
				compatibilityGates,
			); err != nil {
				return fmt.Errorf(
					"snapshot is not compatible with the running node: %w", err,
				)
			}
			return nil
		},
		lifecycle.RestoreStorageConfig{
			Blob:     n.config.pluginSelections[plugin.CapabilityStorageBlob].Config,
			Metadata: n.config.pluginSelections[plugin.CapabilityStorageMetadata].Config,
		},
	)
	if err != nil {
		_ = os.RemoveAll(stagingDir)
		if stopErr := n.stopForPendingRestoreRollback(err, recovery); stopErr != nil {
			return lifecycle.Manifest{}, stopErr
		}
		if resumeErr := n.reinitializeAndResume(context.WithoutCancel(ctx)); resumeErr != nil {
			n.cancel()
			return lifecycle.Manifest{}, fmt.Errorf(
				"restore: %w (resume also failed: %w)", err, resumeErr,
			)
		}
		return lifecycle.Manifest{}, fmt.Errorf("restore: %w", err)
	}

	backupDir, err := n.swapInRestoredDataDir(stagingDir)
	if err != nil {
		if rollbackErr := recovery.Rollback(ctx); rollbackErr != nil {
			n.cancel()
			return lifecycle.Manifest{}, errors.Join(err, rollbackErr)
		}
		if errors.Is(err, errRestoreSwapUnrecoverable) {
			// dataDir's own state is already unknown, so no safe in-process
			// resume target remains.
			n.cancel()
			return lifecycle.Manifest{}, err
		}
		// The original data directory is intact (or was successfully
		// rolled back) — bring the node back up on it rather than leaving
		// it down over a failed swap.
		if resumeErr := n.reinitializeAndResume(context.WithoutCancel(ctx)); resumeErr != nil {
			n.cancel()
			return lifecycle.Manifest{}, fmt.Errorf(
				"%w (resume also failed: %w)", err, resumeErr,
			)
		}
		return lifecycle.Manifest{}, err
	}

	// The swap itself succeeded: n.config.dataDir now holds the restored
	// data, and backupDir still holds the pre-restore original. It stays
	// in place until reinitializeAndResume actually proves the restored
	// data starts — a directory swap succeeding is not by itself proof of
	// that (see swapInRestoredDataDir's doc comment).
	if err := n.reinitializeAndResume(context.WithoutCancel(ctx)); err != nil {
		// The swap already happened and cannot be undone from here (every
		// storage-dependent subsystem has already begun reinitializing
		// against the restored data): bring the process down for a
		// supervised restart rather than leaving it running in an unknown
		// state, and surface backupDir's location so an operator has a
		// last-known-good fallback instead of it silently having been
		// deleted. reconcileInterruptedLiveRestoreSwap resolves this same
		// state (backupDir present, dataDir present) at the next startup.
		n.cancel()
		return lifecycle.Manifest{}, fmt.Errorf(
			"%w (pre-restore local backup preserved at %q and remote rollback backup at %q pending a restart)",
			err,
			backupDir,
			recovery.BackupDir(),
		)
	}
	if err := recovery.Commit(); err != nil {
		n.config.logger.Warn(
			"failed to remove remote rollback backup after a successful restore",
			"dir",
			recovery.BackupDir(),
			"error",
			err,
		)
	}
	if err := os.RemoveAll(backupDir); err != nil {
		n.config.logger.Warn(
			"failed to remove pre-restore backup after a successful restore",
			"dir", backupDir,
			"error", err,
		)
	}
	return manifest, nil
}

// errRestoreSwapUnrecoverable marks a swapInRestoredDataDir failure where
// neither the restored data nor the original data ended up at
// n.config.dataDir — the only case where Restore has no choice but to
// bring the node down rather than resume on whatever is in place.
var errRestoreSwapUnrecoverable = errors.New(
	"data directory swap left no usable data directory in place",
)

// errStorageDrainUnconfirmed marks a quiesceForLiveLifecycleOp or
// closeStorageForLiveLifecycleOp failure where a background goroutine could
// not be confirmed to have exited before its bounded wait timed out —
// currently n.ledgerState.Close()'s rollback-event/dbWorkerPool waits, the
// leios persist writer's drain (PauseLeiosPersistWriterForLiveLifecycleOp),
// or a storage provider whose context-bounded Stop returned before its cleanup
// completed. Unlike every other error
// these functions can return, this one means a goroutine may still be
// reading/writing n.db, not merely that some cleanup step reported failure
// after the resource was already unused. Restore/Truncate must not treat
// this the way they treat every other quiesce/close-storage error (attempt
// reinitializeAndResume against the same on-disk data): reopening storage a
// still-running goroutine may be touching is exactly the use-after-close
// race this whole path exists to prevent, so this forces a supervised
// restart instead.
var errStorageDrainUnconfirmed = errors.New(
	"could not confirm a background goroutine's drain before timeout",
)

// preRestoreBackupSuffix and restoreStagingSuffix name the sibling
// directories swapInRestoredDataDir and Restore use as n.config.dataDir's
// own crash-recovery marker: their mere presence (checked by
// reconcileInterruptedLiveRestoreSwap at every startup, before dataDir is
// ever opened) is what identifies an interrupted swap and which
// intermediate state it was interrupted in, without a separate marker
// file to keep in sync with the directories themselves.
const (
	preRestoreBackupSuffix = ".pre-restore"
	restoreStagingSuffix   = ".restore-staging"
)

// syncDataDirParent is fsyncdir.Sync, indirected through a variable so
// tests can inject a sync failure at any of swapInRestoredDataDir's three
// call sites without needing real filesystem-level fault injection.
var syncDataDirParent = fsyncdir.Sync

// swapInRestoredDataDir atomically replaces n.config.dataDir with
// stagingDir (both must be on the same filesystem for the renames to be
// atomic), keeping the original data around as a same-directory backup at
// backupDir and rolling back to it if activating the restored directory
// fails.
//
// On success, backupDir is returned but deliberately NOT removed: a
// completed rename only proves the two directories swapped names, not
// that the restored data actually starts. The caller must keep backupDir
// in place until reinitializeAndResume has actually proven the restored
// node starts, and only remove it then — see Restore's call site.
//
// Both renames are followed by an fsync of dataDir's parent directory:
// POSIX rename() is atomic the instant it returns, but that instant
// isn't necessarily durable — the containing directory's own updated
// entry can still be lost to a crash or power failure before it reaches
// disk, independently of the renamed data's own content being fsynced.
// Without this, a crash immediately after a rename returns could leave
// the directory listing showing either the pre- or post-rename state on
// the next boot, regardless of what this function itself observed.
// reconcileInterruptedLiveRestoreSwap is what makes every one of those
// intermediate states — including one this fsync doesn't quite manage to
// make durable before a crash — safe to resume from at the next startup.
func (n *Node) swapInRestoredDataDir(
	stagingDir string,
) (backupDir string, err error) {
	dataDir := n.config.dataDir
	parentDir := filepath.Dir(dataDir)
	backupDir = dataDir + preRestoreBackupSuffix
	_ = os.RemoveAll(backupDir)
	if err := os.Rename(dataDir, backupDir); err != nil {
		return "", fmt.Errorf("move aside current data directory: %w", err)
	}
	if err := syncDataDirParent(parentDir); err != nil {
		// dataDir was already renamed to backupDir above -- unlike the
		// rename itself failing, dataDir is NOT left in place here, so
		// the caller's usual "swap failed, dataDir still holds the
		// original, safe to resume on it" assumption does not hold
		// unless this rollback actually succeeds. Roll back immediately
		// rather than returning an ordinary error with dataDir absent.
		if rbErr := os.Rename(backupDir, dataDir); rbErr != nil {
			return "", fmt.Errorf(
				"%w: sync %q after moving aside current data directory: %w (rollback also failed: %w; original data preserved at %q)",
				errRestoreSwapUnrecoverable,
				parentDir,
				err,
				rbErr,
				backupDir,
			)
		}
		if syncErr := syncDataDirParent(parentDir); syncErr != nil {
			return "", fmt.Errorf(
				"sync %q after moving aside current data directory: %w (rolled back, but sync after rollback failed too: %w)",
				parentDir,
				err,
				syncErr,
			)
		}
		return "", fmt.Errorf(
			"sync %q after moving aside current data directory: %w",
			parentDir, err,
		)
	}
	if err := os.Rename(stagingDir, dataDir); err != nil {
		if rbErr := os.Rename(backupDir, dataDir); rbErr != nil {
			return "", fmt.Errorf(
				"%w: activate restored data directory: %w (rollback also failed: %w; restored data preserved at %q)",
				errRestoreSwapUnrecoverable,
				err,
				rbErr,
				stagingDir,
			)
		}
		if syncErr := syncDataDirParent(parentDir); syncErr != nil {
			return "", fmt.Errorf(
				"activate restored data directory: %w (rolled back, but sync %q after rollback failed: %w)",
				err,
				parentDir,
				syncErr,
			)
		}
		return "", fmt.Errorf("activate restored data directory: %w", err)
	}
	if err := syncDataDirParent(parentDir); err != nil {
		return "", fmt.Errorf(
			"sync %q after activating restored data directory: %w",
			parentDir, err,
		)
	}
	return backupDir, nil
}

// reconcileInterruptedLiveRestoreSwap inspects n.config.dataDir's sibling
// preRestoreBackupSuffix/restoreStagingSuffix paths for a live Restore's
// directory swap (swapInRestoredDataDir) that was interrupted -- by a
// process kill, crash, or power failure -- before this node could confirm
// and clean it up, and reconciles whichever intermediate state it finds.
// Must run before anything else opens n.config.dataDir (Run() calls this
// first, before ResolveStorage/database.New).
//
// The two directories' mere presence or absence is the marker: no
// separate marker file is needed; see swapInRestoredDataDir's own doc
// comment on why backupDir outlives a merely-successful swap.
//
//   - Neither exists, or only stagingDir exists (no completed swap ever
//     started, or one was interrupted before the first rename): nothing to
//     reconcile. A leftover stagingDir is harmless -- the next Restore
//     call's own os.RemoveAll(stagingDir) clears it -- and is deliberately
//     left in place here rather than removed, so an operator has
//     something to inspect if a restore keeps failing before ever
//     reaching the swap.
//   - backupDir exists, dataDir does not: interrupted between the swap's
//     two renames (dataDir was moved aside, but the restored data was
//     never moved into its place). Rolled back by renaming backupDir back
//     to dataDir.
//   - backupDir and dataDir both exist: the swap's second rename already
//     completed (dataDir already holds the restored data) before the
//     interruption landed. dataDir is left as-is -- it's what Run() is
//     about to start on -- and backupDir is left in place too: only
//     Run() successfully completing startup on it is proof the restored
//     data actually works, at which point Run() removes backupDir itself
//     (mirroring Restore's own in-process backup removal once
//     reinitializeAndResume proves the same thing without a restart).
func (n *Node) reconcileInterruptedLiveRestoreSwap() error {
	dataDir := n.config.dataDir
	backupDir := dataDir + preRestoreBackupSuffix
	stagingDir := dataDir + restoreStagingSuffix

	_, backupErr := os.Stat(backupDir)
	switch {
	case os.IsNotExist(backupErr):
		return nil
	case backupErr != nil:
		return fmt.Errorf(
			"stat pre-restore backup %q: %w",
			backupDir,
			backupErr,
		)
	}

	_, dataErr := os.Stat(dataDir)
	switch {
	case os.IsNotExist(dataErr):
		n.config.logger.Warn(
			"found an interrupted live restore's pre-restore backup with "+
				"no data directory in its place; rolling back to it",
			"backup_dir", backupDir,
			"data_dir", dataDir,
		)
		if err := os.Rename(backupDir, dataDir); err != nil {
			return fmt.Errorf(
				"roll back interrupted restore swap: move %q to %q: %w",
				backupDir, dataDir, err,
			)
		}
		if err := fsyncdir.Sync(filepath.Dir(dataDir)); err != nil {
			return fmt.Errorf(
				"sync %q after rolling back interrupted restore swap: %w",
				filepath.Dir(dataDir), err,
			)
		}
		_ = os.RemoveAll(stagingDir)
		return nil
	case dataErr != nil:
		return fmt.Errorf("stat data directory %q: %w", dataDir, dataErr)
	default:
		n.config.logger.Warn(
			"found a completed live restore swap whose pre-restore backup "+
				"was never confirmed and removed; starting on the restored "+
				"data directory and will remove the backup once startup succeeds",
			"backup_dir", backupDir,
			"data_dir", dataDir,
		)
		_ = os.RemoveAll(stagingDir)
		return nil
	}
}

// removeConfirmedRestoreBackup removes n.config.dataDir's pre-restore
// backup, if one is present, once Run() has fully started on dataDir --
// the proof (matching reinitializeAndResume's role for the in-process
// Restore path) that the restored data this backup was kept around for
// actually works. A no-op when there is nothing to remove.
func (n *Node) removeConfirmedRestoreBackup() {
	backupDir := n.config.dataDir + preRestoreBackupSuffix
	if _, err := os.Stat(backupDir); err != nil {
		return
	}
	if err := os.RemoveAll(backupDir); err != nil {
		n.config.logger.Warn(
			"failed to remove confirmed pre-restore backup after startup",
			"dir", backupDir,
			"error", err,
		)
		return
	}
	n.config.logger.Info(
		"removed confirmed pre-restore backup after successful startup",
		"dir", backupDir,
	)
}

// Truncate reverts this running node's database to target, per
// database/lifecycle.Truncate, quiescing and reinitializing every
// storage-dependent subsystem in-process. See Restore's doc comment for
// the availability and failure-mode caveats, which apply identically here,
// with one difference: a failure that occurred entirely during read-only
// target validation — a bad/out-of-range target, a target before the
// Mithril trust boundary, or a cancellation landing before any delete
// began (lifecycle.ErrTruncateNotStarted) — is known to have touched
// nothing on disk, so the node resumes normally on it instead of being
// torn down. A failure during or after the actual bulk delete still brings
// the node down: DeleteBlocksAfter batches its deletes (see
// database/lifecycle/blob_bulk_delete.go) rather than wrapping the whole
// truncate in one transaction, so a truncate spanning more than one batch
// can leave a partially-truncated, inconsistent database if interrupted
// mid-delete — recovering from that safely is not implemented today.
// Returns the number of blocks removed.
func (n *Node) Truncate(
	ctx context.Context,
	target dblifecycle.TruncateTarget,
) (uint64, error) {
	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()
	// Excludes a concurrent Snapshot too -- see snapshotMu's doc comment
	// (node.go) for why Snapshot itself only takes this lock, not
	// liveLifecycleMu.
	n.snapshotMu.Lock()
	defer n.snapshotMu.Unlock()

	// See Restore's identical handling for why a quiesce/close-storage
	// failure must still attempt reinitializeAndResume rather than just
	// returning: quiesceForLiveLifecycleOp attempts every stop call
	// regardless of an earlier failure, so the node is already
	// substantially quiesced by the time either call here returns a
	// non-nil error, and nothing on disk has been touched yet — leaving
	// it there would strand the node running but silently unresponsive
	// with no indication a restart is needed.
	if err := n.quiesceForLiveLifecycleOp(ctx); err != nil {
		if errors.Is(err, errStorageDrainUnconfirmed) {
			// See errStorageDrainUnconfirmed's doc comment: reopening
			// storage is not safe here either — reinitializeAndResume
			// would reopen the same data directory a goroutine this
			// quiesce step could not confirm had exited may still be
			// using.
			n.cancel()
			return 0, fmt.Errorf("quiesce: %w", err)
		}
		if resumeErr := n.reinitializeAndResume(context.WithoutCancel(ctx)); resumeErr != nil {
			n.cancel()
			return 0, fmt.Errorf(
				"quiesce: %w (resume also failed: %w)", err, resumeErr,
			)
		}
		return 0, fmt.Errorf("quiesce: %w", err)
	}
	// See Restore's identical PauseDB placement for why this must happen
	// here, before storage is closed, and why every exit path from here on
	// is safe with the gate left paused.
	if n.bark != nil {
		n.bark.PauseDB()
	}
	if err := n.closeStorageForLiveLifecycleOp(ctx); err != nil {
		if errors.Is(err, errStorageDrainUnconfirmed) {
			// See errStorageDrainUnconfirmed's doc comment: unlike every
			// other quiesce/close-storage error, reinitializeAndResume is
			// not safe here — it would reopen the same data directory a
			// goroutine this Close call could not confirm had exited may
			// still be using.
			n.cancel()
			return 0, fmt.Errorf("close storage: %w", err)
		}
		if resumeErr := n.reinitializeAndResume(context.WithoutCancel(ctx)); resumeErr != nil {
			n.cancel()
			return 0, fmt.Errorf(
				"close storage: %w (resume also failed: %w)", err, resumeErr,
			)
		}
		return 0, fmt.Errorf("close storage: %w", err)
	}

	blocksRemoved, truncateErr := func() (uint64, error) {
		deps := n.storageDependencies(n.config.dataDir)
		deps.PromRegistry = n.config.promRegistry
		tmpRuntime, err := internalplugins.OpenDatabase(
			ctx, n.databaseConfig(), n.storageSelections(), deps,
		)
		// OpenDatabase can return a non-nil runtime alongside a nil error
		// even when the underlying database.New hit a recoverable
		// CommitTimestampError, surfacing it instead through
		// tmpRuntime.RecoveryError() below (normal node startup wants the
		// runtime kept open for recovery in that case). Truncate has no
		// use for a half-consistent tmpDB, so that's treated as a hard
		// failure here too. The defer must be registered here, before
		// either check below, so both cases still close tmpRuntime rather
		// than leaking its open badger/sqlite handles: an early return
		// before this line would skip the deferred Close entirely, leaving
		// tmpRuntime's storage locks held. reinitializeAndResume's
		// reinitializeCoreStorage then reopens the very same data
		// directory a moment later (this error is classified as
		// ErrTruncateNotStarted, so nothing on disk was touched and
		// resume is expected to succeed) — reopening storage that a
		// leaked tmpRuntime is still holding open.
		if tmpRuntime != nil {
			defer tmpRuntime.Close(ctx)
		}
		if err != nil {
			return 0, fmt.Errorf(
				"%w: open database for truncate: %w",
				lifecycle.ErrTruncateNotStarted, err,
			)
		}
		if recErr := tmpRuntime.RecoveryError(); recErr != nil {
			return 0, fmt.Errorf(
				"%w: open database for truncate: %w",
				lifecycle.ErrTruncateNotStarted, recErr,
			)
		}
		tmpDB := tmpRuntime.Database

		if pending, pendingErr := lifecycle.GetPendingTruncate(tmpDB); pendingErr != nil {
			return 0, pendingErr
		} else if pending != nil {
			return lifecycle.Truncate(
				ctx,
				tmpDB,
				models.Block{},
				0,
				n.config.delegatorInactivityEnabled,
				n.config.delegatorInactivity,
			)
		}

		block, err := dblifecycle.ResolveTarget(tmpDB, target)
		if err != nil {
			return 0, fmt.Errorf(
				"%w: %w", lifecycle.ErrTruncateNotStarted, err,
			)
		}
		return lifecycle.Truncate(
			ctx,
			tmpDB,
			block,
			0,
			n.config.delegatorInactivityEnabled,
			n.config.delegatorInactivity,
		)
	}()
	// tmpDB's database.New registered its own Prometheus collectors under
	// the same names reinitializeAndResume's database.New is about to use;
	// unregister before proceeding regardless of outcome.
	n.rebuildableMetrics.unregisterAll()
	if truncateErr != nil {
		if !errors.Is(truncateErr, lifecycle.ErrTruncateNotStarted) {
			n.cancel()
			return 0, fmt.Errorf("truncate: %w", truncateErr)
		}
		// Nothing was touched on disk — resume on the untouched data
		// directory rather than tearing the whole node down over a
		// rejected target or a cancellation that landed before any delete.
		if resumeErr := n.reinitializeAndResume(context.WithoutCancel(ctx)); resumeErr != nil {
			return 0, fmt.Errorf(
				"%w (resume also failed: %w)", truncateErr, resumeErr,
			)
		}
		return 0, fmt.Errorf("truncate: %w", truncateErr)
	}

	if err := n.reinitializeAndResume(context.WithoutCancel(ctx)); err != nil {
		return 0, err
	}
	return blocksRemoved, nil
}

// Compile-time assertion that *Node satisfies dblifecycle.LiveNode (see
// that interface's doc comment for why it's defined there, not imported
// from here).
var _ dblifecycle.LiveNode = (*Node)(nil)
