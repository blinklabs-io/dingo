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
// n.eventBus, n.ouroboros (its LedgerState/Mempool/ChainsyncState/PeerGov/
// ConnManager fields are plain exported fields, reassigned below once their
// new dependencies exist), n.chainSelector (holds only a one-time
// SecurityParam snapshot and closures over n).
//
// Everything else that depends on n.db or n.ledgerState — chainManager,
// ledgerState, mempool, chainsyncState, peerGov, connManager, the
// background managers, the optional API servers, and the block-producer
// path — is stopped, discarded, and rebuilt from scratch, mirroring (by
// necessity duplicating, since Run() itself is intentionally left
// unmodified) the equivalent construction in node.go's Run().
package dingo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
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
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/historyexpiry"
	"github.com/blinklabs-io/dingo/internal/node/ledgerpeers"
	"github.com/blinklabs-io/dingo/internal/offchainmetadata"
	dingoversion "github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/mempool"
	midnightindexer "github.com/blinklabs-io/dingo/midnight/indexer"
	midnightserver "github.com/blinklabs-io/dingo/midnight/server"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// quiesceForLiveLifecycleOp stops every subsystem that touches storage, or
// holds a direct (non-self-healing) reference to n.db/n.ledgerState, in
// preparation for closing the database out from under this running node.
// It does not touch n.eventBus, n.ouroboros, n.chainSelector, or n.ctx.
func (n *Node) quiesceForLiveLifecycleOp(ctx context.Context) error {
	var err error

	if n.blockForger != nil {
		n.blockForger.Stop()
	}
	if n.leaderElection != nil {
		if stopErr := n.leaderElection.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("leader election shutdown: %w", stopErr),
			)
		}
	}
	// Pipeline manager stopped before vote manager: it consumes the vote
	// manager's EbQuorumEvent, matching the dependency order Run()'s
	// startup-failure cleanup stack already uses (see node.go).
	if n.leiosPipelineManager != nil {
		if stopErr := n.leiosPipelineManager.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("leios pipeline manager shutdown: %w", stopErr),
			)
		}
	}
	if n.leiosVoteManager != nil {
		if stopErr := n.leiosVoteManager.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("leios vote manager shutdown: %w", stopErr),
			)
		}
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
	if n.dbLifecycleMgr != nil {
		if stopErr := n.dbLifecycleMgr.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("database lifecycle manager shutdown: %w", stopErr),
			)
		}
	}
	if n.utxorpc != nil {
		if stopErr := n.utxorpc.Stop(ctx); stopErr != nil {
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
	if n.offchainMetadataFetcher != nil {
		if stopErr := n.offchainMetadataFetcher.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("off-chain metadata fetcher shutdown: %w", stopErr),
			)
		}
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

	// Unsubscribe the handlers bound to components being discarded, so the
	// EventBus (which is never stopped/restarted here) doesn't accumulate a
	// stale subscriber pointing at an object this function just tore down.
	// See the Node struct field comments (node.go) for why only these three
	// of Run()'s ~19 direct subscriptions need this.
	//
	// UnsubscribeAndWait, not Unsubscribe: closeStorageForLiveLifecycleOp
	// (called right after this returns) nils out n.chainManager and
	// n.chainsyncState with no synchronization of its own. Plain
	// Unsubscribe only stops future deliveries, so a handler goroutine
	// already dispatched before this loop runs could still be reading
	// those fields concurrently with that teardown.
	if n.chainManagerBlockProposedSubId != 0 {
		n.eventBus.UnsubscribeAndWait(
			chain.BlockProposedEventType,
			n.chainManagerBlockProposedSubId,
		)
		n.chainManagerBlockProposedSubId = 0
	}
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
			err = errors.Join(
				err,
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
func (n *Node) reinitializeCoreStorage() error {
	db, err := database.New(n.databaseConfig())
	if db == nil {
		if err != nil {
			return fmt.Errorf("failed to reopen database: %w", err)
		}
		return errors.New("reopen database: empty database returned")
	}
	n.db = db
	dbNeedsRecovery := false
	if err != nil {
		var dbErr database.CommitTimestampError
		if !errors.As(err, &dbErr) {
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
	primaryChain := n.chainManager.PrimaryChain()
	n.chainManagerBlockProposedSubId = n.eventBus.SubscribeFunc(
		chain.BlockProposedEventType,
		primaryChain.HandleBlockProposedEvent,
	)

	enableDijkstra := n.config.experimentalDijkstraEnabled()

	state, err := ledger.NewLedgerState(
		ledger.LedgerStateConfig{
			ChainManager:                  n.chainManager,
			Database:                      n.db,
			EventBus:                      n.eventBus,
			Logger:                        n.config.logger,
			CardanoNodeConfig:             n.config.cardanoNodeConfig,
			PromRegistry:                  n.config.promRegistry,
			ForgeBlocks:                   n.config.isDevMode(),
			ValidateHistorical:            n.config.validateHistorical,
			EnableDijkstra:                enableDijkstra,
			StartInDijkstra:               n.config.startEra.IsDijkstra(),
			EndorserBlockProvider:         n.ouroboros.EndorserBlockTxsByHash,
			EndorserBlockFetcher:          n.ouroboros.FetchEndorserBlockByPoint,
			EndorserBlockWaitSlots:        n.leiosPipelineTiming().CertifyByDeadlineSlots,
			LeiosApplyEndorserBlockTxs:    !n.config.isMusashiNetwork(),
			SkipLeaderStakeThresholdCheck: n.config.isMusashiNetwork(),
			SkipDijkstraTxValidation:      n.config.isMusashiNetwork(),
			BlockfetchRequestRangeFunc:    n.ouroboros.BlockfetchClientRequestRange,
			PeersWithBlockFunc: func(
				origin ouroboros.ConnectionId,
				point ocommon.Point,
			) []ouroboros.ConnectionId {
				if n.chainsyncState == nil {
					return nil
				}
				return n.chainsyncState.PeersWithBlock(origin, point)
			},
			RecordBlockfetchLatencyFunc: func(
				connId ouroboros.ConnectionId,
				latency time.Duration,
			) {
				if n.chainsyncState != nil {
					n.chainsyncState.RecordBlockfetchLatency(connId, latency)
				}
			},
			BlockfetchLatencyFunc: func(
				connId ouroboros.ConnectionId,
			) (time.Duration, bool) {
				if n.chainsyncState == nil {
					return 0, false
				}
				return n.chainsyncState.BlockfetchLatency(connId)
			},
			BlockfetchLatencyMedianFunc: func() (time.Duration, int) {
				if n.chainsyncState == nil {
					return 0, 0
				}
				return n.chainsyncState.BlockfetchLatencyMedian()
			},
			DatabaseWorkerPoolConfig: n.config.DatabaseWorkerPoolConfig,
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				if n.chainsyncState != nil {
					return n.chainsyncState.GetClientConnId()
				}
				return nil
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return n.connManager != nil &&
					n.connManager.GetConnectionById(connId) != nil
			},
			ConnectionSwitchFunc: func() {
				if n.chainsyncState != nil && n.ledgerState != nil {
					n.chainsyncState.ClearSeenHeadersFrom(
						n.ledgerState.Tip().Point.Slot,
					)
				}
			},
			ClearSeenHeadersFromFunc: func(fromSlot uint64) {
				if n.chainsyncState != nil {
					n.chainsyncState.ClearSeenHeadersFrom(fromSlot)
				}
			},
			PeerHeaderLookupFunc: func(
				connId ouroboros.ConnectionId,
				hash []byte,
			) (ledger.ChainsyncEvent, []byte, bool) {
				if n.chainsyncState == nil {
					return ledger.ChainsyncEvent{}, nil, false
				}
				h, prevHash, ok := n.chainsyncState.LookupObservedHeader(connId, hash)
				if !ok {
					return ledger.ChainsyncEvent{}, nil, false
				}
				return ledger.ChainsyncEvent{
					ConnectionId: h.ConnectionId,
					BlockHeader:  h.BlockHeader,
					Point:        h.Point,
					Tip:          h.Tip,
					BlockNumber:  h.BlockNumber,
					Type:         h.Type,
					Rollback:     h.Rollback,
				}, prevHash, true
			},
			FatalErrorFunc: func(err error) {
				n.config.logger.Error(
					"fatal ledger error, initiating shutdown",
					"error", err,
				)
				n.cancel()
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to reload state database: %w", err)
	}
	n.ledgerState = state
	n.ouroboros.LedgerState = n.ledgerState
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

	if dbNeedsRecovery {
		if err := n.ledgerState.RecoverCommitTimestampConflict(); err != nil {
			return fmt.Errorf("failed to recover database: %w", err)
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

// reinitializeBackgroundManagers starts n.ledgerState (deferred until here
// so the Midnight indexer's subscription already exists), then rebuilds
// the stake-snapshot manager, the database-lifecycle manager, and (if
// enabled) the Leios vote/pipeline managers — matching Run()'s order.
func (n *Node) reinitializeBackgroundManagers(ctx context.Context) error {
	if err := n.ledgerState.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to restart ledger: %w", err)
	}

	n.snapshotMgr = snapshot.NewManager(n.db, n.eventBus, n.config.logger)
	n.snapshotMgr.SetPromRegistry(n.config.promRegistry)
	n.ledgerState.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return n.snapshotMgr.CaptureEpochBoundarySnapshot(n.ctx, txn, evt)
		},
	)
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
func (n *Node) reinitializeNetworkingCore() error {
	var err error
	n.mempool, err = mempool.New(
		n.config.mempoolImplementation,
		mempool.MempoolConfig{
			MempoolCapacity:    n.config.mempoolCapacity,
			EvictionWatermark:  n.config.evictionWatermark,
			RejectionWatermark: n.config.rejectionWatermark,
			Logger:             n.config.logger,
			EventBus:           n.eventBus,
			PromRegistry:       n.config.promRegistry,
			Validator:          n.ledgerState,
			CurrentSlotFunc:    n.ledgerState.CurrentOrTipSlot,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to recreate mempool: %w", err)
	}
	n.ledgerState.SetMempool(&ledgerMempoolAdapter{source: n.mempool})
	n.ouroboros.Mempool = n.mempool

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
	n.ouroboros.ChainsyncState = n.chainsyncState
	n.chainsyncClientRemoveSubId = n.eventBus.SubscribeFunc(
		chainsync.ClientRemoveRequestedEventType,
		n.chainsyncState.HandleClientRemoveRequestedEvent,
	)

	tmpListeners := n.ouroboros.ConfigureListeners(n.config.listeners)
	n.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:              n.config.logger,
			EventBus:            n.eventBus,
			Listeners:           tmpListeners,
			OutboundSourcePort:  n.config.outboundSourcePort,
			OutboundConnOpts:    n.ouroboros.OutboundConnOpts(),
			PromRegistry:        n.config.promRegistry,
			MaxConnectionsPerIP: n.config.maxConnectionsPerIP,
			MaxInboundConns:     n.config.maxInboundConns,
		},
	)
	n.connManagerRecycleSubId = n.eventBus.SubscribeFunc(
		connmanager.ConnectionRecycleRequestedEventType,
		n.connManager.HandleConnectionRecycleRequestedEvent,
	)
	n.ouroboros.ConnManager = n.connManager

	ledgerRelayProvider, err := ledger.NewPoolRelayProvider(
		n.ledgerState,
		n.db,
		n.eventBus,
	)
	if err != nil {
		return fmt.Errorf("failed to recreate ledger relay provider: %w", err)
	}
	ledgerPeerProvider := ledgerpeers.NewProvider(ledgerRelayProvider)

	var useLedgerAfterSlot int64 = -1
	if n.config.topologyConfig != nil {
		useLedgerAfterSlot = n.config.topologyConfig.UseLedgerAfterSlot
	}
	n.peerGov = peergov.NewPeerGovernor(
		peergov.PeerGovernorConfig{
			Logger:                               n.config.logger,
			EventBus:                             n.eventBus,
			ConnManager:                          n.connManager,
			DisableOutbound:                      n.config.isDevMode(),
			PromRegistry:                         n.config.promRegistry,
			PeerRequestFunc:                      n.ouroboros.RequestPeersFromPeer,
			LedgerPeerProvider:                   ledgerPeerProvider,
			UseLedgerAfterSlot:                   useLedgerAfterSlot,
			LedgerPeerTarget:                     n.config.ledgerPeerTarget,
			TargetNumberOfKnownPeers:             n.config.targetNumberOfKnownPeers,
			TargetNumberOfEstablishedPeers:       n.config.targetNumberOfEstablishedPeers,
			TargetNumberOfActivePeers:            n.config.targetNumberOfActivePeers,
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
		},
	)
	n.ouroboros.PeerGov = n.peerGov

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
	if err := n.peerGov.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("peer governor restart failed: %w", err)
	}

	return nil
}

// reinitializeAPIServers rebuilds the optional, storage-mode/port-gated API
// servers (utxorpc, midnightServer, blockfrostAPI, meshAPI,
// offchainMetadataFetcher), matching Run()'s gating exactly. The Bark blob-
// store client (n.config.barkBaseUrl) is handled in reinitializeCoreStorage
// since it wires directly onto n.db, not a separate server object.
//
// bark itself is neither stopped nor rebuilt here — see
// quiesceForLiveLifecycleOp's comment on why its server must stay
// reachable — it just gets its Archive service's DB reference updated to
// the freshly rebuilt n.db via SetDB.
//
// Note: the chainsync stall recycler is deliberately NOT restarted here —
// it was never stopped (see quiesceForLiveLifecycleOp's comment) and reads
// n.ledgerState/n.chainSelector fresh each tick, so it picks up the
// rebuilt objects on its own next tick.
func (n *Node) reinitializeAPIServers() error {
	if n.config.storageMode.IsAPI() && n.config.utxorpcPort > 0 {
		n.utxorpc = utxorpc.NewUtxorpc(
			utxorpc.UtxorpcConfig{
				Logger:             n.config.logger,
				EventBus:           n.eventBus,
				LedgerState:        n.ledgerState,
				Mempool:            n.mempool,
				Host:               n.config.bindAddr,
				Port:               n.config.utxorpcPort,
				TlsCertFilePath:    n.config.tlsCertFilePath,
				TlsKeyFilePath:     n.config.tlsKeyFilePath,
				CORSAllowedOrigins: n.config.corsAllowedOrigins,
			},
		)
		if err := n.utxorpc.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("restarting utxorpc: %w", err)
		}
	}

	if n.bark != nil {
		n.bark.SetDB(n.db)
	}

	if n.config.storageMode.IsAPI() && n.config.midnight.Port > 0 {
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
				Host:            n.config.midnight.Host,
				Port:            n.config.midnight.Port,
				TLSCertFilePath: n.config.tlsCertFilePath,
				TLSKeyFilePath:  n.config.tlsKeyFilePath,
				ShutdownTimeout: n.config.shutdownTimeout,
				Database:        midnightserver.NewDatabase(n.db),
				SlotTimer:       n.ledgerState,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to recreate midnight gRPC server: %w", err)
		}
		if err := n.midnightServer.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("restarting midnight gRPC server: %w", err)
		}
	}

	if n.config.storageMode.IsAPI() && n.config.blockfrostPort > 0 {
		listenAddr := net.JoinHostPort(
			n.config.bindAddr,
			strconv.FormatUint(uint64(n.config.blockfrostPort), 10),
		)
		adapter, err := blockfrost.NewNodeAdapter(n.ledgerState, n.mempool)
		if err != nil {
			return fmt.Errorf("recreating blockfrost node adapter: %w", err)
		}
		n.blockfrostAPI = blockfrost.New(
			blockfrost.BlockfrostConfig{
				ListenAddress:      listenAddr,
				CORSAllowedOrigins: n.config.corsAllowedOrigins,
			},
			adapter,
			n.config.logger,
		)
		if err := n.blockfrostAPI.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("restarting blockfrost API: %w", err)
		}
	}

	if n.config.storageMode.IsAPI() && n.config.meshPort > 0 {
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
		listenAddr := net.JoinHostPort(
			n.config.bindAddr,
			strconv.FormatUint(uint64(n.config.meshPort), 10),
		)
		var meshErr error
		n.meshAPI, meshErr = mesh.NewServer(
			mesh.ServerConfig{
				Logger:              n.config.logger,
				LedgerState:         n.ledgerState,
				Database:            mesh.NewMeshDatabase(n.db),
				Chain:               n.ledgerState.Chain(),
				Mempool:             n.mempool,
				ListenAddress:       listenAddr,
				Network:             n.config.network,
				NetworkMagic:        n.config.networkMagic,
				GenesisHash:         genesisHash,
				GenesisStartTimeSec: genesisStartTimeSec,
				CORSAllowedOrigins:  n.config.corsAllowedOrigins,
			},
		)
		if meshErr != nil {
			return fmt.Errorf("recreate mesh API server: %w", meshErr)
		}
		if err := n.meshAPI.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("restarting mesh API: %w", err)
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
		BlobPlugin:           n.config.blobPlugin,
		RunMode:              n.config.runMode,
		MetadataPlugin:       n.config.metadataPlugin,
		MaxConnections:       n.config.DatabaseWorkerPoolConfig.WorkerPoolSize,
		StorageMode:          string(n.config.storageMode),
		Network:              n.config.network,
		StrictUtxoValidation: n.config.strictUtxoValidation,
		CacheConfig: database.CborCacheConfig{
			BlockLRUEntries: n.config.cacheBlockLRUEntries,
			HotUtxoEntries:  n.config.cacheHotUtxoEntries,
			HotTxEntries:    n.config.cacheHotTxEntries,
			HotTxMaxBytes:   n.config.cacheHotTxMaxBytes,
		},
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
		{"core storage", n.reinitializeCoreStorage},
		{"midnight indexer", n.reinitializeMidnightIndexer},
		{"background managers", func() error {
			return n.reinitializeBackgroundManagers(ctx)
		}},
		{"networking core", n.reinitializeNetworkingCore},
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
// serving normally. It still takes liveLifecycleMu, both to serialize
// against a concurrent Restore/Truncate closing n.db out from under it,
// and to match the bark DatabaseService's "only one operation at a time"
// invariant.
func (n *Node) Snapshot(
	ctx context.Context,
	destDir string,
) (lifecycle.Manifest, error) {
	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()

	if n.db == nil {
		return lifecycle.Manifest{}, errors.New(
			"node database is not open",
		)
	}
	return lifecycle.SnapshotToCloud(
		ctx,
		n.db,
		destDir,
		lifecycle.TriggerManual,
		dingoversion.GetVersionString(),
		n.config.databaseLifecycle.SnapshotCloudDestination,
	)
}

// Restore replaces this running node's database with the snapshot at
// snapshotDir, quiescing and reinitializing every storage-dependent
// subsystem in-process (see this file's package comment for exactly what
// stays running and what gets rebuilt). The node is unresponsive to chain
// sync, mempool, and API traffic only for the brief directory-swap window
// below — restoring and validating the snapshot itself happens in a
// staging directory while the node keeps serving normally.
//
// The restored snapshot is fully validated (lifecycle.Restore's own
// manifest/tip checks, plus a check against this node's actual configured
// network/storage-mode/plugins) before this node's real data directory is
// touched at all, so a bad snapshot (corrupted, wrong network, etc.) is
// rejected with the node's existing data completely intact and the node
// still running on it. Only a failure in the swap/reinitialize step
// itself — expected to be rare — brings the node down (n.cancel()) for a
// supervised restart.
func (n *Node) Restore(
	ctx context.Context,
	snapshotDir string,
) (lifecycle.Manifest, error) {
	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()

	stagingDir := n.config.dataDir + ".restore-staging"
	if err := os.RemoveAll(stagingDir); err != nil {
		return lifecycle.Manifest{}, fmt.Errorf(
			"clear restore staging directory: %w", err,
		)
	}

	manifest, err := lifecycle.Restore(ctx, snapshotDir, stagingDir)
	if err != nil {
		return lifecycle.Manifest{}, fmt.Errorf("restore: %w", err)
	}
	if err := n.validateRestoredAgainstNodeConfig(stagingDir); err != nil {
		_ = os.RemoveAll(stagingDir)
		return lifecycle.Manifest{}, fmt.Errorf(
			"validate restored snapshot against node configuration: %w",
			err,
		)
	}

	// Everything about the restored snapshot is now validated — both
	// internally (lifecycle.Restore, against its own manifest) and against
	// this node's actual configuration — while this node's real data
	// directory was never touched and the node kept serving normally
	// throughout. Only the brief directory swap below requires quiescing.
	if err := n.quiesceForLiveLifecycleOp(ctx); err != nil {
		_ = os.RemoveAll(stagingDir)
		return lifecycle.Manifest{}, fmt.Errorf("quiesce: %w", err)
	}
	if err := n.closeStorageForLiveLifecycleOp(ctx); err != nil {
		_ = os.RemoveAll(stagingDir)
		return lifecycle.Manifest{}, fmt.Errorf("close storage: %w", err)
	}

	if err := n.swapInRestoredDataDir(stagingDir); err != nil {
		if errors.Is(err, errRestoreSwapUnrecoverable) {
			n.cancel()
			return lifecycle.Manifest{}, err
		}
		// The original data directory is intact (or was successfully
		// rolled back) — bring the node back up on it rather than leaving
		// it down over a failed swap.
		if resumeErr := n.reinitializeAndResume(ctx); resumeErr != nil {
			return lifecycle.Manifest{}, fmt.Errorf(
				"%w (resume also failed: %w)", err, resumeErr,
			)
		}
		return lifecycle.Manifest{}, err
	}

	if err := n.reinitializeAndResume(ctx); err != nil {
		return lifecycle.Manifest{}, err
	}
	return manifest, nil
}

// validateRestoredAgainstNodeConfig opens the just-restored snapshot at
// stagingDir with this node's actual configuration (network, storage mode,
// plugins) rather than the manifest's own recorded values, so a mismatch
// against what this node is really configured to run — e.g. a devnet
// snapshot restored onto a preview-configured node — is caught here, before
// this node's real data directory has been touched, instead of surfacing
// later during reinitializeCoreStorage after the swap has already happened.
//
// This runs before quiesce, while the live n.db is still open and
// registered under n.config.promRegistry — so the temporary handle opened
// here must not share that registry, or its metrics registration collides
// with n.db's own (unlike Truncate's tmpDB, which only opens after
// closeStorageForLiveLifecycleOp has already unregistered the old db's
// collectors).
func (n *Node) validateRestoredAgainstNodeConfig(stagingDir string) error {
	cfg := n.databaseConfig()
	cfg.DataDir = stagingDir
	cfg.PromRegistry = nil
	db, err := database.New(cfg)
	if db != nil {
		defer db.Close()
	}
	if err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	return nil
}

// errRestoreSwapUnrecoverable marks a swapInRestoredDataDir failure where
// neither the restored data nor the original data ended up at
// n.config.dataDir — the only case where Restore has no choice but to
// bring the node down rather than resume on whatever is in place.
var errRestoreSwapUnrecoverable = errors.New(
	"data directory swap left no usable data directory in place",
)

// swapInRestoredDataDir atomically replaces n.config.dataDir with
// stagingDir (both must be on the same filesystem for the renames to be
// atomic), keeping the original data around as a same-directory backup
// until the swap fully succeeds, and rolling back to it if activating the
// restored directory fails.
func (n *Node) swapInRestoredDataDir(stagingDir string) error {
	backupDir := n.config.dataDir + ".pre-restore"
	_ = os.RemoveAll(backupDir)
	if err := os.Rename(n.config.dataDir, backupDir); err != nil {
		return fmt.Errorf("move aside current data directory: %w", err)
	}
	if err := os.Rename(stagingDir, n.config.dataDir); err != nil {
		if rbErr := os.Rename(backupDir, n.config.dataDir); rbErr != nil {
			return fmt.Errorf(
				"%w: activate restored data directory: %w (rollback also failed: %w; restored data preserved at %q)",
				errRestoreSwapUnrecoverable, err, rbErr, stagingDir,
			)
		}
		return fmt.Errorf("activate restored data directory: %w", err)
	}
	_ = os.RemoveAll(backupDir)
	return nil
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

	if err := n.quiesceForLiveLifecycleOp(ctx); err != nil {
		return 0, fmt.Errorf("quiesce: %w", err)
	}
	if err := n.closeStorageForLiveLifecycleOp(ctx); err != nil {
		return 0, fmt.Errorf("close storage: %w", err)
	}

	blocksRemoved, truncateErr := func() (uint64, error) {
		tmpDB, err := database.New(n.databaseConfig())
		if err != nil {
			return 0, fmt.Errorf(
				"%w: open database for truncate: %w",
				lifecycle.ErrTruncateNotStarted, err,
			)
		}
		defer tmpDB.Close()

		block, err := dblifecycle.ResolveTarget(tmpDB, target)
		if err != nil {
			return 0, fmt.Errorf(
				"%w: %w", lifecycle.ErrTruncateNotStarted, err,
			)
		}
		return lifecycle.Truncate(ctx, tmpDB, block, 0)
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
		if resumeErr := n.reinitializeAndResume(ctx); resumeErr != nil {
			return 0, fmt.Errorf(
				"%w (resume also failed: %w)", truncateErr, resumeErr,
			)
		}
		return 0, fmt.Errorf("truncate: %w", truncateErr)
	}

	if err := n.reinitializeAndResume(ctx); err != nil {
		return 0, err
	}
	return blocksRemoved, nil
}

// Compile-time assertion that *Node satisfies dblifecycle.LiveNode (see
// that interface's doc comment for why it's defined there, not imported
// from here).
var _ dblifecycle.LiveNode = (*Node)(nil)
