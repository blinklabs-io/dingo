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

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/historyexpiry"
	"github.com/blinklabs-io/dingo/internal/node/ledgerpeers"
	"github.com/blinklabs-io/dingo/internal/offchainmetadata"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	midnightserver "github.com/blinklabs-io/dingo/midnight/server"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

func (n *Node) databaseConfig() *database.Config {
	return &database.Config{
		DataDir:              n.config.dataDir,
		Logger:               n.config.logger,
		PromRegistry:         n.config.promRegistry,
		StorageMode:          string(n.config.storageMode),
		IndexerLayout:        n.indexerLayout(),
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
		AlonzoLovelacePerUtxoWord: cardano.AlonzoLovelacePerUtxoWord(
			n.config.cardanoNodeConfig, "", n.config.network,
		),
		Immutable: n.immutableConfig(),
	}
}

func (n *Node) newConnectionManager() *connmanager.ConnectionManager {
	return connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:   n.config.logger,
			EventBus: n.eventBus,
			ListenersProvider: func() []connmanager.ListenerConfig {
				return n.ouroboros().
					ConfigureListeners(n.ctx, n.config.listeners)
			},
			OutboundSourcePort: n.config.outboundSourcePort,
			OutboundConnOptsProvider: func() []ouroboros.ConnectionOptionFunc {
				return n.ouroboros().OutboundConnOpts(n.ctx)
			},
			PromRegistry:            n.config.promRegistry,
			MaxConnectionsPerIP:     n.config.maxConnectionsPerIP,
			MaxInboundConns:         n.config.maxInboundConns,
			MaxNtCConns:             n.config.maxNtCConns,
			MaxNtCConnectionsPerIP:  n.config.maxNtCConnectionsPerIP,
			MaxTrustedLocalNtCConns: n.config.maxTrustedLocalNtCConns,
			TrustedNtCIdleTimeout:   n.config.trustedNtCIdleTimeout,
			ConnClosedOwnerFunc:     n.handleConnManagerClosedOwner,
		},
	)
}

func (n *Node) newPeerGovernor() *peergov.PeerGovernor {
	ledgerPeerProvider := ledgerpeers.NewProvider(n.poolRelayProvider)

	var useLedgerAfterSlot int64 = -1
	if n.config.topologyConfig != nil {
		useLedgerAfterSlot = n.config.topologyConfig.UseLedgerAfterSlot
	}

	peerGovConfig := peergov.PeerGovernorConfig{
		ControlGroups:   n.genesisControlGroups(),
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
	return peergov.NewPeerGovernor(peerGovConfig)
}

func (n *Node) newHistoryExpiryPruner() *historyexpiry.Pruner {
	return historyexpiry.NewPruner(n.newHistoryExpiryPrunerConfig())
}

func (n *Node) newHistoryExpiryPrunerConfig() historyexpiry.PrunerConfig {
	prunerFreq := n.config.historyExpiry.Frequency
	if prunerFreq <= 0 {
		prunerFreq = time.Hour
	}
	return historyexpiry.PrunerConfig{
		LedgerState: n.ledgerState,
		DB:          n.db,
		Logger:      n.config.logger,
		Frequency:   prunerFreq,
		Ceiling:     n.historyExpiryCeiling,
		Held:        n.historyExpiryHeld,
	}
}

// historyExpiryHeld keeps a block while a payload column of the IndexerDB that
// is still in use names its bytes: a datum or script a live output carries or
// names by hash, a pending protocol parameter update, an unspent Midnight
// registration. Without an IndexerDB no column names a block.
func (n *Node) historyExpiryHeld(
	ctx context.Context,
	slot uint64,
	hash []byte,
) (bool, error) {
	if n.indexerDB == nil || n.ledgerState == nil {
		return false, nil
	}
	return n.indexerDB.BlockHoldsActivePayloads(
		ctx, slot, hash, n.ledgerState.CurrentEpoch(),
	)
}

// historyExpiryCeiling keeps history expiry behind the IndexerDB writer,
// which reads block bodies from the consensus store: it expires nothing the
// writer has not indexed, and keeps the writer's tip block, which the writer
// reads again to resume. Without an IndexerDB nothing bounds it.
func (n *Node) historyExpiryCeiling(context.Context) (uint64, bool, error) {
	store := n.indexerStore
	if store == nil {
		return ^uint64(0), true, nil
	}
	tip, err := store.Tip(nil)
	if err != nil {
		return 0, false, err
	}
	if tip.IsZero() || tip.Slot == 0 {
		return 0, false, nil
	}
	return tip.Slot - 1, true, nil
}

func (n *Node) newDatabaseLifecycleManager() *dblifecycle.Manager {
	mgr := dblifecycle.NewManager(
		n.db,
		n.eventBus,
		n.config.databaseLifecycle,
		n.config.pluginSelections[plugin.CapabilityStorageBlob].Provider,
		n.config.pluginSelections[plugin.CapabilityStorageMetadata].Provider,
		n.destinationRegistry,
		n.config.logger,
	)
	mgr.SetUtxoSetSource(n.utxoSetBackuper)
	mgr.SetIndexerPayloadSource(n.indexerPayloadSource())
	return mgr
}

func (n *Node) midnightServerConfig() midnightserver.Config {
	store := n.indexerStore
	midnightDB := midnightserver.NewBoundedDatabase(
		midnightserver.NewDatabase(n.indexerDB),
		func(context.Context) (uint64, uint64, bool, error) {
			tip, err := store.Tip(nil)
			if err != nil {
				return 0, 0, false, err
			}
			return tip.Slot, tip.BlockNumber, !tip.IsZero(), nil
		},
	)
	return midnightserver.Config{
		Logger:   n.config.logger,
		Metadata: n.indexerDB.Metadata(),
		BlockNumberByHash: func(ctx context.Context, hash []byte) (uint64, bool, error) {
			block, err := midnightDB.BlockByHash(ctx, hash)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					return 0, false, nil
				}
				// The Midnight server names the request step.
				return 0, false, err
			}
			return block.Number, true, nil
		},
		Host:              n.config.midnight.Host,
		Port:              n.config.midnight.Port,
		TLSCertFilePath:   n.config.tlsCertFilePath,
		TLSKeyFilePath:    n.config.tlsKeyFilePath,
		ReflectionEnabled: n.config.midnight.ReflectionEnabled,
		ShutdownTimeout:   n.config.shutdownTimeout,
		Database:          midnightDB,
		SlotTimer:         n.ledgerState,
		PromRegistry:      n.config.promRegistry,
	}
}

func (n *Node) offchainMetadataConfig() offchainmetadata.Config {
	return offchainmetadata.Config{
		Logger:                n.config.logger,
		Store:                 n.apiDatabase().Metadata(),
		HTTPClient:            n.config.offchainMetadata.HTTPClient,
		Interval:              n.config.offchainMetadata.Interval,
		RequestTimeout:        n.config.offchainMetadata.RequestTimeout,
		UserAgent:             n.config.offchainMetadata.UserAgent,
		IPFSGatewayURL:        n.config.offchainMetadata.IPFSGatewayURL,
		BatchSize:             n.config.offchainMetadata.BatchSize,
		MaxBytes:              n.config.offchainMetadata.MaxBytes,
		AllowPrivateAddresses: n.config.offchainMetadata.AllowPrivateAddresses,
	}
}

func (n *Node) configureSnapshotManager() error {
	// The snapshot manager writes through the ledger's LedgerDB, so a live
	// UTxO set that keeps epoch state gets its snapshot rows too.
	var ldb ledgerdb.LedgerDB = dbadapter.New(n.db)
	if n.ledgerState != nil {
		ldb = n.ledgerState.LedgerDB()
	}
	n.snapshotMgr = snapshot.NewManager(
		ldb,
		n.eventBus,
		n.config.logger,
	)
	// Mirror the CIP-0163 reward-account inactivity gate into snapshot capture
	// so it matches the ledger config that drives account expiry stamping.
	if err := n.snapshotMgr.SetDelegatorInactivity(
		n.config.delegatorInactivityEnabled,
		n.config.delegatorInactivity,
	); err != nil {
		return fmt.Errorf("configuring snapshot manager: %w", err)
	}
	n.snapshotMgr.SetPromRegistry(n.config.promRegistry)
	// Prune pool snapshots through the deferred-header retention guard, so a
	// snapshot a queued/deferred header still needs for leader validation is
	// never pruned out from under it and misread as pool absence, and the
	// floor selection is atomic with deferred-header admission.
	// Set before Start; the pin is released automatically as headers resolve.
	n.snapshotMgr.SetPoolSnapshotRetentionGuard(
		n.ledgerState.PrunePoolSnapshotsWithRetentionFloor,
	)
	// Wire the authoritative epoch-boundary capture before block sync begins so
	// each epoch rollover stages its mark snapshot atomically at the SNAP point.
	// Set before CaptureGenesisSnapshot/sync; with a nil hook (never set) no
	// mark snapshot is captured.
	// The stake read runs at the SNAP point (after MIR and before POOLREAP/
	// enactment) and the row write at the end of the rollover, both inside the
	// same transaction.
	n.ledgerState.SetEpochBoundarySnapshotStakeHook(
		func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
			return n.snapshotMgr.ComputeEpochBoundarySnapshot(n.ctx, txn, evt)
		},
	)
	n.ledgerState.SetEpochBoundarySnapshotHook(
		func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
			return n.snapshotMgr.CaptureEpochBoundarySnapshot(n.ctx, txn, evt)
		},
	)
	wireDeferredRewardStakeInputs(n.ledgerState, n.snapshotMgr)
	// Wire governance's same-boundary SPO stake read: RATIFY
	// tallies mark[NewEpoch] -- this same boundary's own mark snapshot -- but
	// that row is not durably written until the hook above runs, later in
	// the same rollover. Without this, governance would silently see zero
	// SPO stake for every SPO-gated action at every boundary.
	n.ledgerState.SetCurrentBoundarySPOStakeHook(
		func(
			txn ledgerdb.Txn,
			evt event.EpochTransitionEvent,
			incomingEraID uint,
		) ([]*models.PoolStakeSnapshot, error) {
			return n.snapshotMgr.CurrentBoundarySPOStakeRows(
				n.ctx, txn, evt, incomingEraID,
			)
		},
	)
	return nil
}
