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

	"github.com/blinklabs-io/dingo/api/mesh"
	"github.com/blinklabs-io/dingo/api/utxorpc"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiready"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/storageadopt"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// errNoSlotClock is returned by the writer's slot clock before the ledger
// state exists; the lag metric then counts one second per slot.
var errNoSlotClock = errors.New("ledger slot clock is not available")

// indexerSlotTime converts a slot to wall-clock time for the writer's lag in
// seconds. It reads n.ledgerState on each call because the writer is built
// before the ledger state is (re)created.
func (n *Node) indexerSlotTime(slot uint64) (time.Time, error) {
	ls := n.ledgerState
	if ls == nil {
		return time.Time{}, errNoSlotClock
	}
	return ls.SlotToTime(slot)
}

// apiDatabase is the database API readers use: the IndexerDB on an API node,
// the consensus database on a core node.
func (n *Node) apiDatabase() *database.Database {
	if n.indexerDB != nil {
		return n.indexerDB
	}
	return n.db
}

// apiReadiness gates IndexerDB-backed API routes and is nil, serving
// everything, on a core node, which has no IndexerDB.
func (n *Node) apiReadiness() apiready.Check {
	store := n.indexerStore
	if store == nil {
		return nil
	}
	return func(kind indexerdb.Kind) (bool, error) {
		return store.ReadyServing(kind, n.ledgerState.Tip().BlockNumber)
	}
}

// apiStorageMode is the storage mode the API servers and indexers gate on.
// The consensus store always runs in core mode; an API node serves the APIs
// from its IndexerDB.
func (n *Node) apiStorageMode() StorageMode {
	if n.splitIndexer() {
		return StorageModeAPI
	}
	return StorageModeCore
}

func (n *Node) splitIndexer() bool {
	return n.config.splitIndexer
}

// indexerLayout is the layout recorded in the database's node settings.
func (n *Node) indexerLayout() string {
	if n.storageSelections().SplitIndexer {
		return internalconfig.IndexerLayoutSplit
	}
	return internalconfig.IndexerLayoutShared
}

// openIndexerWriter opens the IndexerDB beside the consensus database and
// builds the writer that fills it. It does nothing on a core node.
func (n *Node) openIndexerWriter(ctx context.Context) error {
	if !n.splitIndexer() {
		return nil
	}
	// A quiesce that failed without closing storage resumes through here
	// with the previous IndexerDB and writer still held. They read the
	// ledger database being replaced, and a second IndexerDB over the same
	// store would write beside them.
	if err := n.stopIndexerWriter(ctx); err != nil {
		return err
	}
	if err := n.closeIndexerDatabase(); err != nil {
		return err
	}
	db, writer, err := n.buildIndexerWriter(ctx, n.pluginHost, n.db)
	if err != nil {
		return err
	}
	store, err := indexerdb.NewStore(db, db, n.config.indexerMaxReadyLagBlocks)
	if err != nil {
		return errors.Join(err, writer.Stop(ctx), db.Close())
	}
	n.warnMainnetIndexerCapacity(store, localIndexerCapacity)
	n.indexerDB, n.indexerWriter, n.indexerStore = db, writer, store
	n.db.SetRolledBackBlockHold(rolledBackBlockHold(store))
	return nil
}

// rolledBackBlockHold reports the IndexerDB's tip as the block the ledger's
// rolled-back bodies are held for.
func rolledBackBlockHold(store *indexerdb.Store) database.RolledBackBlockHold {
	return func() (ocommon.Point, bool, error) {
		tip, err := store.Tip(nil)
		if err != nil {
			return ocommon.Point{}, false, err
		}
		if tip.IsZero() {
			return ocommon.Point{}, false, nil
		}
		return ocommon.NewPoint(tip.Slot, tip.Hash), true, nil
	}
}

// releaseRolledBackBlocks lets the ledger delete the rolled-back bodies the
// IndexerDB no longer needs. The writer calls it from its own goroutine, so
// the release runs on one of its own, at most one at a time: a pass that
// finds one running leaves its work to that run's successor.
func (n *Node) releaseRolledBackBlocks() {
	ledgerState := n.ledgerState
	if ledgerState == nil || !n.releasingBlocks.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer n.releasingBlocks.Store(false)
		ledgerState.ReleaseRolledBackBlocks(context.Background())
	}()
}

// buildIndexerWriter opens an IndexerDB over consensus, with its metadata
// store on host, and builds the writer for it. The caller closes the returned
// database before host stops and before consensus closes.
func (n *Node) buildIndexerWriter(
	ctx context.Context,
	host *plugin.Host,
	consensus *database.Database,
) (*database.Database, IndexerWriter, error) {
	if n.config.indexerWriterFactory == nil {
		return nil, nil, errors.New(
			"API storage mode has no indexer writer configured",
		)
	}
	deps, err := n.storageDependencies(n.config.dataDir)
	if err != nil {
		return nil, nil, err
	}
	deps.PromRegistry = n.config.promRegistry
	selections := n.storageSelections()
	db, err := internalplugins.OpenIndexerDatabase(
		ctx, host, consensus, n.databaseConfig(), selections, deps,
	)
	if err != nil {
		return nil, nil, err
	}
	db.SetLiveOutputs(nodeLiveOutputs{node: n})
	var factRows metadata.LedgerFacts
	if n.ledgerState != nil && consensus == n.db {
		factRows = n.ledgerState
	}
	writer, err := n.config.indexerWriterFactory(IndexerWriterDeps{
		Consensus:                  consensus,
		FactRows:                   factRows,
		Indexer:                    db,
		NodeConfig:                 n.config.cardanoNodeConfig,
		Logger:                     n.config.logger,
		PromRegistry:               n.config.promRegistry,
		SlotTime:                   n.indexerSlotTime,
		MaxReadyLagBlocks:          n.config.indexerMaxReadyLagBlocks,
		DelegatorInactivityEnabled: n.config.delegatorInactivityEnabled,
		Moved:                      n.releaseRolledBackBlocks,
	})
	if err != nil {
		return nil, nil, errors.Join(
			fmt.Errorf("build indexer writer: %w", err),
			db.Close(),
		)
	}
	return db, writer, nil
}

// nodeLiveOutputs is the IndexerDB's read-only view of the ledger's live UTxO
// set, followed across a live close and reopen of the set.
type nodeLiveOutputs struct{ node *Node }

// LiveUtxoCbor implements types.LiveOutputReader.
func (l nodeLiveOutputs) LiveUtxoCbor(
	_ types.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	set := l.node.anchoredUtxoSet.Load()
	if set == nil {
		return nil, false, nil
	}
	return set.Get(txID, index)
}

// rollbackIndexerForTruncate rolls the IndexerDB back to the block a ledger
// truncate keeps, over the ledger database ledgerDB that still stores the
// blocks being removed so the writer finds exact parents. It runs before the
// ledger truncates: a crash between the two leaves the IndexerDB tip at or
// below the ledger's, and the writer indexes the difference again. It
// does nothing on a core node.
func (n *Node) rollbackIndexerForTruncate(
	ctx context.Context,
	ledgerDB *database.Database,
	target models.Block,
	opts ...lifecycle.TruncateOption,
) error {
	if !n.splitIndexer() {
		return nil
	}
	// The ledger's refusals, including the live UTxO set's, run first: a
	// refused truncate must leave the IndexerDB where it is.
	if err := lifecycle.CheckTruncate(ctx, ledgerDB, target, opts...); err != nil {
		return err
	}
	host, err := internalplugins.NewHost()
	if err != nil {
		return fmt.Errorf("build storage plugin host for indexer: %w", err)
	}
	defer host.Stop(context.WithoutCancel(ctx)) //nolint:errcheck
	db, writer, err := n.buildIndexerWriter(ctx, host, ledgerDB)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck
	if err := writer.RollbackTo(ctx, indexerdb.Tip{
		Slot:        target.Slot,
		BlockNumber: target.Number,
		Hash:        target.Hash,
	}); err != nil {
		return fmt.Errorf("roll back indexer: %w", err)
	}
	return lifecycle.TruncateMidnightJournals(
		ctx, db, ocommon.NewPoint(target.Slot, target.Hash), target.Number,
	)
}

// startIndexerWriter starts the writer and wakes it on ledger tip and reward
// fact events. It runs after the ledger started so the writer sees the tip the
// ledger recovered to.
func (n *Node) startIndexerWriter(ctx context.Context) error {
	if n.indexerWriter == nil {
		return nil
	}
	if err := n.indexerWriter.Start(ctx); err != nil {
		return fmt.Errorf("start indexer writer: %w", err)
	}
	// The writer is captured: a tip event delivered while stop clears the
	// field must not dereference it.
	writer := n.indexerWriter
	n.indexerWakeSub = n.eventBus.SubscribeFunc(
		ledger.TipEventType,
		func(event.Event) { writer.Wake() },
	)
	n.indexerFactsSub = n.eventBus.SubscribeFunc(
		ledger.RewardFactsEventType,
		func(event.Event) { writer.Wake() },
	)
	writer.Wake()
	return nil
}

// stopIndexerWriter stops waking and running the writer and then closes the
// IndexerDB, which must precede the consensus database closing the blob store
// the IndexerDB reads.
func (n *Node) stopIndexerWriter(ctx context.Context) error {
	if n.indexerWriter == nil {
		return nil
	}
	if n.indexerWakeSub != 0 {
		n.eventBus.Unsubscribe(ledger.TipEventType, n.indexerWakeSub)
		n.indexerWakeSub = 0
	}
	if n.indexerFactsSub != 0 {
		n.eventBus.Unsubscribe(ledger.RewardFactsEventType, n.indexerFactsSub)
		n.indexerFactsSub = 0
	}
	var err error
	if stopErr := n.indexerWriter.Stop(ctx); stopErr != nil {
		err = fmt.Errorf("indexer writer shutdown: %w", stopErr)
	}
	n.indexerWriter = nil
	return err
}

func (n *Node) closeIndexerDatabase() error {
	if n.indexerDB == nil {
		return nil
	}
	n.db.SetRolledBackBlockHold(nil)
	db := n.indexerDB
	n.indexerDB = nil
	n.indexerStore = nil
	if err := db.Close(); err != nil {
		return fmt.Errorf("indexer database close: %w", err)
	}
	return nil
}

// meshLedgerState is the ledger state the Mesh server reads. Its account
// endpoints read the UTxO set from the IndexerDB, which holds it for the
// APIs, and not from the consensus store, which keeps only what validation
// needs.
func (n *Node) meshLedgerState() mesh.MeshLedgerState {
	return &indexerMeshLedger{
		LedgerState: n.ledgerState,
		db:          n.indexerDB,
	}
}

// meshChain is the chain whose tip the Mesh server reports: the IndexerDB's
// tip, since the ledger's tip is ahead of the rows the Mesh answers are built
// from.
func (n *Node) meshChain() mesh.MeshChain {
	return &indexerMeshChain{store: n.indexerStore}
}

// meshDatabase is the block and transaction store the Mesh server reads.
// Blocks above the IndexerDB's tip are hidden, since their transactions are
// not indexed yet.
func (n *Node) meshDatabase() mesh.MeshDatabase {
	db := mesh.NewMeshDatabase(n.indexerDB)
	return mesh.NewBoundedDatabase(db, (&indexerMeshChain{
		store: n.indexerStore,
	}).tip)
}

// indexerMeshChain reports the IndexerDB's tip as the Mesh chain tip.
type indexerMeshChain struct {
	store *indexerdb.Store
}

func (c *indexerMeshChain) tip() (ochainsync.Tip, error) {
	tip, err := c.store.Tip(nil)
	if err != nil {
		return ochainsync.Tip{}, err
	}
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(tip.Slot, tip.Hash),
		BlockNumber: tip.BlockNumber,
	}, nil
}

// Tip implements mesh.MeshChain. A tip that cannot be read is the zero tip;
// the readiness gate refuses the routes that would report it.
func (c *indexerMeshChain) Tip() ochainsync.Tip {
	tip, _ := c.tip()
	return tip
}

// indexerMeshLedger answers the Mesh UTxO reads from the IndexerDB and leaves
// the rest to the ledger state.
type indexerMeshLedger struct {
	*ledger.LedgerState
	db *database.Database
}

func (m *indexerMeshLedger) UtxosByAddress(
	ctx context.Context,
	addrs []lcommon.Address,
) ([]models.Utxo, error) {
	return m.db.UtxosByAddress(ctx, addrs, 0, nil)
}

func (m *indexerMeshLedger) ScanUtxosByAddress(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
	visit func([]models.UtxoWithOrdering) error,
) (ochainsync.Tip, error) {
	return m.db.ScanUtxosByAddress(ctx, q, maxCandidates, maxBytes, visit)
}

func (m *indexerMeshLedger) UtxoBalanceByAddressAtSlot(
	ctx context.Context,
	addr lcommon.Address,
	slot uint64,
) (models.AddressBalance, error) {
	txn := database.NewTxnContext(ctx, m.db, false)
	defer txn.Release()
	return m.db.UtxoBalanceByAddressAtSlot(ctx, addr, slot, txn)
}

// utxorpcQueryDatabase is the store UTxO RPC queries read: the IndexerDB.
func (n *Node) utxorpcQueryDatabase() utxorpc.QueryDatabase {
	return utxorpc.NewIndexerQueryDatabase(
		n.indexerDB, n.indexerStore, n.apiReadiness(),
	)
}

// checkServerAdopted refuses a PostgreSQL or MySQL API store a release
// without the split layout wrote and startup has not adopted. The dingo
// command adopts it before any store opens; Run alone does not.
func (n *Node) checkServerAdopted(
	ctx context.Context,
	deps internalplugins.StorageDependencies,
) (err error) {
	selection := n.config.pluginSelections[plugin.CapabilityStorageMetadata]
	if selection.Provider != "postgres" && selection.Provider != "mysql" {
		return nil
	}
	host, err := internalplugins.NewHost()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, host.Stop(context.WithoutCancel(ctx)))
	}()
	return storageadopt.CheckServerAdopted(
		ctx, host, selection, deps.DataDir, deps.GenesisStakingEmpty,
	)
}
