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

package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	datatypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	fxcbor "github.com/fxamacker/cbor/v2"
)

// Mainnet full blocks can overflow practical Badger transaction limits with
// larger import batches, so keep the runtime load batch size aligned with the
// chain import batch cap.
const (
	loadBlockBatchSize    = 50
	chunkRetirementStride = 64
	progressLogInterval   = 10 * time.Second
	loadDecodeMinWorkers  = 2
	loadDecodeMaxWorkers  = 8
)

// loadDecodeWorkerCount bounds the parallel full-block decoder used by the
// trusted Mithril load path. Chain insertion and ledger replay remain serial;
// only the CPU-heavy CBOR decode is parallelized.
func loadDecodeWorkerCount() int {
	n := runtime.GOMAXPROCS(0)
	if n < loadDecodeMinWorkers {
		return loadDecodeMinWorkers
	}
	if n > loadDecodeMaxWorkers {
		return loadDecodeMaxWorkers
	}
	return n
}

// newLedgerStateForLoad is replaceable in tests so load-mode composition can
// be verified without replaying a full ImmutableDB fixture.
var newLedgerStateForLoad = ledger.NewLedgerState

var processTrustedBlockBatchesForLoad = func(
	ls *ledger.LedgerState,
	ctx context.Context,
	batches <-chan []gledger.Block,
) error {
	return ls.ProcessTrustedBlockBatches(ctx, batches)
}

type loadSecurityParam int

func (k loadSecurityParam) SecurityParam() int {
	return int(k)
}

func configureLoadChainSecurityParam(
	cm *chain.ChainManager,
	nodeCfg *cardano.CardanoNodeConfig,
) error {
	k, err := loadSecurityParamForConfig(nodeCfg)
	if err != nil {
		return fmt.Errorf(
			"failed to configure chain security parameter for load: %w",
			err,
		)
	}
	if err := cm.SetLedger(loadSecurityParam(k)); err != nil {
		return fmt.Errorf(
			"failed to configure chain security parameter for load: %w",
			err,
		)
	}
	return nil
}

// loadSecurityParamForConfig validates the raw genesis K values that replay
// can reach and returns the one for the era where replay starts. It must not
// use LedgerState.SecurityParam: before Start that method samples the
// zero-value Byron era and intentionally substitutes its runtime fallback for
// unavailable or invalid values.
func loadSecurityParamForConfig(
	nodeCfg *cardano.CardanoNodeConfig,
) (int, error) {
	if nodeCfg == nil {
		return 0, fmt.Errorf(
			"%w: cardano node config is required",
			chain.ErrInvalidSecurityParam,
		)
	}

	shelleyGenesis := nodeCfg.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0, fmt.Errorf(
			"%w: Shelley genesis is required",
			chain.ErrInvalidSecurityParam,
		)
	}
	if shelleyGenesis.SecurityParam <= 0 {
		return 0, fmt.Errorf(
			"%w: Shelley security parameter K must be positive: got %d",
			chain.ErrInvalidSecurityParam,
			shelleyGenesis.SecurityParam,
		)
	}

	shelleyAtGenesis := false
	if epoch, declared := nodeCfg.DeclaredHardForkEpoch("shelley"); declared {
		shelleyAtGenesis = epoch == 0
	}
	byronGenesis := nodeCfg.ByronGenesis()
	if byronGenesis == nil || shelleyAtGenesis {
		return shelleyGenesis.SecurityParam, nil
	}
	if byronGenesis.ProtocolConsts.K <= 0 {
		return 0, fmt.Errorf(
			"%w: Byron security parameter K must be positive: got %d",
			chain.ErrInvalidSecurityParam,
			byronGenesis.ProtocolConsts.K,
		)
	}
	return byronGenesis.ProtocolConsts.K, nil
}

// installEpochBoundarySnapshotHookForLoad is replaceable in tests so load-mode
// composition can verify the hook is installed without starting ledger workers.
var installEpochBoundarySnapshotHookForLoad = func(
	ls *ledger.LedgerState,
	fn func(ledgerdb.Txn, event.EpochTransitionEvent) error,
) error {
	ls.SetEpochBoundarySnapshotHook(fn)
	return nil
}

// loadCaptureFailureTracker records authoritative epoch-boundary snapshot
// capture failures so `dingo load` can surface them after replay.
//
// The boundary capture is the only capture of an epoch's mark/reward
// snapshot, and a failed one fails the epoch rollover. This tracker names
// the epochs whose capture failed, so load reports them rather than only the
// rollover error.
type loadCaptureFailureTracker struct {
	mu     sync.Mutex
	first  error
	epochs []uint64
}

// record notes a failed capture for the given epoch. The first error is kept as
// the representative cause; every failed epoch is accumulated for reporting.
func (t *loadCaptureFailureTracker) record(epoch uint64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.first == nil {
		t.first = err
	}
	t.epochs = append(t.epochs, epoch)
}

// err returns a wrapped error naming every epoch whose authoritative snapshot
// capture failed, or nil if all captures succeeded.
func (t *loadCaptureFailureTracker) err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.first == nil {
		return nil
	}
	return fmt.Errorf(
		"epoch-boundary snapshot capture failed for epoch(s) %v during load; "+
			"no other capture writes them, so the database is "+
			"missing mark/reward snapshots and must be re-imported: %w",
		t.epochs, t.first,
	)
}

func Load(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	immutableDir string,
) error {
	return LoadWithDB(ctx, cfg, logger, immutableDir, nil)
}

// loadIndexerLayout is the layout a load freezes into a new database: the
// split layout's gate must be filled while the database has no chain state, or
// the node refuses to start over the loaded blocks.
func loadIndexerLayout(cfg *config.Config) string {
	if cfg.SplitIndexer() || cfg.ServerCoreNamespace {
		return config.IndexerLayoutSplit
	}
	return ""
}

// ensureDB returns the provided database or opens a new one from cfg.
// The returned cleanup function must be deferred by the caller; it closes
// the database only when a new one was created.
func ensureDB(
	cfg *config.Config,
	logger *slog.Logger,
	db *database.Database,
	nodeCfg *cardano.CardanoNodeConfig,
) (*database.Database, func(), error) {
	if db != nil {
		return db, func() {}, nil
	}
	genesisStakingEmpty, err := internalplugins.ResolveGenesisStakingEmpty(
		nodeCfg, cfg.CardanoConfig, cfg.Network, cfg.NetworkMagic,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("loading cardano node config: %w", err)
	}
	dbConfig := &database.Config{
		DataDir: cfg.DatabasePath, Logger: logger,
		StorageMode:    cfg.ConsensusStorageMode(),
		IndexerLayout:  loadIndexerLayout(cfg),
		Network:        cfg.Network,
		NetworkMagic:   cfg.NetworkMagic,
		StartEra:       string(cfg.StartEra),
		BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
		AlonzoLovelacePerUtxoWord: cardano.AlonzoLovelacePerUtxoWord(
			nil, cfg.CardanoConfig, cfg.Network,
		),
	}
	// A blob-only copy resolves no node config and so has no chunk geometry
	// to give a new database; a recorded ImmutableDB state installs its read
	// path regardless.
	if nodeCfg != nil {
		dbConfig.Immutable = nodestorage.ImmutableConfig(
			cfg.HistoryExpiry.Enabled, nodeCfg, cfg.ImmutableDB.PromoteBatch,
		)
	}
	runtime, err := internalplugins.OpenDatabase(
		context.Background(),
		dbConfig,
		internalplugins.ConfiguredStorageSelections(cfg),
		internalplugins.StorageDependencies{
			DataDir: cfg.DatabasePath, RunMode: string(cfg.RunMode),
			StorageMode:    cfg.ConsensusStorageMode(),
			MaxConnections: cfg.DatabaseWorkers,
			Logger:         logger, TracingEnabled: cfg.Tracing,
			GenesisStakingEmpty: genesisStakingEmpty,
			MigrateReleased:     nodestorage.MigrateReleased(cfg, dbConfig, nodeCfg, logger),
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating database: %w", err)
	}
	if runtime == nil || runtime.Database == nil {
		return nil, nil, errors.New(
			"creating database: runtime did not provide a database",
		)
	}
	newDB := runtime.Database
	cleanup := func() {
		_ = runtime.Close(context.Background())
	}
	return newDB, cleanup, nil
}

// captureLoadGenesisSnapshot captures the genesis (epoch 0) mark stake
// snapshot during a `dingo load` replay, mirroring the guard node.go applies
// around the equivalent call (see handleGenesisSnapshotError): a block
// producer cannot elect leaders without this snapshot, so a capture failure
// is fatal, while a relay or replay-only load only warns and continues.
func captureLoadGenesisSnapshot(
	ctx context.Context,
	snapshotMgr *snapshot.Manager,
	cfg *config.Config,
	logger *slog.Logger,
) error {
	return snapshot.HandleGenesisSnapshotError(
		cfg.BlockProducer,
		logger,
		snapshotMgr.CaptureGenesisSnapshot(ctx),
	)
}

// DeferredIndexRebuilder owns the drop/rebuild pairing for a single
// bulk-load run: the IndexerDB writer drops the deferred indexes before it
// replays to a Mithril anchor and rebuilds the full manifest, clearing the
// pending marker, once the anchor is reached.
type DeferredIndexRebuilder struct {
	manager metadata.DeferredIndexManager
	logger  *slog.Logger
	ctx     context.Context
}

func (r *DeferredIndexRebuilder) BuildAll() error {
	if r == nil || r.manager == nil {
		return nil
	}
	logger := r.logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if err := ensureAllDeferredIndexes(
		r.ctx, r.manager, logger,
	); err != nil {
		return fmt.Errorf("rebuilding deferred indexes: %w", err)
	}
	return nil
}

// NewDeferredIndexRebuilder returns a rebuilder for db's deferred-index
// manifest without dropping anything. Building skips the indexes that exist.
func NewDeferredIndexRebuilder(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
) *DeferredIndexRebuilder {
	manager, ok := db.Metadata().(metadata.DeferredIndexManager)
	if !ok {
		return &DeferredIndexRebuilder{}
	}
	return &DeferredIndexRebuilder{manager: manager, logger: logger, ctx: ctx}
}

// Drop drops the deferred-index manifest again; dropping what is already
// gone is a no-op.
func (r *DeferredIndexRebuilder) Drop() { r.drop() }

func (r *DeferredIndexRebuilder) drop() {
	if r == nil || r.manager == nil {
		return
	}
	var err error
	if dropper, ok := r.manager.(metadata.ContextDeferredIndexDropper); ok {
		err = dropper.DropDeferredIndexesContext(r.ctx)
	} else {
		err = r.manager.DropDeferredIndexes()
	}
	if err != nil {
		logger := r.logger
		if logger == nil {
			logger = slog.New(slog.DiscardHandler)
		}
		logger.Warn(
			"failed to drop deferred metadata indexes; "+
				"continuing and repairing during rebuild phases",
			"error", err,
		)
	}
}

// deferredIndexCheckLogThreshold is how long the deferred-index check may
// take before it is reported even though nothing in the manifest was
// missing. A complete manifest costs one catalog lookup per entry and
// stays well under it; anything slower did work an operator watching a
// startup should see attributed.
const deferredIndexCheckLogThreshold = time.Second

// ensureAllDeferredIndexes rebuilds the complete manifest, naming the entries
// it is about to build and reporting how long the build took.
//
// BuildDeferredIndexes emits nothing while it runs and has the whole manifest
// to get through, so without this an operator sees nothing at all for however
// long the entries take on a multi-million-row table.
func ensureAllDeferredIndexes(
	ctx context.Context,
	manager metadata.DeferredIndexManager,
	logger *slog.Logger,
) error {
	missing, listed := missingDeferredIndexes(ctx, manager, logger)
	if listed && len(missing) > 0 {
		logger.Info(
			"rebuilding missing deferred metadata indexes",
			"indexes", strings.Join(missing, ","),
			"count", len(missing),
		)
	}
	start := time.Now()
	var err error
	if builder, ok := manager.(metadata.ContextDeferredIndexBuilder); ok {
		err = builder.BuildDeferredIndexesContext(ctx)
	} else {
		err = manager.BuildDeferredIndexes()
	}
	if err != nil {
		return err
	}
	elapsed := time.Since(start)
	if (listed && len(missing) > 0) ||
		elapsed >= deferredIndexCheckLogThreshold {
		attrs := []any{"duration", elapsed}
		if listed {
			indexes := "none"
			if len(missing) > 0 {
				indexes = strings.Join(missing, ",")
			}
			attrs = append(attrs, "deferred_indexes_built", indexes)
		}
		logger.Info("deferred metadata index check complete", attrs...)
	}
	return nil
}

// missingDeferredIndexes names the manifest entries absent from the schema.
// The second return reports whether the store could answer: stores that do
// not implement the lister, and read errors on the catalog query, fall back to
// logging after the rebuild rather than failing over a log line.
func missingDeferredIndexes(
	ctx context.Context,
	manager metadata.DeferredIndexManager,
	logger *slog.Logger,
) ([]string, bool) {
	var missing []string
	var err error
	if lister, ok := manager.(metadata.ContextMissingDeferredIndexLister); ok {
		missing, err = lister.MissingDeferredIndexesContext(ctx)
	} else if lister, ok := manager.(metadata.MissingDeferredIndexLister); ok {
		missing, err = lister.MissingDeferredIndexes()
	} else {
		return nil, false
	}
	if err != nil {
		logger.Warn(
			"could not list missing deferred metadata indexes; "+
				"rebuilding without naming them",
			"error", err,
		)
		return nil, false
	}
	return missing, true
}

// openLoadUtxoSet opens the live UTxO set of the database a load replays
// into, as a node over that database does, so the replay reads and writes the
// ledger state where the node will look for it. It returns the directory the
// set lives under and a function that closes the set, which must be called
// after the ledger has stopped applying blocks.
func openLoadUtxoSet(
	cfg *config.Config,
	logger *slog.Logger,
	db *database.Database,
	nodeCfg *cardano.CardanoNodeConfig,
) (*utxoset.Set, string, func() error, error) {
	// The database in use, not cfg.DatabasePath: a caller that supplies its
	// own database decides where its state lives.
	dataDir := db.DataDir()
	set, tempDir, err := nodestorage.OpenUtxoSet(nodestorage.UtxoSetConfig{
		DataDir:           dataDir,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
		CheckpointKeep:    cfg.LedgerUtxoCheckpointKeep,
	})
	if err != nil {
		return nil, "", nil, err
	}
	if tempDir != "" {
		dataDir = tempDir
	}
	closeSet := func() error {
		err := set.Close()
		if tempDir != "" {
			err = errors.Join(err, os.RemoveAll(tempDir))
		}
		return err
	}
	return set, dataDir, closeSet, nil
}

// startLoadAnchorUpdater feeds the live UTxO set the ledger's immutable point
// while a load replays, as the node does. Without an anchor the set never
// prunes its rollback journal, which would grow with every replayed block.
// The returned function stops the updater and waits for it.
func startLoadAnchorUpdater(
	ctx context.Context,
	ls *ledger.LedgerState,
	set *utxoset.Set,
) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(nodestorage.AnchorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				nodestorage.UpdateAnchor(ctx, ls, set)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// LoadWithDB loads immutable DB blocks into the chain. If db is nil,
// a new database connection is opened (and closed on return).
func LoadWithDB(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	immutableDir string,
	db *database.Database,
) error {
	remoteImmutable, err := classifyRemoteImmutableSource(immutableDir)
	if err != nil {
		return err
	}
	if remoteImmutable && cfg.DatabasePath == "" {
		return errors.New(
			"loading from a remote ImmutableDB requires databasePath " +
				"for its download cache",
		)
	}
	// Derive default config path from cfg.Network when cfg.CardanoConfig is empty
	network := cfg.Network
	if cfg.CardanoConfig == "" && network == "" {
		network = "preview"
	}
	nodeCfg, err := nodestorage.LoadNodeConfig(cfg)
	if err != nil {
		return err
	}
	if cfg.FullPotRewardsEnabled &&
		!cfg.UnsafeFullPotRewardsOnStandardNetworks {
		var genesisNetworkMagic uint32
		if shelleyGenesis := nodeCfg.ShelleyGenesis(); shelleyGenesis != nil {
			genesisNetworkMagic = shelleyGenesis.NetworkMagic
		}
		if network == "" && cfg.NetworkMagic == 0 && genesisNetworkMagic == 0 {
			return errors.New(
				"fullPotRewardsEnabled requires a resolvable network identity",
			)
		}
		if networkName, ok := config.FullPotRewardsStandardNetwork(
			network,
			cfg.NetworkMagic,
		); ok {
			return fmt.Errorf(
				"fullPotRewardsEnabled is not permitted on standard network %q "+
					"without unsafeFullPotRewardsOnStandardNetworks",
				networkName,
			)
		}
		// The Shelley genesis drives ledger state during load, so validate its
		// identity independently. Otherwise a custom configured name or magic
		// could disguise a standard-network Cardano config and bypass the gate.
		if networkName, ok := config.FullPotRewardsStandardNetwork(
			"",
			genesisNetworkMagic,
		); ok {
			return fmt.Errorf(
				"fullPotRewardsEnabled is not permitted on standard network %q "+
					"without unsafeFullPotRewardsOnStandardNetworks",
				networkName,
			)
		}
	}
	var remoteLimits remoteImmutableLimits
	if remoteImmutable {
		var (
			regularSlots  uint64
			canContainEBB bool
		)
		if byronGenesis := nodeCfg.ByronGenesis(); byronGenesis != nil {
			if byronGenesis.ProtocolConsts.K <= 0 ||
				uint64(byronGenesis.ProtocolConsts.K) > math.MaxUint64/10 {
				return errors.New(
					"remote ImmutableDB requires a valid Byron security parameter",
				)
			}
			regularSlots = uint64(
				byronGenesis.ProtocolConsts.K,
			) * 10 //nolint:gosec
			canContainEBB = true
		} else if shelleyGenesis := nodeCfg.ShelleyGenesis(); shelleyGenesis != nil &&
			shelleyGenesis.EpochLength > 0 {
			regularSlots = uint64(shelleyGenesis.EpochLength) //nolint:gosec
		} else {
			return errors.New(
				"remote ImmutableDB requires an initial era epoch length",
			)
		}
		remoteLimits, err = remoteImmutableLimitsForSlots(
			regularSlots, canContainEBB,
		)
		if err != nil {
			return err
		}
	}
	logger.Debug(
		"cardano network config",
		"component", "node",
		"config", nodeCfg,
	)
	// Load database (open new one if not provided)
	db, closeDB, err := ensureDB(cfg, logger, db, nodeCfg) //nolint:contextcheck
	if err != nil {
		return err
	}
	defer closeDB()
	if err := nodestorage.RefuseHistoryExpiryOverImmutable(
		cfg.HistoryExpiry.Enabled, db,
	); err != nil {
		return err
	}
	utxoSet, _, closeUtxoSet, err := openLoadUtxoSet(
		cfg, logger, db, nodeCfg,
	)
	if err != nil {
		return err
	}
	// Deferred before the ledger's close, so it runs after it.
	defer func() {
		if err := closeUtxoSet(); err != nil {
			logger.Error("failed to close the live UTxO set", "error", err)
		}
	}()
	// Immutable load replays trusted block batches directly into the ledger, so
	// it does not need the event-driven reread path here.
	cm, err := chain.NewManager(
		ctx,
		db,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to load chain manager: %w", err)
	}
	c := cm.PrimaryChain()
	if c == nil {
		return errors.New("primary chain not available")
	}
	if err := refuseBehindImmutableTip(db, c); err != nil {
		return err
	}
	// Load state
	ls, err := newLedgerStateForLoad(
		ledger.LedgerStateConfig{
			Database:          db,
			UtxoSet:           utxoSet,
			ChainManager:      cm,
			Logger:            logger,
			CardanoNodeConfig: nodeCfg,
			Network:           cfg.Network,
			CIP0203Enabled:    cfg.CIP0203Enabled,
			DijkstraBlsProofDomain: cardano.DijkstraPoolBlsProofDomain(
				nodeCfg,
				cfg.Network,
				cfg.NetworkMagic,
			),
			ValidateHistorical: cfg.ValidateHistorical,
			// CIP-0163 full-pot reward distribution is consensus-affecting and
			// deterministically changes the reward state written during replay,
			// so load must honor the same operator flag as serve mode; otherwise
			// an import with the feature enabled would persist legacy
			// residual-to-reserves reward state that disagrees with an enabled
			// serve node.
			FullPotRewardsEnabled: cfg.FullPotRewardsEnabled,
			TrustedReplay:         true,
			// Imported blocks never passed admission, so apply must verify
			// each Praos header before the block reaches ledger state.
			VerifyReplayHeaders: true,
			// Immutable load replays blocks already accepted into the trusted
			// database. Structural Leios certificate checks still run;
			// cryptographic verification belongs to live admission, where the
			// vote manager is available.
			ValidateLeiosCertificate: func(uint64, []byte, []byte, []byte) error {
				return nil
			},
			ManualBlockProcessing: true,
			// CIP-0163 reward-account inactivity expiry: consensus-affecting,
			// must match serve mode (node.go) on replay of the same DB.
			DelegatorInactivityEnabled: cfg.DelegatorInactivityEnabled,
			DelegatorInactivity:        cfg.DelegatorInactivity,
			DatabaseWorkerPoolConfig: ledger.DatabaseWorkerPoolConfig{
				WorkerPoolSize: cfg.DatabaseWorkers,
				TaskQueueSize:  cfg.DatabaseQueueSize,
				Disabled:       false,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}
	// The snapshot manager writes through the ledger's LedgerDB, so its
	// snapshot rows reach the live UTxO set, where the drained reward-input
	// families live.
	snapshotMgr := snapshot.NewManager(ls.LedgerDB(), nil, logger)
	// Mirror the CIP-0163 reward-account inactivity gate into snapshot capture
	// so replay matches serve mode (node.go) on the same DB.
	if err := snapshotMgr.SetDelegatorInactivity(
		cfg.DelegatorInactivityEnabled,
		cfg.DelegatorInactivity,
	); err != nil {
		return fmt.Errorf("configuring snapshot manager: %w", err)
	}
	if err := configureLoadChainSecurityParam(cm, nodeCfg); err != nil {
		return err
	}
	captureFailures := &loadCaptureFailureTracker{}
	// SNAP-point stake read: runs after MIR and before POOLREAP/enactment so the
	// mark snapshot includes pre-SNAP credits but excludes post-SNAP credits. If
	// that read fails, the persist hook uses boundary-aware historical
	// reconstruction; the tracker reports only failure of the complete capture.
	ls.SetEpochBoundarySnapshotStakeHook(
		func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
			return snapshotMgr.ComputeEpochBoundarySnapshot(ctx, txn, evt)
		},
	)
	// Governance's same-boundary SPO stake read: RATIFY tallies
	// mark[NewEpoch] -- this same boundary's own mark snapshot -- which is not
	// durably written until the capture hook below runs, later in the same
	// rollover. Load replays the exact same governance.ProcessEpoch path as
	// serve mode, so it needs this wired too; without it, every SPO-gated
	// action would silently see zero stake at every replayed boundary.
	ls.SetCurrentBoundarySPOStakeHook(
		func(
			txn ledgerdb.Txn,
			evt event.EpochTransitionEvent,
			incomingEraID uint,
		) ([]*models.PoolStakeSnapshot, error) {
			return snapshotMgr.CurrentBoundarySPOStakeRows(
				ctx, txn, evt, incomingEraID,
			)
		},
	)
	if err := installEpochBoundarySnapshotHookForLoad(
		ls,
		func(txn ledgerdb.Txn, evt event.EpochTransitionEvent) error {
			// Record the failure so LoadWithDB can name the epoch whose
			// mark/reward snapshot is missing.
			if err := snapshotMgr.CaptureEpochBoundarySnapshot(
				ctx, txn, evt,
			); err != nil {
				captureFailures.record(evt.NewEpoch, err)
				return err
			}
			return nil
		},
	); err != nil {
		return fmt.Errorf("installing epoch-boundary snapshot hook: %w", err)
	}
	// The node's startup sequence: the set is made equal to the ledger
	// before the ledger writes its genesis or applies a block. A set that
	// cannot be brought to the database's ledger tip stops the load here.
	if err := (nodestorage.Reconcile{
		DB:                db,
		Set:               utxoSet,
		Logger:            logger,
		Resume:            ls.ResumeUtxoSetFrom,
		CardanoNodeConfig: nodeCfg,
	}).Run(ctx); err != nil {
		return fmt.Errorf("failed to prepare the live UTxO set: %w", err)
	}
	if err := ls.Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}
	defer ls.Close()
	defer startLoadAnchorUpdater(ctx, ls, utxoSet)()

	// Capture the genesis stake snapshot (epoch 0) now that ls.Start has
	// applied genesis (including any Shelley-genesis staking), mirroring
	// node.go's normal startup path. Without this, replaying a devnet chain
	// with genesis staking through `dingo load` never creates the epoch-0
	// mark RewardSnapshot, silently skipping the first reward round applied
	// at the epoch-3 boundary. This must run before any epoch
	// boundaries are processed below.
	if err := captureLoadGenesisSnapshot(ctx, snapshotMgr, cfg, logger); err != nil {
		return err
	}

	replayCtx, cancelReplay := context.WithCancel(ctx)
	defer cancelReplay()
	replayBatches := make(chan []gledger.Block, 1)
	replayErrCh := make(chan error, 1)
	go func() {
		err := processTrustedBlockBatchesForLoad(
			ls,
			replayCtx,
			replayBatches,
		)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Cancel immediately so copyBlocksDirect and the
			// forwarding goroutine inside ProcessTrustedBlockBatches
			// unblock via ctx.Done() instead of deadlocking.
			cancelReplay()
		}
		replayErrCh <- err
	}()

	var (
		blocksCopied     int
		immutableTipSlot uint64
	)
	gapBlocks, err := replayChainBlocksAheadOfLedger(
		replayCtx, c, ls.Tip().Point, replayBatches,
	)
	if err == nil {
		if gapBlocks > 0 {
			logger.Info(
				"replayed chain blocks the ledger had not applied",
				"blocks", gapBlocks,
				"chain_tip_slot", c.Tip().Point.Slot,
			)
		}
		if remoteImmutable {
			blocksCopied, immutableTipSlot, err = copyBlocksRemote(
				replayCtx, logger, immutableDir,
				filepath.Join(cfg.DatabasePath, remoteImmutableCacheDir),
				remoteLimits, c, replayBatches,
			)
		} else {
			blocksCopied, immutableTipSlot, err = copyBlocksDirect(
				replayCtx, logger, immutableDir, c, replayBatches,
			)
		}
	}
	close(replayBatches)
	if err != nil {
		cancelReplay()
		// A non-nil, non-context.Canceled replayErr here is the real
		// cause: it's what made ProcessTrustedBlockBatches call
		// cancelReplay in the first place, which is what unblocked
		// copyBlocksDirect's channel send and produced err (usually
		// just "context canceled"). Join both so the operator sees the
		// actual failure instead of only its downstream symptom.
		replayErr := <-replayErrCh
		if replayErr != nil && !errors.Is(replayErr, context.Canceled) {
			return loadReplayFailure(
				ctx,
				c,
				ls,
				errors.Join(fmt.Errorf("loading blocks: %w", err), replayErr),
			)
		}
		return loadReplayFailure(
			ctx,
			c,
			ls,
			fmt.Errorf("loading blocks: %w", err),
		)
	}
	if err := <-replayErrCh; err != nil {
		return loadReplayFailure(
			ctx,
			c,
			ls,
			fmt.Errorf("processing trusted block batches: %w", err),
		)
	}
	logger.Info(
		"finished processing blocks from immutable DB",
		"blocks_copied", blocksCopied,
		"tip_slot", immutableTipSlot,
	)
	// Surface any authoritative epoch-boundary capture the ledger suppressed:
	// load has no fallback to recapture it, so a silent success here would leave
	// the database missing mark/reward snapshots for those epochs.
	if err := captureFailures.err(); err != nil {
		return err
	}
	return nil
}

// replayChainBlocksAheadOfLedger sends the primary chain's blocks after
// ledgerTip to the replay. The immutable copy starts at the chain tip, so a
// chain that already holds blocks the ledger has not applied -- the blob-only
// copy `dingo mithril sync` makes past its ledger-state snapshot -- would
// otherwise hand the ledger a first block that does not extend its tip.
func replayChainBlocksAheadOfLedger(
	ctx context.Context,
	c *chain.Chain,
	ledgerTip ocommon.Point,
	replayBatches chan<- []gledger.Block,
) (int, error) {
	chainTip := c.Tip().Point
	if chainTip.Slot == ledgerTip.Slot &&
		bytes.Equal(chainTip.Hash, ledgerTip.Hash) {
		return 0, nil
	}
	iter, err := c.FromPointContext(ctx, ledgerTip, false)
	if err != nil {
		return 0, fmt.Errorf(
			"ledger tip (slot %d) is not on the primary chain: %w",
			ledgerTip.Slot, err,
		)
	}
	defer iter.Cancel()
	// The blob-only copy never decoded these blocks, so verify their body
	// hashes here as the immutable copy does.
	verifyCfg := lcommon.VerifyConfig{}
	var replayed int
	for done := false; !done; {
		batch := make([]gledger.Block, 0, loadBlockBatchSize)
		for len(batch) < cap(batch) {
			next, err := iter.Next(false)
			if errors.Is(err, chain.ErrIteratorChainTip) {
				done = true
				break
			}
			if err != nil {
				return replayed, fmt.Errorf(
					"reading chain block after ledger tip: %w", err,
				)
			}
			if next.Rollback {
				return replayed, errors.New(
					"primary chain rolled back during load",
				)
			}
			blk, err := next.Block.Decode(verifyCfg)
			if err != nil {
				return replayed, fmt.Errorf(
					"decoding chain block at slot %d: %w",
					next.Block.Slot, err,
				)
			}
			batch = append(batch, blk)
		}
		if len(batch) == 0 {
			break
		}
		select {
		case replayBatches <- batch:
		case <-ctx.Done():
			return replayed, fmt.Errorf("loading blocks: %w", ctx.Err())
		}
		replayed += len(batch)
	}
	return replayed, nil
}

func loadReplayFailure(
	ctx context.Context,
	c *chain.Chain,
	ls *ledger.LedgerState,
	cause error,
) error {
	appliedTip := ls.Tip()
	chainTip := c.Tip()
	if chainTip.Point.Slot == appliedTip.Point.Slot &&
		bytes.Equal(chainTip.Point.Hash, appliedTip.Point.Hash) {
		return cause
	}
	if err := c.RollbackUnbounded(
		context.WithoutCancel(ctx),
		appliedTip.Point,
	); err != nil {
		return errors.Join(
			cause,
			fmt.Errorf("rewinding unapplied immutable blocks: %w", err),
		)
	}
	return cause
}

// LoadBlobsResult contains the result of a blob-only ImmutableDB load.
type LoadBlobsResult struct {
	BlocksCopied     int
	ImmutableTipSlot uint64
}

// LoadBlobsProgress reports ImmutableDB blob-copy progress.
type LoadBlobsProgress struct {
	BlocksCopied    int
	CurrentSlot     uint64
	TipSlot         uint64
	BlocksPerSecond float64
	Percent         float64
}

type loadBlobsOptions struct {
	onProgress     func(LoadBlobsProgress)
	immutableDB    *immutable.ImmutableDb
	immutableDBSet bool
	// retireChunks, when set, enables deleting source chunks once committed.
	retireChunks   bool
	onChunksCommit func(firstKept uint64) error
}

// LoadBlobsOption customizes LoadBlobsWithDB behavior.
type LoadBlobsOption func(*loadBlobsOptions)

// WithLoadBlobsProgress registers a callback for blob-copy progress.
func WithLoadBlobsProgress(
	onProgress func(LoadBlobsProgress),
) LoadBlobsOption {
	return func(opts *loadBlobsOptions) {
		opts.onProgress = onProgress
	}
}

// WithChunkRetirement deletes each source chunk's files from the ImmutableDB
// once every block in it is committed to the blob store, so the extracted tree
// and the blob store do not both hold the whole chain.
//
// The chunk holding the most recently committed block is kept: it is where a
// resumed copy restarts. onCommitted, when non-nil, is called with that chunk's
// number before the chunks below it are deleted, so a caller can record it
// durably; every chunk below it is then committed and may be absent from the
// tree on the next run. An error from it stops the copy before any deletion.
//
// The copy must be the only reader of the tree once it starts.
func WithChunkRetirement(
	onCommitted func(firstKept uint64) error,
) LoadBlobsOption {
	return func(opts *loadBlobsOptions) {
		opts.retireChunks = true
		opts.onChunksCommit = onCommitted
	}
}

// WithImmutableDB supplies an already-open ImmutableDB to copy from, instead of
// opening immutableDir by pathname.
//
// Callers that vetted the directory before handing it over need this: opening
// the pathname here would let a tree substituted in the meantime be read as
// though it were the one that was checked. Passing the open database rather
// than a directory handle also leaves one place where the decision how to
// resolve the directory was made, so it cannot be made differently twice.
//
// A nil argument is an error rather than a fallback to the pathname — a caller
// that meant to supply a vetted database and passed nothing must not silently
// get the open it was avoiding. Omit the option entirely to open by pathname.
//
// The caller keeps ownership and must keep the database usable for the duration
// of the call.
func WithImmutableDB(imm *immutable.ImmutableDb) LoadBlobsOption {
	return func(opts *loadBlobsOptions) {
		opts.immutableDB = imm
		opts.immutableDBSet = true
	}
}

// LoadBlobsWithDB copies blocks from an ImmutableDB directory into the blob
// store without starting the ledger processing pipeline. This is used after
// a Mithril snapshot import where the ledger state has already been loaded
// from the snapshot. Returns the number of blocks copied and the immutable
// tip slot so the caller can update the metadata tip to match.
func LoadBlobsWithDB(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	immutableDir string,
	db *database.Database,
	options ...LoadBlobsOption,
) (*LoadBlobsResult, error) {
	opts := loadBlobsOptions{}
	for _, option := range options {
		if option == nil {
			continue
		}
		option(&opts)
	}
	if opts.immutableDBSet && opts.immutableDB == nil {
		return nil, errors.New(
			"WithImmutableDB was given no ImmutableDB; refusing to fall " +
				"back to opening the directory by pathname",
		)
	}
	imm := opts.immutableDB
	if imm == nil {
		var err error
		if imm, err = immutable.New(immutableDir); err != nil {
			return nil, fmt.Errorf("failed to read immutable DB: %w", err)
		}
	}
	// Load database (open new one if not provided)
	db, closeDB, err := ensureDB(cfg, logger, db, nil) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	defer closeDB()
	// Load chain without event bus (no ledger processing)
	cm, err := chain.NewManager(ctx, db, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load chain manager: %w", err)
	}
	c := cm.PrimaryChain()
	if c == nil {
		return nil, errors.New("primary chain not available")
	}
	if err := refuseBehindImmutableTip(db, c); err != nil {
		return nil, err
	}

	var utxoOffsetsStored int
	var callback func(chain.RawBlock, *database.Txn) error
	if db.StorageMode() != datatypes.StorageModeCore {
		callback = func(rb chain.RawBlock, txn *database.Txn) error {
			stored, err := storeRawBlockUtxoOffsets(txn, rb)
			if err != nil {
				return err
			}
			utxoOffsetsStored += stored
			return nil
		}
	}
	var retire func(uint64) error
	if opts.retireChunks {
		retire = func(firstKept uint64) error {
			// A blob-only batch commit is not fsynced. Flush before the
			// chunks become deletable, or a host failure could keep the
			// deletion while losing the blocks it was supposed to back.
			if err := db.Blob().Sync(); err != nil {
				return fmt.Errorf(
					"syncing blob store before retiring chunks: %w",
					err,
				)
			}
			if opts.onChunksCommit != nil {
				if err := opts.onChunksCommit(firstKept); err != nil {
					return err
				}
			}
			return imm.RemoveChunksBefore(firstKept)
		}
	}
	blocksCopied, immutableTipSlot, err := copyBlocksRawWithCallback(
		ctx, logger, imm, db, c,
		callback,
		opts.onProgress,
		retire,
	)
	if err != nil {
		return nil, fmt.Errorf("loading blocks: %w", err)
	}
	if callback == nil {
		logger.Info(
			"skipped immutable UTxO offset writes for Core storage mode",
			"blocks_copied", blocksCopied,
		)
	} else {
		logger.Info(
			"finished storing immutable UTxO offsets during copy",
			"blocks_copied", blocksCopied,
			"utxo_offsets_stored", utxoOffsetsStored,
		)
	}

	return &LoadBlobsResult{
		BlocksCopied:     blocksCopied,
		ImmutableTipSlot: immutableTipSlot,
	}, nil
}

// ErrLoadBehindImmutableTip reports a body copy onto a database whose recorded
// ImmutableDB tip is ahead of its chain, which an interrupted chunk adoption
// leaves. Copied bodies at indexes at or below that tip are never promoted.
var ErrLoadBehindImmutableTip = errors.New(
	"chain is behind the recorded ImmutableDB tip",
)

// refuseBehindImmutableTip returns ErrLoadBehindImmutableTip when db records
// an ImmutableDB tip that the chain has not reached. Finish the interrupted
// adoption, or truncate the database, before copying block bodies.
func refuseBehindImmutableTip(db *database.Database, c *chain.Chain) error {
	st, ok := db.ImmutableState()
	if !ok || !st.HasTip {
		return nil
	}
	tip := c.Tip().Point
	if tip.Slot >= st.TipSlot &&
		(tip.Slot != 0 || len(tip.Hash) != 0) {
		return nil
	}
	return fmt.Errorf(
		"%w: immutable tip is slot %d, chain tip is slot %d; finish the "+
			"chunk adoption or truncate the database",
		ErrLoadBehindImmutableTip, st.TipSlot, tip.Slot,
	)
}

// copyBlocksDirect reads immutable blocks once, persists them to the chain,
// and streams the decoded batches to the trusted ledger replay path.
func copyBlocksDirect(
	ctx context.Context,
	logger *slog.Logger,
	immutableDir string,
	c *chain.Chain,
	replayBatches chan<- []gledger.Block,
) (int, uint64, error) {
	immDB, err := immutable.New(immutableDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB: %w", err)
	}
	immutableTip, err := immDB.GetTip()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB tip: %w", err)
	}
	if immutableTip == nil {
		return 0, 0, errors.New("immutable DB tip is nil")
	}
	logger.Info("copying blocks from immutable DB")
	chainTip := c.Tip()
	if chainTip.Point.Slot > immutableTip.Slot {
		logger.Info(
			"chain tip already beyond immutable DB tip; skipping immutable copy",
			"chain_tip_slot",
			chainTip.Point.Slot,
			"immutable_tip_slot",
			immutableTip.Slot,
		)
		return 0, immutableTip.Slot, nil
	}
	iter, err := immDB.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to get immutable DB iterator: %w",
			err,
		)
	}
	defer iter.Close()
	var blocksCopied int
	startTime := time.Now()
	lastProgressLog := time.Time{}
	var lastProgressSlot uint64
	// The chunk files may come from another party, so every block's body must
	// match the body hash its signed header commits to.
	verifyCfg := lcommon.VerifyConfig{}
	for {
		rawBatch := make([]immutable.Block, 0, loadBlockBatchSize)
		for {
			next, err := iter.Next()
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"reading next block: %w", err,
				)
			}
			if next == nil {
				break
			}
			if blocksCopied == 0 &&
				next.Slot == chainTip.Point.Slot &&
				bytes.Equal(next.Hash, chainTip.Point.Hash) {
				continue
			}
			rawBatch = append(rawBatch, *next)
			if len(rawBatch) == cap(rawBatch) {
				break
			}
		}
		if len(rawBatch) == 0 {
			break
		}
		blockBatch, err := decodeImmutableBlockBatch(
			ctx, rawBatch, verifyCfg, loadDecodeWorkerCount(),
		)
		if err != nil {
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"decoding block CBOR: %w", err,
			)
		}
		if len(blockBatch) == 0 {
			return blocksCopied, immutableTip.Slot, errors.New(
				"decoding block CBOR: non-empty batch decoded to no blocks",
			)
		}
		if err := c.AddBlocks(ctx, blockBatch); err != nil {
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"failed to import block: %w",
				err,
			)
		}
		replayBatch := append(
			make([]gledger.Block, 0, len(blockBatch)),
			blockBatch...,
		)
		select {
		case replayBatches <- replayBatch:
		case <-ctx.Done():
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"loading blocks: %w",
				ctx.Err(),
			)
		}
		blocksCopied += len(blockBatch)
		lastProgressSlot = replayBatch[len(replayBatch)-1].SlotNumber()
		maybeLogBlockCopyProgress(
			logger,
			"copying blocks from immutable DB",
			blocksCopied,
			lastProgressSlot,
			immutableTip.Slot,
			startTime,
			&lastProgressLog,
		)
		if err := ctx.Err(); err != nil {
			return blocksCopied, immutableTip.Slot,
				fmt.Errorf("loading blocks: %w", err)
		}
	}
	return blocksCopied, immutableTip.Slot, nil
}

type immutableDecodeResult struct {
	index int
	block gledger.Block
	err   error
}

type immutableDecodeJob struct {
	index int
	block immutable.Block
}

// immutableBlockDecoder is a test seam: production decoding ignores the
// context and index, while tests can fail a selected job and observe the
// derived cancellation context. NewBlockFromCbor itself is not cancellable.
type immutableBlockDecoder func(
	context.Context,
	int,
	immutable.Block,
	lcommon.VerifyConfig,
) (gledger.Block, error)

func decodeImmutableBlock(
	_ context.Context,
	_ int,
	block immutable.Block,
	verifyCfg lcommon.VerifyConfig,
) (gledger.Block, error) {
	return models.DecodeBlockCbor(block.Type, block.Cbor, verifyCfg)
}

// decodeImmutableBlockBatch decodes a bounded batch with ordered results.
// Sending jobs through a small buffered channel provides backpressure, while
// the result index prevents completion order from changing chain order. A
// cancellation stops both the dispatcher and workers without leaving a
// goroutine blocked on a full channel.
func decodeImmutableBlockBatch(
	ctx context.Context,
	rawBlocks []immutable.Block,
	verifyCfg lcommon.VerifyConfig,
	workerCount int,
) ([]gledger.Block, error) {
	return decodeImmutableBlockBatchWithDecoder(
		ctx, rawBlocks, verifyCfg, workerCount, decodeImmutableBlock,
	)
}

func decodeImmutableBlockBatchWithDecoder(
	ctx context.Context,
	rawBlocks []immutable.Block,
	verifyCfg lcommon.VerifyConfig,
	workerCount int,
	decoder immutableBlockDecoder,
) ([]gledger.Block, error) {
	if len(rawBlocks) == 0 {
		return nil, nil
	}
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(rawBlocks) {
		workerCount = len(rawBlocks)
	}
	decodeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	queueSize := min(workerCount*2, len(rawBlocks))
	jobs := make(chan immutableDecodeJob, queueSize)
	results := make(chan immutableDecodeResult, queueSize)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-decodeCtx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					block, err := decoder(
						decodeCtx, job.index, job.block, verifyCfg,
					)
					select {
					case results <- immutableDecodeResult{
						index: job.index, block: block, err: err,
					}:
					case <-decodeCtx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for index, block := range rawBlocks {
			select {
			case jobs <- immutableDecodeJob{index: index, block: block}:
			case <-decodeCtx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	decoded := make([]gledger.Block, len(rawBlocks))
	seen := 0
	var firstErr error
	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
			cancel()
			continue
		}
		if result.err == nil {
			decoded[result.index] = result.block
			seen++
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if seen != len(rawBlocks) {
		return nil, errors.New("parallel immutable block decode ended early")
	}
	return decoded, nil
}

// CopyImmutableBlobsBounded copies immutable blocks into the blob store from
// the current chain tip up to and including maxSlot, storing produced-UTxO
// offsets per block. maxSlot of 0 copies to the immutable tip. It reuses a
// caller-opened ImmutableDb and Chain so it can be invoked repeatedly during a
// download to overlap the copy with fetching (parallel fetch, sequenced
// processing). Returns the number of blocks copied and the highest slot copied.
//
// Safe against chunks that arrive out of order: bounding by maxSlot (the last
// slot of a known-contiguous chunk prefix) stops the copy before it can reach a
// higher out-of-order chunk past a gap. Blocks above maxSlot are left for a
// later call with a higher bound.
func CopyImmutableBlobsBounded(
	ctx context.Context,
	logger *slog.Logger,
	imm *immutable.ImmutableDb,
	c *chain.Chain,
	maxSlot uint64,
	onProgress func(LoadBlobsProgress),
) (int, uint64, error) {
	return copyImmutableBlobsBounded(
		ctx, logger, imm, c, maxSlot, onProgress, true,
	)
}

// CopyImmutableBlobsBoundedWithoutUtxoOffsets copies immutable blocks without
// materializing API UTxO offset references. Callers must leave the completion
// marker unset so a later API backfill rebuilds those references.
func CopyImmutableBlobsBoundedWithoutUtxoOffsets(
	ctx context.Context,
	logger *slog.Logger,
	imm *immutable.ImmutableDb,
	c *chain.Chain,
	maxSlot uint64,
	onProgress func(LoadBlobsProgress),
) (int, uint64, error) {
	return copyImmutableBlobsBounded(
		ctx, logger, imm, c, maxSlot, onProgress, false,
	)
}

func copyImmutableBlobsBounded(
	ctx context.Context,
	logger *slog.Logger,
	imm *immutable.ImmutableDb,
	c *chain.Chain,
	maxSlot uint64,
	onProgress func(LoadBlobsProgress),
	storeUtxoOffsets bool,
) (int, uint64, error) {
	callback := func(rb chain.RawBlock, txn *database.Txn) error {
		_, err := storeRawBlockUtxoOffsets(txn, rb)
		return err
	}
	if !storeUtxoOffsets {
		callback = nil
	}

	startPoint := c.Tip().Point
	if maxSlot > 0 && startPoint.Slot >= maxSlot {
		return 0, startPoint.Slot, nil
	}

	// Progress denominator: the bound when set, else the immutable tip.
	tipSlot := maxSlot
	if tipSlot == 0 {
		immutableTip, err := imm.GetTip()
		if err != nil {
			return 0, startPoint.Slot, fmt.Errorf(
				"reading immutable DB tip: %w", err,
			)
		}
		if immutableTip != nil {
			tipSlot = immutableTip.Slot
		}
	}

	iter, err := imm.BlocksFromPoint(startPoint)
	if err != nil {
		return 0, startPoint.Slot, fmt.Errorf(
			"failed to get immutable DB iterator: %w", err,
		)
	}
	defer iter.Close()

	blockBatch := make([]chain.RawBlock, 0, loadBlockBatchSize)
	blocksCopied := 0
	lastSlot := startPoint.Slot
	startTime := time.Now()
	lastProgressLog := time.Time{}
	done := false
	for !done {
		for {
			next, err := iter.Next()
			if err != nil {
				return blocksCopied, lastSlot, fmt.Errorf(
					"reading next block: %w", err,
				)
			}
			if next == nil {
				done = true
				break
			}
			// Skip the resume anchor block (already in the chain).
			if blocksCopied == 0 && len(blockBatch) == 0 &&
				next.Slot == startPoint.Slot &&
				bytes.Equal(next.Hash, startPoint.Hash) {
				continue
			}
			// Stop at the contiguous bound BEFORE decoding the block. next.Slot
			// comes from the secondary index, so this defers a higher
			// out-of-order chunk's block without decoding it (decoding could
			// otherwise fail or waste work on a block meant for a later call).
			if maxSlot > 0 && next.Slot > maxSlot {
				done = true
				break
			}
			rawBlock, err := rawBlockFromImmutableBlock(next)
			if err != nil {
				return blocksCopied, lastSlot, fmt.Errorf(
					"building raw block: %w", err,
				)
			}
			blockBatch = append(blockBatch, rawBlock)
			if len(blockBatch) == cap(blockBatch) {
				break
			}
		}
		if len(blockBatch) > 0 {
			if err := c.AddRawBlocksWithCallback(ctx, blockBatch, callback); err != nil {
				return blocksCopied, lastSlot, fmt.Errorf(
					"failed to import block: %w", err,
				)
			}
			blocksCopied += len(blockBatch)
			lastSlot = blockBatch[len(blockBatch)-1].Slot
			blockBatch = blockBatch[:0]
			reportLoadBlobsProgress(
				onProgress, blocksCopied, lastSlot, tipSlot, startTime,
			)
			maybeLogBlockCopyProgress(
				logger, "copying blocks from immutable DB (pipelined)",
				blocksCopied, lastSlot, tipSlot, startTime, &lastProgressLog,
			)
		}
		if err := ctx.Err(); err != nil {
			return blocksCopied, lastSlot, fmt.Errorf("loading blocks: %w", err)
		}
	}
	return blocksCopied, lastSlot, nil
}

// CopyImmutableBlocks appends blocks, in ascending order, to c and, outside
// Core storage mode, stores each block's produced-UTxO offsets in the same
// transaction, as the bulk ImmutableDB copy does. Core resolution never reads
// those offsets, so a Core copy skips them; it does not record the immutable
// offset completion marker in either mode. The blocks must come from a
// verified immutable trio: only the header is decoded, and only for the
// chain's continuity fields. It returns the number of blocks added.
func CopyImmutableBlocks(
	ctx context.Context,
	c *chain.Chain,
	blocks []immutable.Block,
) (int, error) {
	callback := func(rb chain.RawBlock, txn *database.Txn) error {
		if db := txn.DB(); db != nil &&
			db.StorageMode() == datatypes.StorageModeCore {
			return nil
		}
		_, err := storeRawBlockUtxoOffsets(txn, rb)
		return err
	}
	copied := 0
	batch := make([]chain.RawBlock, 0, min(len(blocks), loadBlockBatchSize))
	for i := range blocks {
		rawBlock, err := rawBlockFromImmutableBlock(&blocks[i])
		if err != nil {
			return copied, fmt.Errorf(
				"building raw block at slot %d: %w", blocks[i].Slot, err,
			)
		}
		batch = append(batch, rawBlock)
		if len(batch) < loadBlockBatchSize && i < len(blocks)-1 {
			continue
		}
		if err := c.AddRawBlocksWithCallback(ctx, batch, callback); err != nil {
			return copied, fmt.Errorf("failed to import block: %w", err)
		}
		copied += len(batch)
		batch = batch[:0]
		if err := ctx.Err(); err != nil {
			return copied, err
		}
	}
	return copied, nil
}

// copyBlocksRawWithCallback is a lightweight variant of copyBlocks that
// decodes only block headers instead of full blocks. This is significantly
// faster for bulk loading since it skips decoding transaction bodies and
// witnesses (which can be 50-90KB per block) while extracting just the
// ~200-500 byte header needed for chain indexing. The optional callback
// runs in the same transaction after each block is persisted, giving
// callers a hook to attach derived blob-side state such as UTxO offsets.
//
// The ImmutableDB is passed in already open so the caller decides how its
// directory was resolved — by pathname, or through a handle that binds the
// reads to a directory somebody else cannot repoint. See WithImmutableDB.
//
// When retire is non-nil it is called after each retirement stride and at the
// end of the copy, with the chunk holding the batch's last block; blocks arrive
// in ascending order, so every chunk below it is fully committed.
func copyBlocksRawWithCallback(
	ctx context.Context,
	logger *slog.Logger,
	imm *immutable.ImmutableDb,
	db *database.Database,
	c *chain.Chain,
	callback func(chain.RawBlock, *database.Txn) error,
	onProgress func(LoadBlobsProgress),
	retire func(firstKept uint64) error,
) (int, uint64, error) {
	immutableTip, err := imm.GetTip()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB tip: %w", err)
	}
	if immutableTip == nil {
		return 0, 0, errors.New("immutable DB tip is nil")
	}
	logger.Info("copying blocks from immutable DB (header-only decode)")
	chainTip := c.Tip()
	if chainTip.Point.Slot > immutableTip.Slot {
		logger.Info(
			"chain tip already beyond immutable DB tip; skipping immutable copy",
			"chain_tip_slot",
			chainTip.Point.Slot,
			"immutable_tip_slot",
			immutableTip.Slot,
		)
		if callback != nil && db != nil {
			complete, err := immutableUtxoOffsetsComplete(
				db,
				immutableTip.Slot,
			)
			if err != nil {
				return 0, immutableTip.Slot, err
			}
			if complete {
				logger.Info(
					"immutable raw block callback state already backfilled",
					"immutable_tip_slot", immutableTip.Slot,
				)
			} else {
				blocksBackfilled, err := backfillRawBlockCallbacks(
					ctx,
					imm,
					db,
					callback,
				)
				if err != nil {
					return 0, immutableTip.Slot, fmt.Errorf(
						"backfill immutable raw block callback state: %w",
						err,
					)
				}
				if err := markImmutableUtxoOffsetsComplete(
					db,
					immutableTip.Slot,
				); err != nil {
					return 0, immutableTip.Slot, err
				}
				logger.Info(
					"backfilled immutable raw block callback state",
					"blocks_backfilled", blocksBackfilled,
				)
			}
		}
		return 0, immutableTip.Slot, nil
	}
	iter, err := imm.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to get immutable DB iterator: %w",
			err,
		)
	}
	defer iter.Close()
	var blocksCopied int
	startTime := time.Now()
	lastProgressLog := time.Time{}
	lastProgressSlot := chainTip.Point.Slot
	blockBatch := make([]chain.RawBlock, 0, loadBlockBatchSize)
	var (
		batchChunk, firstChunk, retiredChunk     uint64
		batchChunkSet, firstChunkSet, retiredSet bool
	)
	for {
		for {
			next, err := iter.Next()
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"reading next block: %w", err,
				)
			}
			if next == nil {
				break
			}
			// EBBs (Byron Epoch Boundary Blocks) must be
			// imported: the next regular block's PrevHash
			// references the EBB, not the block before it.
			// Skip first block when continuing a load operation
			if blocksCopied == 0 &&
				next.Slot == chainTip.Point.Slot &&
				bytes.Equal(next.Hash, chainTip.Point.Hash) {
				continue
			}
			rawBlock, err := rawBlockFromImmutableBlock(next)
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"building raw block: %w",
					err,
				)
			}
			blockBatch = append(blockBatch, rawBlock)
			if num, ok := iter.Chunk(); ok {
				batchChunk, batchChunkSet = num, true
				if !firstChunkSet {
					firstChunk, firstChunkSet = num, true
				}
			}
			if len(blockBatch) == cap(blockBatch) {
				break
			}
		}
		if len(blockBatch) == 0 {
			break
		}
		if err := c.AddRawBlocksWithCallback(ctx, blockBatch, callback); err != nil {
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"failed to import block: %w",
				err,
			)
		}
		if retire != nil && batchChunkSet && batchChunk > firstChunk &&
			(!retiredSet || batchChunk-retiredChunk >= chunkRetirementStride) {
			if err := retire(batchChunk); err != nil {
				return blocksCopied + len(blockBatch), immutableTip.Slot,
					fmt.Errorf("retiring committed chunks: %w", err)
			}
			retiredChunk, retiredSet = batchChunk, true
		}
		blocksCopied += len(blockBatch)
		if tmpLen := len(blockBatch); tmpLen > 0 {
			lastProgressSlot = blockBatch[tmpLen-1].Slot
		}
		blockBatch = blockBatch[:0]
		reportLoadBlobsProgress(
			onProgress,
			blocksCopied,
			lastProgressSlot,
			immutableTip.Slot,
			startTime,
		)
		maybeLogBlockCopyProgress(
			logger,
			"copying blocks from immutable DB",
			blocksCopied,
			lastProgressSlot,
			immutableTip.Slot,
			startTime,
			&lastProgressLog,
		)
		// Check for cancellation after each batch
		if err := ctx.Err(); err != nil {
			return blocksCopied, immutableTip.Slot,
				fmt.Errorf("loading blocks: %w", err)
		}
	}
	// A resumed copy that stays inside one chunk never advances past its
	// first chunk, so the committed prefix below it is retired here.
	if retire != nil && batchChunkSet &&
		(!retiredSet || batchChunk > retiredChunk) {
		if err := retire(batchChunk); err != nil {
			return blocksCopied, immutableTip.Slot,
				fmt.Errorf("retiring committed chunks: %w", err)
		}
	}
	logger.Info(
		"finished copying blocks from immutable DB",
		"blocks_copied", blocksCopied,
	)
	if callback != nil && db != nil && blocksCopied > 0 {
		if err := markImmutableUtxoOffsetsComplete(
			db,
			immutableTip.Slot,
		); err != nil {
			return blocksCopied, immutableTip.Slot, err
		}
	}
	return blocksCopied, immutableTip.Slot, nil
}

// immutableUtxoOffsetsMark reads the mark for db's own rows. The ledger keeps
// it in its block store. An IndexerDB reads that block store too, but the
// ledger's mark says nothing about the offsets of the IndexerDB's rows, so an
// IndexerDB keeps its own in its sync_state.
func immutableUtxoOffsetsMark(db *database.Database) (string, error) {
	if db.IsIndexer() {
		return db.GetSyncState(database.ImmutableUtxoOffsetsTipKey, nil)
	}
	return db.GetChainState(database.ImmutableUtxoOffsetsTipKey, nil)
}

func immutableUtxoOffsetsComplete(
	db *database.Database,
	immutableTipSlot uint64,
) (bool, error) {
	slot, ok, err := ImmutableUtxoOffsetsTipSlot(db)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return slot >= immutableTipSlot, nil
}

// ImmutableUtxoOffsetsTipSlot reports the latest slot for which the Mithril
// immutable-copy phase persisted produced-UTxO offset references. The second
// return value is false when no immutable copy has run (the chain state value
// is unset); callers must treat that as "no skip threshold" and write offsets
// normally.
func ImmutableUtxoOffsetsTipSlot(
	db *database.Database,
) (uint64, bool, error) {
	val, err := immutableUtxoOffsetsMark(db)
	if err != nil {
		return 0, false, fmt.Errorf(
			"checking immutable UTxO offset state: %w",
			err,
		)
	}
	if val == "" {
		return 0, false, nil
	}
	slot, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf(
			"parsing immutable UTxO offset state %q: %w",
			val,
			err,
		)
	}
	return slot, true, nil
}

// MarkImmutableUtxoOffsetsComplete records that produced-UTxO offset references
// are stored for every immutable block up to immutableTipSlot, so a later API
// backfill can skip re-writing them. It is idempotent. Callers that split the
// immutable copy (for example the pipelined download/copy) must call this after
// the copy completes, because the per-block copy stores the offsets but does
// not advance this marker on its own.
func MarkImmutableUtxoOffsetsComplete(
	db *database.Database,
	immutableTipSlot uint64,
) error {
	return markImmutableUtxoOffsetsComplete(db, immutableTipSlot)
}

func markImmutableUtxoOffsetsComplete(
	db *database.Database,
	immutableTipSlot uint64,
) error {
	set := db.SetChainState
	if db.IsIndexer() {
		set = db.SetSyncState
	}
	if err := set(
		database.ImmutableUtxoOffsetsTipKey,
		strconv.FormatUint(immutableTipSlot, 10),
		nil,
	); err != nil {
		return fmt.Errorf(
			"marking immutable UTxO offset state complete: %w",
			err,
		)
	}
	return nil
}

func backfillRawBlockCallbacks(
	ctx context.Context,
	imm *immutable.ImmutableDb,
	db *database.Database,
	callback func(chain.RawBlock, *database.Txn) error,
) (int, error) {
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	if err != nil {
		return 0, fmt.Errorf("failed to get immutable DB iterator: %w", err)
	}
	defer iter.Close()

	var blocksBackfilled int
	blockBatch := make([]chain.RawBlock, 0, loadBlockBatchSize)
	flush := func() error {
		if len(blockBatch) == 0 {
			return nil
		}
		txn := db.BlobTxn(true)
		defer txn.Release()
		if err := txn.Do(func(txn *database.Txn) error {
			for _, rb := range blockBatch {
				if err := callback(rb, txn); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		blocksBackfilled += len(blockBatch)
		blockBatch = blockBatch[:0]
		return nil
	}

	for {
		next, err := iter.Next()
		if err != nil {
			return blocksBackfilled, fmt.Errorf("reading next block: %w", err)
		}
		if next == nil {
			break
		}
		rawBlock, err := rawBlockFromImmutableBlock(next)
		if err != nil {
			return blocksBackfilled, fmt.Errorf("building raw block: %w", err)
		}
		blockBatch = append(blockBatch, rawBlock)
		if len(blockBatch) == cap(blockBatch) {
			if err := flush(); err != nil {
				return blocksBackfilled, err
			}
			if err := ctx.Err(); err != nil {
				return blocksBackfilled, fmt.Errorf("loading blocks: %w", err)
			}
		}
	}
	if err := flush(); err != nil {
		return blocksBackfilled, err
	}
	if err := ctx.Err(); err != nil {
		return blocksBackfilled, fmt.Errorf("loading blocks: %w", err)
	}
	return blocksBackfilled, nil
}

func rawBlockFromImmutableBlock(
	block *immutable.Block,
) (chain.RawBlock, error) {
	// Extract header CBOR from the block's outer array (first element for all
	// eras), then decode just the header without decoding transaction bodies.
	headerCbor, err := extractHeaderCbor(block.Cbor)
	if err != nil {
		return chain.RawBlock{}, fmt.Errorf(
			"extracting block header CBOR: %w",
			err,
		)
	}
	header, err := gledger.NewBlockHeaderFromCbor(block.Type, headerCbor)
	if err != nil {
		return chain.RawBlock{}, fmt.Errorf("decoding block header: %w", err)
	}
	return chain.RawBlock{
		Slot:        header.SlotNumber(),
		Hash:        header.Hash().Bytes(),
		BlockNumber: header.BlockNumber(),
		Type:        block.Type,
		PrevHash:    header.PrevHash().Bytes(),
		Cbor:        block.Cbor,
	}, nil
}

func storeRawBlockUtxoOffsets(
	txn *database.Txn,
	block chain.RawBlock,
) (int, error) {
	// Byron epoch-boundary blocks carry no transactions. Mainnet's slot-0
	// EBB body can look like a large Shelley-style tx body array to the
	// generic offset extractor, so skip it before attempting extraction.
	if block.Type == gledger.BlockTypeByronEbb {
		return 0, nil
	}
	if txn == nil || txn.Blob() == nil {
		return 0, errors.New("blob transaction not available")
	}
	blob := txn.BlobStore()
	if blob == nil {
		return 0, errors.New("blob store not available")
	}
	if block.Type == gledger.BlockTypeDijkstra {
		return storeDijkstraRawBlockUtxoOffsets(txn, blob, block)
	}
	var blockHash [32]byte
	copy(blockHash[:], block.Hash)
	totalUtxos := 0

	offsets, extractErr := lcommon.ExtractTransactionOffsets(block.Cbor)
	if extractErr != nil {
		return 0, fmt.Errorf(
			"block at slot %d: extract transaction offsets: %w",
			block.Slot,
			extractErr,
		)
	}
	if offsets == nil || len(offsets.Transactions) == 0 {
		return 0, nil
	}
	validity, err := rawBlockTransactionValidity(offsets)
	if err != nil {
		return 0, fmt.Errorf("block at slot %d: %w", block.Slot, err)
	}
	for txIdx, txLoc := range offsets.Transactions {
		bodyEnd := txLoc.Body.Offset + txLoc.Body.Length
		if int(bodyEnd) > len(block.Cbor) {
			return 0, fmt.Errorf(
				"block at slot %d: body range [%d:%d] exceeds block size %d",
				block.Slot,
				txLoc.Body.Offset,
				bodyEnd,
				len(block.Cbor),
			)
		}
		bodyBytes := block.Cbor[txLoc.Body.Offset:bodyEnd]
		txHash := lcommon.Blake2b256Hash(bodyBytes)
		if validity[txIdx] {
			for i, outLoc := range txLoc.Outputs {
				if outLoc.Length == 0 {
					continue
				}
				offset := database.CborOffset{
					BlockSlot:  block.Slot,
					BlockHash:  blockHash,
					ByteOffset: outLoc.Offset,
					ByteLength: outLoc.Length,
				}
				if err := blob.SetUtxo(
					txn.Blob(),
					txHash[:],
					uint32(i), // #nosec G115
					database.EncodeUtxoOffset(&offset),
				); err != nil {
					return 0, fmt.Errorf("storing UTxO offset: %w", err)
				}
				totalUtxos++
			}
			continue
		}
		collReturnOffset, collReturnLen, found, err := txBodyMapValueRange(
			block.Cbor[txLoc.Body.Offset:bodyEnd],
			txLoc.Body.Offset,
			database.TxBodyKeyCollateralReturn,
		)
		if err != nil {
			return 0, fmt.Errorf(
				"block at slot %d: transaction %x collateral return offset: %w",
				block.Slot,
				txHash[:8],
				err,
			)
		}
		if !found {
			continue
		}
		offset := database.CborOffset{
			BlockSlot:  block.Slot,
			BlockHash:  blockHash,
			ByteOffset: collReturnOffset,
			ByteLength: collReturnLen,
		}
		outputIdx := uint32(len(txLoc.Outputs)) // #nosec G115
		if err := blob.SetUtxo(
			txn.Blob(),
			txHash[:],
			outputIdx,
			database.EncodeUtxoOffset(&offset),
		); err != nil {
			return 0, fmt.Errorf(
				"storing collateral return UTxO offset: %w",
				err,
			)
		}
		totalUtxos++
	}
	return totalUtxos, nil
}

// storeDijkstraRawBlockUtxoOffsets stores the UTxO offsets of a Dijkstra
// block from its decoded transactions. A CDDL Dijkstra block marks phase-2
// validity with each transaction's trailing is_valid field, which the raw
// offset extractor does not report, and a valid transaction also produces its
// sub-transactions' outputs. The block indexer follows Produced(), as ledger
// block application does, so the copy stores the same UTxO set.
func storeDijkstraRawBlockUtxoOffsets(
	txn *database.Txn,
	store blob.BlobStore,
	block chain.RawBlock,
) (int, error) {
	// Stored blocks can keep the early Musashi layout, which only the
	// stored-block decoder accepts.
	decoded, err := models.DecodeBlockCbor(block.Type, block.Cbor)
	if err != nil {
		return 0, fmt.Errorf(
			"block at slot %d: decode Dijkstra block: %w",
			block.Slot,
			err,
		)
	}
	offsets, err := database.NewBlockIndexer(block.Slot, block.Hash).
		ComputeOffsets(block.Cbor, decoded)
	if err != nil {
		return 0, fmt.Errorf(
			"block at slot %d: compute Dijkstra UTxO offsets: %w",
			block.Slot,
			err,
		)
	}
	for ref, offset := range offsets.UtxoOffsets {
		if err := store.SetUtxo(
			txn.Blob(),
			ref.TxId[:],
			ref.OutputIdx,
			database.EncodeUtxoOffset(&offset),
		); err != nil {
			return 0, fmt.Errorf("storing UTxO offset: %w", err)
		}
	}
	return len(offsets.UtxoOffsets), nil
}

// rawBlockTransactionValidity reads invalid_transactions the way
// cardano-ledger's alignedValidFlags does. The list is walked in wire order,
// not treated as a set: a descending index marks only the later transaction
// and a repeated index also marks the one after it, so [1, 0] leaves the
// first transaction valid and [0, 0] invalidates both. The reference block
// decoder rejects an index outside the transaction list, and so does this.
func rawBlockTransactionValidity(
	offsets *lcommon.BlockTransactionOffsets,
) ([]bool, error) {
	count := len(offsets.Transactions)
	for _, index := range offsets.InvalidTransactions {
		if index >= uint(count) {
			return nil, fmt.Errorf(
				"invalid transaction index %d outside transaction list length %d",
				index,
				count,
			)
		}
	}
	return lcommon.TransactionValidityFlags(
		count,
		offsets.InvalidTransactions,
	), nil
}

func txBodyMapValueRange(
	bodyCbor []byte,
	bodyOffset uint32,
	key uint64,
) (uint32, uint32, bool, error) {
	decoder, err := gcbor.NewStreamDecoder(bodyCbor)
	if err != nil {
		return 0, 0, false, err
	}
	count, _, _, err := decoder.DecodeMapHeader()
	if err != nil {
		return 0, 0, false, err
	}
	if count < 0 {
		return 0, 0, false, errors.New("indefinite tx body map")
	}
	for range count {
		var currentKey uint64
		if _, _, err := decoder.Decode(&currentKey); err != nil {
			return 0, 0, false, err
		}
		valueOffset, valueLen, err := decoder.Skip()
		if err != nil {
			return 0, 0, false, err
		}
		if currentKey != key {
			continue
		}
		valueOffset32, err := checkedUint32(valueOffset)
		if err != nil {
			return 0, 0, false, err
		}
		valueLen32, err := checkedUint32(valueLen)
		if err != nil {
			return 0, 0, false, err
		}
		if valueOffset32 > ^uint32(0)-bodyOffset {
			return 0, 0, false, fmt.Errorf(
				"value offset %d overflows body offset %d",
				valueOffset32,
				bodyOffset,
			)
		}
		return bodyOffset + valueOffset32, valueLen32, true, nil
	}
	return 0, 0, false, nil
}

func checkedUint32(v int) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("negative value %d", v)
	}
	if uint64(v) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("value %d overflows uint32", v)
	}
	return uint32(v), nil // #nosec G115
}

func reportLoadBlobsProgress(
	onProgress func(LoadBlobsProgress),
	blocksCopied int,
	currentSlot uint64,
	tipSlot uint64,
	startTime time.Time,
) {
	if onProgress == nil {
		return
	}
	elapsed := time.Since(startTime).Seconds()
	progress := LoadBlobsProgress{
		BlocksCopied: blocksCopied,
		CurrentSlot:  currentSlot,
		TipSlot:      tipSlot,
	}
	if elapsed > 0 {
		progress.BlocksPerSecond = float64(blocksCopied) / elapsed
	}
	if tipSlot > 0 {
		progress.Percent = float64(currentSlot) / float64(tipSlot) * 100
	}
	onProgress(progress)
}

func maybeLogBlockCopyProgress(
	logger *slog.Logger,
	msg string,
	blocksCopied int,
	currentSlot uint64,
	tipSlot uint64,
	startTime time.Time,
	lastLogTime *time.Time,
) {
	now := time.Now()
	if !lastLogTime.IsZero() && now.Sub(*lastLogTime) < progressLogInterval {
		return
	}
	*lastLogTime = now

	elapsed := now.Sub(startTime)
	attrs := []any{
		"blocks_copied", blocksCopied,
		"slot", currentSlot,
	}
	if elapsed > 0 {
		attrs = append(
			attrs,
			"blocks_per_sec",
			fmt.Sprintf("%.0f", float64(blocksCopied)/elapsed.Seconds()),
		)
	}
	if tipSlot > 0 {
		attrs = append(
			attrs,
			"progress",
			fmt.Sprintf("%.1f%%", float64(currentSlot)/float64(tipSlot)*100),
		)
	}
	logger.Info(msg, attrs...)
}

// extractHeaderCbor extracts the header CBOR from a full block's CBOR.
// All Cardano block eras encode as a CBOR array where the first element
// is the block header.
//
// This uses fxamacker/cbor's package-level UnmarshalFirst directly
// (bare defaults: 131,072 max array elements/map pairs, 32 max
// nested levels) rather than gouroboros's raised-limit wrapper,
// because it only ever extracts a single block header's raw bytes
// (at most a few KB, with shallow nesting) — not a mainnet-scale
// map or array, so the fxamacker defaults are already more than
// sufficient here.
func extractHeaderCbor(blockCbor []byte) ([]byte, error) {
	headerLen, err := cborArrayHeaderLen(blockCbor)
	if err != nil {
		return nil, err
	}
	var headerCbor gcbor.RawMessage
	if _, err := fxcbor.UnmarshalFirst(blockCbor[headerLen:], &headerCbor); err != nil {
		return nil, fmt.Errorf("decoding block header CBOR: %w", err)
	}
	if len(headerCbor) == 0 {
		return nil, errors.New("empty block header")
	}
	return []byte(headerCbor), nil
}

func cborArrayHeaderLen(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, errors.New("empty CBOR data")
	}
	majorType := data[0] & gcbor.CborTypeMask
	if majorType != gcbor.CborTypeArray {
		return 0, errors.New("block CBOR is not an array")
	}
	additional := data[0] &^ gcbor.CborTypeMask
	switch {
	case additional <= gcbor.CborMaxUintSimple:
		return 1, nil
	case additional == 24:
		if len(data) < 2 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 2, nil
	case additional == 25:
		if len(data) < 3 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 3, nil
	case additional == 26:
		if len(data) < 5 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 5, nil
	case additional == 27:
		if len(data) < 9 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 9, nil
	case additional == 31:
		return 1, nil
	default:
		return 0, fmt.Errorf(
			"unsupported CBOR array additional info: %d",
			additional,
		)
	}
}
