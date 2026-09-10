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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/mempool"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/plugin"
	gouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// liveLifecycleTestDataDir returns the immutable testdata directory used
// elsewhere in the repo (internal/integration, database package tests) —
// real preview-testnet blocks.
func liveLifecycleTestDataDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(
		filepath.Dir(thisFile),
		"database",
		"immutable",
		"testdata",
	)
}

// newLiveLifecycleTestNode hand-builds a partial but real *Node — real
// database, chain manager, ledger state (loaded with real blocks), event
// bus, and ouroboros object, wired the same way Run() wires them — without
// going through Run() itself, matching this file's existing
// newNodeTestDivergedLedger-style test convention (real subsystems, no
// networking/APIs/block production). Returns the node and the ordered
// points of every block loaded, for resolving truncate/verifying restore
// targets.
func newLiveLifecycleTestNode(
	t *testing.T,
	numBlocks int,
) (*Node, []ocommon.Point) {
	t.Helper()
	return newLiveLifecycleTestNodeWithGenesis(
		t, numBlocks, nil, disabledLiveLifecycleTestWorkerPoolCfg,
	)
}

// disabledLiveLifecycleTestWorkerPoolCfg is the dbWorkerPool config every
// live-lifecycle test used before workerPoolCfg became overridable —
// disabled, so most tests don't pay for or synchronize around a real async
// worker they don't care about.
var disabledLiveLifecycleTestWorkerPoolCfg = ledger.DatabaseWorkerPoolConfig{
	WorkerPoolSize: 1,
	TaskQueueSize:  1,
	Disabled:       true,
}

// newLiveLifecycleTestNodeWithGenesis is newLiveLifecycleTestNode with an
// overridable Cardano genesis config — nil uses the real preview genesis
// (newNodeTestCardanoNodeCfg), matching newLiveLifecycleTestNode. Tests that
// need epoch boundaries to actually fall within their loaded block range
// (the real preview genesis's epochLength is far larger than any small
// block count could reach) pass a custom config with a small epochLength
// instead. workerPoolCfg is overridable too, for tests that need a real
// (not disabled) dbWorkerPool worker to hold busy.
func newLiveLifecycleTestNodeWithGenesis(
	t *testing.T,
	numBlocks int,
	cardanoNodeCfgOverride *cardano.CardanoNodeConfig,
	workerPoolCfg ledger.DatabaseWorkerPoolConfig,
) (*Node, []ocommon.Point) {
	t.Helper()

	tmpDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	pluginHost, err := internalplugins.NewHost()
	require.NoError(t, err)
	storageSelections := internalplugins.StorageSelections{
		Blob:     plugin.Selection{Provider: "badger"},
		Metadata: plugin.Selection{Provider: "sqlite"},
	}
	stores, err := internalplugins.ResolveStorage(
		context.Background(),
		pluginHost,
		storageSelections,
		internalplugins.StorageDependencies{DataDir: tmpDir, Logger: logger},
	)
	require.NoError(t, err)
	db, err := database.New(&database.Config{
		DataDir: tmpDir,
		Logger:  logger,
		Network: "preview",
	}, stores)
	require.NoError(t, err)

	eventBus := event.NewEventBus(nil, nil)

	cm, err := chain.NewManager(db, eventBus)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(nodeTestSecurityParamLedger{securityParam: 432}),
	)

	points := loadLiveLifecycleTestBlocks(t, cm.PrimaryChain(), numBlocks)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       points[len(points)-1],
		BlockNumber: uint64(len(points)),
	}, nil))

	cardanoNodeCfg := cardanoNodeCfgOverride
	if cardanoNodeCfg == nil {
		cardanoNodeCfg = newNodeTestCardanoNodeCfg(t)
	}

	cfg := NewConfig(
		WithDatabasePath(tmpDir),
		WithLogger(logger),
		WithNetwork("preview"),
		WithCardanoNodeConfig(cardanoNodeCfg),
		WithPluginSelection(
			plugin.CapabilityStorageBlob,
			storageSelections.Blob,
		),
		WithPluginSelection(
			plugin.CapabilityStorageMetadata,
			storageSelections.Metadata,
		),
		WithDatabaseWorkerPoolConfig(workerPoolCfg),
	)

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		config:       cfg,
		eventBus:     eventBus,
		db:           db,
		pluginHost:   pluginHost,
		chainManager: cm,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Build the initial LedgerState through the node's own
	// ledgerStateConfig(), the way Run() does, instead of a hand-written
	// config literal. reinitializeCoreStorage rebuilds it from that same
	// method, so a behavior asserted before and after a live
	// restore/truncate is compared against identical production wiring on
	// both sides rather than against a harness config that happens to omit
	// the field under test.
	ledgerState, err := ledger.NewLedgerState(n.ledgerStateConfig())
	require.NoError(t, err)
	n.ledgerState = ledgerState
	// Build the harness ouroboros the way Run() does: every dependency up
	// front, through the validating constructor, so the live-restore
	// reconstruction assertions below exercise the production path rather
	// than a hand-assembled instance.
	harnessMempool, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       ledgerState,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	harnessConnManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	ouroborosCfg := ouroborosPkg.OuroborosConfig{
		Logger:         logger,
		EventBus:       eventBus,
		NetworkMagic:   2, // preview
		LedgerState:    ledgerState,
		Mempool:        &mempool.FIFO{Mempool: harnessMempool},
		ChainsyncState: chainsync.NewState(eventBus, ledgerState),
		ConnManager:    harnessConnManager,
		PeerGov: peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger:      logger,
			EventBus:    eventBus,
			ConnManager: harnessConnManager,
		}),
	}
	ouro, err := ouroborosPkg.NewOuroboros(ouroborosCfg)
	require.NoError(t, err)
	// Retained so reinitializeNetworkingCore can rebuild ouroboros the way
	// Run() does.
	n.ouroborosConfig = ouroborosCfg
	n.ouroborosRef.Store(ouro)
	require.NoError(t, ledgerState.Start(context.Background()))
	t.Cleanup(func() {
		cancel()
		if n.pluginHost != nil {
			_ = n.pluginHost.StopCapability(
				context.Background(),
				plugin.CapabilityMempool,
			)
		}
		if n.connManager != nil {
			_ = n.connManager.Stop(context.Background())
		}
		if n.peerGov != nil {
			_ = n.peerGov.Stop(context.Background())
		}
		if n.snapshotMgr != nil {
			_ = n.snapshotMgr.Stop()
		}
		if n.dbLifecycleMgr != nil {
			_ = n.dbLifecycleMgr.Stop()
		}
		if n.ledgerState != nil {
			_ = n.ledgerState.Close()
		}
		if n.db != nil {
			_ = n.db.Close()
		}
		if n.pluginHost != nil {
			_ = n.pluginHost.Stop(context.Background())
		}
		eventBus.Stop()
	})

	return n, points
}

// loadLiveLifecycleTestBlocks loads numBlocks real blocks from the
// immutable testdata into c, mirroring
// internal/integration.loadBlocksFromImmutable (a different package, so
// not directly reusable).
func loadLiveLifecycleTestBlocks(
	t *testing.T,
	c *chain.Chain,
	numBlocks int,
) []ocommon.Point {
	t.Helper()
	imm, err := immutable.New(liveLifecycleTestDataDir())
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err)
	defer iter.Close()

	var points []ocommon.Point
	for range numBlocks {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		require.NoError(t, c.AddBlock(block, nil))
		points = append(points, ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		})
	}
	require.NotEmpty(t, points, "no blocks loaded from testdata")
	if len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(points),
			numBlocks,
		)
	}
	return points
}

// loadRawLiveLifecycleTestBlocks loads the first numBlocks real blocks from
// the immutable testdata WITHOUT adding them to any chain, so a caller can
// feed them in one at a time later (e.g. to simulate new blocks arriving
// live after a truncate/restore, extending from whatever tip the operation
// left rather than from the original full chain).
func loadRawLiveLifecycleTestBlocks(
	t *testing.T,
	numBlocks int,
) []gledger.Block {
	t.Helper()
	imm, err := immutable.New(liveLifecycleTestDataDir())
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err)
	defer iter.Close()

	var blocks []gledger.Block
	for range numBlocks {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blocks = append(blocks, block)
	}
	if len(blocks) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}
	return blocks
}

// lifecycleSnapshot snapshots n's current database directly via
// database/lifecycle, the same primitive internal/dblifecycle.Service
// uses for the offline CLI path.
func lifecycleSnapshot(
	t *testing.T,
	n *Node,
	destDir string,
) (lifecycle.Manifest, error) {
	t.Helper()
	return lifecycle.Snapshot(
		context.Background(),
		n.db,
		destDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
}

// TestInitLeiosManagersDoNotRequireOuroboros pins the startup ordering. Run
// initializes the Leios managers well before it constructs Ouroboros, so the
// init path must not reach for the instance: doing so dereferences a nil
// pointer and panics before the node ever finishes starting. The handlers are
// attached separately, once the instance exists, by attachLeiosHandlers.
func TestInitLeiosManagersDoNotRequireOuroboros(t *testing.T) {
	n, _ := newLiveLifecycleTestNode(t, 2)
	// Exactly the state Run is in when it reaches the Leios init calls.
	n.ouroborosRef.Store(nil)
	require.Nil(t, n.ouroboros())

	require.NotPanics(t, func() {
		require.NoError(t, n.initLeiosVoteManager(t.Context()))
		require.NoError(t, n.initLeiosPipelineManager(t.Context()))
	})
	require.NotNil(t, n.leiosVoteManager)
	require.NotNil(t, n.leiosPipelineManager)
}

// TestLiveLifecycleRebuildPreservesLeiosHandlers pins a regression that the
// storage-rebuild assertions cannot see. The Leios vote and pipeline handlers
// are not part of OuroborosConfig -- their managers start on a separate path
// that runs BEFORE reinitializeNetworkingCore replaces Ouroboros -- so a
// rebuild that does not carry them across silently drops Leios vote and
// pipeline handling on a Dijkstra node, while everything else keeps working.
func TestLiveLifecycleRebuildPreservesLeiosHandlers(t *testing.T) {
	n, _ := newLiveLifecycleTestNode(t, 4)

	// Zero-value managers: this asserts only that the same handlers survive
	// the rebuild, and never invokes them.
	votes := &leios.VoteManager{}
	pipeline := &leios.PipelineManager{}
	n.leiosVoteManager = votes
	n.leiosPipelineManager = pipeline
	require.NoError(t, n.ouroboros().SetLeiosVotes(votes))
	require.NoError(t, n.ouroboros().SetLeiosPipeline(pipeline))

	before := n.ouroboros()
	require.NoError(t, n.reinitializeNetworkingCore(context.Background()))

	require.NotSame(t, before, n.ouroboros())
	require.NotNil(
		t,
		n.ouroboros().LeiosVotes(),
		"leios vote handler was dropped by the rebuild",
	)
	require.NotNil(
		t,
		n.ouroboros().LeiosPipeline(),
		"leios pipeline handler was dropped by the rebuild",
	)
	require.Same(t, votes, n.ouroboros().LeiosVotes())
	require.Same(t, pipeline, n.ouroboros().LeiosPipeline())
}

// TestLiveTruncateRebuildsStorageAndKeepsNodeUsable is the core Phase 2
// verification: truncating a real, multi-block chain on an already-running
// node's live objects must (a) actually remove the truncated blocks and
// fix up the tip, (b) rebuild every storage-dependent subsystem so it
// points at the new db/ledgerState rather than the closed ones, and (c)
// leave n.ctx alone, so the node's normal shutdown signalling is
// unaffected by having gone through a live truncate.
func TestLiveTruncateRebuildsStorageAndKeepsNodeUsable(t *testing.T) {
	const numBlocks = 20
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	oldDB := n.db
	oldLedgerState := n.ledgerState
	oldChainManager := n.chainManager
	oldCtx := n.ctx
	ouroBeforeRestore := n.ouroboros()

	targetIndex := numBlocks / 2
	targetSlot := points[targetIndex].Slot

	blocksRemoved, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{
			Slot: &targetSlot,
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(numBlocks-1-targetIndex), blocksRemoved)

	// n.ctx must survive untouched — a live truncate must never cancel or
	// replace it, or SIGINT/SIGTERM handling would silently stop working.
	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())

	// Storage objects must be genuinely new, not the same (now-closed)
	// instances.
	require.NotSame(t, oldDB, n.db)
	require.NotSame(t, oldLedgerState, n.ledgerState)
	require.NotSame(t, oldChainManager, n.chainManager)

	// Every rebuilt subsystem must exist.
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)
	require.NotNil(t, n.snapshotMgr)
	require.NotNil(t, n.dbLifecycleMgr)

	// Ouroboros takes its dependencies at construction and never reassigns
	// them, so rebuilding those dependencies must have produced a REPLACEMENT
	// instance holding the new ones -- not the original still pointing at the
	// closed ledger, mempool and connection manager.
	require.NotSame(t, ouroBeforeRestore, n.ouroboros())
	require.Same(t, n.ledgerState, n.ouroboros().LedgerState())
	require.Same(t, n.mempool, n.ouroboros().Mempool())
	require.Same(t, n.chainsyncState, n.ouroboros().ChainsyncState())
	require.Same(t, n.connManager, n.ouroboros().ConnManager())
	require.Same(t, n.peerGov, n.ouroboros().PeerGov())

	// Everything the node handed to other components as a callback must
	// resolve the replacement, not the instance it was built alongside. This
	// is the failure mode that silently stops a restored node from syncing:
	// the callbacks still run, but against a closed Ouroboros. Asserting on
	// the connection manager's providers covers the listener and outbound
	// paths, which are resolved lazily for exactly this reason.
	require.NotNil(t, n.connManager.ResolvedListeners())

	// The truncate itself must have taken effect: tip at the target, and
	// blocks after it gone from the new database.
	tip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, targetSlot, tip.Point.Slot)

	for i, p := range points {
		_, err := database.BlockByHash(n.db, p.Hash)
		if i <= targetIndex {
			require.NoErrorf(
				t,
				err,
				"block %d (slot %d) should survive",
				i,
				p.Slot,
			)
		} else {
			require.Errorf(t, err, "block %d (slot %d) should be truncated away", i, p.Slot)
		}
	}
}

// TestLiveTruncateReinitializationPreservesDelegatorInactivityConfig
// guards against the reconstructed ledger.LedgerStateConfig in
// node_lifecycle.go silently dropping operator-configured fields it
// doesn't explicitly copy from n.config -- the same class of bug
// previously found for MinPoolMargin/PledgeLeverageEnabled/
// PledgeLeverage/FullPotRewardsEnabled (see the comment at that
// construction site). A node configured for CIP-0163 delegator-
// inactivity expiry must keep that gate enabled, with its exact
// configured window, after a live truncate rebuilds its LedgerState --
// not silently disabled until a full process restart.
func TestLiveTruncateReinitializationPreservesDelegatorInactivityConfig(
	t *testing.T,
) {
	const numBlocks = 20
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	n.config.delegatorInactivityEnabled = true
	n.config.delegatorInactivity = 42

	targetIndex := numBlocks / 2
	targetSlot := points[targetIndex].Slot

	_, err := n.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)

	enabled, window := n.ledgerState.DelegatorInactivityConfig()
	require.True(
		t,
		enabled,
		"DelegatorInactivityEnabled must survive live truncate reinitialization",
	)
	require.Equal(
		t,
		uint64(42),
		window,
		"DelegatorInactivity window must survive live truncate reinitialization",
	)
}

// TestLiveTruncateReinitializationPreservesSnapshotManagerDelegatorInactivityConfig
// guards the companion gap to the test above: the rebuilt stake/reward
// snapshot manager (n.snapshotMgr) also needs SetDelegatorInactivity called
// with the operator's configured value before CaptureGenesisSnapshot/Start
// locks its configuration -- reinitializeBackgroundManagers used to skip
// this call entirely (unlike Run()'s identical one in node.go), so a live
// restore/truncate would silently capture post-CIP-163 stake and reward
// snapshots using pre-CIP behavior even though the operator enabled it.
func TestLiveTruncateReinitializationPreservesSnapshotManagerDelegatorInactivityConfig(
	t *testing.T,
) {
	const numBlocks = 20
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	n.config.delegatorInactivityEnabled = true
	n.config.delegatorInactivity = 42

	targetIndex := numBlocks / 2
	targetSlot := points[targetIndex].Slot

	_, err := n.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)

	enabled, window := n.snapshotMgr.DelegatorInactivityConfig()
	require.True(
		t,
		enabled,
		"snapshot manager's DelegatorInactivityEnabled must survive live truncate reinitialization",
	)
	require.Equal(
		t,
		uint64(42),
		window,
		"snapshot manager's DelegatorInactivity window must survive live truncate reinitialization",
	)
}

// genesisSelectionTestWindowSlots is the Genesis density window the test
// chain selector reports. The preview testdata blocks are 20 slots apart,
// so a 25-slot window anchored at a fork intersection contains exactly one
// local block: a competing fork with two headers packed into the slots
// right after that intersection has density two against the local chain's
// one, while still being SHORTER in block number. That is the only shape
// where the Genesis density decision and the Praos length decision
// disagree, which is what makes the fork tests fail if the injected
// GenesisSelectionStateFunc goes missing.
const genesisSelectionTestWindowSlots = 25

// newGenesisSelectionTestNode is newLiveLifecycleTestNode plus the two
// fields fork resolution needs before it will consult Genesis selection at
// all: n.chainSelector, the source the injected GenesisSelectionStateFunc
// resolves, parked in Genesis mode; and n.connManager, because the ledger
// drops a chainsync event whose connection is not registered with it.
func newGenesisSelectionTestNode(
	t *testing.T,
	numBlocks int,
) (*Node, []ocommon.Point) {
	t.Helper()
	n, points := newLiveLifecycleTestNode(t, numBlocks)
	// Reuse the connection manager the harness ouroboros was built with, so
	// the ledger's liveness gate and its blockfetch requests resolve the
	// same instance -- exactly as they do after a live rebuild, where
	// reinitializeNetworkingCore hands the rebuilt manager to both.
	n.connManager = n.ouroboros().ConnManager()
	// No EventBus: the selector must stay in Genesis mode for the whole
	// test, and a live one would let peer-tip and rollback events drive its
	// one-way transition to Praos.
	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			Logger:             n.config.logger,
			GenesisMode:        true,
			GenesisWindowSlots: genesisSelectionTestWindowSlots,
		},
	)
	active, window := n.chainSelector.GenesisSelectionState()
	require.True(t, active, "test selector must start in Genesis mode")
	require.Equal(t, uint64(genesisSelectionTestWindowSlots), window)
	return n, points
}

// registerGenesisForkTestPeer registers a mock node-to-node connection with
// the node's CURRENT connection manager and returns its connection ID.
// ledger.LedgerState ignores a chainsync event whose connection is not
// registered, so each phase of a lifecycle test needs its own peer: the
// live rebuild replaces the connection manager along with everything else.
func registerGenesisForkTestPeer(
	t *testing.T,
	n *Node,
) gouroboros.ConnectionId {
	t.Helper()
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		ouroboros_mock.ConversationKeepAlive,
	)
	oConn, err := gouroboros.New(
		gouroboros.WithConnection(mockConn),
		gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		gouroboros.WithNodeToNode(true),
	)
	require.NoError(t, err)
	require.True(
		t,
		n.connManager.AddConnection(oConn, false, "127.0.0.1:33273"),
	)
	t.Cleanup(func() { _ = oConn.Close() })
	return oConn.Id()
}

// requireGenesisDeepForkWins drives a competing fork that only Ouroboros
// Genesis density selection can win, and requires the ledger to switch to
// it.
//
// The fork branches off the local chain at points[ancestorIdx] -- several
// blocks behind the local tip, so resolving it is a deep rollback -- and
// carries two headers packed into the first few slots after that
// intersection. It is strictly SHORTER than the local chain in block
// number and its headers sit at earlier slots than the local tip, so Praos
// length comparison rejects it outright: without Genesis selection the
// ledger discards both headers as stale roll-forwards and the tip never
// moves. With Genesis selection active the same two headers are denser
// inside the window anchored at the intersection than the local chain is
// (two blocks versus one), so the ledger must roll back to the
// intersection.
//
// The first header alone only ties the local density, which exercises the
// documented equal-density fallback to Praos; the second header is what
// makes the peer's branch win, and it can only be evaluated by walking the
// peer's recorded header history back to the intersection.
func requireGenesisDeepForkWins(
	t *testing.T,
	n *Node,
	points []ocommon.Point,
	ancestorIdx int,
) {
	t.Helper()
	require.Greater(t, len(points), ancestorIdx+1)
	ancestor := points[ancestorIdx]
	localTipBefore := n.ledgerState.Tip()
	require.Greater(
		t,
		localTipBefore.Point.Slot,
		ancestor.Slot,
		"the local tip must be ahead of the fork intersection for this to be a deep fork",
	)

	// Real Conway blocks from the shared ouroboros-mock fixtures rather than
	// a locally defined header stub: their headers round-trip through CBOR,
	// so the ledger sees the same hashes and prev-hashes a peer would send.
	// Block numbers continue from the intersection, which keeps the branch
	// shorter than the local chain.
	forkBlocks, err := fixtures.GenerateConwayChain(
		uint64(ancestorIdx)+1,
		lcommon.NewBlake2b256(ancestor.Hash),
		ancestor.Slot+2,
		2,
		2,
	)
	require.NoError(t, err)
	forkTipBlock := forkBlocks[len(forkBlocks)-1]
	require.Less(
		t,
		forkTipBlock.SlotNumber(),
		localTipBefore.Point.Slot,
		"the fork must stay behind the local tip so Praos alone rejects it",
	)
	require.Less(
		t,
		forkTipBlock.BlockNumber(),
		localTipBefore.BlockNumber,
		"the fork must be shorter than the local chain so Praos alone rejects it",
	)
	forkTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			forkTipBlock.SlotNumber(),
			forkTipBlock.Hash().Bytes(),
		),
		BlockNumber: forkTipBlock.BlockNumber(),
	}

	connId := registerGenesisForkTestPeer(t, n)
	for _, blk := range forkBlocks {
		n.eventBus.Publish(
			ledger.ChainsyncEventType,
			event.NewEvent(ledger.ChainsyncEventType, ledger.ChainsyncEvent{
				ConnectionId: connId,
				Point: ocommon.NewPoint(
					blk.SlotNumber(),
					blk.Hash().Bytes(),
				),
				BlockHeader: blk.Header(),
				Tip:         forkTip,
				BlockNumber: blk.BlockNumber(),
				Type:        uint(blk.Type()),
			}),
		)
	}

	require.EventuallyWithT(
		t,
		func(collect *assert.CollectT) {
			assert.Equal(
				collect,
				ancestor.Slot,
				n.ledgerState.Tip().Point.Slot,
				"current ledger tip slot",
			)
		},
		10*time.Second,
		20*time.Millisecond,
		"ledger did not roll back to the Genesis-selected fork intersection at slot %d",
		ancestor.Slot,
	)
}

// TestLiveTruncatePreservesGenesisForkSelection is the behavioral guard for
// issue #3273: reinitializeCoreStorage rebuilt LedgerStateConfig without
// GenesisSelectionStateFunc, so a node that had Ouroboros Genesis selection
// active silently fell back to Praos-length-only fork resolution after a
// live truncate and stayed there until the process restarted.
//
// Both halves drive the same deep fork that only Genesis density can win --
// before the truncate against the ledger Run() would have built, and after
// it against the one the lifecycle path rebuilt.
func TestLiveTruncatePreservesGenesisForkSelection(t *testing.T) {
	const numBlocks = 25
	n, points := newGenesisSelectionTestNode(t, numBlocks)

	// Before: Genesis density resolves a deep fork 4 blocks behind the tip.
	requireGenesisDeepForkWins(t, n, points, numBlocks-5)

	targetSlot := points[10].Slot
	_, err := n.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)
	require.Equal(t, targetSlot, n.ledgerState.Tip().Point.Slot)

	// After: the rebuilt ledger must still consult the same selector. A
	// different intersection is used because the truncate discarded the
	// blocks the first half rolled back to.
	requireGenesisDeepForkWins(t, n, points, 5)
}

// TestLiveRestorePreservesGenesisForkSelection is
// TestLiveTruncatePreservesGenesisForkSelection for the Restore half of the
// same live-lifecycle path, which rebuilds the ledger through the same
// reinitializeCoreStorage call.
func TestLiveRestorePreservesGenesisForkSelection(t *testing.T) {
	const numBlocks = 25
	n, points := newGenesisSelectionTestNode(t, numBlocks)

	snapshotDir := filepath.Join(t.TempDir(), "snap-3273")
	manifest, err := lifecycleSnapshot(t, n, snapshotDir)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, manifest.TipSlot)

	// Before: Genesis density resolves a deep fork 4 blocks behind the tip.
	requireGenesisDeepForkWins(t, n, points, numBlocks-5)

	_, err = n.Restore(context.Background(), snapshotDir)
	require.NoError(t, err)
	require.Equal(
		t,
		points[len(points)-1].Slot,
		n.ledgerState.Tip().Point.Slot,
		"restore must bring the pre-fork tip back",
	)

	// After: the rebuilt ledger must still consult the same selector.
	requireGenesisDeepForkWins(t, n, points, numBlocks-10)
}

// TestLiveTruncateIsSerializedAgainstConcurrentCalls exercises
// n.liveLifecycleMu: two Truncate calls racing must not interleave their
// quiesce/rebuild sequences.
func TestLiveTruncateIsSerializedAgainstConcurrentCalls(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	slotA := points[numBlocks/2].Slot
	slotB := points[numBlocks/2].Slot

	errCh := make(chan error, 2)
	go func() {
		_, err := n.Truncate(
			context.Background(),
			dblifecycle.TruncateTarget{Slot: &slotA},
		)
		errCh <- err
	}()
	go func() {
		_, err := n.Truncate(
			context.Background(),
			dblifecycle.TruncateTarget{Slot: &slotB},
		)
		errCh <- err
	}()

	err1 := <-errCh
	err2 := <-errCh
	// Both target the same (already-truncated-to) point, so the second
	// call to actually run should be a no-op success, not a race/panic.
	require.NoError(t, err1)
	require.NoError(t, err2)
}

// TestLiveTruncateRejectsTargetAheadOfTipWithoutTearingDownNode guards
// against a severe finding from live testing (dingo#1651 follow-up): a
// live Truncate whose target is rejected during read-only validation (here,
// a block number ahead of the current tip — unlike a too-high slot, which
// ResolveTargetBySlot treats as a no-op resolving to the tip itself, an
// out-of-range block number is a real ResolveTarget error) used to
// unconditionally call n.cancel(), tearing down the entire node over a
// request that never touched any data. Since ResolveTarget/
// lifecycle.Truncate's own pre-DeleteBlocksAfter checks are provably
// read-only, lifecycle.ErrTruncateNotStarted now lets Node.Truncate resume
// normally instead.
func TestLiveTruncateRejectsTargetAheadOfTipWithoutTearingDownNode(
	t *testing.T,
) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	oldCtx := n.ctx
	aheadNumber := uint64(len(points)) + 1_000_000

	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{BlockNumber: &aheadNumber},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, blockErr := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t, blockErr,
			"block at slot %d missing after a rejected truncate", p.Slot,
		)
	}
}

// TestLiveTruncateCancelsWhenStorageProviderDrainIsUnconfirmed guards the
// boundary between context-bounded provider shutdown and live storage reopen.
// A provider may honor the deadline by returning while its one-time cleanup
// continues in the background. StopCapability has already relinquished that
// instance, so reopening the same data directory would race the old cleanup.
// The node must request a supervised restart instead.
func TestLiveTruncateCancelsWhenStorageProviderDrainIsUnconfirmed(
	t *testing.T,
) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	// Expire the operation context before provider shutdown. The real Badger
	// provider then returns from CloseContext at the deadline while its owned
	// close continues asynchronously.
	n.deferredIndexMaintenanceDone = make(chan struct{})

	shortCtx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()

	targetSlot := points[len(points)/2].Slot
	_, err := n.Truncate(
		shortCtx,
		dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "close storage")
	require.ErrorContains(t, err, "could not confirm")
	require.Error(t, n.ctx.Err())
	require.Nil(t, n.db, "storage must not be reopened before provider drain")

	// Wait for the provider-owned background close before TempDir cleanup. A
	// scratch runtime can acquire the same path only after the old Badger lock
	// is released; the cancelled Node itself remains stopped and never reopens.
	require.Eventually(t, func() bool {
		deps := n.storageDependencies(n.config.dataDir)
		deps.PromRegistry = n.config.promRegistry
		runtime, openErr := internalplugins.OpenDatabase(
			context.Background(),
			n.databaseConfig(),
			n.storageSelections(),
			deps,
		)
		if openErr != nil {
			return false
		}
		return runtime.Close(context.Background()) == nil
	}, 5*time.Second, 10*time.Millisecond)
}

// TestLiveTruncateResumesAfterCompletedStorageStopFailure proves that an
// ordinary provider Stop error still resumes the quiesced node when every
// provider has completed cleanup. The synthetic provider returns its error
// synchronously; the real Badger provider behind it also closes before
// StopCapability returns.
func TestLiveTruncateResumesAfterCompletedStorageStopFailure(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	stopFailure := errors.New("completed storage stop failure")
	require.NoError(t, plugin.Register[string, struct{}, struct{}](
		n.pluginHost,
		plugin.Descriptor{
			Capability: plugin.CapabilityStorageBlob,
			Name:       "test-stop-error",
		},
		func() struct{} { return struct{}{} },
		func(context.Context, struct{}, struct{}) (string, plugin.Instance, error) {
			return "test", plugin.Lifecycle{
				StopFunc: func(context.Context) error { return stopFailure },
			}, nil
		},
	))
	_, err := plugin.Resolve[string](
		context.Background(),
		n.pluginHost,
		plugin.CapabilityStorageBlob,
		"test-stop-error",
		nil,
		struct{}{},
	)
	require.NoError(t, err)

	oldCtx := n.ctx
	oldDB := n.db
	targetSlot := points[len(points)/2].Slot
	_, err = n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.ErrorIs(t, err, stopFailure)
	require.ErrorContains(t, err, "close storage")

	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.NotSame(t, oldDB, n.db, "storage must be reopened fresh on resume")
	require.NotNil(t, n.db)
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, blockErr := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t, blockErr,
			"block at slot %d missing after a resumed stop failure", p.Slot,
		)
	}
}

// TestLiveTruncateCancelsInsteadOfResumingWhenStorageDrainUnconfirmed guards
// against the actual use-after-close race errStorageDrainUnconfirmed exists
// to prevent. Here the trigger is a real dbWorkerPool worker still executing an
// operation against the *old* database when Close's bounded wait gives up
// on it, which is exactly the case reinitializeAndResume must not paper
// over: reopening storage while that worker might still be using it would
// race the new database instance against the old one. Truncate must
// instead cancel the node for a supervised restart.
func TestLiveTruncateCancelsInsteadOfResumingWhenStorageDrainUnconfirmed(
	t *testing.T,
) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNodeWithGenesis(
		t, numBlocks, nil,
		ledger.DatabaseWorkerPoolConfig{WorkerPoolSize: 1, TaskQueueSize: 1},
	)

	origTimeout := ledger.CloseDBWorkerPoolShutdownTimeout
	ledger.CloseDBWorkerPoolShutdownTimeout = 20 * time.Millisecond
	t.Cleanup(func() { ledger.CloseDBWorkerPoolShutdownTimeout = origTimeout })

	oldDB := n.db
	oldLedgerState := n.ledgerState

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		_ = oldLedgerState.SubmitAsyncDBOperation(
			func(db *database.Database) error {
				close(started)
				<-release
				return nil
			},
		)
	}()
	<-started

	targetSlot := points[len(points)/2].Slot
	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "close storage")
	require.ErrorContains(t, err, "could not confirm")

	// The node must have been brought down for a supervised restart, not
	// resumed against the same data directory the still-running worker
	// above may still be using.
	require.Error(t, n.ctx.Err())
	require.Same(
		t, oldDB, n.db,
		"storage must not be reopened while drain is unconfirmed",
	)
	require.Same(
		t, oldLedgerState, n.ledgerState,
		"ledgerState must not be replaced while drain is unconfirmed",
	)
}

// TestLiveTruncateClosesTmpDBBeforeResumingAfterOpenFailure guards against
// a real storage-lock leak: Truncate's tmpDB (opened purely
// to resolve the truncate target) used to only be deferred-closed AFTER
// checking database.New's error — but database.New can return a non-nil
// *Database alongside a non-nil, recoverable CommitTimestampError (see
// its own doc comment, "database is available for recovery, so return it
// with error"), and an early return on that error before the defer
// statement executes skips the deferred Close entirely. tmpDB's badger
// directory lock would then still be held when reinitializeAndResume's
// reinitializeCoreStorage tries to reopen that very same data directory a
// moment later, turning what should be a gracefully recovered
// CommitTimestampError into a hard lock-contention failure that brings
// the whole node down instead of resuming it.
//
// This reproduces exactly that CommitTimestampError condition (metadata
// commit timestamp set without a matching blob one, mirroring
// TestCheckCommitTimestamp_MetadataOnly in the database package) via the
// node's own already-open db handle, then invokes Truncate with a target
// ahead of the tip so the resulting error is classified
// lifecycle.ErrTruncateNotStarted (nothing on disk was touched, so resume
// is expected to succeed). If tmpDB leaked its lock, reinitializeCoreStorage's
// own reopen attempt fails with a lock error instead of gracefully
// recovering the very same CommitTimestampError, and reinitializeAndResume
// fails ("resume also failed"); with the fix, it must succeed and leave
// the node fully usable.
func TestLiveTruncateClosesTmpDBBeforeResumingAfterOpenFailure(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	// Corrupt the on-disk commit timestamps via the node's own already-open
	// db handle: set metadata's without touching blob's, so the NEXT fresh
	// database.New against this same data directory (Truncate's tmpDB, and
	// later reinitializeCoreStorage's reopen) observes a mismatch and
	// returns a recoverable CommitTimestampError.
	metaTxn := n.db.Metadata().Transaction(t.Context())
	require.NoError(t, n.db.Metadata().SetCommitTimestamp(123456789, metaTxn))
	require.NoError(t, metaTxn.Commit())

	oldCtx := n.ctx
	// The target itself is never actually validated: the corrupted commit
	// timestamp above makes Truncate's tmpDB open fail before
	// dblifecycle.ResolveTarget is ever reached, so any in-range target
	// exercises the same path.
	targetSlot := points[len(points)/2].Slot

	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.NotContains(
		t, err.Error(), "resume also failed",
		"a leaked tmpDB lock must not turn a recoverable CommitTimestampError "+
			"into a failed resume",
	)

	// The node must have come back up fully usable, not been torn down.
	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
}

// TestLiveRestoreRebuildsStorageAndKeepsNodeUsable verifies the Restore
// path end to end: snapshot a node's database, then restore that same
// snapshot back onto the running node, and confirm it comes back with the
// same tip and every subsystem rebuilt and rewired, same as Truncate.
func TestLiveRestoreRebuildsStorageAndKeepsNodeUsable(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	manifest, err := lifecycleSnapshot(t, n, snapshotDir)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, manifest.TipSlot)

	oldCtx := n.ctx
	restoredManifest, err := n.Restore(context.Background(), snapshotDir)
	require.NoError(t, err)
	require.Equal(t, manifest.CommitTimestamp, restoredManifest.CommitTimestamp)

	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)
	require.Same(t, n.ledgerState, n.ouroboros().LedgerState())

	tip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, err := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t,
			err,
			"block at slot %d missing after restore",
			p.Slot,
		)
	}
}

func TestStopForPendingRestoreRollbackCancelsNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{ctx: ctx, cancel: cancel}
	pendingErr := errors.Join(
		lifecycle.ErrRestoreRollbackPending,
		errors.New("injected remote rollback failure"),
	)

	err := n.stopForPendingRestoreRollback(pendingErr, nil)
	require.ErrorIs(t, err, lifecycle.ErrRestoreRollbackPending)
	require.Error(t, n.ctx.Err(), "node must stop instead of reopening unsafe stores")
}

// TestLiveRestoreRejectsCorruptedSnapshotWithoutDataLoss guards against a
// severe regression found via manual live testing (dingo#1651 follow-up):
// a Restore that failed because the blob backup was corrupted used to
// delete the node's OWN current data directory before validating the
// incoming snapshot at all, then bring the whole node process down — an
// operator who tried to restore from a bad snapshot lost the running node
// AND its existing database, with nothing left to restart into. Restore
// now stages the incoming snapshot in a sibling directory and validates it
// fully before touching the node's real data directory, so a corrupted
// snapshot must be rejected with the node's original data and tip
// completely intact and the node still usable.
func TestLiveRestoreRejectsCorruptedSnapshotWithoutDataLoss(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := lifecycleSnapshot(t, n, snapshotDir)
	require.NoError(t, err)

	// Corrupt the blob backup's content without touching manifest.json, so
	// ReadManifest still succeeds and the corruption is only caught once
	// Badger's own restore-stream parsing runs. Garbage bytes (rather than
	// e.g. flipping a few bytes in place, which can land inside an entry's
	// payload and still parse structurally) reliably fail Badger's
	// internal length-prefix parsing — same technique already proven in
	// bark's TestVerifySnapshotFailsForCorruptedSnapshot.
	blobPath := filepath.Join(snapshotDir, lifecycle.BlobBackupFileName)
	require.NoError(t, os.WriteFile(
		blobPath, []byte("not a valid badger backup stream"), 0o644,
	))

	oldCtx := n.ctx
	_, err = n.Restore(context.Background(), snapshotDir)
	require.Error(t, err)

	// The node must still be alive and usable: n.ctx untouched, no
	// cancellation, unchanged tip and blocks. n.db is deliberately not
	// asserted to be the same pointer: Restore now quiesces and closes
	// storage before validating the incoming snapshot at all (a Resettable
	// provider's Reset+RestoreFrom has no staging copy to validate against
	// first the way file-based storage does, so it must never run before
	// this node's own connection to a live client/server database is
	// closed — see Restore's own doc comment), so even a rejected restore
	// reaches reinitializeAndResume and ends up with a freshly reopened,
	// but equally valid, *database.Database over the same untouched data.
	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, blockErr := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t, blockErr,
			"block at slot %d missing after a rejected restore", p.Slot,
		)
	}

	// No leftover staging or backup directory should remain.
	_, statErr := os.Stat(n.config.dataDir + restoreStagingSuffix)
	require.True(t, os.IsNotExist(statErr))
	_, statErr = os.Stat(n.config.dataDir + preRestoreBackupSuffix)
	require.True(t, os.IsNotExist(statErr))
}

// TestLiveRestoreRejectsNetworkMismatchWithoutDataLoss confirms the other
// half of the same fix: restoring a snapshot from a genuinely different
// network onto a running node must be rejected by the manifest compatibility
// callback before restore preflight can reset either remote store, with the node's own
// data and tip left completely untouched and the node still usable,
// rather than the node being torn down (dingo#1651 follow-up).
func TestLiveRestoreRejectsNetworkMismatchWithoutDataLoss(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	// Build a standalone database configured for a different network and
	// snapshot IT, rather than hand-tampering a preview snapshot's
	// manifest: this way the snapshot is entirely self-consistent (valid
	// checksum, tip matches its own data) and only mismatches what n is
	// actually configured to run — the real-world scenario this guards.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	otherDB, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  logger,
		Network: "devnet",
	})
	require.NoError(t, err)
	require.NoError(t, otherDB.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 0, Hash: []byte{}},
		BlockNumber: 0,
	}, nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err = lifecycle.Snapshot(
		context.Background(),
		otherDB,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(otherDB))

	oldCtx := n.ctx
	_, err = n.Restore(context.Background(), snapshotDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest network")

	// n.db is deliberately not asserted to be the same pointer here --
	// see TestLiveRestoreRejectsCorruptedSnapshotWithoutDataLoss's
	// identical comment: Restore now quiesces and closes storage before
	// the manifest compatibility callback runs, so a rejected restore
	// still ends up with a freshly reopened *database.Database over the
	// same untouched data, not the original pointer.
	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, blockErr := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t, blockErr,
			"block at slot %d missing after a rejected restore", p.Slot,
		)
	}
}

// --- Crash-recoverable directory swap (dingo#1651 follow-up) ---
//
// These exercise swapInRestoredDataDir, reconcileInterruptedLiveRestoreSwap,
// and removeConfirmedRestoreBackup directly against a minimal *Node
// carrying only the config fields those functions actually read (dataDir,
// logger) -- unlike the live-node tests above, no real database/ledger/
// chain manager is needed to test the directory-swap state machine
// itself, and constructing one for every intermediate crash state would
// only make these tests slower without making them any more precise.

// newSwapTestNode returns a *Node with just enough config to exercise the
// directory-swap functions above, none of which touch anything else on
// Node.
// TestQuiesceForLiveLifecycleOpHandlesUninitializedOuroborosAndUnconfirmedConnShutdown
// guards two related gaps flagged in review: (1) quiesceForLiveLifecycleOp
// used to unconditionally dereference n.ouroboros to pause the Leios
// persist writer, panicking if quiesce ever ran against a node that isn't
// fully initialized (e.g. a partially-constructed Node, or a future call
// site); (2) a connManager.Stop failure -- meaning it could not confirm
// every connection/listener goroutine actually exited -- was not escalated
// to errStorageDrainUnconfirmed, even though PauseLeiosPersistWriterFor-
// LiveLifecycleOp's own safety (see its doc comment) depends on
// connManager.Stop having actually succeeded: an unconfirmed shutdown means
// a straggling connection could still call enqueueLeiosPersist concurrently
// with the reset PauseLeiosPersistWriterForLiveLifecycleOp performs.
func TestQuiesceForLiveLifecycleOpHandlesUninitializedOuroborosAndUnconfirmedConnShutdown(
	t *testing.T,
) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{
		Logger: logger,
	})

	n := &Node{
		config:      NewConfig(WithLogger(logger)),
		connManager: cm,
	}

	// Already past its deadline before quiesceForLiveLifecycleOp ever calls
	// connManager.Stop(ctx), so Stop's own select reliably takes its
	// ctx.Done() branch (ready from the moment this context was created)
	// well before its closeDone/goroutineDone goroutines could plausibly
	// finish, forcing Stop to return its own unconfirmed-shutdown error
	// deterministically rather than racing a real timer.
	ctx, cancel := context.WithDeadline(
		context.Background(), time.Now().Add(-time.Hour),
	)
	defer cancel()

	var err error
	require.NotPanics(t, func() {
		err = n.quiesceForLiveLifecycleOp(ctx)
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errStorageDrainUnconfirmed)
}

func newSwapTestNode(t *testing.T, dataDir string) *Node {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Node{
		config: NewConfig(WithDatabasePath(dataDir), WithLogger(logger)),
	}
}

// writeMarkerFile creates dir (and any missing parents) and writes a
// small file called name directly inside it, so a later assertion can
// tell which of two directories' original contents ended up at a given
// path after a swap/rollback, without needing a real database there.
func writeMarkerFile(t *testing.T, dir string, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(
		t, os.WriteFile(filepath.Join(dir, name), []byte("marker"), 0o644),
	)
}

func requireMarkerFile(t *testing.T, dir string, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	require.NoErrorf(t, err, "expected marker file %q in %q", name, dir)
}

func requireNoDir(t *testing.T, dir string) {
	t.Helper()
	_, err := os.Stat(dir)
	require.Truef(t, os.IsNotExist(err), "expected %q to not exist", dir)
}

// TestSwapInRestoredDataDirRetainsBackupUntilCallerConfirms guards the
// core fix: a directory swap succeeding is not by itself proof the
// restored data actually works, so swapInRestoredDataDir must never
// remove the pre-restore backup on its own -- only a caller that has
// separately confirmed the restored data starts (Restore's
// reinitializeAndResume call, or -- across a restart --
// removeConfirmedRestoreBackup once Run() itself succeeds) may do that.
func TestSwapInRestoredDataDirRetainsBackupUntilCallerConfirms(t *testing.T) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, dataDir, "original")
	writeMarkerFile(t, stagingDir, "restored")

	n := newSwapTestNode(t, dataDir)
	backupDir, err := n.swapInRestoredDataDir(stagingDir)
	require.NoError(t, err)
	require.Equal(t, dataDir+preRestoreBackupSuffix, backupDir)

	requireMarkerFile(t, dataDir, "restored")
	// The backup must still be there, holding the original content --
	// swapInRestoredDataDir itself must never remove it.
	requireMarkerFile(t, backupDir, "original")
	requireNoDir(t, stagingDir)
}

// withInjectedFirstSyncFailure replaces syncDataDirParent for the calling
// test's duration so its very first invocation returns injectErr (and runs
// beforeReturn, if non-nil, immediately before returning it) while every
// later invocation behaves like the real fsyncdir.Sync -- used to simulate
// an fsync failure landing exactly between swapInRestoredDataDir's first
// rename and its rollback, without needing real filesystem-level fault
// injection.
func withInjectedFirstSyncFailure(
	t *testing.T,
	injectErr error,
	beforeReturn func(),
) {
	t.Helper()
	orig := syncDataDirParent
	t.Cleanup(func() { syncDataDirParent = orig })
	calls := 0
	syncDataDirParent = func(path string) error {
		calls++
		if calls == 1 {
			if beforeReturn != nil {
				beforeReturn()
			}
			return injectErr
		}
		return orig(path)
	}
}

// TestSwapInRestoredDataDirRollsBackWhenFirstSyncFails guards against a
// real gap: dataDir has already been renamed to backupDir by the time the
// first fsync runs, so a failure there is not the same as the rename
// itself failing -- returning an ordinary error at that point, without
// rolling back, would leave dataDir absent while the caller's normal
// "swap failed, safe to resume on the original data directory" handling
// assumes it's still in place. swapInRestoredDataDir must roll the first
// rename back and report a normal (recoverable) error when that rollback
// succeeds.
func TestSwapInRestoredDataDirRollsBackWhenFirstSyncFails(t *testing.T) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, dataDir, "original")
	writeMarkerFile(t, stagingDir, "restored")

	withInjectedFirstSyncFailure(t, errors.New("injected sync failure"), nil)

	n := newSwapTestNode(t, dataDir)
	_, err := n.swapInRestoredDataDir(stagingDir)
	require.Error(t, err)
	require.NotErrorIs(t, err, errRestoreSwapUnrecoverable)

	// Rolled back: dataDir must hold the ORIGINAL content again, with no
	// leftover backup directory -- not left absent, and not left holding
	// the restored content that was never actually activated.
	requireMarkerFile(t, dataDir, "original")
	requireNoDir(t, dataDir+preRestoreBackupSuffix)
	// stagingDir (the restored data) was never consumed -- still there,
	// untouched, for a retry.
	requireMarkerFile(t, stagingDir, "restored")
}

// TestSwapInRestoredDataDirUnrecoverableWhenFirstSyncFailsAndRollbackFails
// covers the other half: if the rollback rename itself also fails (here,
// simulated by making the parent directory read-only in the instant
// between the first sync failing and the rollback attempt), dataDir is
// left absent with nothing this function can do about it -- that must be
// classified as errRestoreSwapUnrecoverable, the same as the existing
// second-rename-then-rollback-failure case, not returned as an ordinary
// "safe to resume" error.
func TestSwapInRestoredDataDirUnrecoverableWhenFirstSyncFailsAndRollbackFails(
	t *testing.T,
) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permission bits don't apply the same way on windows")
	}
	if os.Getuid() == 0 {
		t.Skip("permission checks don't apply when running as root")
	}

	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, dataDir, "original")
	writeMarkerFile(t, stagingDir, "restored")
	t.Cleanup(func() { _ = os.Chmod(base, 0o755) })

	// The rollback rename needs to create a new entry named dataDir
	// directly inside base, which a read-only base refuses -- made
	// read-only right as the injected sync failure is reported, so the
	// first rename (which already succeeded by then) is unaffected.
	withInjectedFirstSyncFailure(
		t,
		errors.New("injected sync failure"),
		func() {
			require.NoError(t, os.Chmod(base, 0o500))
		},
	)

	n := newSwapTestNode(t, dataDir)
	_, err := n.swapInRestoredDataDir(stagingDir)
	require.Error(t, err)
	require.ErrorIs(t, err, errRestoreSwapUnrecoverable)
}

// TestReconcileInterruptedLiveRestoreSwapNoOpWithoutInterruption verifies
// the common (no crash at all) case does nothing and reports no error,
// whether or not a stray staging directory from an earlier restore
// attempt that never reached the swap happens to exist alongside a
// perfectly normal data directory -- left for the next Restore call's own
// cleanup, not this one's, per reconcileInterruptedLiveRestoreSwap's doc
// comment.
func TestReconcileInterruptedLiveRestoreSwapNoOpWithoutInterruption(
	t *testing.T,
) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	writeMarkerFile(t, dataDir, "original")

	n := newSwapTestNode(t, dataDir)
	require.NoError(t, n.reconcileInterruptedLiveRestoreSwap())
	requireMarkerFile(t, dataDir, "original")

	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, stagingDir, "stale-attempt")
	require.NoError(t, n.reconcileInterruptedLiveRestoreSwap())
	requireMarkerFile(t, dataDir, "original")
	requireMarkerFile(t, stagingDir, "stale-attempt")
}

// TestReconcileInterruptedLiveRestoreSwapRollsBackWhenInterruptedBetweenRenames
// simulates a crash landing exactly between swapInRestoredDataDir's two
// renames: dataDir has already been moved aside to backupDir, but the
// restored data was never moved into dataDir's place, so dataDir doesn't
// exist at all. Reconciliation must restore the original by renaming
// backupDir back into place, rather than leaving the node with no data
// directory to start on at all.
func TestReconcileInterruptedLiveRestoreSwapRollsBackWhenInterruptedBetweenRenames(
	t *testing.T,
) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	backupDir := dataDir + preRestoreBackupSuffix
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, backupDir, "original")
	writeMarkerFile(t, stagingDir, "restored")

	n := newSwapTestNode(t, dataDir)
	require.NoError(t, n.reconcileInterruptedLiveRestoreSwap())

	requireMarkerFile(t, dataDir, "original")
	requireNoDir(t, backupDir)
	requireNoDir(t, stagingDir)
}

// TestReconcileInterruptedLiveRestoreSwapKeepsRestoredDataWhenBothPresent
// simulates a crash landing after swapInRestoredDataDir's second rename
// completed (dataDir already holds the restored data) but before this
// node could confirm the restored data actually works and remove the
// backup. Reconciliation must leave dataDir exactly as-is -- it's what
// Run() is about to start on -- and must NOT remove the backup itself:
// only removeConfirmedRestoreBackup, called once startup actually
// succeeds, does that -- otherwise a node that crashes on every attempt
// to start on genuinely bad restored data would silently lose its last
// good backup on the very first restart attempt.
func TestReconcileInterruptedLiveRestoreSwapKeepsRestoredDataWhenBothPresent(
	t *testing.T,
) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	backupDir := dataDir + preRestoreBackupSuffix
	stagingDir := dataDir + restoreStagingSuffix
	writeMarkerFile(t, dataDir, "restored")
	writeMarkerFile(t, backupDir, "original")
	writeMarkerFile(t, stagingDir, "leftover")

	n := newSwapTestNode(t, dataDir)
	require.NoError(t, n.reconcileInterruptedLiveRestoreSwap())

	requireMarkerFile(t, dataDir, "restored")
	requireMarkerFile(t, backupDir, "original")
	requireNoDir(t, stagingDir)

	// Only once startup is confirmed successful does the backup go away.
	n.removeConfirmedRestoreBackup()
	requireNoDir(t, backupDir)
	requireMarkerFile(t, dataDir, "restored")
}

// TestReconcileInterruptedLiveRestoreSwapPropagatesRollbackFailure verifies
// that a rollback failure -- not expected in ordinary operation, since
// backupDir and dataDir are always siblings on the same filesystem, but a
// permissions or filesystem error is always possible -- is surfaced as a
// real error rather than silently leaving the node with no usable data
// directory and no indication anything went wrong.
func TestReconcileInterruptedLiveRestoreSwapPropagatesRollbackFailure(
	t *testing.T,
) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permission bits don't apply the same way on windows")
	}
	if os.Getuid() == 0 {
		t.Skip("permission checks don't apply when running as root")
	}
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	backupDir := dataDir + preRestoreBackupSuffix
	writeMarkerFile(t, backupDir, "original")

	// The rollback rename needs to create a new entry named dataDir
	// directly inside base, which a read-only base refuses.
	require.NoError(t, os.Chmod(base, 0o500))
	t.Cleanup(func() { _ = os.Chmod(base, 0o755) })

	n := newSwapTestNode(t, dataDir)
	require.Error(t, n.reconcileInterruptedLiveRestoreSwap())
}

// smallEpochGenesisCfgForLifecycleTest returns a CardanoNodeConfig with
// epochLength=100 — the real preview genesis newNodeTestCardanoNodeCfg
// loads has an epochLength far larger than any small block count could
// reach, which mattered for a manual-testing bug (dingo#1651 follow-up)
// specifically tied to crossing epoch boundaries after a live truncate:
// with the real genesis, a reproduction never actually exercises the
// epoch-rollover/stake-snapshot code path at all.
func smallEpochGenesisCfgForLifecycleTest(
	t *testing.T,
) *cardano.CardanoNodeConfig {
	t.Helper()
	// Start from the real preview genesis (already has valid Byron/Shelley
	// data, genesis UTxOs, hashes, etc.) and override just the one field
	// that matters here, rather than hand-building a minimal genesis from
	// scratch — a from-scratch genesis needs realistic bootstrap UTxO/stake
	// data before LedgerState.Start will even get through createGenesisBlock.
	cfg := newNodeTestCardanoNodeCfg(t)
	require.NotNil(t, cfg.ShelleyGenesis())
	cfg.ShelleyGenesis().EpochLength = 100
	return cfg
}

// addBlocksSerially adds each block one at a time, waiting for the
// ledger's committed tip to actually reach it before adding the next.
//
// A tight back-to-back loop of AddBlock calls that crosses more than one
// epoch boundary fires several epoch-transition EventBus events with no
// synchronization between them, each spawning its own concurrent async
// handler (reward precompute, stake/reward snapshots, and automatic database
// lifecycle snapshots). Adding blocks one at a time both keeps this test
// deterministic and more accurately simulates blocks arriving live instead
// of as an instantaneous burst.
func addBlocksSerially(t *testing.T, n *Node, blocks []gledger.Block) {
	t.Helper()
	for _, b := range blocks {
		require.NoError(t, n.chainManager.PrimaryChain().AddBlock(b, nil))
		targetSlot := b.SlotNumber()
		require.Eventually(t, func() bool {
			tip, err := n.db.GetTip(nil)
			return err == nil && tip.Point.Slot == targetSlot
		}, 5*time.Second, 10*time.Millisecond,
			"tip did not advance to slot %d after adding its block", targetSlot)
	}
}

// TestSecondLiveTruncateResumesTipAdvancement is a regression reproduction
// found via manual live testing (dingo#1651 follow-up): after a SECOND
// consecutive live truncate on the same long-running node, new blocks kept
// getting added to the chain but the reported tip stayed stuck at the
// second truncate's landing point forever — the rebuilt ledger-processing
// pipeline never committed them, specifically once an epoch boundary was
// involved. A single live truncate's rebuild works fine
// (TestLiveTruncateRebuildsStorageAndKeepsNodeUsable already proves that
// without crossing an epoch boundary); this test truncates twice in a row,
// each time re-adding blocks that cross at least one epoch boundary
// (epochLength=100, real testdata blocks are 20 slots apart), and confirms
// the tip actually advances again after each one, not just the first.
func TestSecondLiveTruncateResumesTipAdvancement(t *testing.T) {
	const numBlocks = 20
	n, points := newLiveLifecycleTestNodeWithGenesis(
		t, numBlocks, smallEpochGenesisCfgForLifecycleTest(t),
		disabledLiveLifecycleTestWorkerPoolCfg,
	)
	// Re-loaded separately (not added to any chain yet) so they can be fed
	// back in one at a time after each truncate, simulating new blocks
	// arriving live — reusing the real, already-validated blocks 10-19
	// keeps their prev-hash chain correct without fabricating new ones.
	tailBlocks := loadRawLiveLifecycleTestBlocks(t, numBlocks)[10:20]

	// First truncate: back to block 9 (slot 180, epoch 1), removing blocks
	// 10-19. Re-adding them crosses epoch 1->2 (block 10, slot 200) and
	// epoch 2->3 (block 15, slot 300).
	target1 := points[9].Slot
	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &target1},
	)
	require.NoError(t, err)

	addBlocksSerially(t, n, tailBlocks)

	// Second truncate: back to block 14 (slot 280, epoch 2), removing
	// blocks 15-19. Re-adding them crosses epoch 2->3 (block 15, slot 300)
	// again — this time on storage rebuilt by a SECOND live truncate.
	target2 := points[14].Slot
	_, err = n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &target2},
	)
	require.NoError(t, err)

	addBlocksSerially(t, n, tailBlocks[5:])
}
