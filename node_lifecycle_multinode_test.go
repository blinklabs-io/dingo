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
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// This file is the Phase 3 substitute for Docker-based DevNet validation:
// DevNet's harness only ever talks to a dingo-producer container over real
// Ouroboros network protocols from outside the process, with no channel to
// trigger the new in-process Truncate/Restore methods on it. Since a real
// trigger surface for that is Phase 4's job (the gRPC surface), this file
// instead builds two real, full *dingo.Node instances in the same test
// process — one an actual block producer using the devnet genesis
// delegate's real VRF/KES/OpCert credentials (config/cardano/devnet/keys),
// the other a normal-mode node syncing from it over a real loopback TCP
// connection using the real Ouroboros NtN handshake/chainsync/blockfetch
// stack — and calls Truncate/Restore on the syncing node while blocks are
// actively arriving, verifying it recovers and keeps syncing afterward.
// Both nodes go through the real, unmodified Run(), so this is the first
// exercise of the live-lifecycle code against a node that wasn't
// hand-assembled by a test.
//
// Dev-mode forging (ledger/forging/forger.go's ModeDev) was tried first and
// rejected: it forges with zero-value VRF/OpCert/IssuerVkey fields, which a
// real header-verifying peer always rejects ("VRF verification failed") —
// ModeDev is documented as single-node-only. Production-mode forging
// instead uses real leader election, so the devnet genesis's single stake
// pool (100% of stake, activeSlotsCoeff=1.0, so it is elected leader for
// effectively every slot) needs its own credentials, which devnetCredPaths
// (node_forging_test.go) already points at. See devnetSystemStartLeadTime's
// doc comment for why genesis's SystemStart is set a few seconds into the
// future rather than to "now".

// devnetSystemStartLeadTime buffers SystemStart into the near future
// (rather than "now") so real node startup — genesis/ledger/snapshot/
// leader-election/forger construction, done twice (forger then syncer) —
// finishes before genesis slot 0 actually arrives.
const devnetSystemStartLeadTime = 3 * time.Second

// devnetTestEpochLength widens the checked-in devnet genesis's epoch from
// 5 slots (500ms) to 100 (10s). Leader election for epoch 0 and 1 relies
// solely on the genesis stake snapshot (CaptureGenesisSnapshot, done at
// startup regardless of block activity); every later epoch instead needs
// a mark snapshot captured at the PRIOR epoch's boundary, which only
// happens inside actual block processing — so if the forger never
// produces a block during epoch 0 or 1, the ledger's own epoch
// bookkeeping never advances past 0 and no block ever gets forged: a
// permanent bootstrap deadlock, not a transient miss. With the real
// 500ms epoch, hitting even one of epoch 0/1's leader slots requires the
// forger's first tick to land within ~1 real second of genesis — which
// two real nodes' full startup (ledger/snapshot/leader-election/forger,
// done twice) cannot reliably guarantee under -race's scheduling jitter,
// no matter how devnetSystemStartLeadTime is tuned (observed drifting
// 1-4+ real seconds past genesis run to run). Widening the epoch to 10
// real seconds gives the same startup jitter a much bigger target to
// land in, without touching production defaults (this only mutates this
// test's in-memory config copy, not the checked-in devnet/*.json files).
const devnetTestEpochLength = 100

// devnetCardanoConfig loads the embedded devnet network config: sub-second
// slots and activeSlotsCoeff=1.0, tuned for fast in-process iteration. The
// checked-in genesis's SystemStart is whatever fixed instant it was
// generated at; with 100ms slots, real wall-clock time is now billions of
// slots past it, which corrupts forged blocks (the slot clock computes an
// astronomical "current slot"). The real DevNet Docker harness sidesteps
// this by regenerating genesis fresh on every run; here it's simplest to
// just rewrite SystemStart to shortly after "now" (see
// devnetSystemStartLeadTime), and to widen the epoch length (see
// devnetTestEpochLength), after loading.
func devnetCardanoConfig(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	cfg.ShelleyGenesis().SystemStart = time.Now().UTC().
		Add(devnetSystemStartLeadTime)
	cfg.ShelleyGenesis().EpochLength = devnetTestEpochLength
	return cfg
}

// dbReadySignalHandler wraps a real slog.Handler and additionally signals
// once (non-blocking) when a specific log message is emitted. Used here for
// two distinct synchronization needs: (1) knowing when a node's
// database.New() (and the global, NOT concurrency-safe plugin-option state
// it mutates — see plugin.SetPluginOption's own doc comment) has finished,
// so a second node's database.New() can be started only afterward rather
// than racing it; (2) giving a background-started Run() goroutine's n.db
// assignment (node.go's `n.db = db`, itself unsynchronized — nothing in
// normal operation needs to read it that early) a real happens-before edge
// against a test goroutine that starts polling n.db immediately after, as
// waitForTipSlotAtLeast does. A channel receive on the same log line
// establishes that edge (Go's memory model); a raw poll loop would not, and
// a log line that fires from inside database.New() itself — before Run()'s
// own `n.db = db` executes — would establish the wrong edge, which is why
// the watched message below is logged later, from LedgerState.Start
// (ledger/state.go), synchronously further down the same Run() call chain.
// Modeled on node_test.go's nodeTestLogSignalHandler, extended to also
// forward to a real handler.
type dbReadySignalHandler struct {
	slog.Handler
	message string
	seen    chan struct{}
}

func (h dbReadySignalHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == h.message {
		select {
		case h.seen <- struct{}{}:
		default:
		}
	}
	return h.Handler.Handle(ctx, record)
}

func (h dbReadySignalHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.Handler = h.Handler.WithAttrs(attrs)
	return h
}

func (h dbReadySignalHandler) WithGroup(name string) slog.Handler {
	h.Handler = h.Handler.WithGroup(name)
	return h
}

// newDBReadyLogger returns a logger that signals on dbReady once Run()'s
// `n.db = db` assignment (node.go) has definitely already happened: the
// "database worker pool initialized" log line is emitted from
// LedgerState.Start (ledger/state.go), which Run() calls synchronously,
// on the same goroutine, strictly after that assignment.
func newDBReadyLogger(w io.Writer) (*slog.Logger, <-chan struct{}) {
	dbReady := make(chan struct{}, 1)
	return slog.New(dbReadySignalHandler{
		Handler: slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}),
		message: "database worker pool initialized",
		seen:    dbReady,
	}), dbReady
}

// newLoopbackListener opens a real TCP listener on an OS-assigned loopback
// port, handing the listener itself (not just an address string) to the
// node so there's no port-discovery race between creating it and a peer
// dialing it.
func newLoopbackListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// newDevnetForgerNode builds a real production-mode block-producer Node
// using the devnet genesis delegate's real VRF/KES/OpCert credentials, and
// accepts inbound connections on ln. It has no bootstrap peers configured —
// it only needs to accept the syncing node's inbound connection.
func newDevnetForgerNode(
	t *testing.T,
	cardanoCfg *cardano.CardanoNodeConfig,
	logger *slog.Logger,
	ln net.Listener,
) *Node {
	t.Helper()
	vrfPath, kesPath, opcertPath := devnetCredPaths()
	cfg := NewConfig(
		WithDatabasePath(t.TempDir()),
		WithLogger(logger),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithNetwork("devnet"),
		WithNetworkMagic(42),
		WithCardanoNodeConfig(cardanoCfg),
		WithListeners(ListenerConfig{Listener: ln}),
		WithShutdownTimeout(5*time.Second),
		WithBlockProducer(true),
		WithShelleyVRFKey(vrfPath),
		WithShelleyKESKey(kesPath),
		WithShelleyOperationalCertificate(opcertPath),
	)
	n, err := New(cfg)
	require.NoError(t, err)
	return n
}

// newDevnetSyncNode builds a real normal-mode Node configured to dial
// forgerAddr as its sole bootstrap peer.
func newDevnetSyncNode(
	t *testing.T,
	cardanoCfg *cardano.CardanoNodeConfig,
	logger *slog.Logger,
	forgerAddr *net.TCPAddr,
) *Node {
	t.Helper()
	cfg := NewConfig(
		WithDatabasePath(t.TempDir()),
		WithLogger(logger),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithNetwork("devnet"),
		WithNetworkMagic(42),
		WithCardanoNodeConfig(cardanoCfg),
		WithGenesisBootstrap(false),
		WithIntersectTip(false),
		WithListeners(ListenerConfig{Listener: newLoopbackListener(t)}),
		WithTopologyConfig(&topology.TopologyConfig{
			BootstrapPeers: []topology.TopologyConfigP2PBootstrapPeer{
				{
					Address: forgerAddr.IP.String(),
					Port:    uint(forgerAddr.Port),
				},
			},
		}),
		WithShutdownTimeout(5*time.Second),
	)
	n, err := New(cfg)
	require.NoError(t, err)
	return n
}

// runNodeInBackground starts n.Run in a goroutine and registers cleanup to
// stop it, mirroring how cmd/dingo actually runs a node (Run blocks until
// shutdown).
func runNodeInBackground(t *testing.T, n *Node) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = n.Stop()
		<-done
	})
}

// startTwoNodeDevnet builds and starts a dev-mode forger and a syncing
// node pointed at it, serializing their startup so the syncer's
// database.New() (and the global, not-concurrency-safe plugin-option
// state it mutates — see plugin.SetPluginOption's doc comment) cannot
// interleave with the forger's: it waits for the forger's own
// database.New() to finish (signalled via its logger, not a raw field
// poll or a sleep) before constructing the syncer at all.
//
// It also waits for the syncer's own database.New() to finish before
// returning. Run() assigns n.db without holding liveLifecycleMu (nothing
// else touches it that early in normal operation), so a test goroutine
// that starts polling n.db immediately — as waitForTipSlotAtLeast does —
// would otherwise race that assignment. The dbReady channel receive gives
// the race detector a real happens-before edge; a raw poll loop would not.
func startTwoNodeDevnet(t *testing.T) (forger, syncer *Node) {
	t.Helper()
	cardanoCfg := devnetCardanoConfig(t)
	ln := newLoopbackListener(t)
	forgerAddr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)

	forgerLogger, forgerDBReady := newDBReadyLogger(io.Discard)
	forger = newDevnetForgerNode(t, cardanoCfg, forgerLogger, ln)
	runNodeInBackground(t, forger)

	select {
	case <-forgerDBReady:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for forger node's database to initialize")
	}

	syncerLogger, syncerDBReady := newDBReadyLogger(io.Discard)
	syncer = newDevnetSyncNode(t, cardanoCfg, syncerLogger, forgerAddr)
	runNodeInBackground(t, syncer)

	select {
	case <-syncerDBReady:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for syncer node's database to initialize")
	}

	return forger, syncer
}

// waitForTipSlotAtLeast polls n's persisted tip (direct field access —
// this test file is in package dingo) until it reaches at least minSlot,
// per CLAUDE.md's no-time.Sleep-for-sync convention.
func waitForTipSlotAtLeast(t *testing.T, n *Node, minSlot uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		n.liveLifecycleMu.Lock()
		db := n.db
		n.liveLifecycleMu.Unlock()
		if db == nil {
			return false
		}
		tip, err := db.GetTip(nil)
		if err != nil {
			return false
		}
		return tip.Point.Slot >= minSlot
	}, 90*time.Second, 20*time.Millisecond)
}

func TestTwoNodeDevnetSyncOverRealNetworking(t *testing.T) {
	_, syncer := startTwoNodeDevnet(t)
	waitForTipSlotAtLeast(t, syncer, 5)
}

// TestLiveTruncateUnderRealForgingAndNetworking is the Phase 3 core
// verification: the syncing node is truncated mid-flight, while the
// forger keeps producing blocks over a real network connection, and must
// resume syncing correctly afterward — not just reconstruct its own
// storage in isolation (already proven by node_lifecycle_test.go), but do
// so while a real peer connection is live and blocks keep arriving.
func TestLiveTruncateUnderRealForgingAndNetworking(t *testing.T) {
	_, syncer := startTwoNodeDevnet(t)
	waitForTipSlotAtLeast(t, syncer, 10)

	// Capture an actually-observed early tip to use as the truncate
	// target, rather than computing an arbitrary midpoint slot: real
	// production-mode leader election only ramps up over the first
	// couple of epochs (see this file's top doc comment), so a computed
	// slot could fall before any block the forger actually produced.
	syncer.liveLifecycleMu.Lock()
	earlyTip, err := syncer.db.GetTip(nil)
	syncer.liveLifecycleMu.Unlock()
	require.NoError(t, err)

	waitForTipSlotAtLeast(t, syncer, earlyTip.Point.Slot+5)

	syncer.liveLifecycleMu.Lock()
	tipBeforeTruncate, err := syncer.db.GetTip(nil)
	syncer.liveLifecycleMu.Unlock()
	require.NoError(t, err)

	targetSlot := earlyTip.Point.Slot
	blocksRemoved, err := syncer.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)
	require.NotZero(t, blocksRemoved)

	syncer.liveLifecycleMu.Lock()
	tipAfterTruncate, err := syncer.db.GetTip(nil)
	syncer.liveLifecycleMu.Unlock()
	require.NoError(t, err)
	require.LessOrEqual(t, tipAfterTruncate.Point.Slot, targetSlot)

	// The forger keeps producing blocks throughout; the syncer must pick
	// back up and advance past its pre-truncate tip, proving the
	// real network connection and full chainsync/blockfetch pipeline
	// still work after a live in-process rebuild.
	waitForTipSlotAtLeast(t, syncer, tipBeforeTruncate.Point.Slot+5)
}

// TestLiveRestoreUnderRealForgingAndNetworking mirrors the truncate test
// above for Restore: snapshot the syncing node while live, restore that
// same snapshot back onto it mid-flight, and confirm it keeps syncing
// from the still-forging peer afterward.
func TestLiveRestoreUnderRealForgingAndNetworking(t *testing.T) {
	_, syncer := startTwoNodeDevnet(t)
	waitForTipSlotAtLeast(t, syncer, 10)

	snapshotDir := t.TempDir() + "/snap"
	syncer.liveLifecycleMu.Lock()
	manifest, err := lifecycleSnapshot(t, syncer, snapshotDir)
	tipBeforeRestore, tipErr := syncer.db.GetTip(nil)
	syncer.liveLifecycleMu.Unlock()
	require.NoError(t, err)
	require.NoError(t, tipErr)

	_, err = syncer.Restore(context.Background(), snapshotDir)
	require.NoError(t, err)
	// Forging and ingestion remain live while Snapshot runs, so the node can
	// advance after the snapshot captures its manifest tip and before the
	// later GetTip above. The snapshot must not be ahead of that observed
	// live tip; equality is not guaranteed by this concurrent test.
	require.LessOrEqual(t, manifest.TipSlot, tipBeforeRestore.Point.Slot)

	waitForTipSlotAtLeast(t, syncer, tipBeforeRestore.Point.Slot+5)
}
