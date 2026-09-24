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
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// forgeCreatedBy and electionCreatedBy identify the goroutines that
// BlockForger.Start and Election.Start launch. Both Stop calls join their
// workers, so these lines disappearing is evidence of a join rather than of
// a cancellation: these tests never cancel the context the components were
// started with.
//
// The match is on the "created by" line rather than on the worker's own
// entry frame, because two renderings of a live goroutine carry no entry
// frame at all: one created by `go` but not yet scheduled has an empty
// stack, and one running on another thread prints "stack unavailable".
// runtime.Stack emits the "created by" line in every case, so matching it is
// not a race against the scheduler. Matching the entry frame is: under a
// loaded test binary the forge loop had reliably not been scheduled by the
// time the assertion ran.
//
// The scan covers every goroutine in the test binary, which is sound because
// these are the only tests in this package that start a forger or an election;
// the rest hold an unstarted forging.BlockForger value.
const (
	forgeCreatedBy = "created by " +
		"github.com/blinklabs-io/dingo/ledger/forging.(*BlockForger).Start"
	electionCreatedBy = "created by " +
		"github.com/blinklabs-io/dingo/ledger/leader.(*Election).start"
)

// goroutineStacksContain reports whether any live goroutine's dump mentions
// marker.
func goroutineStacksContain(marker string) bool {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Contains(string(buf[:n]), marker)
		}
		buf = make([]byte, 2*len(buf))
	}
}

func requireGoroutineGone(t *testing.T, marker string) {
	t.Helper()
	require.Eventually(
		t,
		func() bool { return !goroutineStacksContain(marker) },
		10*time.Second,
		10*time.Millisecond,
		"goroutine %q never exited; its Stop was not run", marker,
	)
}

// newStartupCleanupProducerNode builds the smallest node startBlockProducer
// needs: a real database, chain manager, ledger state and event bus, plus the
// devnet credential fixtures.
func newStartupCleanupProducerNode(t *testing.T) *Node {
	t.Helper()
	vrf, kes, opcert := devnetCredPaths(t)
	// The full devnet config, for the Byron genesis and the genesis hashes
	// LedgerState.Start needs to build the genesis block. Its own Shelley
	// genesis is then replaced with one whose system start is recent, so the
	// devnet opcert fixture (KES period 0) is still current.
	cardanoCfg, err := cardano.NewCardanoNodeConfigFromFile(
		filepath.Join("config", "cardano", "devnet", "config.json"),
	)
	require.NoError(t, err)
	// Moved forward in memory rather than rewritten on disk: the fixture's
	// 2022 system start puts the wall clock about a thousand KES periods past
	// the devnet opcert fixture, which issue 0 at KES period 0 cannot cover,
	// and the initial funds and protocol params the genesis block needs stay
	// exactly as shipped.
	cardanoCfg.ShelleyGenesis().SystemStart = time.Now().Add(-time.Hour)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	t.Cleanup(eventBus.Close)
	chainManager, err := chain.NewManager(db, eventBus)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      chainManager,
		EventBus:          eventBus,
		CardanoNodeConfig: cardanoCfg,
		Logger:            logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ledgerState.Close() })
	// Started because Run starts it long before the block-producer section,
	// and validateBlockProducerStartup reads the slot clock it initializes.
	require.NoError(t, ledgerState.Start(context.Background()))

	n := &Node{
		config: Config{
			logger:                        logger,
			blockProducer:                 true,
			shelleyVRFKey:                 vrf,
			shelleyKESKey:                 kes,
			shelleyOperationalCertificate: opcert,
			cardanoNodeConfig:             cardanoCfg,
			network:                       "devnet",
			promRegistry:                  prometheus.NewRegistry(),
		},
		db:           db,
		eventBus:     eventBus,
		chainManager: chainManager,
		ledgerState:  ledgerState,
	}
	// Whatever the outcome, leave no forge loop or election worker behind for
	// the rest of the package; both Stop calls are idempotent.
	t.Cleanup(func() {
		if n.blockForger != nil {
			n.blockForger.Stop()
		}
		if n.leaderElection != nil {
			_ = n.leaderElection.Stop()
		}
	})
	return n
}

// runStopsLIFO unwinds a startup-cleanup stack the way cleanupFailedStartup
// does, without cancelling the node context: the components must be stopped by
// the registered closures, not by context cancellation.
func runStopsLIFO(stops []func()) {
	for _, stop := range slices.Backward(stops) {
		stop()
	}
}

// TestStartBlockProducerStopIsRegisteredBeforeLeiosVotingCanFail covers the
// ordering defect in the startup-cleanup stack: initBlockForger returns with
// the forge loop and the election's workers already running, and
// enableLeiosVoting runs after it and can fail. With the stop registered after
// that call, a vote-key failure returned a cleanup stack that never joined
// either component, leaving the forge loop running while the LIFO rollback
// closed ledger state, the database and the plugin host.
func TestStartBlockProducerStopIsRegisteredBeforeLeiosVotingCanFail(
	t *testing.T,
) {
	n := newStartupCleanupProducerNode(t)
	kesStopped := false
	n.kesAgentCancel = func() { kesStopped = true }
	// Non-nil so enableLeiosVoting does not take its no-vote-manager early
	// return; it fails on the key file below without ever calling into it.
	n.leiosVoteManager = &leios.VoteManager{}
	n.config.leiosVoteSigningKeyFile = filepath.Join(
		t.TempDir(),
		"absent-vote.skey",
	)

	ctx, cancel := context.WithCancel(context.Background())
	// Deferred, not called during the assertions: cancelling would also stop
	// the forge loop and would make the test pass without the registration.
	defer cancel()

	started, err := n.startBlockProducer(ctx, nil)
	require.ErrorContains(t, err, "failed to enable leios voting")
	require.NotNil(t, n.blockForger)
	require.True(
		t,
		n.blockForger.IsRunning(),
		"forger must be running for this test to mean anything",
	)
	require.True(t, goroutineStacksContain(forgeCreatedBy))
	require.True(
		t,
		goroutineStacksContain(electionCreatedBy),
		"election workers must be running for this test to mean anything",
	)

	runStopsLIFO(started)

	require.True(t, kesStopped, "startup rollback must stop the KES agent loop")
	requireGoroutineGone(t, forgeCreatedBy)
	requireGoroutineGone(t, electionCreatedBy)
	require.False(t, n.blockForger.IsRunning())
	require.Len(
		t,
		started,
		1,
		"the forger and election stop must be registered before enableLeiosVoting",
	)
}

// TestStartBlockProducerStopJoinsBothComponentsOnSuccess is the positive case:
// a startup that completes registers exactly one stop, and running it joins
// the forge loop and the election workers, in that order.
func TestStartBlockProducerStopJoinsBothComponentsOnSuccess(t *testing.T) {
	n := newStartupCleanupProducerNode(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started, err := n.startBlockProducer(ctx, nil)
	require.NoError(t, err)
	require.Len(t, started, 1)
	require.True(t, n.blockForger.IsRunning())
	require.True(t, goroutineStacksContain(forgeCreatedBy))
	require.True(
		t,
		goroutineStacksContain(electionCreatedBy),
		"election workers must be running for this test to mean anything",
	)

	runStopsLIFO(started)

	require.False(t, n.blockForger.IsRunning())
	requireGoroutineGone(t, forgeCreatedBy)
	requireGoroutineGone(t, electionCreatedBy)
}
