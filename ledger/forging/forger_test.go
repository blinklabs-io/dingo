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

package forging

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type forgerTestLeader struct{}

func (forgerTestLeader) ShouldProduceBlock(uint64) bool { return true }

func (forgerTestLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

type forgerBlockingLeader struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once

	mu    sync.Mutex
	calls int
}

func (l *forgerBlockingLeader) ShouldProduceBlock(uint64) bool {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	l.enteredOnce.Do(func() { close(l.entered) })
	<-l.release
	return true
}

func (l *forgerBlockingLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

func (l *forgerBlockingLeader) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type forgerCountingLeader struct {
	mu    sync.Mutex
	calls int
}

func (l *forgerCountingLeader) ShouldProduceBlock(uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	return true
}

func (l *forgerCountingLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

func (l *forgerCountingLeader) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type forgerTestSlotClock struct {
	currentSlot  uint64
	chainTipSlot uint64
	chainTipHash []byte
	// frontierExplicit selects whether frontierSlot/frontierHash are used
	// verbatim. When false the frontier mirrors the applied tip, which is the
	// caught-up steady state and what every test that does not care about the
	// distinction wants.
	frontierExplicit  bool
	frontierSlot      uint64
	frontierHash      []byte
	upstreamTipSlot   uint64
	upstreamActive    bool
	slotsPerKESPeriod uint64
}

func (c forgerTestSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c forgerTestSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c forgerTestSlotClock) ChainTip() ocommon.Point {
	return ocommon.Point{Slot: c.chainTipSlot, Hash: c.chainTipHash}
}

// PrimaryChainTip mirrors the applied tip unless the test describes a frontier
// of its own. Mirroring is the caught-up steady state, so a test that sets no
// frontier field observes no backlog and no divergence.
//
// Setting frontierSlot or frontierHash is itself enough to opt in: a test that
// set frontierSlot but forgot frontierExplicit would otherwise silently get the
// mirrored applied tip, so its gap would read 0 and it would pass no matter
// what the forger did -- which is exactly what happened to the configurable
// tolerance test. frontierExplicit remains for the one case the values cannot
// express on their own: an explicitly empty frontier (slot 0, no hash), which
// is an uninitialised primary chain.
//
// The values are used verbatim, including a frontier BEHIND the applied tip,
// which is a real state the forger must handle and which a clamp would hide.
func (c forgerTestSlotClock) PrimaryChainTip() ocommon.Point {
	if !c.frontierExplicit && c.frontierSlot == 0 && c.frontierHash == nil {
		return ocommon.Point{Slot: c.chainTipSlot, Hash: c.chainTipHash}
	}
	return ocommon.Point{Slot: c.frontierSlot, Hash: c.frontierHash}
}

func (forgerTestSlotClock) NextSlotTime() (time.Time, error) {
	return time.Now(), nil
}

// ChainTipHash satisfies the optional ChainTipHashProvider. It returns
// nil unless a test sets chainTipHash, so every existing test keeps the
// fence-only behaviour.
func (c forgerTestSlotClock) ChainTipHash() []byte {
	return c.chainTipHash
}

func (c forgerTestSlotClock) UpstreamTipSlot() uint64 {
	return c.upstreamTipSlot
}

func (c forgerTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return c.upstreamTipSlot, c.upstreamActive || c.upstreamTipSlot > 0
}

func TestCheckAndForgeProductionWaitsForUnknownActiveUpstreamTarget(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			upstreamActive:    true,
			slotsPerKESPeriod: 100,
		},
		ForgeSyncToleranceSlots: 99,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
}

func TestCheckAndForgeProductionStopsAtProtocolKESExpiry(t *testing.T) {
	creds := setupTestCredentials(t)
	genesis := synthGenesis(1, 2, time.Second, time.Unix(0, 0))
	require.NoError(t, creds.ValidateKESPeriod(genesis, 0))

	block := newForgerTestBlock(1, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	clock := &forgerTestSlotClock{
		currentSlot:       1,
		chainTipSlot:      0,
		slotsPerKESPeriod: 1,
	}
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(&logs, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// The final period in [start, start+maxEvolutions) remains valid.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, leader.callCount())
	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
	lastValidCurrent := testutil.ToFloat64(forger.metrics.currentKESPeriod)
	lastValidRemaining := testutil.ToFloat64(
		forger.metrics.remainingKESPeriods,
	)
	lastValidExpiry := testutil.ToFloat64(forger.metrics.opCertExpiryKES)

	// The same loaded producer reaches the first expired period while running.
	clock.currentSlot = 2
	clock.chainTipSlot = 1
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		1,
		leader.callCount(),
		"expired period must not run leader selection",
	)
	require.Equal(t, 1, builder.calls, "expired period must not build a block")
	require.Equal(
		t,
		1,
		broadcaster.calls,
		"expired period must not broadcast a block",
	)
	require.Contains(t, logs.String(), "operational certificate expired")
	require.Equal(t, float64(1), lastValidCurrent)
	require.Equal(t, float64(1), lastValidRemaining)
	require.Equal(t, float64(2), lastValidExpiry)

	require.Equal(
		t,
		float64(2),
		testutil.ToFloat64(forger.metrics.currentKESPeriod),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.remainingKESPeriods),
	)
	require.Equal(
		t,
		float64(2),
		testutil.ToFloat64(forger.metrics.opCertExpiryKES),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestCheckAndForgeProductionStopsBeforeOpCertStart(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.mu.Lock()
	creds.generation++
	creds.opCert.KESPeriod = 5
	creds.opCertStartKES = 5
	creds.maxKESEvolutions = 2
	creds.opCertExpiryKES = 7
	creds.opCertValidated = true
	creds.mu.Unlock()

	block := newForgerTestBlock(4, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leiosChecker := &forgerTestLeiosChecker{reason: "not eligible"}
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(&logs, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       4,
			chainTipSlot:      3,
			slotsPerKESPeriod: 1,
		},
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosMempool:        forgerTestMempoolProvider{},
		LeiosTxValidator:    &mockTxValidator{},
		PromRegistry:        prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(
		t,
		forger.checkAndForgeProduction(context.Background()),
		"pre-start policy gate must decline before KES evolution",
	)
	require.Zero(
		t,
		leader.callCount(),
		"pre-start period must not run leader selection",
	)
	require.Zero(t, leiosChecker.calls, "pre-start period must not run Leios")
	require.Zero(t, builder.calls, "pre-start period must not build a block")
	require.Zero(
		t,
		broadcaster.calls,
		"pre-start period must not broadcast a block",
	)
	require.Contains(
		t,
		logs.String(),
		"operational certificate is not yet valid",
	)
	require.Equal(
		t,
		float64(4),
		testutil.ToFloat64(forger.metrics.currentKESPeriod),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.remainingKESPeriods),
	)
	require.Equal(
		t,
		float64(5),
		testutil.ToFloat64(forger.metrics.opCertStartKES),
	)
	require.Equal(
		t,
		float64(7),
		testutil.ToFloat64(forger.metrics.opCertExpiryKES),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestCheckAndForgeProductionCountsKESUpdateFailure(t *testing.T) {
	creds := setupTestCredentials(t)
	require.NoError(t, creds.UpdateKESPeriod(1))

	builder := &forgerTestBuilder{block: newForgerTestBlock(1, 2)}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "failed to update KES period")
	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestSignBlockHeaderEnforcesProtocolKESLifetime(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.mu.Lock()
	creds.generation++
	creds.opCertStartKES = 0
	creds.maxKESEvolutions = 2
	creds.opCertExpiryKES = 2
	creds.opCertValidated = true
	creds.mu.Unlock()
	require.NoError(t, creds.UpdateKESPeriod(2))

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			slotsPerKESPeriod: 1,
		},
	})
	require.NoError(t, err)

	signature, err := forger.SignBlockHeader(2, []byte("expired header"))
	require.ErrorIs(t, err, errOpCertExpired)
	require.Nil(t, signature)
}

func TestCheckAndForgeProductionRejectsIdentityReloadDuringSelection(
	t *testing.T,
) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	leader := &forgerBlockingLeader{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(1, 2),
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 1,
		},
	})
	require.NoError(t, err)

	forgeDone := make(chan error, 1)
	go func() {
		forgeDone <- forger.checkAndForgeProduction(context.Background())
	}()
	dingotestutil.RequireReceive(
		t,
		leader.entered,
		time.Second,
		"leader entered",
	)

	alternateVRFPath := createAlternateTestVRFKey(t)
	reloadDone := make(chan error, 1)
	go func() {
		reloadDone <- creds.LoadFromFiles(
			alternateVRFPath,
			kesPath,
			opCertPath,
		)
	}()
	reloadErr := dingotestutil.RequireReceive(
		t,
		reloadDone,
		time.Second,
		"identity-changing reload completion",
	)
	require.ErrorContains(t, reloadErr, "cannot change pool or VRF identity")
	close(leader.release)
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		forgeDone,
		time.Second,
		"forge completion",
	))
	require.Equal(t, 1, leader.callCount())
	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
}

type forgerReentrantBuilder struct {
	callback    func() error
	callbackErr error
	block       ledger.Block
	cbor        []byte
	calls       int
}

func (b *forgerReentrantBuilder) BuildBlock(
	_ uint64,
	_ uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	if b.callback != nil {
		b.callbackErr = b.callback()
	}
	if b.callbackErr != nil {
		return nil, nil, b.callbackErr
	}
	return b.block, b.cbor, nil
}

func TestCheckAndForgeProductionRejectsReentrantBuilderReload(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	block := newForgerTestBlock(1, 2)
	builder := &forgerReentrantBuilder{
		block: block,
		cbor:  block.cbor,
		callback: func() error {
			if err := creds.LoadFromFiles(
				vrfPath,
				kesPath,
				opCertPath,
			); err != nil {
				return err
			}
			return creds.ValidateKESPeriod(
				synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
				0,
			)
		},
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &forgerTestSlotClock{
		currentSlot:       1,
		chainTipSlot:      0,
		slotsPerKESPeriod: 1,
	}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	forgeDone := make(chan error, 1)
	go func() {
		forgeDone <- forger.checkAndForgeProduction(context.Background())
	}()
	forgeErr := dingotestutil.RequireReceive(
		t,
		forgeDone,
		time.Second,
		"reentrant builder reload completion",
	)
	require.ErrorContains(t, forgeErr, "credential generation changed")
	require.NoError(t, builder.callbackErr)
	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		broadcaster.calls,
		"stale builder output must not be adopted",
	)
}

func TestCheckAndForgeProductionRejectsReentrantLeiosRevalidation(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	genesis := synthGenesis(1, 3, time.Second, time.Unix(0, 0))
	require.NoError(t, creds.ValidateKESPeriod(genesis, 0))

	builder := &forgerTestBuilder{
		block: newForgerTestBlock(1, 2),
	}
	broadcaster := &forgerTestBroadcaster{}
	leiosChecker := &forgerTestLeiosChecker{
		reason: "revalidated",
		callback: func() error {
			return creds.ValidateKESPeriod(genesis, 0)
		},
	}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 1,
		},
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosMempool:        forgerTestMempoolProvider{},
		LeiosTxValidator:    &mockTxValidator{},
	})
	require.NoError(t, err)

	forgeDone := make(chan error, 1)
	go func() {
		forgeDone <- forger.checkAndForgeProduction(context.Background())
	}()
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		forgeDone,
		time.Second,
		"reentrant Leios revalidation completion",
	))
	require.NoError(t, leiosChecker.callbackErr)
	require.Equal(t, 1, leiosChecker.calls)
	require.Zero(t, builder.calls, "stale Leios attempt must not build")
	require.Zero(
		t,
		broadcaster.calls,
		"stale Leios attempt must not be adopted",
	)
}

func TestCheckAndForgeProductionFailsClosedAfterKESRevalidation(t *testing.T) {
	creds := setupTestCredentials(t)
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	block := newForgerTestBlock(1, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	var logs bytes.Buffer
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(&logs, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 1,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	revalidationErr := creds.ValidateKESPeriod(
		synthGenesis(1, 1, time.Second, time.Unix(0, 0)),
		1,
	)
	require.ErrorContains(t, revalidationErr, "operational certificate expired")

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls, "failed revalidation must disable building")
	require.Zero(
		t,
		broadcaster.calls,
		"failed revalidation must disable broadcasting",
	)
	require.Contains(t, logs.String(), "KES protocol lifetime is not validated")
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.remainingKESPeriods),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.opCertExpiryKES),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestNewBlockForgerRejectsUnvalidatedKESLifetime(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))

	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			slotsPerKESPeriod: 1,
		},
	})
	require.ErrorContains(t, err, "validated KES protocol lifetime")
}

func TestNewBlockForgerRejectsInvalidOpCertGeneration(t *testing.T) {
	vrfPath, kesPath, _ := createTestKeys(t)
	corrupted := strings.Replace(testOpCertJSON, "89fc9e9f", "88fc9e9f", 1)
	require.NotEqual(t, testOpCertJSON, corrupted)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(
		vrfPath,
		kesPath,
		writeTestOpCert(t, corrupted),
	))
	require.ErrorContains(
		t,
		creds.ValidateKESPeriod(
			synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
			0,
		),
		"signature verification failed",
	)

	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			slotsPerKESPeriod: 1,
		},
	})
	require.ErrorContains(t, err, "operational certificate is not validated")
}

type forgerTestBuilder struct {
	block      ledger.Block
	cbor       []byte
	calls      int
	leiosCalls int
	leiosData  LeiosBlockData
}

func (b *forgerTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	return b.block, b.cbor, nil
}

func (b *forgerTestBuilder) BuildBlockWithLeios(
	_ uint64,
	_ uint64,
	leiosData LeiosBlockData,
) (ledger.Block, []byte, error) {
	b.leiosCalls++
	b.leiosData = leiosData
	return b.block, b.cbor, nil
}

type forgerTestBroadcaster struct {
	err   error
	panic bool
	calls int
}

func (b *forgerTestBroadcaster) AddBlock(
	ledger.Block,
	[]byte,
) error {
	b.calls++
	if b.panic {
		panic("broadcaster panic")
	}
	return b.err
}

// forgerTestPanicOnceLeader panics on its first ShouldProduceBlock
// call and reports leadership normally afterward, for exercising the
// forge cycle that follows a recovered panic.
type forgerTestPanicOnceLeader struct {
	calls int
}

func (l *forgerTestPanicOnceLeader) ShouldProduceBlock(uint64) bool {
	l.calls++
	if l.calls == 1 {
		panic("leader check panic")
	}
	return true
}

func (l *forgerTestPanicOnceLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

type forgerTestBlock struct {
	hash         lcommon.Blake2b256
	prevHash     lcommon.Blake2b256
	slot         uint64
	blockNumber  uint64
	cbor         []byte
	transactions []lcommon.Transaction
}

func newForgerTestBlock(slot, blockNumber uint64) *forgerTestBlock {
	return &forgerTestBlock{
		hash:        lcommon.NewBlake2b256(bytes.Repeat([]byte{0x01}, 32)),
		prevHash:    lcommon.NewBlake2b256(bytes.Repeat([]byte{0x02}, 32)),
		slot:        slot,
		blockNumber: blockNumber,
		cbor:        []byte{0x83, 0x01, 0x02},
	}
}

func (b *forgerTestBlock) Header() lcommon.BlockHeader { return b }

func (b *forgerTestBlock) Type() int { return int(babbage.BlockTypeBabbage) }
func (b *forgerTestBlock) Transactions() []lcommon.Transaction {
	return b.transactions
}
func (b *forgerTestBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}
func (b *forgerTestBlock) Hash() lcommon.Blake2b256 { return b.hash }

func (b *forgerTestBlock) PrevHash() lcommon.Blake2b256 { return b.prevHash }

func (b *forgerTestBlock) BlockNumber() uint64 { return b.blockNumber }
func (b *forgerTestBlock) SlotNumber() uint64  { return b.slot }

func (b *forgerTestBlock) IssuerVkey() lcommon.IssuerVkey { return lcommon.IssuerVkey{} }
func (b *forgerTestBlock) BlockBodySize() uint64          { return 0 }

func (b *forgerTestBlock) Era() lcommon.Era { return babbage.EraBabbage }
func (b *forgerTestBlock) Cbor() []byte     { return b.cbor }

func (b *forgerTestBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

type forgerTestLeiosChecker struct {
	calls       int
	allowed     bool
	reason      string
	err         error
	callback    func() error
	callbackErr error
}

type forgerTestConfirmedTxRemover struct {
	hashes []string
}

func (r *forgerTestConfirmedTxRemover) RemoveTxsByHash(hashes []string) {
	r.hashes = append(r.hashes, hashes...)
}

func TestCheckAndForgeProductionRemovesConfirmedTransactions(t *testing.T) {
	creds := setupTestCredentials(t)
	tx, err := conway.NewConwayTransactionFromCbor(
		makeMinimalTxCbor(t, 0x42, 0),
	)
	require.NoError(t, err)
	block := newForgerTestBlock(10, 2)
	block.transactions = []lcommon.Transaction{tx}
	remover := &forgerTestConfirmedTxRemover{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		ConfirmedTxs:     remover,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, []string{tx.Hash().String()}, remover.hashes)
}

func TestCheckAndForgeProductionUsesRetainedReconnectFrontier(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(114220801, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       114220801,
			chainTipSlot:      114220600,
			upstreamTipSlot:   114220800,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeSyncSkip),
	)
}

func TestCheckAndForgeProductionWaitsForEventPairedCorroboratedTarget(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(101, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       101,
			chainTipSlot:      100,
			upstreamTipSlot:   200,
			upstreamActive:    true,
			slotsPerKESPeriod: 100,
		},
		ForgeSyncToleranceSlots: 99,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
}

func TestCheckAndForgeProductionProceedsWithoutUpstreamFrontier(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// This is the value exposed after a close-before-switch event.
			upstreamTipSlot: 0,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
}

func (c *forgerTestLeiosChecker) MayProduceEndorserBlock(
	uint64,
) (bool, string, error) {
	c.calls++
	if c.callback != nil {
		c.callbackErr = c.callback()
		if c.callbackErr != nil {
			return false, "", c.callbackErr
		}
	}
	return c.allowed, c.reason, c.err
}

type forgerTestLeiosCaster struct {
	slot     uint64
	hash     []byte
	cbor     []byte
	txBodies [][]byte
}

func (c *forgerTestLeiosCaster) BroadcastEndorserBlock(
	slot uint64,
	hash []byte,
	cbor []byte,
	txBodies [][]byte,
) error {
	c.slot = slot
	c.hash = append([]byte(nil), hash...)
	c.cbor = append([]byte(nil), cbor...)
	c.txBodies = append([][]byte(nil), txBodies...)
	return nil
}

type forgerTestMempoolProvider struct {
	txs []MempoolTransaction
}

func (p forgerTestMempoolProvider) Transactions() []MempoolTransaction {
	return p.txs
}

type forgerTestLeiosCerts struct {
	eligible       []LeiosCertifiedEndorserBlock
	txHashes       []string
	txHashesOK     bool
	marked         []lcommon.Blake2b256
	gotEbSlot      uint64
	gotEbSlotCalls int
}

func (p *forgerTestLeiosCerts) EligibleCertifiedEndorserBlocks() []LeiosCertifiedEndorserBlock {
	return p.eligible
}

func (p *forgerTestLeiosCerts) CertifiedEndorserBlockTxHashes(
	_ lcommon.Blake2b256,
	ebSlot uint64,
) ([]string, bool) {
	p.gotEbSlot = ebSlot
	p.gotEbSlotCalls++
	return p.txHashes, p.txHashesOK
}

func (p *forgerTestLeiosCerts) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
) {
	p.marked = append(p.marked, ebHash)
}

type forgerTestLeiosParentAnnouncement struct {
	rbHash lcommon.Blake2b256
	hash   lcommon.Blake2b256
	ok     bool
	err    error
	calls  int
}

func (p *forgerTestLeiosParentAnnouncement) ParentLeiosAnnouncement() (
	lcommon.Blake2b256,
	lcommon.Blake2b256,
	bool,
	error,
) {
	p.calls++
	return p.rbHash, p.hash, p.ok, p.err
}

// TestCheckAndForgeProductionSkipsObserverWhenNotAdopted holds the
// contract that the blockForged observer publishes only after durable
// acceptance. The production observer republishes the block on the event
// bus and enqueues the Leios announcement that diffuses it to peers, so
// running it for a block AddBlock rejected would advertise a block this
// node never adopted.
//
// The forgeForged counter still increments before adoption, which is what
// PR #2323 required: build-versus-adopt remains observable through
// forgeForged and forgeCouldNot without publishing an unadopted block.
func TestCheckAndForgeProductionSkipsObserverWhenNotAdopted(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	innerBroadcaster := &forgerTestBroadcaster{
		err: errors.New("not adopted"),
	}
	var callOrder []string
	broadcaster := &trackingBroadcaster{
		inner: innerBroadcaster,
		onAdd: func() { callOrder = append(callOrder, "adopt") },
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			ledger.Block,
			[]byte,
			time.Duration,
		) {
			callOrder = append(callOrder, "observe")
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to add block")

	assert.Equal(t, []string{"adopt"}, callOrder)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, innerBroadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

// TestCheckAndForgeProductionObservesForgedBlockAfterAdoption is the
// positive half of the contract: the observer runs, with the built block
// and CBOR, once AddBlock has accepted the block.
func TestCheckAndForgeProductionObservesForgedBlockAfterAdoption(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	innerBroadcaster := &forgerTestBroadcaster{}
	var callOrder []string
	broadcaster := &trackingBroadcaster{
		inner: innerBroadcaster,
		onAdd: func() { callOrder = append(callOrder, "adopt") },
	}
	var (
		observedBlock   ledger.Block
		observedCbor    []byte
		observedLatency time.Duration
	)

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			block ledger.Block,
			cbor []byte,
			latency time.Duration,
		) {
			callOrder = append(callOrder, "observe")
			observedBlock = block
			observedCbor = append([]byte(nil), cbor...)
			observedLatency = latency
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Same(t, block, observedBlock)
	assert.Equal(t, blockCbor, observedCbor)
	assert.GreaterOrEqual(t, observedLatency, time.Duration(0))
	assert.Equal(t, []string{"adopt", "observe"}, callOrder)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, innerBroadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockForgedObserverPanic(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	broadcaster := &forgerTestBroadcaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			ledger.Block,
			[]byte,
			time.Duration,
		) {
			panic("observer panic")
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversLeaderCheckPanic(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leader := &forgerTestPanicOnceLeader{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// A panic from the leader checker must not escape checkAndForgeProduction
	// (which would otherwise crash the producer-loop goroutine); it is
	// treated as "not leader" for the slot, same as a checker that simply
	// returns false.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, leader.calls)
	assert.Equal(t, 0, builder.calls)
	assert.Equal(t, 0, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeNotLeader),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("selection"),
		),
	)

	// The following forge cycle proceeds normally: worker accounting and
	// running state were not corrupted by the recovered panic.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, leader.calls)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockValidatorPanic(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{panic: true}

	forger, clock := newForgerWithValidator(
		t, block, nil, broadcaster, validator,
	)

	// A panic from the validator must not escape checkAndForgeProduction; it
	// is treated as a validation failure so the block is dropped rather than
	// adopted with unknown validity.
	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "self-validation failed")
	assert.Equal(t, 1, validator.calls)
	assert.Equal(t, 0, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeValidationFailed),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("validation"),
		),
	)

	// The following forge cycle proceeds normally. It runs at the next
	// slot because the fence refuses a slot already signed for.
	validator.panic = false
	clock.currentSlot = 11
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, validator.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockBroadcasterPanic(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{panic: true}
	clock := &forgerTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// A panic from the broadcaster must not escape checkAndForgeProduction;
	// it is treated as a publish failure, matching the existing error path
	// for a broadcaster that returns an error.
	err = forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to add block")
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("publication"),
		),
	)

	// The following forge cycle proceeds normally. It runs at the next
	// slot because the fence refuses a slot already signed for.
	broadcaster.panic = false
	clock.currentSlot = 11
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestNewBlockForgerRejectsProductionLeiosWithoutTxValidator(t *testing.T) {
	creds := setupTestCredentials(t)
	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosMempool:        forgerTestMempoolProvider{},
	})
	require.EqualError(
		t,
		err,
		"production Leios forging requires transaction validator",
	)
}

func TestCheckAndForgeProductionAnnouncesForgedLeiosEB(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leiosChecker := &forgerTestLeiosChecker{allowed: true}
	leiosCaster := &forgerTestLeiosCaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  leiosCaster,
		LeiosTxValidator:    &mockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: []MempoolTransaction{
				{
					Hash: strings.Repeat("11", 32),
					Cbor: makeMinimalTxCbor(t, 0x11, 0),
					Type: conway.TxTypeConway,
				},
			},
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, leiosChecker.calls)
	require.NotEmpty(t, leiosCaster.hash)
	require.Equal(t, uint64(10), leiosCaster.slot)
	require.Equal(t, 1, builder.leiosCalls)
	require.NotNil(t, builder.leiosData.Announcement)
	require.Nil(t, builder.leiosData.Certificate)
	assert.Equal(
		t,
		leiosCaster.hash,
		builder.leiosData.Announcement.Hash.Bytes(),
	)
	assert.Equal(
		t,
		uint64(len(leiosCaster.cbor)),
		builder.leiosData.Announcement.Size,
	)
}

func TestCheckAndForgeProductionCertifiesLeiosEBAfterAdoption(t *testing.T) {
	for _, test := range []struct {
		name        string
		txHashesOK  bool
		canAnnounce bool
	}{
		{name: "closure available", txHashesOK: true, canAnnounce: true},
		{name: "closure unavailable", txHashesOK: false, canAnnounce: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			creds := setupTestCredentials(t)
			block := newForgerTestBlock(10, 2)
			builder := &forgerTestBuilder{block: block, cbor: block.cbor}
			broadcaster := &forgerTestBroadcaster{}
			ebHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
			rbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
			cert := &lcommon.LeiosEbCertificate{
				SlotNo:            9,
				EndorserBlockHash: ebHash,
				Signers:           []byte{0x80},
				AggregatedSignature: make(
					[]byte,
					lcommon.LeiosBlsSignatureSize,
				),
			}
			leiosCerts := &forgerTestLeiosCerts{
				txHashes:   []string{strings.Repeat("11", 32)},
				txHashesOK: test.txHashesOK,
				eligible: []LeiosCertifiedEndorserBlock{
					{
						SlotNo:            9,
						EndorserBlockHash: ebHash,
						Certificate:       cert,
						AnnouncingRbHash:  rbHash,
					},
				},
			}
			parent := &forgerTestLeiosParentAnnouncement{
				rbHash: rbHash, hash: ebHash, ok: true,
			}
			leiosChecker := &forgerTestLeiosChecker{allowed: true}
			leiosCaster := &forgerTestLeiosCaster{}

			forger, err := NewBlockForger(ForgerConfig{
				Mode: ModeProduction,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
				Credentials:      creds,
				LeaderChecker:    forgerTestLeader{},
				BlockBuilder:     builder,
				BlockBroadcaster: broadcaster,
				SlotClock: forgerTestSlotClock{
					currentSlot:       10,
					chainTipSlot:      9,
					slotsPerKESPeriod: 100,
				},
				LeiosCertificateProvider:        leiosCerts,
				LeiosParentAnnouncementProvider: parent,
				LeiosProduceChecker:             leiosChecker,
				LeiosEBBroadcaster:              leiosCaster,
				LeiosTxValidator:                &mockTxValidator{},
				LeiosMempool: forgerTestMempoolProvider{
					txs: []MempoolTransaction{
						{
							Hash: strings.Repeat("11", 32),
							Cbor: makeMinimalTxCbor(t, 0x11, 0),
							Type: conway.TxTypeConway,
						},
						{
							Hash: strings.Repeat("22", 32),
							Cbor: makeMinimalTxCbor(t, 0x22, 0),
							Type: conway.TxTypeConway,
						},
					},
				},
				PromRegistry: prometheus.NewRegistry(),
			})
			require.NoError(t, err)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Equal(t, 1, builder.leiosCalls)
			require.Same(t, cert, builder.leiosData.Certificate)
			require.Equal(t, test.canAnnounce, leiosChecker.calls == 1)
			if test.canAnnounce {
				require.NotNil(t, builder.leiosData.Announcement)
				require.NotEmpty(t, leiosCaster.hash)
				require.Equal(
					t,
					[][]byte{makeMinimalTxCbor(t, 0x22, 0)},
					leiosCaster.txBodies,
				)
			} else {
				require.Nil(t, builder.leiosData.Announcement)
				require.Empty(t, leiosCaster.hash)
			}
			require.Equal(t, []lcommon.Blake2b256{ebHash}, leiosCerts.marked)
			require.Equal(t, 1, parent.calls)
			// CertifiedEndorserBlockTxHashes must be called with the
			// eligible certificate's own slot (9, from eb.SlotNo above), not
			// the forged ranking block's slot (10) or zero: the manifest is
			// content-addressed, so the same hash could be a distinct,
			// unrelated occurrence at another slot, and the wrong slot here
			// would resolve the wrong occurrence (issue #3513 review).
			require.Equal(t, 1, leiosCerts.gotEbSlotCalls)
			require.Equal(t, uint64(9), leiosCerts.gotEbSlot)
		})
	}
}

func TestCheckAndForgeProductionCertifiesOnlyParentAnnouncedLeiosEB(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	wrongHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x22}, 32))
	parentHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
	parentRbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
	wrongCert := &lcommon.LeiosEbCertificate{
		SlotNo:              8,
		EndorserBlockHash:   wrongHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	parentCert := &lcommon.LeiosEbCertificate{
		SlotNo:              9,
		EndorserBlockHash:   parentHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	wrongContextCert := &lcommon.LeiosEbCertificate{
		SlotNo:              8,
		EndorserBlockHash:   parentHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	leiosCerts := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				SlotNo:            8,
				EndorserBlockHash: wrongHash,
				Certificate:       wrongCert,
				AnnouncingRbHash:  parentRbHash,
			},
			{
				SlotNo:            8,
				EndorserBlockHash: parentHash,
				Certificate:       wrongContextCert,
				AnnouncingRbHash: lcommon.NewBlake2b256(
					bytes.Repeat([]byte{0x55}, 32),
				),
			},
			{
				SlotNo:            9,
				EndorserBlockHash: parentHash,
				Certificate:       parentCert,
				AnnouncingRbHash:  parentRbHash,
			},
		},
	}
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: parentRbHash, hash: parentHash, ok: true,
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.leiosCalls)
	require.Nil(t, builder.leiosData.Announcement)
	require.Same(t, parentCert, builder.leiosData.Certificate)
	require.Equal(t, []lcommon.Blake2b256{parentHash}, leiosCerts.marked)
	require.Equal(t, 1, parent.calls)
}
