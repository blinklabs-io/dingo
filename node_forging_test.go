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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEpochInfoAdapterProvidesExactActiveSlotCoeff pins the wiring that makes
// the leader schedule use the exact Shelley genesis active slot coefficient.
//
// leader.ActiveSlotCoeffRatProvider is an optional interface: computeSchedule
// type-asserts it and silently falls back to the float64 accessor when it is not
// satisfied. Without this assertion, dropping or renaming
// epochInfoAdapter.ActiveSlotCoeffRat would compile cleanly and quietly restore
// the float64 approximation, which yields a strictly larger leadership threshold
// than the reference node's.
func TestEpochInfoAdapterProvidesExactActiveSlotCoeff(t *testing.T) {
	t.Parallel()

	var adapter any = &epochInfoAdapter{}
	if _, ok := adapter.(leader.EpochInfoProvider); !ok {
		t.Fatal("epochInfoAdapter must satisfy leader.EpochInfoProvider")
	}
	if _, ok := adapter.(leader.ActiveSlotCoeffRatProvider); !ok {
		t.Fatal(
			"epochInfoAdapter must satisfy " +
				"leader.ActiveSlotCoeffRatProvider so the leader schedule " +
				"uses the exact genesis coefficient",
		)
	}
}

func TestBlockBroadcasterAddsWithoutEventSubscriber(t *testing.T) {
	t.Parallel()

	blocks, err := fixtures.GenerateConwayChain(
		0,
		lcommon.Blake2b256{},
		1,
		1,
		1,
	)
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), nil, nil)
	require.NoError(t, err)
	broadcaster := &blockBroadcaster{
		chain:  cm.PrimaryChain(),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	require.NoError(
		t,
		broadcaster.AddBlock(context.Background(), blocks[0], blocks[0].Cbor()),
	)
	require.Equal(
		t,
		blocks[0].Hash().Bytes(),
		cm.PrimaryChain().Tip().Point.Hash,
	)
}

func TestBlockBroadcasterRejectsUnavailableChain(t *testing.T) {
	t.Parallel()

	blocks, err := fixtures.GenerateConwayChain(
		0,
		lcommon.Blake2b256{},
		1,
		1,
		1,
	)
	require.NoError(t, err)
	broadcaster := &blockBroadcaster{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err = broadcaster.AddBlock(
		context.Background(),
		blocks[0],
		blocks[0].Cbor(),
	)
	require.EqualError(t, err, "chain unavailable")
}

const (
	sigmaDenomEpoch = uint64(7)
	// The mark rows sum to 4_000_000 ...
	sigmaDenomPoolAStake = uint64(3_000_000)
	sigmaDenomPoolBStake = uint64(1_000_000)
	sigmaDenomRowSum     = sigmaDenomPoolAStake + sigmaDenomPoolBStake
	// ... while epoch_summary.total_active_stake carries a different value.
	//
	// Rotation normally writes both from one calculation, so they match. This
	// fixture drives them apart on purpose, because "they agree by
	// construction" is a property of the WRITER: it makes the two readers
	// indistinguishable in every ordinary fixture and so hides which one a
	// given code path actually consults. Separating them is the only way to
	// observe that choice, and is precisely the report that the forge
	// and verify paths made it differently.
	sigmaDenomSummaryTotal = uint64(5_000_000)
)

// newSigmaDenominatorLedger builds a real LedgerState over a real database,
// so the assertions below run against the production forging adapter rather
// than a reimplementation of it.
func newSigmaDenominatorLedger(
	t *testing.T,
) (*ledger.LedgerState, *database.Database) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	chainManager, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		Logger:       logger,
	})
	require.NoError(t, err)
	return ledgerState, db
}

func sigmaDenomPoolKeyHash(fill byte) []byte {
	hash := make([]byte, 28)
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

// seedSigmaDenominatorSnapshot writes the mark rows and the epoch summary with
// deliberately different totals.
func seedSigmaDenominatorSnapshot(
	t *testing.T,
	db *database.Database,
	poolA, poolB []byte,
) {
	t.Helper()
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:          sigmaDenomEpoch,
				SnapshotType:   models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:    poolA,
				TotalStake:     dbtypes.Uint64(sigmaDenomPoolAStake),
				DelegatorCount: 1,
				CapturedSlot:   1,
			},
			{
				Epoch:          sigmaDenomEpoch,
				SnapshotType:   models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:    poolB,
				TotalStake:     dbtypes.Uint64(sigmaDenomPoolBStake),
				DelegatorCount: 1,
				CapturedSlot:   1,
			},
		},
		nil,
	))
	require.NoError(t, db.Metadata().SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            sigmaDenomEpoch,
			TotalActiveStake: dbtypes.Uint64(sigmaDenomSummaryTotal),
			TotalPoolCount:   2,
			TotalDelegators:  2,
			BoundarySlot:     1,
			// Required for GetTotalActiveStake to prefer the summary; this is
			// what rotation sets, so it is the state a synced node is in.
			SnapshotReady: true,
		},
		nil,
	))
}

// TestStakeDistributionAdapterResolvesDenominatorThroughVerifyAccessor pins
// the forging adapter's stake denominator.
//
// The forging adapter used to return ledger.StakeDistribution.TotalStake,
// which LedgerView.GetStakeDistribution accumulates by summing the mark rows
// itself. Header verification instead reads
// epoch_summary.total_active_stake through Metadata().GetTotalActiveStake.
// Two derivations of one consensus quantity: a node whose forge denominator
// differs from its verify denominator can forge a block it would itself
// reject, or decline a slot it is genuinely eligible for.
//
// The fixture makes the summary and the row sum differ, then asserts the
// adapter reports the value VERIFICATION would use. Before the fix the
// adapter returns sigmaDenomRowSum (4_000_000) and both assertions fail.
func TestStakeDistributionAdapterResolvesDenominatorThroughVerifyAccessor(
	t *testing.T,
) {
	t.Parallel()

	ledgerState, db := newSigmaDenominatorLedger(t)
	poolA := sigmaDenomPoolKeyHash(0x41)
	poolB := sigmaDenomPoolKeyHash(0x42)
	seedSigmaDenominatorSnapshot(t, db, poolA, poolB)

	// The denominator header verification resolves, read through the accessor
	// verify_header.go uses. Captured from the database rather than restated
	// as a literal, so the test compares the two paths instead of comparing
	// one path to a number this test chose.
	verifyTotal, err := db.Metadata().GetTotalActiveStake(
		sigmaDenomEpoch,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, sigmaDenomSummaryTotal, verifyTotal,
		"fixture precondition: the verify accessor must serve the summary")

	adapter := &stakeDistributionAdapter{ledgerState: ledgerState}
	poolStake, forgeTotal, err := adapter.GetPoolAndTotalActiveStake(
		sigmaDenomEpoch,
		poolA,
	)
	require.NoError(t, err)

	assert.Equal(t, verifyTotal, forgeTotal,
		"forge and verify must resolve one denominator through one accessor "+
			"(dingo #3814)")
	assert.NotEqual(t, sigmaDenomRowSum, forgeTotal,
		"the forge denominator must not be re-derived by summing the mark "+
			"rows; that is the second derivation #3814 removes")

	// The numerator is unchanged by this fix and must still come from the
	// pool's own mark row.
	assert.Equal(t, sigmaDenomPoolAStake, poolStake,
		"the numerator must remain the pool's mark-snapshot stake")
}

// TestStakeDistributionAdapterSigmaPairSurvivesRecapture checks that each
// adapter read yields a self-consistent sigma across a snapshot re-capture.
//
// Scope, stated plainly: this drives a re-capture between two SEPARATE
// adapter calls, not between the two halves of a single call. It therefore
// does NOT by itself prove the atomicity property -- a write
// landing inside one call is not reachable from outside the adapter without
// a seam that does not exist. What it does prove is that both halves of a
// given read move together to the new generation rather than one of them
// lagging, and it would catch a fix that made only one half transactional.
//
// The atomicity property itself is pinned two ways instead:
// TestStakeDistributionProviderForbidsTornSigmaRead below makes the split
// read unexpressible in the provider interface, and
// TestComputeScheduleReadsSigmaPairInOneProviderCall in ledger/leader
// asserts the real schedule computation performs exactly one paired read.
//
// The two generations are chosen with DIFFERENT absolute values but the SAME
// sigma, so a torn pair is detectable as a sigma matching neither.
func TestStakeDistributionAdapterSigmaPairSurvivesRecapture(t *testing.T) {
	t.Parallel()

	ledgerState, db := newSigmaDenominatorLedger(t)
	poolA := sigmaDenomPoolKeyHash(0x41)
	poolB := sigmaDenomPoolKeyHash(0x42)

	// Generation one: sigma = 3_000_000 / 5_000_000.
	seedSigmaDenominatorSnapshot(t, db, poolA, poolB)

	adapter := &stakeDistributionAdapter{ledgerState: ledgerState}
	poolStake, total, err := adapter.GetPoolAndTotalActiveStake(
		sigmaDenomEpoch,
		poolA,
	)
	require.NoError(t, err)

	// Generation two: every value doubled, so sigma is identical while both
	// halves differ. Written AFTER the read above, then read again below.
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:          sigmaDenomEpoch,
				SnapshotType:   models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:    poolA,
				TotalStake:     dbtypes.Uint64(sigmaDenomPoolAStake * 2),
				DelegatorCount: 1,
				CapturedSlot:   2,
			},
		},
		nil,
	))
	require.NoError(t, db.Metadata().SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            sigmaDenomEpoch,
			TotalActiveStake: dbtypes.Uint64(sigmaDenomSummaryTotal * 2),
			TotalPoolCount:   2,
			TotalDelegators:  2,
			BoundarySlot:     2,
			SnapshotReady:    true,
		},
		nil,
	))

	poolStake2, total2, err := adapter.GetPoolAndTotalActiveStake(
		sigmaDenomEpoch,
		poolA,
	)
	require.NoError(t, err)

	// Each read must be self-consistent: numerator*otherDenominator equals
	// denominator*otherNumerator only when both pairs carry the same sigma.
	// Cross-multiplied to keep this exact rather than float.
	assert.Equal(t,
		poolStake*sigmaDenomSummaryTotal,
		total*sigmaDenomPoolAStake,
		"the first read's sigma must come from a single snapshot generation",
	)
	assert.Equal(t,
		poolStake2*sigmaDenomSummaryTotal,
		total2*sigmaDenomPoolAStake,
		"the second read's sigma must come from a single snapshot generation",
	)
	// And the second read must actually have observed the re-capture, or the
	// assertions above would be vacuous.
	assert.Equal(t, sigmaDenomPoolAStake*2, poolStake2,
		"the second read must observe the re-captured snapshot")
	assert.Equal(t, sigmaDenomSummaryTotal*2, total2,
		"the second read must observe the re-captured summary")
}

// TestStakeDistributionProviderForbidsTornSigmaRead pins the interface shape
// that makes the defect unexpressible.
//
// The fix is not only that the adapter now reads both halves in one
// transaction; it is that StakeDistributionProvider no longer offers a way to
// read them separately. A future adapter cannot reintroduce the torn read
// without changing the interface, which this test makes a visible decision
// rather than an accident.
func TestStakeDistributionProviderForbidsTornSigmaRead(t *testing.T) {
	t.Parallel()

	var adapter any = &stakeDistributionAdapter{}

	if _, ok := adapter.(leader.StakeDistributionProvider); !ok {
		t.Fatal(
			"stakeDistributionAdapter must satisfy " +
				"leader.StakeDistributionProvider",
		)
	}

	// The separate accessors must be gone. Either one surviving means a
	// caller can still take the numerator and the denominator from different
	// transactions.
	type poolStakeReader interface {
		GetPoolStake(uint64, []byte) (uint64, error)
	}
	type totalStakeReader interface {
		GetTotalActiveStake(uint64) (uint64, error)
	}
	if _, ok := adapter.(poolStakeReader); ok {
		t.Error(
			"stakeDistributionAdapter must not expose a standalone " +
				"GetPoolStake; the sigma pair is read together (dingo #3815)",
		)
	}
	if _, ok := adapter.(totalStakeReader); ok {
		t.Error(
			"stakeDistributionAdapter must not expose a standalone " +
				"GetTotalActiveStake; the sigma pair is read together " +
				"(dingo #3815)",
		)
	}
}

func replaceSigmaSnapshotAtomically(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	poolStake, totalStake uint64,
	capturedSlot uint64,
) {
	t.Helper()
	txn := db.Transaction(context.Background(), true)
	defer func() { require.NoError(t, txn.Rollback()) }()

	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{{
			Epoch:          sigmaDenomEpoch,
			SnapshotType:   models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:    poolKeyHash,
			TotalStake:     dbtypes.Uint64(poolStake),
			DelegatorCount: 1,
			CapturedSlot:   capturedSlot,
		}},
		txn.Metadata(),
	))
	require.NoError(t, db.Metadata().SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            sigmaDenomEpoch,
			TotalActiveStake: dbtypes.Uint64(totalStake),
			TotalPoolCount:   2,
			TotalDelegators:  2,
			BoundarySlot:     capturedSlot,
			SnapshotReady:    true,
		},
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())
}

// TestStakeDistributionAdapterKeepsSigmaConsistentAcrossRecapture proves the
// reader's transaction is the consistency boundary for the sigma pair.
//
// The hook releases an atomic recapture after the numerator query has fixed the
// read transaction's snapshot but before the denominator query. A reader that
// opens one transaction per half can combine generation one and generation two;
// the paired accessor must return generation one in full.
func TestStakeDistributionAdapterKeepsSigmaConsistentAcrossRecapture(
	t *testing.T,
) {
	t.Parallel()

	ledgerState, db := newSigmaDenominatorLedger(t)
	poolA := sigmaDenomPoolKeyHash(0x41)
	poolB := sigmaDenomPoolKeyHash(0x42)
	replaceSigmaSnapshotAtomically(
		t, db, poolA,
		sigmaDenomPoolAStake, sigmaDenomSummaryTotal, 1,
	)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{{
			Epoch:          sigmaDenomEpoch,
			SnapshotType:   models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:    poolB,
			TotalStake:     dbtypes.Uint64(sigmaDenomPoolBStake),
			DelegatorCount: 1,
			CapturedSlot:   1,
		}},
		nil,
	))

	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRead) }) }
	defer release()
	result := make(chan struct {
		poolStake  uint64
		totalStake uint64
		err        error
	}, 1)
	adapter := &stakeDistributionAdapter{
		ledgerState: ledgerState,
		afterPoolStakeReadFn: func() {
			close(readStarted)
			<-releaseRead
		},
	}
	go func() {
		poolStake, totalStake, err := adapter.GetPoolAndTotalActiveStake(
			sigmaDenomEpoch,
			poolA,
		)
		result <- struct {
			poolStake  uint64
			totalStake uint64
			err        error
		}{poolStake, totalStake, err}
	}()

	select {
	case <-readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("sigma reader did not reach the coordinated recapture point")
	}
	// Generation two changes both halves while the reader is paused between
	// its two SQL statements. The write is one transaction, matching the
	// snapshot publication path in ledger/snapshot/rotation.go.
	replaceSigmaSnapshotAtomically(
		t, db, poolA,
		sigmaDenomPoolAStake*2, sigmaDenomSummaryTotal*2, 2,
	)
	release()

	var got struct {
		poolStake  uint64
		totalStake uint64
		err        error
	}
	select {
	case got = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("sigma reader did not finish after recapture")
	}
	require.NoError(t, got.err)
	require.Equal(t, sigmaDenomPoolAStake, got.poolStake,
		"the numerator must remain from the reader's snapshot generation")
	require.Equal(t, sigmaDenomSummaryTotal, got.totalStake,
		"the denominator must not come from the recaptured generation")
	require.Equal(t,
		got.poolStake*sigmaDenomSummaryTotal,
		got.totalStake*sigmaDenomPoolAStake,
		"a sigma read must use one committed snapshot generation",
	)
}

// opCertFixtureWithCounter writes an operational certificate carrying
// issueNumber over the devnet KES verification key, signed by a freshly
// generated cold key. The shipped devnet opcert is fixed at issue number 0,
// which cannot express a counter gap against an observed on-chain value.
func opCertFixtureWithCounter(t *testing.T, issueNumber uint64) string {
	t.Helper()
	devnetCert, err := bursa.LoadKeyFromFile(
		filepath.Join(devnetKeysDir, "opcert.cert"),
	)
	if err != nil {
		t.Fatalf("load devnet opcert fixture: %v", err)
	}
	kesVKey := devnetCert.VKey
	kesPeriod := devnetCert.OpCertKesPeriod

	coldVKey, coldSKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate cold key: %v", err)
	}
	// cardano-ledger OCertSignable.getSignableRepresentation:
	//   KES vkey (32) || issue number (8 BE) || KES period (8 BE)
	var certBody [48]byte
	copy(certBody[:32], kesVKey)
	binary.BigEndian.PutUint64(certBody[32:40], issueNumber)
	binary.BigEndian.PutUint64(certBody[40:48], kesPeriod)
	signature := ed25519.Sign(coldSKey, certBody[:])

	certCbor, err := cbor.Encode([]any{
		[]any{kesVKey, issueNumber, kesPeriod, signature},
		[]byte(coldVKey),
	})
	if err != nil {
		t.Fatalf("encode operational certificate: %v", err)
	}
	envelope, err := json.Marshal(map[string]string{
		"type":        "NodeOperationalCertificate",
		"description": "",
		"cborHex":     hex.EncodeToString(certCbor),
	})
	if err != nil {
		t.Fatalf("encode operational certificate envelope: %v", err)
	}
	path := filepath.Join(t.TempDir(), "opcert.cert")
	if err := os.WriteFile(path, envelope, 0o644); err != nil {
		t.Fatalf("write operational certificate: %v", err)
	}
	return path
}

// laggingEraSource models a LedgerState whose applied tip is behind
// wall-clock time. ProtocolParamsForSlot mirrors
// LedgerState.ProtocolParamsForSlot: it forecasts forward through the era
// shape for a slot beyond the applied tip, so a wall-clock slot resolves to a
// Praos era the applied chain has not reached.
type laggingEraSource struct {
	tipSlot       uint64
	wallSlot      uint64
	praosFromSlot uint64
}

func (s laggingEraSource) Tip() ochainsync.Tip {
	return ochainsync.Tip{Point: ocommon.Point{Slot: s.tipSlot}}
}

func (s laggingEraSource) CurrentSlot() (uint64, error) {
	return s.wallSlot, nil
}

func (s laggingEraSource) GetCurrentPParams() lcommon.ProtocolParameters {
	return s.ProtocolParamsForSlot(s.tipSlot)
}

func (s laggingEraSource) ProtocolParamsForSlot(
	slot uint64,
) lcommon.ProtocolParameters {
	if slot >= s.praosFromSlot {
		return &babbage.BabbageProtocolParameters{}
	}
	return &alonzo.AlonzoProtocolParameters{}
}

// opCertSeqLedgerView reports a pool registration matching the loaded VRF key
// and a fixed observed on-chain opcert counter.
type opCertSeqLedgerView struct {
	regVRFHash [32]byte
	latestSeq  uint64
}

func (v opCertSeqLedgerView) PoolRegistrationVRFKeyHash(
	[28]byte,
) ([32]byte, bool, error) {
	return v.regVRFHash, true, nil
}

func (v opCertSeqLedgerView) LatestOpCertSequence(
	[28]byte,
) (uint64, bool, error) {
	return v.latestSeq, true, nil
}

// TestValidateBlockProducerLedger_LaggingTipStarts covers the operational
// case: a producer restarting with an applied tip well behind wall-clock time.
// The observed counter is the pre-catch-up value, so measuring it against the
// era wall-clock time has reached reports a gap that does not exist on the
// chain the node has actually applied. Startup must proceed; the forge loop
// applies the era-scoped rule per leader slot once the node is near the tip.
func TestValidateBlockProducerLedger_LaggingTipStarts(t *testing.T) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 7)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	err = n.validateBlockProducerLedgerWithSource(
		creds,
		view,
		laggingEraSource{
			tipSlot:       1_000,
			wallSlot:      2_000,
			praosFromSlot: 1_500,
		},
	)
	if err != nil {
		t.Fatalf(
			"block producer with a lagging applied tip must start, got: %v",
			err,
		)
	}
}

// TestValidateBlockProducerLedger_SyncedTipRejectsGap pins the property the
// lagging-tip allowance must not cost: on a node whose applied tip is already
// in a Praos era, a genuine counter gap still refuses startup.
func TestValidateBlockProducerLedger_SyncedTipRejectsGap(t *testing.T) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 7)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	err = n.validateBlockProducerLedgerWithSource(
		creds,
		view,
		laggingEraSource{
			tipSlot:       2_000,
			wallSlot:      2_000,
			praosFromSlot: 1_500,
		},
	)
	if err == nil {
		t.Fatal("expected a gapped counter on a synced node to be rejected")
	}
	if !strings.Contains(err.Error(), "skips ahead") {
		t.Fatalf("expected a gapped-rotation error, got: %v", err)
	}
}

// TestValidateBlockProducerLedger_SyncedTipRejectsStaleCounter pins the other
// half of the rule: a counter below the observed on-chain value is a stale or
// stolen hot key and refuses startup regardless of era.
func TestValidateBlockProducerLedger_SyncedTipRejectsStaleCounter(
	t *testing.T,
) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 4)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	err = n.validateBlockProducerLedgerWithSource(
		creds,
		view,
		laggingEraSource{
			tipSlot:       1_000,
			wallSlot:      2_000,
			praosFromSlot: 1_500,
		},
	)
	if err == nil {
		t.Fatal("expected a stale counter to be rejected")
	}
	if !strings.Contains(err.Error(), "below last seen") {
		t.Fatalf("expected a stale-counter error, got: %v", err)
	}
}

// TestValidateBlockProducerLedger_NilSourceStarts covers the absent era
// context: no slot clock, no protocol parameters. That is an unevaluated rule,
// not a violated one, so startup proceeds on the staleness rule alone.
func TestValidateBlockProducerLedger_NilSourceStarts(t *testing.T) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 7)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	if err := n.validateBlockProducerLedgerWithSource(
		creds, view, nil,
	); err != nil {
		t.Fatalf("missing era context must not refuse startup, got: %v", err)
	}
}

// unobservedOpCertLedgerView reports a pool registration matching the loaded
// VRF key and no opcert counter observed on chain for it.
type unobservedOpCertLedgerView struct {
	regVRFHash [32]byte
}

func (v unobservedOpCertLedgerView) PoolRegistrationVRFKeyHash(
	[28]byte,
) ([32]byte, bool, error) {
	return v.regVRFHash, true, nil
}

func (v unobservedOpCertLedgerView) LatestOpCertSequence(
	[28]byte,
) (uint64, bool, error) {
	return 0, false, nil
}

// TestValidateBlockProducerLedger_SyncedTipUnobservedCounterUsesZeroBaseline
// pins startup to the rule block application applies to a registered pool
// with no observed counter: zero is the baseline, so on a Praos applied tip
// counter 1 starts and counter 2 is a gapped rotation.
func TestValidateBlockProducerLedger_SyncedTipUnobservedCounterUsesZeroBaseline(
	t *testing.T,
) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		counter uint64
		wantErr bool
	}{
		{name: "counter one starts", counter: 1},
		{name: "counter two is refused", counter: 2, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vrf, kes, _ := devnetCredPaths(t)
			opcert := opCertFixtureWithCounter(t, tt.counter)
			cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
			n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
			n.config.network = "preview"
			creds, err := n.validateBlockProducerStartupAtSlot(0)
			if err != nil {
				t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
			}
			view := unobservedOpCertLedgerView{
				regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
			}
			err = n.validateBlockProducerLedgerWithSource(
				creds,
				view,
				laggingEraSource{
					tipSlot:       2_000,
					wallSlot:      2_000,
					praosFromSlot: 1_500,
				},
			)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("counter %d must start, got: %v", tt.counter, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected counter %d to be refused", tt.counter)
			}
			if !strings.Contains(err.Error(), "skips ahead of last seen 0") {
				t.Fatalf("expected a gapped-rotation error, got: %v", err)
			}
		})
	}
}

// devnetKeysDir locates the credential fixtures shipped with the repo.
// Path is relative to this file (top-level dingo package).
const devnetKeysDir = "config/cardano/devnet/keys"

func devnetCredPaths(t testing.TB) (vrf, kes, opcert string) {
	t.Helper()
	tmpDir := t.TempDir()
	copyFixture := func(name string, mode os.FileMode) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(devnetKeysDir, name))
		if err != nil {
			t.Fatalf("read devnet credential fixture %s: %v", name, err)
		}
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, data, mode); err != nil {
			t.Fatalf("copy devnet credential fixture %s: %v", name, err)
		}
		return path
	}

	vrf = copyFixture("vrf.skey", 0o600)
	kes = copyFixture("kes.skey", 0o600)
	testutil.RestrictFileToCurrentUser(t, vrf)
	testutil.RestrictFileToCurrentUser(t, kes)
	// OpCerts contain only public artifacts and intentionally remain exempt
	// from the secret-key permission policy.
	opcert = copyFixture("opcert.cert", 0o644)
	return vrf, kes, opcert
}

// shelleyGenesisCfgForBP returns a CardanoNodeConfig with a Shelley
// genesis that is plausible for the devnet opcert (KESPeriod=0,
// IssueNumber=0). systemStart slightly in the past, slotsPerKESPeriod
// generous so the opcert is current rather than expired.
func shelleyGenesisCfgForBP(
	t *testing.T,
	systemStart time.Time,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "` + systemStart.UTC().Format(time.RFC3339Nano) + `",
		"securityParam": 10,
		"activeSlotsCoeff": 0.5,
		"slotsPerKESPeriod": 129600,
		"maxKESEvolutions": 62,
		"slotLength": 1
	}`)); err != nil {
		t.Fatalf("LoadShelleyGenesisFromReader: %v", err)
	}
	return cfg
}

func newTestNodeForBP(
	t *testing.T,
	enabled bool,
	vrf, kes, opcert string,
	cardanoCfg *cardano.CardanoNodeConfig,
) *Node {
	t.Helper()
	cfg := Config{
		logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		blockProducer:                 enabled,
		shelleyVRFKey:                 vrf,
		shelleyKESKey:                 kes,
		shelleyOperationalCertificate: opcert,
		cardanoNodeConfig:             cardanoCfg,
	}
	return &Node{config: cfg}
}

func TestValidateBlockProducerStartup_HappyPath(t *testing.T) {
	t.Parallel()

	vrf, kes, opcert := devnetCredPaths(t)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	if !creds.IsLoaded() {
		t.Error("expected credentials to be loaded")
	}
}

func TestValidateBlockProducerStartup_NoCardanoConfig(t *testing.T) {
	t.Parallel()

	vrf, kes, opcert := devnetCredPaths(t)
	n := newTestNodeForBP(t, true, vrf, kes, opcert, nil)
	_, err := n.validateBlockProducerStartup()
	if err == nil {
		t.Fatal("expected error for missing cardano node config")
	}
	if !strings.Contains(err.Error(), "Cardano node config") {
		t.Errorf("expected 'Cardano node config' in error, got: %v", err)
	}
}

func TestValidateBlockProducerStartup_ExpiredKESPeriod(t *testing.T) {
	t.Parallel()

	// systemStart a year in the past with slotsPerKESPeriod=10 means
	// many KES periods have elapsed; maxKESEvolutions=1 makes anything
	// past period 1 expired, so the devnet opcert (KESPeriod=0) is well
	// outside its validity window and validation must reject it.
	vrf, kes, opcert := devnetCredPaths(t)
	cfg := &cardano.CardanoNodeConfig{}
	systemStart := time.Now().Add(-365 * 24 * time.Hour)
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "` + systemStart.UTC().Format(time.RFC3339Nano) + `",
		"securityParam": 10,
		"activeSlotsCoeff": 0.5,
		"slotsPerKESPeriod": 10,
		"maxKESEvolutions": 1,
		"slotLength": 1
	}`)); err != nil {
		t.Fatalf("LoadShelleyGenesisFromReader: %v", err)
	}
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cfg)
	_, err := n.validateBlockProducerStartupAtSlot(20)
	if err == nil {
		t.Fatal("expected error for expired opcert KES period")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected 'expired' in error, got: %v", err)
	}
}

func TestValidateBlockProducerStartup_MissingFile(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(
		t, true,
		filepath.Join(tmp, "missing-vrf.skey"),
		filepath.Join(tmp, "missing-kes.skey"),
		filepath.Join(tmp, "missing-opcert.cert"),
		cardanoCfg,
	)
	_, err := n.validateBlockProducerStartupAtSlot(0)
	if err == nil {
		t.Fatal("expected error for missing credential files")
	}
	if !strings.Contains(err.Error(), "load pool credentials") {
		t.Errorf("expected 'load pool credentials' in error, got: %v", err)
	}
}

type testBlockProducerLedgerView struct {
	registered bool
	regVRFHash [32]byte
}

func (v testBlockProducerLedgerView) PoolRegistrationVRFKeyHash(
	[28]byte,
) ([32]byte, bool, error) {
	return v.regVRFHash, v.registered, nil
}

func (v testBlockProducerLedgerView) LatestOpCertSequence(
	[28]byte,
) (uint64, bool, error) {
	return 0, false, nil
}

func mismatchedVRFHash() [32]byte {
	var h [32]byte
	for i := range h {
		h[i] = 0xdd
	}
	return h
}

func TestValidateBlockProducerLedger_NonDevnetVRFMismatchIsFatal(t *testing.T) {
	t.Parallel()

	vrf, kes, opcert := devnetCredPaths(t)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	err = n.validateBlockProducerLedgerWithViewAtSlot(
		creds,
		testBlockProducerLedgerView{
			registered: true,
			regVRFHash: mismatchedVRFHash(),
		},
		nil,
		0,
	)
	if !errors.Is(err, forging.ErrVRFKeyHashMismatch) {
		t.Fatalf("expected VRF mismatch error, got: %v", err)
	}
}

func TestValidateBlockProducerLedger_DevnetVRFMismatchWarns(t *testing.T) {
	t.Parallel()

	vrf, kes, opcert := devnetCredPaths(t)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "devnet"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	err = n.validateBlockProducerLedgerWithViewAtSlot(
		creds,
		testBlockProducerLedgerView{
			registered: true,
			regVRFHash: mismatchedVRFHash(),
		},
		nil,
		0,
	)
	if err != nil {
		t.Fatalf("devnet mismatch should warn and continue: %v", err)
	}
}

func TestHandleGenesisSnapshotError_BlockProducerFatal(t *testing.T) {
	t.Parallel()

	n := &Node{
		config: Config{
			logger:        slog.New(slog.NewJSONHandler(io.Discard, nil)),
			blockProducer: true,
		},
	}
	sentinel := errors.New("db unavailable")
	err := n.handleGenesisSnapshotError(sentinel)
	if err == nil {
		t.Fatal("expected fatal error for block producer, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel wrapped in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to capture genesis snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHandleGenesisSnapshotError_RelayWarnsAndContinues(t *testing.T) {
	t.Parallel()

	n := &Node{
		config: Config{
			logger:        slog.New(slog.NewJSONHandler(io.Discard, nil)),
			blockProducer: false,
		},
	}
	err := n.handleGenesisSnapshotError(errors.New("db unavailable"))
	if err != nil {
		t.Errorf("expected nil for relay node, got: %v", err)
	}
}

type testMempoolTransactionSource struct {
	txs []mempool.MempoolTransaction
}

func (s testMempoolTransactionSource) Transactions() []mempool.MempoolTransaction {
	return s.txs
}

func (s testMempoolTransactionSource) RemoveTxsByHash(_ []string) {}

// TestMempoolAdaptersPreservePendingTransactionView verifies the node-level
// adapters preserve the pending transaction fields needed for block building.
func TestMempoolAdaptersPreservePendingTransactionView(t *testing.T) {
	t.Parallel()

	source := testMempoolTransactionSource{
		txs: []mempool.MempoolTransaction{
			{
				Hash: "0123456789abcdef",
				Cbor: []byte{0x84, 0xa0, 0xa0, 0xf5, 0xf6},
				Type: 7,
			},
		},
	}

	var _ ledger.MempoolProvider = (*ledgerMempoolAdapter)(nil)
	ledgerTxs := (&ledgerMempoolAdapter{source: source}).Transactions()
	if len(ledgerTxs) != 1 {
		t.Fatalf("expected 1 ledger transaction, got %d", len(ledgerTxs))
	}
	if ledgerTxs[0].Hash != source.txs[0].Hash {
		t.Fatalf("ledger hash mismatch: got %q want %q",
			ledgerTxs[0].Hash, source.txs[0].Hash)
	}
	if ledgerTxs[0].Type != source.txs[0].Type {
		t.Fatalf("ledger type mismatch: got %d want %d",
			ledgerTxs[0].Type, source.txs[0].Type)
	}
	if !bytes.Equal(ledgerTxs[0].Cbor, source.txs[0].Cbor) {
		t.Fatalf("ledger CBOR mismatch: got %x want %x",
			ledgerTxs[0].Cbor, source.txs[0].Cbor)
	}

	var _ forging.MempoolProvider = (*forgingMempoolAdapter)(nil)
	forgingTxs := (&forgingMempoolAdapter{source: source}).Transactions()
	if len(forgingTxs) != 1 {
		t.Fatalf("expected 1 forging transaction, got %d", len(forgingTxs))
	}
	if forgingTxs[0].Hash != source.txs[0].Hash {
		t.Fatalf("forging hash mismatch: got %q want %q",
			forgingTxs[0].Hash, source.txs[0].Hash)
	}
	if forgingTxs[0].Type != source.txs[0].Type {
		t.Fatalf("forging type mismatch: got %d want %d",
			forgingTxs[0].Type, source.txs[0].Type)
	}
	if !bytes.Equal(forgingTxs[0].Cbor, source.txs[0].Cbor) {
		t.Fatalf("forging CBOR mismatch: got %x want %x",
			forgingTxs[0].Cbor, source.txs[0].Cbor)
	}
}

type testLeiosParentChain struct {
	tip   ochainsync.Tip
	block models.Block
	err   error
}

func (c testLeiosParentChain) Tip() ochainsync.Tip {
	return c.tip
}

func (c testLeiosParentChain) BlockByPoint(
	context.Context,
	ocommon.Point,
	*database.Txn,
) (models.Block, error) {
	if c.err != nil {
		return models.Block{}, c.err
	}
	return c.block, nil
}

func TestLeiosPipelineAdapterParentAnnouncementUsesHeaderAnnouncement(
	t *testing.T,
) {
	t.Parallel()

	ebHashBytes := testLeiosHash(0x40)
	parent := leiosParentBlock(t, ebHashBytes, 8192)
	adapter := &leiosPipelineAdapter{
		chain: testLeiosParentChain{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: parent.Slot,
					Hash: parent.Hash,
				},
				BlockNumber: parent.Number,
			},
			block: parent,
		},
	}

	gotRbHash, gotHash, ok, err := adapter.ParentLeiosAnnouncement(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("ParentLeiosAnnouncement: %v", err)
	}
	if !ok {
		t.Fatal("expected parent announcement")
	}
	if !bytes.Equal(gotHash.Bytes(), ebHashBytes) {
		t.Fatalf("announcement hash mismatch: got %x want %x",
			gotHash.Bytes(), ebHashBytes)
	}
	if !bytes.Equal(gotRbHash.Bytes(), parent.Hash) {
		t.Fatalf("ranking block hash mismatch: got %x want %x",
			gotRbHash.Bytes(), parent.Hash)
	}
}

func testLeiosHash(seed byte) []byte {
	hash := make([]byte, lcommon.Blake2b256Size)
	for i := range hash {
		hash[i] = seed + byte(i)
	}
	return hash
}

func leiosParentBlock(
	t *testing.T,
	ebHash []byte,
	ebSize uint64,
) models.Block {
	t.Helper()
	body := dijkstra.DijkstraBlockBody{
		InvalidTransactions: []uint{},
		Transactions:        []dijkstra.DijkstraTransaction{},
	}
	bodyCbor, err := body.MarshalCBOR()
	if err != nil {
		t.Fatalf("marshal Dijkstra body: %v", err)
	}
	var prevHash lcommon.Blake2b256
	var issuerVkey lcommon.IssuerVkey
	headerBody := []any{
		uint64(7),
		uint64(42),
		prevHash,
		issuerVkey,
		make([]byte, 32),
		lcommon.VrfResult{
			Output: []byte{},
			Proof:  make([]byte, 80),
		},
		uint64(len(bodyCbor)),
		body.Hash(),
		babbage.BabbageOpCert{
			HotVkey:   make([]byte, 32),
			Signature: make([]byte, 64),
		},
		babbage.BabbageProtoVersion{
			Major: dijkstra.MinProtocolVersionDijkstra,
		},
		false,
		[]any{ebHash, ebSize},
	}
	headerCbor, err := cbor.Encode([]any{headerBody, make([]byte, 448)})
	if err != nil {
		t.Fatalf("encode Dijkstra header: %v", err)
	}
	blockCbor, err := cbor.Encode([]any{
		cbor.RawMessage(headerCbor),
		cbor.RawMessage(bodyCbor),
	})
	if err != nil {
		t.Fatalf("encode Dijkstra block: %v", err)
	}
	decoded, err := dijkstra.NewDijkstraBlockFromCbor(blockCbor)
	if err != nil {
		t.Fatalf("decode test Dijkstra block: %v", err)
	}
	return models.Block{
		Hash:   decoded.Hash().Bytes(),
		Cbor:   blockCbor,
		Slot:   decoded.SlotNumber(),
		Number: decoded.BlockNumber(),
		Type:   dijkstra.BlockTypeDijkstra,
	}
}

// expiredKESGenesisForBP builds a Shelley genesis under which the devnet
// opcert (KESPeriod 0) is well outside its validity window: many KES periods
// have elapsed since systemStart and maxKESEvolutions=1 makes anything past
// period 1 expired. The strict preflight must reject it, which is what makes
// it useful for showing the deferred path is a deferral and not a bypass.
func expiredKESGenesisForBP(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	systemStart := time.Now().Add(-365 * 24 * time.Hour)
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "` + systemStart.UTC().Format(time.RFC3339Nano) + `",
		"securityParam": 10,
		"activeSlotsCoeff": 0.5,
		"slotsPerKESPeriod": 10,
		"maxKESEvolutions": 1,
		"slotLength": 1
	}`)); err != nil {
		t.Fatalf("LoadShelleyGenesisFromReader: %v", err)
	}
	return cfg
}

// TestValidateBlockProducerStartupForClock_SupportedStillRejectsExpiredOpCert
// is the property that keeps the deferral from being a relaxation of the
// gate. When the confirmed era history does span the wall clock there is
// nothing to defer, so an expired or future-staged certificate must still
// fail startup exactly as it did before the deferral existed.
func TestValidateBlockProducerStartupForClock_SupportedStillRejectsExpiredOpCert(
	t *testing.T,
) {
	vrf, kes, opcert := devnetCredPaths(t)
	n := newTestNodeForBP(t, true, vrf, kes, opcert, expiredKESGenesisForBP(t))
	_, err := n.validateBlockProducerStartupForClock(20, true)
	if err == nil {
		t.Fatal(
			"supported wall clock must still reject an expired opcert;" +
				" the deferral would otherwise be a relaxation of the gate",
		)
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected 'expired' in error, got: %v", err)
	}
}

// TestValidateBlockProducerStartupForClock_DeferredArmsInsteadOfFailing pins
// the other half, against the identical genesis and credentials that the
// supported case above rejects. The only difference between the two calls is
// whether the confirmed era history supports the wall-clock slot, so this
// pair isolates the behavior change to exactly that condition.
//
// The armed protocol lifetime is asserted, not just the absence of an error:
// the per-slot gate in the forger is what enforces the certificate in the
// deferred state, and it fails closed on a zero expiry period. Returning
// unarmed credentials here would leave the node unable to forge at all
// rather than deferring the judgement.
func TestValidateBlockProducerStartupForClock_DeferredArmsInsteadOfFailing(
	t *testing.T,
) {
	vrf, kes, opcert := devnetCredPaths(t)
	n := newTestNodeForBP(t, true, vrf, kes, opcert, expiredKESGenesisForBP(t))
	creds, err := n.validateBlockProducerStartupForClock(20, false)
	if err != nil {
		t.Fatalf(
			"deferred path must not fail startup on the slot-dependent"+
				" check it is deferring: %v",
			err,
		)
	}
	if !creds.IsLoaded() {
		t.Error("expected credentials to be loaded")
	}
	if creds.OpCertExpiryPeriod() == 0 {
		t.Error(
			"expected the KES protocol lifetime to be armed;" +
				" the forger's per-slot gate fails closed on a zero expiry",
		)
	}
}

// TestValidateBlockProducerStartupForClock_DeferredStillValidatesMaterial
// pins what the deferral does *not* cover. Only the slot-dependent
// KES-period judgement is deferred; the credential material itself is still
// loaded and its cold-key signature still checked, so missing or unreadable
// key files fail startup on this path too.
func TestValidateBlockProducerStartupForClock_DeferredStillValidatesMaterial(
	t *testing.T,
) {
	tmp := t.TempDir()
	n := newTestNodeForBP(
		t, true,
		filepath.Join(tmp, "missing-vrf.skey"),
		filepath.Join(tmp, "missing-kes.skey"),
		filepath.Join(tmp, "missing-opcert.cert"),
		shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour)),
	)
	_, err := n.validateBlockProducerStartupForClock(20, false)
	if err == nil {
		t.Fatal("deferred path must still validate credential material")
	}
	if !strings.Contains(err.Error(), "load pool credentials") {
		t.Errorf("expected 'load pool credentials' in error, got: %v", err)
	}
}

// TestApplyForgeTuningCarriesTheForgingKnobs covers the hop the binary
// takes between dingo.Config and the forger: internal/node's
// buildDingoConfig fills the former, initBlockForger builds the forger
// from the latter, and nothing else connects them. A field missing here
// reaches the forger as its zero value, which the forger then replaces
// with its own default -- so the operator's yaml, env or CLI setting
// disappears without an error anywhere.
func TestApplyForgeTuningCarriesTheForgingKnobs(t *testing.T) {
	t.Parallel()

	const refs, maxBytes = uint64(4321), uint64(98765)
	cfg := NewConfig(
		WithForgeSyncToleranceSlots(11),
		WithForgeStaleGapThresholdSlots(22),
		WithForgeEBSelectionReserve(750*time.Millisecond),
		WithForgeEBMaxTxRefs(refs),
		WithForgeEBMaxBytes(maxBytes),
	)

	var fc forging.ForgerConfig
	applyForgeTuning(&fc, &cfg)

	if fc.ForgeSyncToleranceSlots != 11 {
		t.Fatalf(
			"forgeSyncToleranceSlots = %d, want 11",
			fc.ForgeSyncToleranceSlots,
		)
	}
	if fc.ForgeStaleGapThresholdSlots != 22 {
		t.Fatalf(
			"forgeStaleGapThresholdSlots = %d, want 22",
			fc.ForgeStaleGapThresholdSlots,
		)
	}
	if fc.ForgeEBSelectionReserve != 750*time.Millisecond {
		t.Fatalf(
			"forgeEbSelectionReserve = %s, want 750ms",
			fc.ForgeEBSelectionReserve,
		)
	}
	if fc.ForgeEBMaxTxRefs == nil || *fc.ForgeEBMaxTxRefs != refs {
		t.Fatalf("forgeEbMaxTxRefs = %v, want %d", fc.ForgeEBMaxTxRefs, refs)
	}
	if fc.ForgeEBMaxBytes == nil || *fc.ForgeEBMaxBytes != maxBytes {
		t.Fatalf("forgeEbMaxBytes = %v, want %d", fc.ForgeEBMaxBytes, maxBytes)
	}
}

type forgedValidationRecorder struct {
	ctx            context.Context
	aggregateCalls int
	fullCalls      int
	err            error
}

func (v *forgedValidationRecorder) ValidateForgedBlock(ctx context.Context,
	_ gledger.Block,
	_ []byte,
) error {
	v.ctx = ctx
	v.fullCalls++
	return v.err
}

func (v *forgedValidationRecorder) ValidateBlockReferenceScripts(
	ctx context.Context,
	_ gledger.Block,
) error {
	v.ctx = ctx
	v.aggregateCalls++
	return v.err
}

func TestForgedBlockValidatorDefaultAndFullModes(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "default"
		if full {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			failure := errors.New("aggregate reference-script budget exceeded")
			state := &forgedValidationRecorder{err: failure}
			validator := newForgedBlockValidator(state, full)
			require.NotNil(
				t,
				validator,
				"default mode must retain aggregate validation",
			)
			require.ErrorIs(
				t,
				validator.ValidateForgedBlock(
					context.Background(),
					&conway.ConwayBlock{},
					nil,
				),
				failure,
			)
			state.err = nil
			require.NoError(
				t,
				validator.ValidateForgedBlock(
					context.Background(),
					&conway.ConwayBlock{},
					nil,
				),
			)
			if full {
				require.Equal(t, 2, state.fullCalls)
				require.Zero(
					t,
					state.aggregateCalls,
					"full validation owns its aggregate check",
				)
			} else {
				require.Equal(t, 2, state.aggregateCalls)
				require.Zero(t, state.fullCalls, "default mode must not execute full validation")
			}
		})
	}
}

func TestForgedBlockValidatorPreservesCallerContext(t *testing.T) {
	t.Parallel()
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			state := &forgedValidationRecorder{}
			validator := newForgedBlockValidator(state, full)
			require.NoError(
				t,
				validator.ValidateForgedBlock(ctx, &conway.ConwayBlock{}, nil),
			)
			require.Equal(t, ctx, state.ctx)
		})
	}
}
