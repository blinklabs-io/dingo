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

package nodeparity

import (
	"bytes"
	"context"
	"encoding/hex"
	"log/slog"
	"maps"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStakeEntry mirrors dingo's own unexported stakeDistributionEntry
// (ledger/queries_stakedistribution.go): a real cardano-node's
// GetStakeDistribution wire reply, and dingo's own, both decode into this
// exact shape client-side, so a fake server must produce it too for a real
// gouroboros client to accept the reply as it would either real peer's.
type fakeStakeEntry struct {
	cbor.StructAsArray
	StakeFraction *cbor.Rat
	VrfHash       ledger.Blake2b256
}

// fakeLSQState is one node's synthetic LocalStateQuery answers: mutable and
// mutex-protected so a test can change what a node reports between blocks
// (e.g. to simulate an epoch transition, or to flip a clean match into a
// mismatch), matching testChainSyncServer's release-gate philosophy of
// deterministic, test-controlled timing over relying on real network luck.
type fakeLSQState struct {
	mu                sync.Mutex
	protocolParams    *conway.ConwayProtocolParameters
	stakeDistribution map[ledger.PoolId]fakeStakeEntry
	epoch             int
	// tip is what this node's ChainSync half reports to GetCurrentTip (a
	// one-shot MsgFindIntersect with an empty points list -- see
	// findIntersect). Only meaningful for the dingo-only server
	// (serveLSQOnly): the combined cardano server reports its real,
	// advancing chain tip instead (fakeCardanoServer.findIntersect).
	// Fixed for a whole test rather than advancing, since Check's baseline
	// only needs both nodes to report the *same* tip once, at startup.
	tip chainsync.Tip
	// rejectSlot, when set, makes this node's fake AcquireFunc fail
	// (ErrAcquireFailurePointNotOnChain) for exactly this slot, as if the
	// point were pruned or rolled off this node's chain by a fork -- for
	// exercising establishBaseline's fallback path (pointReachable), which
	// the AcquireFunc's own always-succeeds default (see config's doc
	// comment) cannot otherwise simulate. nil means "accept every point",
	// the default.
	rejectSlot *uint64
}

func newFakeLSQState() *fakeLSQState {
	return &fakeLSQState{
		protocolParams:    newFakeProtocolParams(),
		stakeDistribution: map[ledger.PoolId]fakeStakeEntry{},
	}
}

// newFakeProtocolParams returns a fully-populated Conway protocol parameter
// set -- every field set to some valid value, matching
// ledgerstate/snapshot_test.go's own testConwayPParams fixture (copied
// rather than imported: that one is unexported, in a different package, and
// this package has no dependency on ledgerstate otherwise). A zero-value
// &conway.ConwayProtocolParameters{} is not safe to encode: several fields
// are *cbor.Rat or embed a bare cbor.Rat, and cbor.Rat.MarshalCBOR panics on
// a nil underlying *big.Rat rather than encoding it as CBOR null -- caught
// live by this test file's own first run, which paniced the fake server's
// connection-handling goroutine.
func newFakeProtocolParams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               500,
		A0:                 &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:                &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:                &cbor.Rat{Rat: big.NewRat(1, 5)},
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
		MinPoolCost:    340000000,
		AdaPerUtxoByte: 4310,
		CostModels:     map[uint][]int64{1: {0}},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10000000,
			Steps:  10000000000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50000000,
			Steps:  40000000000,
		},
		MaxValueSize:         5000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(1, 2)},
			PpSecurityGroup:       cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			UpdateToConstitution:  cbor.Rat{Rat: big.NewRat(1, 2)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(1, 2)},
			PpNetworkGroup:        cbor.Rat{Rat: big.NewRat(1, 2)},
			PpEconomicGroup:       cbor.Rat{Rat: big.NewRat(1, 2)},
			PpTechnicalGroup:      cbor.Rat{Rat: big.NewRat(1, 2)},
			PpGovGroup:            cbor.Rat{Rat: big.NewRat(1, 2)},
			TreasuryWithdrawal:    cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		MinCommitteeSize:        5,
		CommitteeTermLimit:      146,
		GovActionValidityPeriod: 20,
		GovActionDeposit:        100000000000,
		DRepDeposit:             500000000,
		DRepInactivityPeriod:    20,
		MinFeeRefScriptCostPerByte: &cbor.Rat{
			Rat: big.NewRat(1, 1),
		},
	}
}

// findIntersect answers ChainSync's GetCurrentTip (the empty-points
// MsgFindIntersect it sends) with s.tip, regardless of what points are
// requested. This is the only ChainSync behavior the dingo-only fake server
// needs: RunIncremental never runs a Sync loop against dingo, only Check's
// one-shot tip read.
func (s *fakeLSQState) findIntersect(
	_ chainsync.CallbackContext, _ []pcommon.Point,
) (pcommon.Point, chainsync.Tip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return csmock.OriginPoint(), s.tip, nil
}

func (s *fakeLSQState) snapshot() (
	*conway.ConwayProtocolParameters,
	map[ledger.PoolId]fakeStakeEntry,
	int,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pp := *s.protocolParams
	dist := make(map[ledger.PoolId]fakeStakeEntry, len(s.stakeDistribution))
	maps.Copy(dist, s.stakeDistribution)
	return &pp, dist, s.epoch
}

func (s *fakeLSQState) setEpoch(epoch int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch = epoch
}

func (s *fakeLSQState) setMinFeeA(minFeeA uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protocolParams.MinFeeA = minFeeA
}

// setStakeDistribution replaces this node's whole synthetic stake
// distribution, for a test that wants to inject a stake-distribution
// divergence between dingo and cardano-node directly (see
// TestRunIncremental_PerBlockCheckIgnoresStakeDistributionDivergence).
func (s *fakeLSQState) setStakeDistribution(
	dist map[ledger.PoolId]fakeStakeEntry,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stakeDistribution = dist
}

// rejectAcquireAtSlot makes this node's fake AcquireFunc fail for exactly
// this slot going forward -- see rejectSlot's own doc comment.
func (s *fakeLSQState) rejectAcquireAtSlot(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejectSlot = &slot
}

// config builds the localstatequery.Config a real gouroboros server uses to
// answer exactly the four query types incremental mode's per-block and full
// checks need (GetCurrentProtocolParams, GetStakeDistribution,
// GetUTxOByTxIn, GetEpochNo) plus GetChainPoint (Check's own point
// resolution) -- every wire shape below matches dingo's real server-side
// handlers (ledger/queries_currentprotocolparams.go,
// ledger/queries_stakedistribution.go, ledger/queries.go's
// queryShelleyUtxoByTxIn/queryShelleyEpochNo) exactly, so a real gouroboros
// client decodes these replies the same way it would a real node's.
// Acquire succeeds regardless of target by default -- these tests mostly
// care about per-block content, not point-based historical rejection --
// except for exactly the slot named by a prior rejectAcquireAtSlot call, if
// any, which fails as ErrAcquireFailurePointNotOnChain to let a test
// exercise establishBaseline's own fallback path over the real wire
// protocol.
func (s *fakeLSQState) config() localstatequery.Config {
	return localstatequery.NewConfig(
		localstatequery.WithAcquireFunc(
			func(
				_ localstatequery.CallbackContext,
				target localstatequery.AcquireTarget,
				_ bool,
			) error {
				s.mu.Lock()
				reject := s.rejectSlot
				s.mu.Unlock()
				if reject != nil {
					if sp, ok := target.(localstatequery.AcquireSpecificPoint); ok &&
						sp.Point.Slot == *reject {
						return localstatequery.ErrAcquireFailurePointNotOnChain
					}
				}
				return nil
			},
		),
		localstatequery.WithQueryFunc(
			func(
				_ localstatequery.CallbackContext,
				q localstatequery.QueryWrapper,
			) (any, error) {
				pp, dist, epoch := s.snapshot()
				// The wire query nests three levels deep, matching dingo's
				// own dispatch (ledger/queries.go Query -> queryBlock ->
				// queryHardFork/queryShelleyLeaf): the top-level decoded
				// type is always *BlockQuery, whose own .Query is either
				// *HardForkQuery (era detection) or *ShelleyQuery (era
				// number plus the actual leaf query in *its* .Query).
				block, ok := q.Query.(*localstatequery.BlockQuery)
				if !ok {
					return nil, nil //nolint:nilnil // not exercised by these tests
				}
				switch inner := block.Query.(type) {
				case *localstatequery.HardForkQuery:
					// GetCurrentProtocolParams calls GetCurrentEra first to
					// pick which era's concrete type to decode its own
					// reply into -- unlike the Shelley* leaf queries below,
					// this reply is a bare int, not wrapped in []any (see
					// dingo's own ledger/queries.go queryHardFork).
					switch inner.Query.(type) {
					case *localstatequery.HardForkCurrentEraQuery:
						return int(ledger.EraIdConway), nil
					default:
						return nil, nil //nolint:nilnil // not exercised by these tests
					}
				case *localstatequery.ShelleyQuery:
					switch inner.Query.(type) {
					case *localstatequery.ShelleyCurrentProtocolParamsQuery:
						return []any{pp}, nil
					case *localstatequery.ShelleyStakeDistributionQuery:
						return []any{dist}, nil
					case *localstatequery.ShelleyUtxoByTxinQuery:
						return []any{
							map[localstatequery.UtxoId]ledger.TransactionOutput{},
						}, nil
					case *localstatequery.ShelleyEpochNoQuery:
						return []any{epoch}, nil
					default:
						return nil, nil //nolint:nilnil // unhandled query types are not exercised by these tests
					}
				default:
					return nil, nil //nolint:nilnil // unhandled query types are not exercised by these tests
				}
			},
		),
		localstatequery.WithReleaseFunc(
			func(_ localstatequery.CallbackContext) error { return nil },
		),
	)
}

// serveLSQOnly starts a LocalStateQuery-only fake server on listener --
// dingoConn's role in a real incrementalSession, which never runs
// ChainSync.
func (s *fakeLSQState) serveLSQOnly(
	t *testing.T,
	listener net.Listener,
	magic uint32,
) {
	t.Helper()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				oconn, err := ouroboros.New(
					ouroboros.WithConnection(conn),
					ouroboros.WithServer(true),
					ouroboros.WithNetworkMagic(magic),
					ouroboros.WithNodeToNode(false),
					ouroboros.WithLocalStateQueryConfig(s.config()),
					ouroboros.WithChainSyncConfig(chainsync.NewConfig(
						chainsync.WithFindIntersectFunc(s.findIntersect),
					)),
				)
				if err != nil {
					_ = conn.Close()
					return
				}
				defer oconn.Close() //nolint:errcheck
				<-oconn.ErrorChan()
			}()
		}
	}()
}

// fakeCardanoServer combines a real ChainSync block feed (matching
// testChainSyncServer's own real-wire-protocol philosophy) with LocalStateQuery
// answers from a fakeLSQState, on the same connection -- exactly mirroring
// incrementalSession's own design of reusing one connection to cardano-node
// for both. rollbackTo, if set, fires a genuine RollBackward to that point
// (not the initial intersection point) after the chain is otherwise
// exhausted, once per connection, letting a test exercise
// handleIncrementalRollback's real branch over the real wire protocol
// instead of only via synthetic unit-test inputs.
type fakeCardanoServer struct {
	mu                 sync.Mutex
	chain              csmock.Chain
	cursor             int
	rolledBack         bool
	lsq                *fakeLSQState
	rollbackTo         *pcommon.Point
	firedExtraRollback bool
	// baselineTipIndex is what GetCurrentTip (an empty-points
	// MsgFindIntersect) reports as "the current tip" -- deliberately not
	// the chain's actual last block. csmock's Chain is a fixed, static
	// sequence with no notion of new blocks arriving after some point, so
	// reporting the true last block as "current" would leave nothing for
	// incremental mode's Sync loop to roll forward into once Check's
	// baseline pins to it: every block would already be "in the past" by
	// the time RunIncremental even starts. Reporting an earlier index
	// instead leaves the remaining blocks as real, still-to-be-delivered
	// RollForwards, exactly like a live node whose baseline is behind its
	// most recent blocks.
	baselineTipIndex int
	// step, if non-nil, gates every real RollForward reply (not the
	// initial post-Sync RollBackward confirmation): requestNext blocks on
	// it before serving each block past the baseline. Without this, a test
	// that injects a state change (setEpoch, setMinFeeA) partway through
	// races the fake server, which has no artificial per-block delay and
	// so can drain its whole remaining chain before the test goroutine is
	// even scheduled again -- observed live, intermittently, even with
	// hundreds of blocks of nominal "headroom" once enough other parallel
	// subtests were competing for CPU. A test that needs a state change to
	// land at a precise point calls allowStep once per block it wants
	// delivered before making that change.
	step chan struct{}
}

// allowStep permits fakeCardanoServer's next gated RollForward reply to
// proceed. Only meaningful when the server was built with a non-nil step
// channel (see newGatedFakeCardanoServer).
func (s *fakeCardanoServer) allowStep(t *testing.T) {
	t.Helper()
	select {
	case s.step <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not consume the step signal in time")
	}
}

func newFakeCardanoServer(
	t *testing.T, blockCount int, lsq *fakeLSQState,
) *fakeCardanoServer {
	t.Helper()
	chain, err := csmock.BuildChain(1, ledger.Blake2b256{}, 100, 20, blockCount)
	require.NoError(t, err)
	baselineTipIndex := max(blockCount-5, 0)
	return &fakeCardanoServer{
		chain:            chain,
		lsq:              lsq,
		baselineTipIndex: baselineTipIndex,
	}
}

func (s *fakeCardanoServer) findIntersect(
	_ chainsync.CallbackContext, points []pcommon.Point,
) (pcommon.Point, chainsync.Tip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rolledBack = false
	// An empty points list is GetCurrentTip's own MsgFindIntersect (see
	// fakeLSQState.findIntersect's identical convention): report
	// baselineTipIndex, not the chain's true last block, so Check's
	// baseline pins somewhere with real, still-undelivered blocks ahead of
	// it -- see baselineTipIndex's own doc comment for why.
	if len(points) == 0 {
		s.cursor = s.baselineTipIndex + 1
		return s.chain.Points[s.baselineTipIndex],
			s.chain.Tips[s.baselineTipIndex], nil
	}
	// Otherwise intersect at whatever point the client asked for (its
	// cursor), not always origin: incrementalSession always Syncs from its
	// own cursor's current point, which after the first block or two is no
	// longer the baseline tip either.
	for i, p := range s.chain.Points {
		if p.Slot == points[0].Slot {
			s.cursor = i + 1
			return points[0], s.chain.Tips[i], nil
		}
	}
	s.cursor = 0
	return csmock.OriginPoint(), s.chain.Tip(), nil
}

func (s *fakeCardanoServer) requestNext(ctx chainsync.CallbackContext) error {
	s.mu.Lock()
	if !s.rolledBack {
		s.rolledBack = true
		defer s.mu.Unlock()
		return ctx.Server.RollBackward(
			s.chain.Points[max(s.cursor-1, 0)], s.chain.Tip(),
		)
	}
	if s.rollbackTo != nil && !s.firedExtraRollback &&
		s.cursor > 0 && s.cursor < s.chain.Len() {
		s.firedExtraRollback = true
		defer s.mu.Unlock()
		return ctx.Server.RollBackward(*s.rollbackTo, s.chain.Tip())
	}
	if s.cursor >= s.chain.Len() {
		defer s.mu.Unlock()
		return ctx.Server.AwaitReply()
	}
	step := s.step
	s.mu.Unlock()
	// Gated only for a genuine new-block reply, not the setup replies
	// above: those must always proceed immediately for a test to ever
	// reach the point of calling allowStep at all.
	if step != nil {
		<-step
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	block := s.chain.Blocks[s.cursor]
	tip := s.chain.Tips[s.cursor]
	s.cursor++
	return ctx.Server.RollForward(uint(block.Type()), block.Cbor(), tip)
}

func (s *fakeCardanoServer) serve(
	t *testing.T,
	listener net.Listener,
	magic uint32,
) {
	t.Helper()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				oconn, err := ouroboros.New(
					ouroboros.WithConnection(conn),
					ouroboros.WithServer(true),
					ouroboros.WithNetworkMagic(magic),
					ouroboros.WithNodeToNode(false),
					ouroboros.WithChainSyncConfig(chainsync.NewConfig(
						chainsync.WithFindIntersectFunc(s.findIntersect),
						chainsync.WithRequestNextFunc(s.requestNext),
					)),
					ouroboros.WithLocalStateQueryConfig(s.lsq.config()),
				)
				if err != nil {
					_ = conn.Close()
					return
				}
				defer oconn.Close() //nolint:errcheck
				<-oconn.ErrorChan()
			}()
		}
	}()
}

// newIncrementalHarness starts a fake dingo (LocalStateQuery only) and a
// fake cardano-node (ChainSync + LocalStateQuery) serving blockCount real,
// decodable (but empty-bodied -- see blockUtxoDelta's callers, which then
// see no consumed/produced refs at all) Conway blocks, and returns their
// addresses plus both nodes' mutable synthetic state.
func newIncrementalHarness(
	t *testing.T, blockCount int,
) (dingoAddr, cardanoAddr string, dingoState, cardanoState *fakeLSQState) {
	t.Helper()
	dingoAddr, cardanoAddr, dingoState, cardanoState, _ =
		newIncrementalHarnessWithServer(t, blockCount, false)
	return dingoAddr, cardanoAddr, dingoState, cardanoState
}

// newIncrementalHarnessWithServer is newIncrementalHarness's full form,
// additionally returning the cardano fake server itself. gated controls
// whether its RollForward replies are step-gated (see
// fakeCardanoServer.step): a test that injects a state change partway
// through (setEpoch, setMinFeeA) needs this to land the change at a precise
// point rather than racing an ungated server, which has no artificial
// per-block delay and can drain its whole remaining chain before the test
// goroutine is next scheduled -- observed live, intermittently, even with a
// large chain, once enough other parallel subtests were competing for CPU.
func newIncrementalHarnessWithServer(
	t *testing.T, blockCount int, gated bool,
) (
	dingoAddr, cardanoAddr string,
	dingoState, cardanoState *fakeLSQState,
	cardanoServer *fakeCardanoServer,
) {
	t.Helper()
	const magic = 42

	// cardanoServer built first: dingoState.tip is set to its chain's tip
	// below, so both nodes report the same tip to Check's baseline
	// tip-agreement check (tipsAgree) -- required for establishBaseline to
	// ever produce a trustworthy, non-skipped result at all.
	cardanoState = newFakeLSQState()
	cardanoServer = newFakeCardanoServer(t, blockCount, cardanoState)
	if gated {
		cardanoServer.step = make(chan struct{})
	}
	cardanoListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = cardanoListener.Close() })
	cardanoServer.serve(t, cardanoListener, magic)

	dingoState = newFakeLSQState()
	dingoState.tip = cardanoServer.chain.Tips[cardanoServer.baselineTipIndex]
	dingoListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = dingoListener.Close() })
	dingoState.serveLSQOnly(t, dingoListener, magic)

	return dingoListener.Addr().String(), cardanoListener.Addr().String(),
		dingoState, cardanoState, cardanoServer
}

// runIncrementalForTest starts RunIncremental against a harness and returns
// channels of every block and full-check callback it makes, plus a cancel
// func to stop it. fullCheckInterval and fullCheckTimeout are the caller's
// choice, matching what each test needs to force.
// retryRemoveAll removes dir, tolerating the benign race documented on
// runIncrementalForTest's cursorDir: a ChainSync callback goroutine still
// finishing a SaveCursor write for a few microseconds after RunIncremental
// itself has already returned. A handful of short retries comfortably
// outlasts that window; silently gives up after, since a leftover test
// temp-file is a cosmetic /tmp nuisance, never a correctness problem.
func retryRemoveAll(dir string) {
	for range 5 {
		if err := os.RemoveAll(dir); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runIncrementalForTest(
	t *testing.T,
	dingoAddr, cardanoAddr string,
	fullCheckInterval uint64,
) (
	blocks <-chan struct {
		tip  Tip
		diff Diff
	},
	fullChecks <-chan struct {
		reason FullCheckReason
		result *CheckResult
		err    error
	},
) {
	t.Helper()
	blockCh := make(chan struct {
		tip  Tip
		diff Diff
	}, 64)
	fullCheckCh := make(chan struct {
		reason FullCheckReason
		result *CheckResult
		err    error
	}, 64)

	ctx, cancel := context.WithCancel(context.Background())
	// A manually-managed directory, not t.TempDir(): RunIncremental
	// returning (awaited below via done) guarantees incrementalSession's
	// own select loop has exited, but not that a ChainSync RollForward
	// callback already in flight on gouroboros's own goroutine -- which is
	// what actually calls SaveCursor -- has finished doing so. That
	// callback keeps running to completion independently of ctx being
	// cancelled (closing the connection does not interrupt an
	// already-in-progress synchronous call), so a write can still land
	// microseconds after done closes. Harmless in production (the next
	// process start's mandatory full-Check baseline supersedes whatever
	// the cursor file says regardless -- see RunIncremental's own doc
	// comment), but t.TempDir()'s single-attempt RemoveAll has no
	// tolerance for a file appearing during its own cleanup. retryRemoveAll
	// below gives that in-flight write a moment to land before removing.
	cursorDir, err := os.MkdirTemp("", "node-parity-incremental-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { retryRemoveAll(cursorDir) })

	cfg := IncrementalConfig{
		DingoAddr:         dingoAddr,
		CardanoAddr:       cardanoAddr,
		Magic:             42,
		FullCheckInterval: fullCheckInterval,
		FullCheckTimeout:  10 * time.Second,
		CursorFile:        cursorDir + "/cursor.json",
		Logger:            testDiscardLogger(),
		OnBlockCheck: func(tip Tip, diff Diff) {
			select {
			case blockCh <- struct {
				tip  Tip
				diff Diff
			}{tip, diff}:
			default:
			}
		},
		OnFullCheck: func(reason FullCheckReason, result *CheckResult, err error) {
			select {
			case fullCheckCh <- struct {
				reason FullCheckReason
				result *CheckResult
				err    error
			}{reason, result, err}:
			default:
			}
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunIncremental(ctx, cfg)
	}()
	// Registered after cursorDir (t.TempDir() above), so by cleanup's LIFO
	// order this runs *before* cursorDir's own removal: RunIncremental's
	// background goroutine must fully stop writing to the cursor file
	// before that directory is removed, or a write racing the removal
	// intermittently fails cleanup with "directory not empty".
	t.Cleanup(func() {
		cancel()
		<-done
	})

	return blockCh, fullCheckCh
}

// drainFullCheck waits for the next full-check callback matching reason,
// ignoring the mandatory startup baseline (FullCheckStartup) unless reason
// is itself that.
func drainFullCheck(
	t *testing.T,
	fullChecks <-chan struct {
		reason FullCheckReason
		result *CheckResult
		err    error
	},
	reason FullCheckReason,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case fc := <-fullChecks:
			if fc.reason == reason {
				require.NoError(
					t,
					fc.err,
					"full check for reason %s must succeed against the fake harness",
					reason,
				)
				return
			}
		case <-deadline:
			t.Fatalf(
				"never saw a full check for reason %s within %s",
				reason,
				timeout,
			)
		}
	}
}

// TestRunIncremental_ReportsCleanMatch covers a case never once observed
// live against a real network during this package's development
// (blinklabs-io/dingo#3854 guarantees a stake-distribution mismatch on
// effectively every real block): when both nodes genuinely agree, the
// per-block pipeline must report an empty Diff, not something that merely
// looks empty by omission.
func TestRunIncremental_ReportsCleanMatch(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, _, _ := newIncrementalHarness(t, 5)
	blocks, fullChecks := runIncrementalForTest(t, dingoAddr, cardanoAddr, 1000)
	drainFullCheck(t, fullChecks, FullCheckStartup, 10*time.Second)

	select {
	case b := <-blocks:
		require.True(
			t,
			b.diff.Empty(),
			"both nodes report identical zero-value state; the pipeline must report a clean match: %v",
			b.diff.Lines(),
		)
	case <-time.After(10 * time.Second):
		t.Fatal("never received a per-block result")
	}
}

// TestRunIncremental_FullCheckIntervalFiresOnItsOwn covers
// --full-check-interval's own trigger firing without a mismatch or epoch
// transition ever preempting it -- untestable live for the same reason as
// the clean-match case above (a mismatch always fires first against a real,
// currently-affected network).
func TestRunIncremental_FullCheckIntervalFiresOnItsOwn(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, _, _ := newIncrementalHarness(t, 10)
	_, fullChecks := runIncrementalForTest(t, dingoAddr, cardanoAddr, 3)
	drainFullCheck(t, fullChecks, FullCheckStartup, 10*time.Second)
	drainFullCheck(t, fullChecks, FullCheckInterval, 15*time.Second)
}

// TestRunIncremental_EpochTransitionFiresOnItsOwn covers the epoch-transition
// trigger firing on a clean block whose epoch differs from the cursor's --
// untestable live within any practical test window (a real epoch boundary
// is 1-5 days away depending on network).
func TestRunIncremental_EpochTransitionFiresOnItsOwn(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, cardanoState, cardanoServer :=
		newIncrementalHarnessWithServer(t, 20, true)
	// FullCheckInterval set high enough that only the epoch transition
	// (never the interval) can explain the second full check.
	blocks, fullChecks := runIncrementalForTest(t, dingoAddr, cardanoAddr, 1000)
	drainFullCheck(t, fullChecks, FullCheckStartup, 10*time.Second)

	// Let exactly two gated blocks through and drain both, so the state
	// change below is guaranteed to land strictly between block 2 and
	// block 3 -- not racing however fast an ungated server would otherwise
	// deliver its remaining chain.
	for range 2 {
		cardanoServer.allowStep(t)
		select {
		case <-blocks:
		case <-time.After(5 * time.Second):
			t.Fatal("gated server did not deliver an allowed block in time")
		}
	}

	dingoState.setEpoch(1)
	cardanoState.setEpoch(1)
	cardanoServer.allowStep(t)
	drainFullCheck(t, fullChecks, FullCheckEpochTransition, 15*time.Second)
}

// TestRunIncremental_MismatchFiresFullCheck covers the one trigger that IS
// exercisable live (and was -- extensively, against blinklabs-io/dingo#3854's
// live effect): included here too so the full trigger matrix has coverage
// in one place, independent of any live network's current state.
func TestRunIncremental_MismatchFiresFullCheck(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, _, cardanoServer :=
		newIncrementalHarnessWithServer(t, 20, true)
	blocks, fullChecks := runIncrementalForTest(t, dingoAddr, cardanoAddr, 1000)
	drainFullCheck(t, fullChecks, FullCheckStartup, 10*time.Second)

	// See TestRunIncremental_EpochTransitionFiresOnItsOwn's comment on why
	// this gates rather than relying on chain size alone.
	for range 2 {
		cardanoServer.allowStep(t)
		select {
		case <-blocks:
		case <-time.After(5 * time.Second):
			t.Fatal("gated server did not deliver an allowed block in time")
		}
	}

	dingoState.setMinFeeA(1)
	cardanoServer.allowStep(t)
	drainFullCheck(t, fullChecks, FullCheckMismatch, 15*time.Second)
}

// TestRunIncremental_GenuineRollbackFiresFullCheck covers a real RollBackward
// to a point other than the cursor's own current one, sent over the real
// wire protocol by the fake cardano-node mid-stream -- the branch
// TestHandleIncrementalRollback_TriggersFullCheckWhenPointDiffers already
// covers at the unit level with synthetic inputs; this exercises the same
// decision end to end through a real ChainSync session, which no live run
// ever did (no real reorg occurred on the testnet during this package's
// development).
func TestRunIncremental_GenuineRollbackFiresFullCheck(t *testing.T) {
	t.Parallel()
	const magic = 42

	cardanoState := newFakeLSQState()
	cardanoServer := newFakeCardanoServer(t, 10, cardanoState)
	// Roll back to the chain's own second point, a real, decodable point
	// that genuinely differs from wherever the cursor has advanced to by
	// the time this fires (well past it, since rollbackTo only fires once
	// the client has already moved past index 0).
	rollbackPoint := cardanoServer.chain.Points[1]
	cardanoServer.rollbackTo = &rollbackPoint
	cardanoListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = cardanoListener.Close() })
	cardanoServer.serve(t, cardanoListener, magic)

	dingoState := newFakeLSQState()
	dingoState.tip = cardanoServer.chain.Tips[cardanoServer.baselineTipIndex]
	dingoListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = dingoListener.Close() })
	dingoState.serveLSQOnly(t, dingoListener, magic)

	_, fullChecks := runIncrementalForTest(
		t, dingoListener.Addr().String(), cardanoListener.Addr().String(), 1000,
	)
	drainFullCheck(t, fullChecks, FullCheckStartup, 10*time.Second)
	drainFullCheck(t, fullChecks, FullCheckRollback, 15*time.Second)
}

// TestRunIncremental_PerBlockCheckIgnoresStakeDistributionDivergence is a
// regression test for blinklabs-io/dingo#1900's incremental-mode audit
// finding: querying/comparing stake distribution in the per-block check
// permanently stalled a real incremental session, since Dingo's
// GetStakeDistribution handler only answers when the pinned point equals
// its live tip -- never true for a per-block walk that is behind tip by
// design. Injecting a stake-distribution divergence between the two fake
// nodes must NOT surface in the per-block Diff; stake distribution is only
// compared by this mode's periodic full checkpoints.
func TestRunIncremental_PerBlockCheckIgnoresStakeDistributionDivergence(
	t *testing.T,
) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, cardanoState, cardanoServer :=
		newIncrementalHarnessWithServer(t, 20, true)
	blocks, fullChecks := runIncrementalForTest(t, dingoAddr, cardanoAddr, 1000)
	drainFullCheck(t, fullChecks, FullCheckStartup, 10*time.Second)

	// See TestRunIncremental_EpochTransitionFiresOnItsOwn's comment on why
	// this gates rather than relying on chain size alone.
	for range 2 {
		cardanoServer.allowStep(t)
		select {
		case <-blocks:
		case <-time.After(5 * time.Second):
			t.Fatal("gated server did not deliver an allowed block in time")
		}
	}

	poolID := ledger.PoolId{0x01}
	dingoState.setStakeDistribution(map[ledger.PoolId]fakeStakeEntry{
		poolID: {StakeFraction: &cbor.Rat{Rat: big.NewRat(1, 2)}},
	})
	cardanoState.setStakeDistribution(map[ledger.PoolId]fakeStakeEntry{
		poolID: {StakeFraction: &cbor.Rat{Rat: big.NewRat(1, 3)}},
	})
	cardanoServer.allowStep(t)

	select {
	case b := <-blocks:
		assert.True(
			t,
			b.diff.Empty(),
			"a stake-distribution divergence must not surface in the per-block incremental check: %v",
			b.diff.Lines(),
		)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"never received the block after injecting a stake distribution divergence",
		)
	}
}

// TestEstablishBaseline_ResumesFromReachablePriorCursor is a regression test
// for blinklabs-io/dingo#1900's incremental-mode audit finding: a restart
// used to always discard a saved cursor's own point in favor of wherever
// the fresh baseline Check happened to land (the live tip at process
// start), silently skipping every block that arrived during any downtime in
// between. A prior cursor whose point both nodes can still Acquire must be
// resumed from instead -- while the fresh, live-tip baseline Check still
// runs and is still reported via OnFullCheck, exactly as before.
func TestEstablishBaseline_ResumesFromReachablePriorCursor(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, _, cardanoState, cardanoServer :=
		newIncrementalHarnessWithServer(t, 10, false)
	// The resume gate additionally requires the prior cursor's own epoch to
	// still match the live tip's (see buildStartupCursor's doc comment on
	// why replaying across an epoch boundary is not yet safe) -- set both
	// to the same non-default value so this test cannot pass by coincidence
	// against the harness's zero-value default epoch.
	cardanoState.setEpoch(3)

	priorPoint := cardanoServer.chain.Points[2]
	priorTip := Tip{
		Slot:        priorPoint.Slot,
		Hash:        hex.EncodeToString(priorPoint.Hash),
		BlockNumber: 2,
	}
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	require.NoError(t, SaveCursor(cursorFile, &IncrementalCursor{
		Tip: priorTip, Epoch: 3, BlocksSinceFullCheck: 17,
	}))

	var mu sync.Mutex
	var gotResult *CheckResult
	cfg := IncrementalConfig{
		DingoAddr:        dingoAddr,
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		FullCheckTimeout: 10 * time.Second,
		CursorFile:       cursorFile,
		Logger:           testDiscardLogger(),
		OnFullCheck: func(_ FullCheckReason, result *CheckResult, _ error) {
			mu.Lock()
			gotResult = result
			mu.Unlock()
		},
	}

	cursor, err := establishBaseline(context.Background(), cfg)
	require.NoError(t, err)
	mu.Lock()
	require.NotNil(
		t,
		gotResult,
		"the mandatory startup full check must still run at the live tip even when resuming",
	)
	mu.Unlock()

	assert.Equal(
		t,
		priorTip.Slot,
		cursor.Tip.Slot,
		"must resume from the prior cursor's own point, not the fresh baseline's",
	)
	assert.Equal(t, priorTip.Hash, cursor.Tip.Hash)
	assert.Equal(
		t, 3, cursor.Epoch,
		"must keep the prior cursor's own epoch, not the fresh baseline's",
	)
	assert.Equal(t, uint64(17), cursor.BlocksSinceFullCheck)

	persisted, loadErr := LoadCursor(cursorFile)
	require.NoError(t, loadErr)
	require.NotNil(t, persisted)
	assert.Equal(t, priorTip.Slot, persisted.Tip.Slot)
}

// TestEstablishBaseline_FallsBackWhenPriorPointUnreachable covers the
// fallback branch TestEstablishBaseline_ResumesFromReachablePriorCursor's
// fix must not have broken: when the prior cursor's own point is no longer
// reachable (simulated here via rejectAcquireAtSlot, standing in for a
// pruned point or a fork moving it off the chain), establishBaseline must
// fall back to the fresh live-tip baseline's own point rather than looping
// forever or crashing -- while still carrying over BlocksSinceFullCheck from
// the prior cursor, since that bookkeeping is independent of which point the
// cursor itself resumes from.
func TestEstablishBaseline_FallsBackWhenPriorPointUnreachable(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, _, cardanoServer :=
		newIncrementalHarnessWithServer(t, 10, false)

	priorPoint := cardanoServer.chain.Points[2]
	priorTip := Tip{
		Slot:        priorPoint.Slot,
		Hash:        hex.EncodeToString(priorPoint.Hash),
		BlockNumber: 2,
	}
	dingoState.rejectAcquireAtSlot(priorTip.Slot)

	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	require.NoError(t, SaveCursor(cursorFile, &IncrementalCursor{
		Tip: priorTip, Epoch: 3, BlocksSinceFullCheck: 17,
	}))

	cfg := IncrementalConfig{
		DingoAddr:        dingoAddr,
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		FullCheckTimeout: 10 * time.Second,
		CursorFile:       cursorFile,
		Logger:           testDiscardLogger(),
		OnFullCheck:      func(FullCheckReason, *CheckResult, error) {},
	}

	cursor, err := establishBaseline(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotEqual(
		t,
		priorTip.Slot,
		cursor.Tip.Slot,
		"an unreachable prior point must fall back to the fresh live-tip baseline, not resume from it",
	)
	assert.Equal(
		t,
		uint64(17),
		cursor.BlocksSinceFullCheck,
		"BlocksSinceFullCheck must still carry over from the prior cursor even when its point itself is discarded",
	)
}

// TestEstablishBaseline_FallsBackWhenPriorEpochDiffersFromLiveTip covers a
// second, distinct reason a resume must not proceed even though the prior
// cursor's point is itself perfectly reachable: replaying every block
// between an old cursor and the live tip re-runs the per-block protocol-
// parameters query at each one, and Dingo's protocol-parameters handler
// only tolerates a pinned point within the live tip's current epoch
// (queryShelleyCurrentProtocolParams) -- so resuming across an epoch
// boundary would make every replayed block's query fail identically, the
// session erroring out immediately rather than making the progress
// resuming exists to provide. A prior cursor whose recorded epoch differs
// from the live tip's must fall back to a fresh baseline, the same as an
// unreachable point.
func TestEstablishBaseline_FallsBackWhenPriorEpochDiffersFromLiveTip(
	t *testing.T,
) {
	t.Parallel()
	dingoAddr, cardanoAddr, _, cardanoState, cardanoServer :=
		newIncrementalHarnessWithServer(t, 10, false)
	cardanoState.setEpoch(5) // the live tip's epoch

	priorPoint := cardanoServer.chain.Points[2]
	priorTip := Tip{
		Slot:        priorPoint.Slot,
		Hash:        hex.EncodeToString(priorPoint.Hash),
		BlockNumber: 2,
	}
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	require.NoError(t, SaveCursor(cursorFile, &IncrementalCursor{
		// An older epoch than the live tip's (5), even though the point
		// itself remains perfectly reachable in this fake harness (Acquire
		// always succeeds regardless of target -- see config's doc
		// comment).
		Tip: priorTip, Epoch: 4, BlocksSinceFullCheck: 17,
	}))

	cfg := IncrementalConfig{
		DingoAddr:        dingoAddr,
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		FullCheckTimeout: 10 * time.Second,
		CursorFile:       cursorFile,
		Logger:           testDiscardLogger(),
		OnFullCheck:      func(FullCheckReason, *CheckResult, error) {},
	}

	cursor, err := establishBaseline(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotEqual(
		t,
		priorTip.Slot,
		cursor.Tip.Slot,
		"a prior cursor whose epoch differs from the live tip's must fall back to the fresh live-tip baseline, not resume across the epoch boundary",
	)
	assert.Equal(
		t,
		uint64(17),
		cursor.BlocksSinceFullCheck,
		"BlocksSinceFullCheck must still carry over even when the epoch mismatch discards the prior point",
	)
}

// TestEstablishBaseline_DivergentBaselineLogsDisputed is a regression test
// for blinklabs-io/dingo#1900's incremental-mode audit finding (b): a
// startup baseline whose own full Check found a real divergence (not just
// Skipped) was accepted as the incremental cursor's starting point with no
// distinct signal that the session begins life on a disputed point rather
// than a confirmed-agreed one -- an operator would only see the same
// routine "ledger state diverged" line any other full check produces, not
// something that flags this one is foundational. A protocol-parameter
// mismatch injected into the fresh baseline itself must be logged with a
// distinct "disputed" marker.
func TestEstablishBaseline_DivergentBaselineLogsDisputed(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, _ := newIncrementalHarness(t, 5)
	dingoState.setMinFeeA(999)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	cfg := IncrementalConfig{
		DingoAddr:        dingoAddr,
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		FullCheckTimeout: 10 * time.Second,
		CursorFile:       filepath.Join(t.TempDir(), "cursor.json"),
		Logger:           logger,
		OnFullCheck:      func(FullCheckReason, *CheckResult, error) {},
	}

	cursor, err := establishBaseline(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, cursor)
	assert.Contains(
		t,
		buf.String(),
		"disputed",
		"a startup baseline that itself diverged must be logged distinctly as a disputed cursor, not just via the routine per-cycle diff report",
	)
}
