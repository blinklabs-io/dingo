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
	"errors"
	"io"
	"log/slog"
	"maps"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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
	// stall, when non-nil, makes this node's fake AcquireFunc block until
	// it is closed, simulating a peer that accepts the connection and
	// then stalls mid-query -- for exercising that establishBaseline's
	// context.WithTimeout(ctx, cfg.FullCheckTimeout) wrapping actually
	// bounds this instead of hanging indefinitely (
	// review). nil means "never stall", the default.
	stall <-chan struct{}
	// stallQuery is stall's counterpart for the QueryFunc step instead of
	// Acquire: gouroboros's LocalStateQuery client has its own built-in
	// AcquireTimeout (5s default, independent of Dial's WithQueryTimeout(0)
	// override, which only affects the Querying state), so a stalled
	// Acquire is already bounded regardless of ctx -- stalling here instead
	// exercises the genuinely-unbounded case Dial's WithQueryTimeout(0)
	// creates for a query after a successful Acquire.
	stallQuery <-chan struct{}
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

// stallAcquireUntil makes this node's fake AcquireFunc block on done
// instead of answering, until done is closed -- see the stall field's doc
// comment.
func (s *fakeLSQState) stallAcquireUntil(done <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stall = done
}

// stallQueryUntil makes this node's fake QueryFunc block on done instead of
// answering, until done is closed -- see the stallQuery field's doc
// comment.
func (s *fakeLSQState) stallQueryUntil(done <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stallQuery = done
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
				stall := s.stall
				s.mu.Unlock()
				if stall != nil {
					<-stall
				}
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
				s.mu.Lock()
				stallQuery := s.stallQuery
				s.mu.Unlock()
				if stallQuery != nil {
					<-stallQuery
				}
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
	dingoAddr, cardanoAddr, dingoState, cardanoState, _ = newIncrementalHarnessWithServer(t, blockCount, false)
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
// ( guarantees a stake-distribution mismatch on
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
	dingoAddr, cardanoAddr, dingoState, cardanoState, cardanoServer := newIncrementalHarnessWithServer(t, 20, true)
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
// exercisable live (and was -- extensively, against
// live effect): included here too so the full trigger matrix has coverage
// in one place, independent of any live network's current state.
func TestRunIncremental_MismatchFiresFullCheck(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, _, cardanoServer := newIncrementalHarnessWithServer(t, 20, true)
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
// regression test for incremental-mode audit
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
	dingoAddr, cardanoAddr, dingoState, cardanoState, cardanoServer := newIncrementalHarnessWithServer(t, 20, true)
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
// for incremental-mode audit finding: a restart
// used to always discard a saved cursor's own point in favor of wherever
// the fresh baseline Check happened to land (the live tip at process
// start), silently skipping every block that arrived during any downtime in
// between. A prior cursor whose point both nodes can still Acquire must be
// resumed from instead -- while the fresh, live-tip baseline Check still
// runs and is still reported via OnFullCheck, exactly as before.
func TestEstablishBaseline_ResumesFromReachablePriorCursor(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, _, cardanoState, cardanoServer := newIncrementalHarnessWithServer(t, 10, false)
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
	dingoAddr, cardanoAddr, dingoState, _, cardanoServer := newIncrementalHarnessWithServer(t, 10, false)

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
	dingoAddr, cardanoAddr, _, cardanoState, cardanoServer := newIncrementalHarnessWithServer(t, 10, false)
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
// for incremental-mode audit finding (b): a
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

// TestEstablishBaseline_StalledPeerTimesOutAndRetries is the regression
// test for a review finding: establishBaseline
// called Check with the raw, long-lived ctx RunIncremental receives (which
// only ever cancels at process shutdown), not one bounded by
// cfg.FullCheckTimeout the way every other full Check this package
// triggers (fullCheckWorker.run) already is. Dial deliberately disables
// both the mux segment-read and LocalStateQuery query timeouts on this
// trusted NtC channel (see its doc comment), so nothing else stops a peer
// that accepts the connection and then stalls mid-query: the startup
// retry loop would hang on that one attempt forever instead of timing out
// and retrying with backoff like every other failure mode here already
// does.
//
// dingoState.stallAcquireUntil makes the fake dingo server accept the
// connection and then block forever on the very first LocalStateQuery
// call (Acquire) -- Dial's own ctx-triggered close (see its doc comment)
// is what is supposed to unstick a bounded attempt by closing the
// connection out from under the blocked client call once checkCtx
// expires, letting establishBaseline's retry loop observe an error and
// try again rather than waiting on a reply that will never come.
//
// This test's own outer ctx (2s) is deliberately longer than
// cfg.FullCheckTimeout (100ms) so the two are distinguishable: with the
// fix, each attempt is bounded by the short per-attempt timeout, so
// several retries fit inside the 2s window. Without it (verified via a
// stash/apply round-trip against the pre-fix code), Check's single
// attempt is bounded only by whichever ctx happens to reach it -- here,
// coincidentally, this test's own outer ctx, which then also makes
// establishBaseline's own ctx.Err() check stop the loop right away
// instead of retrying, so the failure surfaces as attempts staying at 1
// rather than as an actual hang. Against the real caller (RunIncremental,
// whose ctx only cancels at process shutdown) the same missing bound
// would hang the whole startup baseline indefinitely instead.
func TestEstablishBaseline_StalledPeerTimesOutAndRetries(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, _ := newIncrementalHarness(t, 5)

	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) })
	dingoState.stallAcquireUntil(stall)

	var mu sync.Mutex
	attempts := 0
	cfg := IncrementalConfig{
		DingoAddr:        dingoAddr,
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		FullCheckTimeout: 100 * time.Millisecond,
		CursorFile:       filepath.Join(t.TempDir(), "cursor.json"),
		Logger:           testDiscardLogger(),
		OnFullCheck: func(_ FullCheckReason, _ *CheckResult, err error) {
			mu.Lock()
			attempts++
			mu.Unlock()
			assert.Error(
				t, err,
				"a stalled peer must surface as an error for this "+
					"attempt, not be silently treated as a successful "+
					"check",
			)
		},
	}

	// Bounds the whole test, independent of cfg.FullCheckTimeout: this is
	// the process-lifetime-style context establishBaseline's real caller
	// (RunIncremental) supplies, which only ever cancels at shutdown. If
	// establishBaseline is not itself bounding each attempt by
	// cfg.FullCheckTimeout, the stalled peer above blocks it forever and
	// this outer context is the only thing that will ever end the test
	// (after a generous margin well beyond several retry attempts).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	var baselineErr error
	go func() {
		_, baselineErr = establishBaseline(ctx, cfg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal(
			"establishBaseline did not return -- a stalled peer during " +
				"the startup baseline check must be bounded by " +
				"cfg.FullCheckTimeout, not hang indefinitely",
		)
	}
	require.Error(t, baselineErr)

	mu.Lock()
	defer mu.Unlock()
	assert.Greater(
		t, attempts, 1,
		"a stalled peer must be retried with backoff, not given up on "+
			"after a single attempt",
	)
}

// TestBuildStartupCursor_StalledEpochQueryIsBounded is the regression test
// for a review finding: establishBaseline's own
// Check call is bounded by cfg.FullCheckTimeout (see
// TestEstablishBaseline_StalledPeerTimesOutAndRetries above), but
// buildStartupCursor's separate queryEpochAt call right after it (this
// function's own fresh-baseline epoch lookup, or pointReachable's probes
// for a prior cursor) still used the raw, unbounded ctx. Dial deliberately
// disables both the mux segment-read and LocalStateQuery query timeouts
// on this trusted NtC channel; gouroboros's own AcquireTimeout (5s
// default) already bounds a stalled Acquire regardless, but a stall
// *after* a successful Acquire, during the actual query, has nothing left
// to bound it once Dial's WithQueryTimeout(0) disables the Querying
// state's timeout too -- exactly the queryEpochAt call this covers.
//
// Calls buildStartupCursor directly (not through establishBaseline) with
// a hand-built CheckResult, so this isolates its own epoch query from
// Check's already-covered one: cardanoState's query step (not Acquire,
// which is expected to succeed normally) is stalled from the start, and
// no prior cursor is supplied, so the only query this exercises is
// buildStartupCursor's fresh-baseline queryEpochAt call.
func TestBuildStartupCursor_StalledEpochQueryIsBounded(t *testing.T) {
	t.Parallel()
	_, cardanoAddr, _, cardanoState := newIncrementalHarness(t, 5)

	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) })
	cardanoState.stallQueryUntil(stall)

	cfg := IncrementalConfig{
		// DingoAddr is never touched: with no prior cursor, only the
		// fresh-baseline epoch query (cfg.CardanoAddr only) runs --
		// pointReachable (which would touch DingoAddr too) only runs for
		// a supplied prior cursor.
		DingoAddr:        "127.0.0.1:1",
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		FullCheckTimeout: 100 * time.Millisecond,
		CursorFile:       filepath.Join(t.TempDir(), "cursor.json"),
		Logger:           testDiscardLogger(),
	}
	result := &CheckResult{
		Tip: Tip{Slot: 1, Hash: strings.Repeat("ab", 32)},
	}

	done := make(chan struct{})
	var cursor *IncrementalCursor
	var err error
	go func() {
		cursor, err = buildStartupCursor(
			context.Background(), cfg, result, nil, 0,
		)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal(
			"buildStartupCursor did not return -- its own queryEpochAt " +
				"call must be bounded by cfg.FullCheckTimeout, not hang " +
				"indefinitely on a stalled peer",
		)
	}
	// The epoch query failing (stalled Acquire never succeeds) must
	// degrade gracefully -- liveEpoch falls back to -1 -- not fail
	// buildStartupCursor outright, matching its own documented behavior
	// for any other epoch-lookup failure.
	require.NoError(t, err)
	require.NotNil(t, cursor)
	assert.Equal(t, -1, cursor.Epoch)
}

// TestIncrementalSession_NoProgressWhenFirstBlockAlwaysFails is the
// regression test for a review finding:
// incrementalSession used to report progressed=true (established) the
// instant cs.Client.Sync was accepted, regardless of whether any block
// that followed actually validated. A session whose very first block
// fails every single reconnect (e.g. a persistent per-block
// LocalStateQuery failure) never gets past that same point, so it should
// report no progress at all -- letting RunIncremental's reconnect loop
// back off (nextBackoff) instead of resetting to watcherMinBackoff on
// every attempt, which would otherwise redial both nodes roughly every
// 250ms indefinitely.
//
// dingoState.rejectAcquireAtSlot targets the exact slot
// incrementalSession's first RollForward callback will be for (the block
// right after the cursor's starting point), so handleIncrementalBlock's
// own dingo-side query fails immediately, before blockValidated is ever
// set.
func TestIncrementalSession_NoProgressWhenFirstBlockAlwaysFails(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, dingoState, _, cardanoServer := newIncrementalHarnessWithServer(t, 10, false)

	startPoint := cardanoServer.chain.Points[cardanoServer.baselineTipIndex]
	startTip := Tip{
		Slot:        startPoint.Slot,
		Hash:        hex.EncodeToString(startPoint.Hash),
		BlockNumber: uint64(cardanoServer.baselineTipIndex), //nolint:gosec
	}
	firstBlockPoint := cardanoServer.chain.Points[cardanoServer.baselineTipIndex+1]
	dingoState.rejectAcquireAtSlot(firstBlockPoint.Slot)

	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{Tip: startTip})
	worker := &fullCheckWorker{
		cfg: IncrementalConfig{
			DingoAddr:        dingoAddr,
			CardanoAddr:      cardanoAddr,
			Magic:            42,
			FullCheckTimeout: 10 * time.Second,
			Logger:           testDiscardLogger(),
			OnFullCheck:      func(FullCheckReason, *CheckResult, error) {},
		},
		cursor: cursor,
		wake:   make(chan struct{}, 1),
	}
	cfg := IncrementalConfig{
		DingoAddr:         dingoAddr,
		CardanoAddr:       cardanoAddr,
		Magic:             42,
		FullCheckInterval: 1000,
		Logger:            testDiscardLogger(),
		OnBlockCheckError: func(error) {},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	progressed, err := incrementalSession(ctx, cfg, cursor, worker)
	require.Error(
		t, err,
		"the first block's rejected Acquire must surface as this "+
			"session's own error",
	)
	assert.False(
		t, progressed,
		"a session whose first block always fails must report no "+
			"progress, not the mere fact that Sync was accepted",
	)
}

// TestLoadCursor_MissingFileReturnsNilNotError covers the expected-first-run
// shape: RunIncremental always establishes its own baseline via a full
// Check regardless of a prior cursor (see its doc comment), so a caller that
// has never run before must see (nil, nil) here, not an error, to tell that
// apart from a real read/parse failure it should actually report.
func TestLoadCursor_MissingFileReturnsNilNotError(t *testing.T) {
	t.Parallel()
	cursor, err := LoadCursor(filepath.Join(t.TempDir(), "does-not-exist.json"))
	require.NoError(t, err)
	assert.Nil(t, cursor)
}

// TestSaveCursorLoadCursor_RoundTrips covers the persistence contract
// RunIncremental relies on to resume across restarts: whatever SaveCursor
// writes, LoadCursor must read back unchanged.
func TestSaveCursorLoadCursor_RoundTrips(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cursor.json")
	want := &IncrementalCursor{
		Tip: Tip{
			Slot:        123,
			Hash:        "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
			BlockNumber: 45,
		},
		Epoch:                7,
		BlocksSinceFullCheck: 89,
	}
	require.NoError(t, SaveCursor(path, want))

	got, err := LoadCursor(path)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want, got)
}

// TestSaveCursor_OverwritesPriorContent covers the case RunIncremental's
// steady-state loop actually exercises every block: saving a second cursor
// to the same path must fully replace the first, not merge or append --
// LoadCursor afterward must see only the latest write.
func TestSaveCursor_OverwritesPriorContent(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cursor.json")
	require.NoError(t, SaveCursor(path, &IncrementalCursor{
		Tip: Tip{Slot: 1}, Epoch: 1, BlocksSinceFullCheck: 1,
	}))
	require.NoError(t, SaveCursor(path, &IncrementalCursor{
		Tip: Tip{Slot: 2}, Epoch: 2, BlocksSinceFullCheck: 2,
	}))

	got, err := LoadCursor(path)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, uint64(2), got.Tip.Slot)
	assert.Equal(t, 2, got.Epoch)
}

// txIn builds a real ShelleyTransactionInput for a fixed, deterministic
// hash+index pair, matching how queryUTxOByRefs/blockRefsToQuery build the
// same type from a real block's decoded transactions.
func txIn(t *testing.T, hash string, idx int) lcommon.TransactionInput {
	t.Helper()
	return shelley.NewShelleyTransactionInput(hash, idx)
}

// txHashA and txHashB are two distinct, well-formed 64-hex-character
// (32-byte) transaction hashes for test fixtures.
var (
	txHashA = strings.Repeat("11", 32)
	txHashB = strings.Repeat("22", 32)
)

// TestBlockRefsToQuery_DedupesRepeatedRefs covers the defensive dedup
// blockRefsToQuery's doc comment describes: the same ref named by both a
// consumed input and a produced output (not possible for a single real TxIn,
// but kept defensive against two transactions in the same block both naming
// it) must appear exactly once in the resulting query list.
func TestBlockRefsToQuery_DedupesRepeatedRefs(t *testing.T) {
	t.Parallel()
	shared := txIn(t, txHashA, 0)
	refs := blockRefsToQuery(
		[]lcommon.TransactionInput{shared, shared},
		[]lcommon.Utxo{{Id: shared}},
	)
	assert.Len(
		t,
		refs,
		1,
		"the same ref named three times must be queried once",
	)
}

// TestBlockRefsToQuery_DistinctRefsAllIncluded covers the ordinary case:
// distinct consumed and produced refs must all appear in the query list,
// none silently dropped.
func TestBlockRefsToQuery_DistinctRefsAllIncluded(t *testing.T) {
	t.Parallel()
	consumed := txIn(t, txHashA, 0)
	produced := txIn(t, txHashB, 1)
	refs := blockRefsToQuery(
		[]lcommon.TransactionInput{consumed},
		[]lcommon.Utxo{{Id: produced}},
	)
	assert.Len(t, refs, 2)
}

// TestDiffBlockUtxoDelta_MatchingDeltaIsEmpty covers the clean-match case:
// a consumed ref absent from both nodes' answers (correctly spent) and a
// produced ref present with identical content on both must report no
// divergence lines at all.
func TestDiffBlockUtxoDelta_MatchingDeltaIsEmpty(t *testing.T) {
	t.Parallel()
	consumedRef := txIn(t, txHashA, 0)
	producedRef := txIn(t, txHashB, 0)
	consumed := []lcommon.TransactionInput{consumedRef}
	produced := []lcommon.Utxo{{Id: producedRef}}

	key := producedRef.Id().String() + "#0"
	entries := map[string]string{key: "addr1|1000000"}

	lines := diffBlockUtxoDelta(consumed, produced, entries, entries)
	assert.Empty(t, lines)
}

// TestDiffBlockUtxoDelta_ConsumedStillPresentIsReported covers the
// "should be spent but wasn't" case for each node independently: a consumed
// ref still showing up in dingo's answer, cardano-node's answer, or both,
// must be reported, naming which node(s) still have it live.
func TestDiffBlockUtxoDelta_ConsumedStillPresentIsReported(t *testing.T) {
	t.Parallel()
	consumedRef := txIn(t, txHashA, 0)
	key := consumedRef.Id().String() + "#0"
	consumed := []lcommon.TransactionInput{consumedRef}

	t.Run("still present in dingo only", func(t *testing.T) {
		t.Parallel()
		lines := diffBlockUtxoDelta(
			consumed, nil,
			map[string]string{key: "addr1|1000000"},
			map[string]string{},
		)
		require.Len(t, lines, 1)
		assert.Contains(t, lines[0], "still present in dingo")
	})

	t.Run("still present in cardano-node only", func(t *testing.T) {
		t.Parallel()
		lines := diffBlockUtxoDelta(
			consumed, nil,
			map[string]string{},
			map[string]string{key: "addr1|1000000"},
		)
		require.Len(t, lines, 1)
		assert.Contains(t, lines[0], "still present in cardano-node")
	})

	t.Run("still present in both", func(t *testing.T) {
		t.Parallel()
		lines := diffBlockUtxoDelta(
			consumed, nil,
			map[string]string{key: "addr1|1000000"},
			map[string]string{key: "addr1|1000000"},
		)
		assert.Len(
			t,
			lines,
			2,
			"both nodes still holding a spent ref live must produce two lines, one per node",
		)
	})
}

// TestDiffBlockUtxoDelta_ProducedMismatchIsReported covers every way a
// produced output can fail to match: missing from dingo, missing from
// cardano-node (the reference), missing from both, and present in both with
// different content.
func TestDiffBlockUtxoDelta_ProducedMismatchIsReported(t *testing.T) {
	t.Parallel()
	producedRef := txIn(t, txHashB, 0)
	produced := []lcommon.Utxo{{Id: producedRef}}
	key := producedRef.Id().String() + "#0"

	cases := []struct {
		name          string
		dingo         map[string]string
		cardano       map[string]string
		wantSubstring string
	}{
		{
			name:          "missing from dingo",
			dingo:         map[string]string{},
			cardano:       map[string]string{key: "addr1|1000000"},
			wantSubstring: "present in cardano-node, missing in dingo",
		},
		{
			name:          "missing from cardano-node",
			dingo:         map[string]string{key: "addr1|1000000"},
			cardano:       map[string]string{},
			wantSubstring: "present in dingo, missing in cardano-node",
		},
		{
			name:          "missing from both",
			dingo:         map[string]string{},
			cardano:       map[string]string{},
			wantSubstring: "missing from both",
		},
		{
			name:          "content differs",
			dingo:         map[string]string{key: "addr1|1000000"},
			cardano:       map[string]string{key: "addr1|2000000"},
			wantSubstring: "differs",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lines := diffBlockUtxoDelta(nil, produced, tc.dingo, tc.cardano)
			require.Len(t, lines, 1)
			assert.Contains(t, lines[0], tc.wantSubstring)
		})
	}
}

// TestDiffBlockUtxoDelta_IntraBlockSpendIsNotReported is a regression test
// for the incremental-mode audit finding: an output
// created by one transaction and spent by a later transaction in the same
// block correctly does not appear in either node's live UTxO query result --
// that is the expected outcome of a real, valid intra-block spend, not a
// divergence. Before the fix, the produced loop flagged any produced-but-
// absent output as "missing from both dingo and cardano-node" without first
// checking whether that same ref was also consumed within this block.
func TestDiffBlockUtxoDelta_IntraBlockSpendIsNotReported(t *testing.T) {
	t.Parallel()
	ref := txIn(t, txHashA, 0)
	consumed := []lcommon.TransactionInput{ref}
	produced := []lcommon.Utxo{{Id: ref}}

	lines := diffBlockUtxoDelta(
		consumed, produced, map[string]string{}, map[string]string{},
	)
	assert.Empty(
		t,
		lines,
		"an output created and spent within the same block must not be reported as a divergence just because it is absent from both nodes' live answers",
	)
}

// TestDiffBlockUtxoDelta_ProducedThenAbsentIsStillReportedWhenNotConsumed
// covers the case TestDiffBlockUtxoDelta_IntraBlockSpendIsNotReported's fix
// must not have broken: a produced ref that is genuinely missing from both
// nodes, and was NOT also consumed within this same block, is still a real
// divergence and must still be reported -- see
// TestDiffBlockUtxoDelta_ProducedMismatchIsReported's own "missing from
// both" case for the non-regression-specific version of this; this test
// exists specifically to prove the new intra-block check does not
// over-suppress unrelated produced refs.
func TestDiffBlockUtxoDelta_ProducedThenAbsentIsStillReportedWhenNotConsumed(
	t *testing.T,
) {
	t.Parallel()
	producedRef := txIn(t, txHashB, 0)
	unrelatedConsumedRef := txIn(t, txHashA, 0)

	lines := diffBlockUtxoDelta(
		[]lcommon.TransactionInput{unrelatedConsumedRef},
		[]lcommon.Utxo{{Id: producedRef}},
		map[string]string{},
		map[string]string{},
	)
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], "missing from both")
}

// TestDiffBlockUtxoDelta_DedupesRepeatedProducedRef covers the same
// defensive dedup as blockRefsToQuery, but for the diff side: the same
// produced ref appearing twice (e.g. if a caller failed to dedup upstream)
// must still only be reported once, not doubled.
func TestDiffBlockUtxoDelta_DedupesRepeatedProducedRef(t *testing.T) {
	t.Parallel()
	producedRef := txIn(t, txHashB, 0)
	lines := diffBlockUtxoDelta(
		nil,
		[]lcommon.Utxo{{Id: producedRef}, {Id: producedRef}},
		map[string]string{},
		map[string]string{},
	)
	assert.Len(t, lines, 1)
}

// TestRunIncremental_RequiresPositiveFullCheckInterval and
// TestRunIncremental_RequiresCursorFile cover RunIncremental's own
// validation, ahead of ever dialing anything -- a caller that got past
// cmd/node-parity's flag validation by constructing IncrementalConfig
// directly (e.g. a future embedder of this package) must not be able to
// start a loop that can never checkpoint or resume.
func TestRunIncremental_RequiresPositiveFullCheckInterval(t *testing.T) {
	t.Parallel()
	err := RunIncremental(context.Background(), IncrementalConfig{
		DingoAddr:   "127.0.0.1:1",
		CardanoAddr: "127.0.0.1:1",
		CursorFile:  filepath.Join(t.TempDir(), "cursor.json"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FullCheckInterval must be positive")
}

func TestRunIncremental_RequiresCursorFile(t *testing.T) {
	t.Parallel()
	err := RunIncremental(context.Background(), IncrementalConfig{
		DingoAddr:         "127.0.0.1:1",
		CardanoAddr:       "127.0.0.1:1",
		FullCheckInterval: 1000,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CursorFile is required")
}

// TestHandleIncrementalRollback_NoOpWhenPointMatchesCursor is a regression
// test for a bug caught live against a real node: the ChainSync protocol
// reports a RollBackward to the exact intersection point as the first
// message after any Sync/FindIntersect (confirming the negotiated reading
// position), not just on a genuine reorg. Treating every RollBackward as a
// real rollback fired a full checkpoint on every single session start
// (startup and every reconnect) at zero information gain, since the
// reported point was the cursor's own already-trusted point. This must be a
// pure no-op: cfg.OnFullCheck must never be called, and the cursor must be
// left exactly as it was.
func TestHandleIncrementalRollback_NoOpWhenPointMatchesCursor(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{
			Slot:        100,
			Hash:        "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
			BlockNumber: 10,
		}, Epoch: 5,
	})
	cfg := IncrementalConfig{
		DingoAddr:   "127.0.0.1:1",
		CardanoAddr: "127.0.0.1:1",
		CursorFile:  cursorFile,
		Logger:      testDiscardLogger(),
	}
	// A bare worker, its background goroutine never started: this test
	// only asserts on whether handleIncrementalRollback enqueues a request,
	// not on a full check actually running -- see
	// TestFullCheckWorker_RunsRequestsOneAtATime for that.
	worker := &fullCheckWorker{
		cfg: cfg, cursor: cursor, wake: make(chan struct{}, 1),
	}
	hashBytes, err := hex.DecodeString(
		"abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
	)
	require.NoError(t, err)
	samePoint := pcommon.NewPoint(100, hashBytes)

	require.NoError(
		t,
		handleIncrementalRollback(cfg, cursor, worker, samePoint),
	)

	assert.Nil(
		t,
		worker.pendingReq,
		"a rollback to the cursor's own current point must not dispatch a full check",
	)
	assert.Equal(
		t,
		Tip{
			Slot:        100,
			Hash:        "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
			BlockNumber: 10,
		},
		cursor.snapshot().Tip,
		"the cursor must be left unchanged by a no-op rollback",
	)
	_, statErr := os.Stat(cursorFile)
	assert.True(
		t,
		os.IsNotExist(statErr),
		"a no-op rollback must not even write the cursor file, since nothing changed",
	)
}

// TestHandleIncrementalRollback_TriggersFullCheckWhenPointDiffers covers the
// genuine-rollback case: a reported point that actually differs from the
// cursor's current one must reset the cursor to it, persist the new cursor,
// and dispatch a full check (FullCheckRollback) to the worker -- the real
// behavior TestHandleIncrementalRollback_NoOpWhenPointMatchesCursor's fix
// must not have broken while suppressing the false-positive case.
func TestHandleIncrementalRollback_TriggersFullCheckWhenPointDiffers(
	t *testing.T,
) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{
			Slot:        100,
			Hash:        "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
			BlockNumber: 10,
		}, Epoch: 5,
	})
	cfg := IncrementalConfig{
		DingoAddr:        "127.0.0.1:1",
		CardanoAddr:      "127.0.0.1:1",
		CursorFile:       cursorFile,
		FullCheckTimeout: 2 * time.Second,
		Logger:           testDiscardLogger(),
	}
	worker := &fullCheckWorker{
		cfg: cfg, cursor: cursor, wake: make(chan struct{}, 1),
	}
	olderHash, err := hex.DecodeString("ffff")
	require.NoError(t, err)
	rollbackPoint := pcommon.NewPoint(50, olderHash)

	require.NoError(
		t,
		handleIncrementalRollback(cfg, cursor, worker, rollbackPoint),
	)

	require.NotNil(
		t, worker.pendingReq,
		"a genuine rollback must dispatch a full check request",
	)
	assert.Equal(t, FullCheckRollback, worker.pendingReq.reason)
	assert.Equal(t, uint64(50), worker.pendingReq.at.Slot)
	assert.Equal(t, "ffff", worker.pendingReq.at.Hash)
	assert.Equal(t, uint64(50), cursor.snapshot().Tip.Slot)

	got, loadErr := LoadCursor(cursorFile)
	require.NoError(t, loadErr)
	require.NotNil(t, got)
	assert.Equal(t, uint64(50), got.Tip.Slot)
}

// TestFullCheckWorker_CoalescesByPriorityWhilePendingOneQueued covers the
// coalescing contract that keeps a fast-moving chain from queueing up
// redundant full checks behind a slow one: at most one request stays
// pending at a time, but -- unlike the drop-everything-unconditionally
// behavior this replaced -- which one survives now depends on priority
// (fullCheckReasonPriority), not simply which arrived first. This is the
// rationale: unconditionally dropping a second
// request meant a one-shot Mismatch/Rollback/EpochTransition trigger could
// be silently lost behind an already-queued, merely-due Interval
// checkpoint, even though the interval trigger costs nothing to drop
// instead (it is level-triggered and re-evaluates true again next block).
func TestFullCheckWorker_CoalescesByPriorityWhilePendingOneQueued(
	t *testing.T,
) {
	t.Parallel()

	t.Run(
		"higher priority replaces a lower priority pending request",
		func(t *testing.T) {
			t.Parallel()
			w := &fullCheckWorker{wake: make(chan struct{}, 1)}
			w.request(FullCheckInterval, Tip{Slot: 1})
			w.request(FullCheckMismatch, Tip{Slot: 2})

			require.NotNil(t, w.pendingReq)
			assert.Equal(
				t, FullCheckMismatch, w.pendingReq.reason,
				"a higher-priority request must replace an already-pending "+
					"lower-priority one",
			)
			assert.Equal(t, uint64(2), w.pendingReq.at.Slot)
		},
	)

	t.Run(
		"lower priority is dropped behind a higher priority pending request",
		func(t *testing.T) {
			t.Parallel()
			w := &fullCheckWorker{wake: make(chan struct{}, 1)}
			w.request(FullCheckMismatch, Tip{Slot: 1})
			w.request(FullCheckInterval, Tip{Slot: 2})

			require.NotNil(t, w.pendingReq)
			assert.Equal(
				t, FullCheckMismatch, w.pendingReq.reason,
				"a lower-priority request must not replace an already-pending "+
					"higher-priority one",
			)
			assert.Equal(
				t, uint64(1), w.pendingReq.at.Slot,
				"the original higher-priority request's own point must be kept",
			)
		},
	)

	t.Run("equal priority keeps the newest point", func(t *testing.T) {
		t.Parallel()
		w := &fullCheckWorker{wake: make(chan struct{}, 1)}
		w.request(FullCheckInterval, Tip{Slot: 1})
		w.request(FullCheckInterval, Tip{Slot: 2})

		require.NotNil(t, w.pendingReq)
		assert.Equal(t, FullCheckInterval, w.pendingReq.reason)
		assert.Equal(
			t, uint64(2), w.pendingReq.at.Slot,
			"a same-priority request must still replace the pending one, "+
				"so the newest required point is what actually gets "+
				"checked",
		)
	})
}

// TestFullCheckWorker_FailedAttemptDoesNotResetCounter covers the worker's
// actual run loop end to end against unreachable addresses, so Check fails
// fast on a dial error rather than genuinely completing: a dispatched
// request must still reach cfg.OnFullCheck with the request's own reason,
// but BlocksSinceFullCheck must NOT be reset afterward, since no check
// actually completed. This is a regression test for
// the incremental-mode audit finding (a): resetting
// the countdown on every attempt regardless of outcome (the prior behavior)
// silently delayed the next legitimate interval checkpoint by up to a full
// --full-check-interval's worth of blocks even though nothing was ever
// confirmed. See TestFullCheckWorker_SuccessfulCheckResetsCounter for the
// case that must still reset it.
func TestFullCheckWorker_FailedAttemptDoesNotResetCounter(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{
			Slot:        100,
			Hash:        "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
			BlockNumber: 10,
		},
		Epoch:                5,
		BlocksSinceFullCheck: 42,
	})

	var mu sync.Mutex
	var gotReason FullCheckReason
	var gotErr error
	called := make(chan struct{})
	cfg := IncrementalConfig{
		DingoAddr:        "127.0.0.1:1",
		CardanoAddr:      "127.0.0.1:1",
		CursorFile:       cursorFile,
		FullCheckTimeout: 5 * time.Second,
		Logger:           testDiscardLogger(),
		OnFullCheck: func(reason FullCheckReason, _ *CheckResult, err error) {
			mu.Lock()
			gotReason = reason
			gotErr = err
			mu.Unlock()
			close(called)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := startFullCheckWorker(ctx, cfg, cursor)
	worker.request(
		FullCheckMismatch,
		Tip{
			Slot: 200,
			Hash: "ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01",
		},
	)

	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("worker never ran the dispatched request")
	}
	mu.Lock()
	assert.Equal(t, FullCheckMismatch, gotReason)
	require.Error(
		t,
		gotErr,
		"unreachable addresses must make Check fail outright",
	)
	mu.Unlock()

	// A short, fixed wait rather than testutil.WaitForCondition: this test
	// asserts a state does NOT change, so it must observe the worker's own
	// goroutine settle rather than succeed immediately before that goroutine
	// had a chance to (incorrectly) reset the counter.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(
		t, uint64(42), cursor.snapshot().BlocksSinceFullCheck,
		"a failed full-check attempt must not reset BlocksSinceFullCheck",
	)
	assert.Equal(
		t, uint64(100), cursor.snapshot().Tip.Slot,
		"a failed attempt must not disturb Tip either",
	)

	cancel()
	worker.stop()
}

// TestFullCheckWorker_SuccessfulCheckResetsCounter covers the case
// TestFullCheckWorker_FailedAttemptDoesNotResetCounter's fix must not have
// broken: a full check that actually completes (against the fake harness,
// which always accepts Acquire regardless of target -- see
// newIncrementalHarness) must still reset BlocksSinceFullCheck, without
// disturbing Tip/Epoch. The request's own at.BlockNumber is set to match
// the cursor's current Tip.BlockNumber (10) -- no blocks advance the
// cursor between dispatch and completion here, so the check's own point
// and the live tip coincide and the result is 0. See
// TestCursorState_ResetFullCheckCounterIsRelativeToAt for the case where
// blocks do arrive in between, and
// TestCursorState_ResetFullCheckCounterHandlesOverlappingRequests for two
// such checks completing out of order.
func TestFullCheckWorker_SuccessfulCheckResetsCounter(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, _, _ := newIncrementalHarness(t, 3)

	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{
			Slot:        100,
			Hash:        "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
			BlockNumber: 10,
		},
		Epoch:                5,
		BlocksSinceFullCheck: 42,
	})

	var mu sync.Mutex
	var gotErr error
	called := make(chan struct{})
	cfg := IncrementalConfig{
		DingoAddr:        dingoAddr,
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		CursorFile:       cursorFile,
		FullCheckTimeout: 10 * time.Second,
		Logger:           testDiscardLogger(),
		OnFullCheck: func(_ FullCheckReason, _ *CheckResult, err error) {
			mu.Lock()
			gotErr = err
			mu.Unlock()
			close(called)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := startFullCheckWorker(ctx, cfg, cursor)
	worker.request(
		FullCheckInterval,
		Tip{
			Slot:        200,
			Hash:        "ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01ef01",
			BlockNumber: cursor.snapshot().Tip.BlockNumber,
		},
	)

	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("worker never ran the dispatched request")
	}
	mu.Lock()
	require.NoError(
		t,
		gotErr,
		"the fake harness must let this full check actually complete",
	)
	mu.Unlock()

	testutil.WaitForCondition(t, func() bool {
		return cursor.snapshot().BlocksSinceFullCheck == 0
	}, 2*time.Second, "a completed full check must reset BlocksSinceFullCheck")
	assert.Equal(
		t, uint64(100), cursor.snapshot().Tip.Slot,
		"resetting the checkpoint counter must not disturb Tip",
	)

	cancel()
	worker.stop()
}

// TestFullCheckSucceeded covers fullCheckSucceeded's own decision in
// isolation: only a nil error and a non-nil, non-Skipped result counts as a
// completed check.
func TestFullCheckSucceeded(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		result *CheckResult
		err    error
		want   bool
	}{
		{"matched or diverged result, no error", &CheckResult{}, nil, true},
		{
			"query error, even with a result",
			&CheckResult{},
			assert.AnError,
			false,
		},
		{"nil result, no error", nil, nil, false},
		{"skipped result", &CheckResult{Skipped: true}, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, fullCheckSucceeded(tc.result, tc.err))
		})
	}
}

// TestCursorState_AdvanceIncrementsAndPersists covers the per-block
// bookkeeping path: advance must update Tip, increment
// BlocksSinceFullCheck, apply a non-negative epoch, leave a negative one
// (the epoch-lookup-failed signal) alone, and persist every change.
func TestCursorState_AdvanceIncrementsAndPersists(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{Slot: 1}, Epoch: 5, BlocksSinceFullCheck: 3,
	})

	got, err := cursor.advance(Tip{Slot: 2}, 6)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), got.Tip.Slot)
	assert.Equal(t, 6, got.Epoch)
	assert.Equal(t, uint64(4), got.BlocksSinceFullCheck)

	got, err = cursor.advance(Tip{Slot: 3}, -1)
	require.NoError(t, err)
	assert.Equal(
		t,
		6,
		got.Epoch,
		"a negative epoch (lookup failed) must not overwrite the last known one",
	)
	assert.Equal(t, uint64(5), got.BlocksSinceFullCheck)

	persisted, loadErr := LoadCursor(cursorFile)
	require.NoError(t, loadErr)
	require.NotNil(t, persisted)
	assert.Equal(t, got, *persisted)
}

// TestCursorState_ResetFullCheckCounterIsRelativeToAt covers a bug in
// the full-check counter reset: a full check runs
// asynchronously (fullCheckWorker's doc comment) precisely so the ChainSync
// callback goroutine can keep validating and advancing the cursor for every
// block that arrives while it's in flight -- a real full check commonly
// takes several minutes (see resetFullCheckCounter's own doc comment), long
// enough for hundreds of blocks to land in the meantime against a real
// node. The prior resetFullCheckCounter zeroed BlocksSinceFullCheck
// unconditionally once the check completed, discarding however many of
// those blocks had already advanced the counter -- silently delaying the
// next periodic checkpoint by that same amount, since it now had to count
// all the way back up from 0 instead of from where the in-flight blocks had
// already brought it.
//
// This simulates that interleaving without any goroutine timing: at is the
// point the check was dispatched at (matching request()'s own capture),
// then 100 blocks are applied directly to the cursor before
// resetFullCheckCounter runs against that same at -- deterministic, and
// fails against the prior unconditional-zero behavior (confirmed by
// temporarily reverting the fix and re-running: this test then asserts 0
// but observes 100).
func TestCursorState_ResetFullCheckCounterIsRelativeToAt(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip:                  Tip{BlockNumber: 1000},
		BlocksSinceFullCheck: 1000,
	})

	// at is the point request() would have captured at dispatch time --
	// the cursor's own Tip right before the check that is about to
	// "complete" was ever requested.
	at := cursor.snapshot().Tip
	require.Equal(t, uint64(1000), at.BlockNumber)

	// 100 blocks arrive and are validated while that check is in flight.
	for i := range uint64(100) {
		_, err := cursor.advance(Tip{BlockNumber: 1001 + i}, 5)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1100), cursor.snapshot().BlocksSinceFullCheck)

	// The check now completes and resets against the point captured
	// before those 100 advances.
	require.NoError(t, cursor.resetFullCheckCounter(at))
	assert.Equal(
		t, uint64(100), cursor.snapshot().BlocksSinceFullCheck,
		"the 100 blocks that arrived after this check was dispatched must "+
			"survive the reset, not be discarded by zeroing the counter "+
			"outright",
	)

	persisted, err := LoadCursor(cursorFile)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, uint64(100), persisted.BlocksSinceFullCheck)
}

// TestCursorState_ResetFullCheckCounterHandlesOverlappingRequests guards
// against subtracting a snapshot of the counter's own absolute value
// (captured at each request's dispatch time) instead of recomputing from
// at.BlockNumber. That design breaks under two overlapping requests --
// exactly what this test reproduces. Request A is dispatched, then request
// B is dispatched later (more blocks having landed by then, while A is
// still conceptually "in flight"), then A completes first and resets
// against its own point, then more blocks land during B's own "run", then
// B completes and resets against its own point. Under the
// baseline-subtraction design, B's captured baseline (1100) would already
// exceed the counter A's completion had reduced to (100), so B's completion
// would incorrectly floor the
// counter to 0 -- discarding the real blocks that arrived during B's own
// run. Recomputing from each request's own at.BlockNumber sidesteps this
// entirely: neither completion's arithmetic depends on whether the other
// has already reset the counter.
func TestCursorState_ResetFullCheckCounterHandlesOverlappingRequests(
	t *testing.T,
) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip:                  Tip{BlockNumber: 1000},
		BlocksSinceFullCheck: 1000,
	})

	// Request A dispatched at block 1000.
	atA := cursor.snapshot().Tip

	// 100 more blocks land while A is conceptually still running --
	// including the block where request B gets dispatched, captured
	// partway through.
	for i := range uint64(100) {
		_, err := cursor.advance(Tip{BlockNumber: 1001 + i}, 5)
		require.NoError(t, err)
	}
	// Request B dispatched here, at block 1100 -- a real, later point,
	// unlike the old design's baseline snapshot (which would have captured
	// the same 1100 value, but as an absolute counter snapshot rather than
	// a stable point).
	atB := cursor.snapshot().Tip
	require.Equal(t, uint64(1100), atB.BlockNumber)

	// A completes first (it was dispatched first) and resets against its
	// own point.
	require.NoError(t, cursor.resetFullCheckCounter(atA))
	require.Equal(
		t, uint64(100), cursor.snapshot().BlocksSinceFullCheck,
		"A's own completion must reduce the counter by exactly the blocks "+
			"validated since A's own dispatch point",
	)

	// 50 more blocks land while B is now running.
	for i := range uint64(50) {
		_, err := cursor.advance(Tip{BlockNumber: 1101 + i}, 5)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(150), cursor.snapshot().BlocksSinceFullCheck)

	// B completes and resets against its own point (1100) -- not the
	// counter's current absolute value, which A's completion already
	// shifted.
	require.NoError(t, cursor.resetFullCheckCounter(atB))
	assert.Equal(
		t, uint64(50), cursor.snapshot().BlocksSinceFullCheck,
		"B's completion must preserve the 50 blocks validated since B's "+
			"own dispatch point, not discard them because B's point predates "+
			"A's already-applied reset",
	)
}

// TestCursorState_ResetFullCheckCounterFloorsAtZero covers the defensive
// floor resetFullCheckCounter falls back to for a rollback-triggered
// request, whose Tip carries no BlockNumber (setRollback's doc comment:
// a rollback point does not have one) -- there is no meaningful delta to
// compute across a fork switch, so this must floor at 0 rather than
// treating the whole live block height as the delta.
func TestCursorState_ResetFullCheckCounterFloorsAtZero(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip:                  Tip{BlockNumber: 500},
		BlocksSinceFullCheck: 5,
	})

	require.NoError(t, cursor.resetFullCheckCounter(Tip{Slot: 999}))
	assert.Equal(t, uint64(0), cursor.snapshot().BlocksSinceFullCheck)
}

// TestDecideFullCheckReason covers every trigger decideFullCheckReason
// makes, including the interval and epoch-transition paths that no live
// testnet run could exercise on its own (a stake-distribution divergence made a
// mismatch fire on effectively every block, always preempting the other
// two before their own conditions are ever reached) -- this is that
// coverage, independent of that divergence or any live network at all.
func TestDecideFullCheckReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                 string
		diffEmpty            bool
		epoch, beforeEpoch   int
		blocksSinceFullCheck uint64
		fullCheckInterval    uint64
		wantReason           FullCheckReason
		wantDue              bool
	}{
		{
			name:      "clean block, interval not yet due: no trigger",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
		{
			name:      "mismatch fires regardless of everything else",
			diffEmpty: false, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 0, fullCheckInterval: 1000,
			wantReason: FullCheckMismatch, wantDue: true,
		},
		{
			name:      "epoch transition fires on a clean block",
			diffEmpty: true, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantReason: FullCheckEpochTransition, wantDue: true,
		},
		{
			name:      "interval reached on a clean block, same epoch",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckInterval, wantDue: true,
		},
		{
			name:      "interval reached exactly at the boundary still fires",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckInterval, wantDue: true,
		},
		{
			name:      "interval past the boundary still fires (a missed exact match must not suppress it)",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 1001, fullCheckInterval: 1000,
			wantReason: FullCheckInterval, wantDue: true,
		},
		{
			name:      "mismatch outranks an epoch transition on the same block",
			diffEmpty: false, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantReason: FullCheckMismatch, wantDue: true,
		},
		{
			name:      "epoch transition outranks a due interval on the same block",
			diffEmpty: true, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckEpochTransition, wantDue: true,
		},
		{
			name:      "mismatch outranks both an epoch transition and a due interval together",
			diffEmpty: false, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckMismatch, wantDue: true,
		},
		{
			name:      "current epoch lookup failed (-1): never reports a transition",
			diffEmpty: true, epoch: -1, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
		{
			name:      "prior epoch unknown (-1, first block after baseline epoch lookup failed): never reports a transition",
			diffEmpty: true, epoch: 6, beforeEpoch: -1,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
		{
			name:      "same epoch reported twice: not a transition",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reason, due := decideFullCheckReason(
				tc.diffEmpty, tc.epoch, tc.beforeEpoch,
				tc.blocksSinceFullCheck, tc.fullCheckInterval,
			)
			assert.Equal(t, tc.wantDue, due)
			if tc.wantDue {
				assert.Equal(t, tc.wantReason, reason)
			}
		})
	}
}

// testDiscardLogger returns a logger that writes nowhere, for tests that
// need a non-nil *slog.Logger but do not assert on its output.
func testDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestReportSessionEnd_RecordsErrorViaCallback is a regression test for
// the incremental-mode audit finding: a per-block
// query failure that ends an incrementalSession previously reached only a
// log line, never any metric-recording callback, so
// node_parity_check_errors_total could never see this failure class (the
// stake-distribution stall from the same audit's other finding would never
// have paged anyone). A non-nil sessionErr must reach OnBlockCheckError.
func TestReportSessionEnd_RecordsErrorViaCallback(t *testing.T) {
	t.Parallel()
	var got error
	cfg := IncrementalConfig{
		Logger:            testDiscardLogger(),
		OnBlockCheckError: func(err error) { got = err },
	}
	wantErr := errors.New("block delta check at slot 5: dingo query: boom")

	reportSessionEnd(cfg, wantErr, time.Second, 5)

	assert.Equal(t, wantErr, got)
}

// TestReportSessionEnd_NilErrorDoesNotInvokeCallback covers the clean-ending
// case reportSessionEnd's own caller already excludes in practice (ctx
// cancellation returns before this is ever called), but which this function
// itself still guards defensively: a nil sessionErr must not invoke
// OnBlockCheckError at all.
func TestReportSessionEnd_NilErrorDoesNotInvokeCallback(t *testing.T) {
	t.Parallel()
	called := false
	cfg := IncrementalConfig{
		Logger:            testDiscardLogger(),
		OnBlockCheckError: func(error) { called = true },
	}

	reportSessionEnd(cfg, nil, time.Second, 5)

	assert.False(
		t,
		called,
		"a nil session error must not invoke OnBlockCheckError",
	)
}
