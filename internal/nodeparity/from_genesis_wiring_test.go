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

// This file exercises RunFromGenesis itself -- the real production entry
// point cmd/node-parity calls -- rather than only the extracted helpers
// (utxoVerdict, applyTxInfoResults) from_genesis_test.go already pins in
// isolation.
//
// utxoTaintedThisEpoch is closure state inside RunFromGenesis's own function
// literal: from_genesis_test.go proves utxoVerdict reads it correctly and
// applyTxInfoResults computes the right failure decision, but nothing proves
// RunFromGenesis's real roll-forward/roll-backward callbacks actually wire
// that decision into the flag, or actually clear it again once reported.
// Reverting either assignment to a discarded no-op in place (`_ = true` /
// `_ = false`) leaves every test in from_genesis_test.go and
// koios_check_wiring_test.go green. Closing that gap needs a seam driving
// RunFromGenesis's real callbacks through a real ChainSync+LocalStateQuery
// session and a genuine RollBackward -- tracked as dingo#4365.
//
// genesisFakeServer is a purpose-built fake for this file rather than a
// reuse of incremental_harness_test.go's fakeCardanoServer/fakeLSQState:
// those answer a different query set (GetStakeDistribution, GetUTxOByTxIn --
// no GetPoolDistr2 or GetUTxOWhole at all) for RunIncremental's own needs,
// and this file's epoch-per-point resolution (see genesisFakeServer's own
// doc comment) has no equivalent there. Extending the shared harness for a
// shape only this file needs would risk the large existing incremental
// suite for no shared benefit.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/fixtures"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// genesisFakeServer combines a real ChainSync block feed (mirroring
// incremental_harness_test.go's fakeCardanoServer -- FindIntersect, an
// initial forced RollBackward matching a real session's own
// NeedsInitialRollback behavior, a step-gated real block feed, and one
// deliberate extra RollBackward once rollbackAfterCursor blocks have been
// delivered) with the exact LocalStateQuery answers RunFromGenesis needs
// (HardForkCurrentEraQuery, ShelleyCurrentProtocolParamsQuery,
// ShelleyPoolDistr2Query, ShelleyUtxoWholeQuery, ShelleyEpochNoQuery).
//
// GetEpochNo's answer is resolved from epochBySlot, keyed by the slot most
// recently Acquired, rather than a single mutable field a test flips
// between allowStep calls: RunFromGenesis calls currentEpochNo on its own,
// separately-dialed connection from both its roll-forward and
// roll-backward callbacks, and the deliberate RollBackward below fires
// automatically on the chainsync session's own background goroutine as
// soon as the prior callback returns -- not synchronized with the test
// goroutine at all. A shared mutable "current epoch" field would race that
// goroutine's own currentEpochNo call against the test's next setEpoch.
// Keying the answer to the Acquired point instead removes the race:
// epochBySlot is fixed once, before RunFromGenesis ever starts.
type genesisFakeServer struct {
	mu                 sync.Mutex
	chain              csmock.Chain
	cursor             int
	rolledBack         bool
	rollbackTo         *pcommon.Point
	firedExtraRollback bool
	// rollbackAfterCursor delays the deliberate RollBackward until cursor
	// has advanced past it (i.e. that many real blocks have been
	// delivered), instead of firing on the very next requestNext call --
	// letting a test observe at least one genuine clean epoch report
	// before the rollback, not just epochs after it.
	rollbackAfterCursor int
	// step gates every real RollForward reply (not either RollBackward
	// branch): a test calls allowStep once per block it wants delivered,
	// so a state change (nothing here yet, but see epochBySlot) lands at a
	// precise point instead of racing an ungated server.
	step chan struct{}

	epochBySlot      map[uint64]int
	lastAcquiredSlot uint64
}

func newGenesisFakeServer(t *testing.T, blockCount int) *genesisFakeServer {
	t.Helper()
	chain, err := csmock.BuildChain(1, ledger.Blake2b256{}, 100, 20, blockCount)
	require.NoError(t, err)
	return &genesisFakeServer{chain: chain, step: make(chan struct{})}
}

// newGenesisFakeServerWithTransactions is like newGenesisFakeServer, but its
// chain carries one real transaction per block
// (internal/test/fixtures.GenerateConwayChainWithTransactions) instead of
// newGenesisFakeServer's empty-body csmock.BuildChain blocks. RunFromGenesis's
// roll-forward callback only ever appends to pendingTxHashes from a block's
// own Transactions() (see from_genesis.go), so an empty-body chain never
// gives flushPendingTxInfos anything to fetch at all -- its own
// tx_info-chunk-failure taint path (the "if failed" branch inside
// flushPendingTxInfos, distinct from the rollback-triggered taint
// TestRunFromGenesis_UTxOTaintLifecycle already covers) goes completely
// unexercised by every test built on newGenesisFakeServer. See
// TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch.
func newGenesisFakeServerWithTransactions(
	t *testing.T, blockCount int,
) *genesisFakeServer {
	t.Helper()
	blocks, err := fixtures.GenerateConwayChainWithTransactions(blockCount)
	require.NoError(t, err)
	chain := csmock.Chain{
		Blocks: blocks,
		Points: make([]pcommon.Point, len(blocks)),
		Tips:   make([]chainsync.Tip, len(blocks)),
	}
	for i, block := range blocks {
		chain.Points[i] = csmock.PointOf(block)
		chain.Tips[i] = csmock.TipOf(block)
	}
	return &genesisFakeServer{chain: chain, step: make(chan struct{})}
}

// allowStep permits this server's next gated RollForward reply to proceed.
func (s *genesisFakeServer) allowStep(t *testing.T) {
	t.Helper()
	select {
	case s.step <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not consume the step signal in time")
	}
}

// findIntersect always succeeds at whatever point the client asks for (its
// own cursor), matching a fresh from-genesis session's actual FindIntersect
// call ([]pcommon.Point{startPoint}, resolveStartPoint's Origin by
// default), or at Origin if the requested point isn't on this fake's chain
// at all.
func (s *genesisFakeServer) findIntersect(
	_ chainsync.CallbackContext, points []pcommon.Point,
) (pcommon.Point, chainsync.Tip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(points) > 0 {
		for i, p := range s.chain.Points {
			if p.Slot == points[0].Slot {
				s.cursor = i + 1
				return points[0], s.chain.Tips[i], nil
			}
		}
	}
	s.cursor = 0
	return csmock.OriginPoint(), s.chain.Tip(), nil
}

// requestNext mirrors fakeCardanoServer.requestNext's three-branch shape
// (initial forced rollback, one deliberate extra rollback, then a gated
// real block feed) -- see that function's own doc comment in
// incremental_harness_test.go for why the initial rollback exists
// unconditionally on every session.
func (s *genesisFakeServer) requestNext(ctx chainsync.CallbackContext) error {
	s.mu.Lock()
	if !s.rolledBack {
		s.rolledBack = true
		defer s.mu.Unlock()
		return ctx.Server.RollBackward(
			s.chain.Points[max(s.cursor-1, 0)], s.chain.Tip(),
		)
	}
	if s.rollbackTo != nil && !s.firedExtraRollback &&
		s.cursor > s.rollbackAfterCursor && s.cursor < s.chain.Len() {
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
	<-step

	s.mu.Lock()
	defer s.mu.Unlock()
	block := s.chain.Blocks[s.cursor]
	tip := s.chain.Tips[s.cursor]
	s.cursor++
	return ctx.Server.RollForward(uint(block.Type()), block.Cbor(), tip)
}

// epochForLastAcquired returns epochBySlot's answer for the most recently
// Acquired slot -- see this type's own doc comment for why this, not a
// single mutable field, is what GetEpochNo answers from.
func (s *genesisFakeServer) epochForLastAcquired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epochBySlot[s.lastAcquiredSlot]
}

// lsqConfig answers exactly the query types RunFromGenesis needs:
// HardForkCurrentEraQuery (always Conway -- this test has no need for the
// Shelley/Allegra ambiguity koios_check_wiring_test.go exercises),
// ShelleyCurrentProtocolParamsQuery, ShelleyPoolDistr2Query (no pools --
// CheckStakeDistribution then never calls Koios's /pool_history at all, see
// its own doc comment), ShelleyUtxoWholeQuery (always an empty set -- this
// file only cares whether the UTxO comparison ran, tainted or not, never
// about its content), and ShelleyEpochNoQuery.
func (s *genesisFakeServer) lsqConfig() localstatequery.Config {
	pp := newFakeProtocolParams()
	return localstatequery.NewConfig(
		localstatequery.WithAcquireFunc(
			func(
				_ localstatequery.CallbackContext,
				target localstatequery.AcquireTarget,
				_ bool,
			) error {
				if sp, ok := target.(localstatequery.AcquireSpecificPoint); ok {
					s.mu.Lock()
					s.lastAcquiredSlot = sp.Point.Slot
					s.mu.Unlock()
				}
				return nil
			},
		),
		localstatequery.WithQueryFunc(
			func(
				_ localstatequery.CallbackContext,
				q localstatequery.QueryWrapper,
			) (any, error) {
				block, ok := q.Query.(*localstatequery.BlockQuery)
				if !ok {
					return nil, fmt.Errorf("unexpected top-level query %T", q.Query)
				}
				switch inner := block.Query.(type) {
				case *localstatequery.HardForkQuery:
					switch inner.Query.(type) {
					case *localstatequery.HardForkCurrentEraQuery:
						return int(ledger.EraIdConway), nil
					default:
						return nil, fmt.Errorf("unexpected hardfork query %T", inner.Query)
					}
				case *localstatequery.ShelleyQuery:
					switch inner.Query.(type) {
					case *localstatequery.ShelleyCurrentProtocolParamsQuery:
						return []any{pp}, nil
					case *localstatequery.ShelleyPoolDistr2Query:
						return localstatequery.PoolDistr2Result{
							Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{},
						}, nil
					case *localstatequery.ShelleyUtxoWholeQuery:
						return localstatequery.UTxOsResult{
							Results: map[localstatequery.UtxoId]ledger.BabbageTransactionOutput{},
						}, nil
					case *localstatequery.ShelleyEpochNoQuery:
						return []any{s.epochForLastAcquired()}, nil
					default:
						return nil, fmt.Errorf("unexpected shelley query %T", inner.Query)
					}
				default:
					return nil, fmt.Errorf("unexpected block query %T", block.Query)
				}
			},
		),
		localstatequery.WithReleaseFunc(
			func(_ localstatequery.CallbackContext) error { return nil },
		),
	)
}

// serve starts this fake as a real NtC server on listener, answering both
// ChainSync (the one persistent session RunFromGenesis dials via dialRaw)
// and LocalStateQuery (the many short-lived Acquire-query-Release
// connections RunFromGenesis dials via Dial) on every accepted connection,
// exactly like a real Dingo node does.
func (s *genesisFakeServer) serve(t *testing.T, listener net.Listener, magic uint32) {
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
					ouroboros.WithLocalStateQueryConfig(s.lsqConfig()),
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

// TestRunFromGenesis_UTxOTaintLifecycle drives RunFromGenesis through a
// genuine RollBackward and three epoch boundaries, proving
// utxoTaintedThisEpoch's real lifecycle inside RunFromGenesis's own
// closures -- not just utxoVerdict/applyTxInfoResults in isolation (see
// this file's own doc comment, and dingo#4365).
//
// Every session gets one forced initial RollBackward before any real block
// (NeedsInitialRollback -- see genesisFakeServer.requestNext), to the same
// point block 0 itself lands on, so block 0's own epoch always equals the
// epoch that rollback already established and never itself starts a new
// epoch boundary -- it only captures the genesis UTxO baseline. Block 1 is
// this test's first real reported epoch (clean). The deliberate rollback
// then fires once block 1 has been delivered (rollbackAfterCursor),
// re-baselining utxoRefs from Dingo's own (fake) current answer -- which
// this PR's fix must taint, or block 2's report would trivially compare the
// re-baselined set against itself and falsely read "clean". Block 3's
// report must NOT still be tainted, proving the post-report reset ran too,
// not just that the rollback's own assignment did.
//
// Reverting the rollback callback's utxoTaintedThisEpoch = true assignment
// in from_genesis.go, or its post-report utxoTaintedThisEpoch = false
// reset, in place leaves TestUTxOVerdict and TestApplyTxInfoResults green:
// neither drives RunFromGenesis's real callbacks at all.
func TestRunFromGenesis_UTxOTaintLifecycle(t *testing.T) {
	const magic = 42
	const blockCount = 5

	server := newGenesisFakeServer(t, blockCount)
	server.rollbackAfterCursor = 1
	rollbackTo := server.chain.Points[1]
	server.rollbackTo = &rollbackTo
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
		server.chain.Points[3].Slot: 4,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	// A 404-everything Koios fake is enough: CheckStakeDistribution never
	// calls Koios at all with zero pools (see lsqConfig's doc comment), and
	// CheckProtocolParams tolerates a Koios fetch failure as a mismatch, not
	// a ProtocolParamsErr -- this test only asserts on UTxOErr.
	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios,
			func(r EpochResult) { results <- r },
			nil, nil,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for an epoch result")
			return EpochResult{}
		}
	}

	// Block 0: genesis baseline capture only -- shares its epoch with the
	// mandatory initial rollback, so it never starts a new epoch boundary
	// and reports nothing.
	server.allowStep(t)

	// Block 1: first real epoch boundary, before any rollback -- must be
	// clean.
	server.allowStep(t)
	report1 := recv()
	assert.True(t, report1.UTxOAttempted)
	assert.NoError(t, report1.UTxOErr)

	// The deliberate rollback fires automatically on the chainsync
	// session's own next requestNext call, once rollbackAfterCursor blocks
	// have been delivered -- the step gate only applies to a genuine block
	// delivery, not either RollBackward branch, so nothing here needs to
	// wait for it explicitly before the next allowStep.
	server.allowStep(t)

	// Block 2: the epoch right after the rollback -- must be tainted, not
	// a false "clean" match against the just-re-baselined set.
	report2 := recv()
	assert.True(t, report2.UTxOAttempted)
	require.Error(t, report2.UTxOErr)
	assert.ErrorIs(t, report2.UTxOErr, errUTxOTainted)

	server.allowStep(t)

	// Block 3: must NOT still be tainted -- proves the post-report reset
	// ran, not just that the rollback's own assignment did.
	report3 := recv()
	assert.True(t, report3.UTxOAttempted)
	assert.NoError(t, report3.UTxOErr)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch drives RunFromGenesis
// through a genuine Koios tx_info fetch failure -- no rollback at all --
// proving flushPendingTxInfos's own "if failed { utxoTaintedThisEpoch =
// true ... }" branch (from_genesis.go) actually taints the epoch it
// belongs to. TestRunFromGenesis_UTxOTaintLifecycle above only exercises
// the rollback callback's separate utxoTaintedThisEpoch = true assignment;
// its own chain (newGenesisFakeServer's empty-body blocks) never gives
// flushPendingTxInfos a single transaction hash to fetch, so that taint
// source's own wiring goes completely unexercised there. Reverting this
// assignment to a discarded no-op (`_ = true`) in place leaves
// TestRunFromGenesis_UTxOTaintLifecycle, TestUTxOVerdict, and
// TestApplyTxInfoResults all green.
//
// newGenesisFakeServerWithTransactions gives every block one real
// transaction. Block 0 is the mandatory genesis baseline (shares its epoch
// with the forced initial rollback, so it never itself starts a new epoch
// boundary or reports -- see TestRunFromGenesis_UTxOTaintLifecycle's own
// doc comment). Block 1 is the first real epoch boundary: its transaction
// hash is buffered into pendingTxHashes and flushed via flushPendingTxInfos
// right before that epoch's report is built. The Koios fake here 404s
// every request, including /tx_info, so that flush fails and this epoch
// must report errUTxOTainted rather than a false "clean" match against the
// re-baselined set flushPendingTxInfos falls back to on failure.
func TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch(t *testing.T) {
	const magic = 42
	const blockCount = 2

	server := newGenesisFakeServerWithTransactions(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	// A 404-everything Koios fake, same as
	// TestRunFromGenesis_UTxOTaintLifecycle -- CheckStakeDistribution never
	// calls Koios at all with zero pools, and CheckProtocolParams tolerates
	// a Koios fetch failure as a mismatch, not a ProtocolParamsErr; this
	// test only asserts on UTxOErr. The same 404 also fails GetTxInfos'
	// /tx_info call, which is the failure this test exists to taint on.
	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios,
			func(r EpochResult) { results <- r },
			nil, nil,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for an epoch result")
			return EpochResult{}
		}
	}

	// Block 0: genesis baseline capture only -- reports nothing.
	server.allowStep(t)

	// Block 1: first real epoch boundary, with its own transaction hash
	// flushed against a Koios server that 404s /tx_info -- must be
	// tainted, not a false "clean" match against the re-baselined set.
	server.allowStep(t)
	report := recv()
	assert.True(t, report.UTxOAttempted)
	require.Error(t, report.UTxOErr)
	assert.ErrorIs(t, report.UTxOErr, errUTxOTainted)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}
