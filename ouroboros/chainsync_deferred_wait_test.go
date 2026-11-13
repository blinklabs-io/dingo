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

package ouroboros

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestChainsyncMissingAncestryRecoversWithoutPublicationWait(t *testing.T) {
	t.Parallel()
	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6049")
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId, gledger.BlockHeader,
	) (uint64, error) {
		return 0, ledger.ErrHeaderAncestryUnavailable
	}
	requirePromptDeferredRecovery(t, f)
}

func TestChainsyncIdleDeferralRecoversWithoutPublicationWait(t *testing.T) {
	t.Parallel()
	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6050")
	_, err := f.o.chainSelectionVerifyHeaderCrypto(f.conn, nil)
	require.True(t, ledger.IsHeaderVerificationDeferred(err))
	require.False(t, ledger.IsHeaderVerificationContextTransient(err))
	setTestLedgerTip(t, f.o, ochainsync.Tip{
		Point:       ocommon.NewPoint(100, gledger.Blake2b256{0xa0}.Bytes()),
		BlockNumber: 1,
	})
	requirePromptDeferredRecovery(t, f)
}

func requirePromptDeferredRecovery(
	t *testing.T,
	f deferredHeaderRecoveryFixture,
) {
	t.Helper()
	t.Cleanup(f.o.stopFutureHeaderResyncs)
	_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := f.o.eventBus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := f.o.eventBus.Subscribe(
		ledger.ConnectionRecycleRequestedEventType,
	)
	f.o.catchUpBandWaitTimeout = time.Minute
	f.o.config.ChainsyncHeaderVerificationSettled = newSettleSignal().current
	f.o.chainsyncLocalChainTips = func() ([]byte, []byte) {
		hash := gledger.Blake2b256{0xa0}.Bytes()
		return hash, hash
	}
	var attempts atomic.Int32
	verify := f.o.chainSelectionVerifyHeaderCrypto
	f.o.chainSelectionVerifyHeaderCrypto = func(
		conn ouroboros.ConnectionId, header gledger.BlockHeader,
	) (uint64, error) {
		attempts.Add(1)
		return verify(conn, header)
	}
	header := nonByronTestHeader{&testBlockHeader{
		hash: gledger.Blake2b256{0xb9}, prevHash: gledger.Blake2b256{0xb8},
		slotNumber: 400, blockNumber: 10,
	}}
	connDone := make(chan any)
	finished := make(chan struct{})
	result := make(chan error, 1)
	t.Cleanup(func() {
		close(connDone)
		testutil.RequireReceive(t, finished, testutil.AsyncWait,
			"the roll-forward callback must exit on connection close")
	})
	go func() {
		defer close(finished)
		result <- f.o.chainsyncClientRollForward(ochainsync.CallbackContext{
			ConnectionId: f.conn, ConnectionDoneChan: connDone,
		}, 0, header, ochainsync.Tip{
			Point:       ocommon.NewPoint(400, header.Hash().Bytes()),
			BlockNumber: 10,
		})
	}()
	require.NoError(t, testutil.RequireReceive(t, result, 5*time.Second,
		"an idle deferred header must not wait for a publication"))
	require.EqualValues(t, 1, attempts.Load())
	recovery := testutil.RequireReceive(t, f.resyncCh, testutil.AsyncWait,
		"an idle peer must re-intersect to recover discarded predecessors")
	require.Equal(t, event.ChainsyncResyncReasonHeaderValidationRecovery,
		recovery.Data.(event.ChainsyncResyncEvent).Reason)
	for _, ch := range []<-chan event.Event{ledgerCh, tipCh, recycleCh} {
		testutil.RequireNoReceive(
			t,
			ch,
			100*time.Millisecond,
			"unverified ancestry must neither admit the header nor penalize its peer",
		)
	}
}

func TestChainsyncDeferredPublicationWaitPreservesPendingWork(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"queued-header", "unapplied-block", "unknown-tips", "transient-idle"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newForecastHorizonFixture(t, "10.0.0.50:3001")
			f.o.ledgerState = newTestLedgerState(t)
			applied := gledger.Blake2b256{0xa0}.Bytes()
			pending := gledger.Blake2b256{0xb0}.Bytes()
			setTestLedgerTip(t, f.o, ochainsync.Tip{
				Point: ocommon.NewPoint(100, applied), BlockNumber: 1,
			})
			f.o.chainsyncLocalChainTips = func() ([]byte, []byte) {
				switch name {
				case "queued-header":
					return applied, pending
				case "unapplied-block":
					return pending, pending
				case "transient-idle":
					return applied, applied
				default:
					return nil, nil
				}
			}
			deferred := deferredVerificationErr(t)
			if name == "transient-idle" {
				deferred = transientContextErr(t)
			}
			settle := newSettleSignal()
			f.o.config.ChainsyncHeaderVerificationSettled = settle.current
			f.o.catchUpBandWaitTimeout = time.Minute
			attempted := make(chan struct{}, 1)
			var attempts atomic.Int32
			f.o.chainSelectionVerifyHeaderCrypto = func(
				ouroboros.ConnectionId, gledger.BlockHeader,
			) (uint64, error) {
				if attempts.Add(1) == 1 {
					attempted <- struct{}{}
					return 0, deferred
				}
				return 42, nil
			}
			connDone := make(chan any)
			finished := make(chan struct{})
			t.Cleanup(func() {
				close(connDone)
				testutil.RequireReceive(t, finished, testutil.AsyncWait,
					"the verification waiter must exit on connection close")
			})
			type verdict struct {
				generation uint64
				err        error
			}
			result := make(chan verdict, 1)
			go func() {
				defer close(finished)
				var baseline ochainsync.Tip
				var wait deferredVerificationWait
				defer wait.close()
				generation, err := f.o.verifyHeaderAwaitingLedger(
					ochainsync.CallbackContext{
						ConnectionId: f.conn, ConnectionDoneChan: connDone,
					},
					newTestBlockHeader(400, 10, 0xb9),
					&baseline,
					&wait,
				)
				result <- verdict{generation, err}
			}()
			testutil.RequireReceive(t, attempted, testutil.AsyncWait,
				"verification must reach the deferral before testing the wait")
			testutil.RequireNoReceive(t, result, 100*time.Millisecond,
				"pending work or a transient publication must retain the wait")
			settle.publish()
			got := testutil.RequireReceive(t, result, testutil.AsyncWait,
				"the next publication must retry verification")
			require.NoError(t, got.err)
			require.EqualValues(t, 42, got.generation)
			require.EqualValues(t, 2, attempts.Load())
		})
	}
}

// deferredVerificationErr returns a real deferred verification result by
// verifying a header against a ledger with no published state for its slot.
func deferredVerificationErr(t *testing.T) error {
	t.Helper()
	err := newTestLedgerState(t).ValidateChainSelectionHeaderCrypto(
		nonByronTestHeader{&testBlockHeader{slotNumber: 200}},
	)
	require.True(t, ledger.IsHeaderVerificationDeferred(err))
	return err
}

// settleSignal models the ledger's settle notification: every call hands out
// the channel the next publish closes.
type settleSignal struct {
	mu sync.Mutex
	ch chan struct{}
}

func newSettleSignal() *settleSignal {
	return &settleSignal{ch: make(chan struct{})}
}

func (s *settleSignal) current() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ch
}

func (s *settleSignal) publish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.ch)
	s.ch = make(chan struct{})
}

// A header deferred because local state was not ready is verified again after
// the ledger's next publication and admitted on the same connection, without
// parking it for a re-intersection.
func TestChainsyncClientRollForwardWaitsOutDeferredVerification(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.40:3001")
	deferredErr := deferredVerificationErr(t)
	settle := newSettleSignal()
	f.o.config.ChainsyncHeaderVerificationSettled = settle.current
	var attempts atomic.Int32
	attempted := make(chan struct{}, 4)
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		n := attempts.Add(1)
		attempted <- struct{}{}
		if n == 1 {
			return 0, deferredErr
		}
		return 42, nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.rollForward(t, newTestBlockHeader(400, 1, 0xb1))
	}()
	testutil.RequireReceive(t, attempted, testutil.AsyncWait, "first attempt")
	// The callback is parked on the settle signal: no second attempt yet.
	testutil.RequireNoReceive(
		t, attempted, 100*time.Millisecond,
		"a deferred header must wait for the ledger to settle",
	)
	settle.publish()
	testutil.RequireReceive(t, done, testutil.AsyncWait, "callback returns")

	require.EqualValues(t, 2, attempts.Load())
	testutil.RequireReceive(
		t, f.tipCh, testutil.AsyncWait,
		"a header verified after the wait must be observed",
	)
	testutil.RequireReceive(
		t, f.ledgerCh, testutil.AsyncWait,
		"a header verified after the wait must reach the ledger",
	)
	testutil.RequireNoReceive(
		t, f.recycle, 100*time.Millisecond,
		"a deferred header is not a peer fault",
	)
}

// A header that stays deferred past the bound falls back to parking it for
// recovery, with no peer penalty.
func TestChainsyncClientRollForwardParksHeaderDeferredPastWaitBound(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.41:3001")
	deferredErr := deferredVerificationErr(t)
	f.o.ledgerState = newTestLedgerState(t)
	f.o.catchUpBandWaitTimeout = 50 * time.Millisecond
	f.o.config.ChainsyncHeaderVerificationSettled = newSettleSignal().current
	var attempts atomic.Int32
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		attempts.Add(1)
		return 0, deferredErr
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xb2))

	require.EqualValues(t, 1, attempts.Load(),
		"no settle signal arrived, so the header is not verified again")
	f.o.deferredHeaderRecoveryMu.Lock()
	_, parked := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.True(t, parked, "an expired wait must park the header for recovery")
	testutil.RequireNoReceive(
		t, f.ledgerCh, 100*time.Millisecond,
		"a still-deferred header must not reach the ledger",
	)
	testutil.RequireNoReceive(
		t, f.recycle, 100*time.Millisecond,
		"a deferred header is not a peer fault",
	)
}

// A header that fails verification outright is rejected at once: the wait
// applies only to deferred results.
func TestChainsyncClientRollForwardRejectsInvalidHeaderWithoutWaiting(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.42:3001")
	settle := newSettleSignal()
	f.o.config.ChainsyncHeaderVerificationSettled = settle.current
	var attempts atomic.Int32
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		attempts.Add(1)
		return 0, errors.New("invalid VRF proof")
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xb3))

	require.EqualValues(t, 1, attempts.Load())
	testutil.RequireReceive(
		t, f.recycle, testutil.AsyncWait,
		"an invalid header must recycle the connection",
	)
	testutil.RequireNoReceive(
		t, f.ledgerCh, 100*time.Millisecond,
		"an invalid header must not reach the ledger",
	)
}

// A deferred header that extends the local chain is forwarded unverified at
// once, as before: its deferral may clear only by applying its own block, so
// holding it for a settle signal would stall the node for the whole bound.
func TestChainsyncClientRollForwardForwardsDeferredLocalExtensionWithoutWaiting(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.43:3001")
	deferredErr := deferredVerificationErr(t)
	f.o.catchUpBandWaitTimeout = time.Minute
	f.o.config.ChainsyncHeaderVerificationSettled = newSettleSignal().current
	header := newTestBlockHeader(400, 1, 0xb4)
	header.(*testBlockHeader).prevHash[0] = 0xb0
	parent := header.PrevHash().Bytes()
	f.o.chainsyncLocalChainTips = func() ([]byte, []byte) {
		return parent, parent
	}
	var attempts atomic.Int32
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		attempts.Add(1)
		return 0, deferredErr
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.rollForward(t, header)
	}()
	testutil.RequireReceive(
		t, done, 5*time.Second,
		"a deferred local extension must not wait for a settle signal",
	)
	require.EqualValues(t, 1, attempts.Load())
	testutil.RequireReceive(
		t, f.ledgerCh, testutil.AsyncWait,
		"a deferred local extension must reach the ledger queue",
	)
}

// The wait budget covers the whole roll-forward: a header re-admitted past
// the forecast horizon continues the waits it already spent rather than
// starting a second budget.
func TestChainsyncClientRollForwardSharesDeferredWaitBudgetAcrossReadmission(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.44:3001")
	deferredErr := deferredVerificationErr(t)
	settle := newSettleSignal()
	f.o.config.ChainsyncHeaderVerificationSettled = settle.current
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		return true, nil
	}
	var attempts atomic.Int32
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		n := attempts.Add(1)
		// Every attempt lands a publication, so no wait blocks and only the
		// wait count ends the retries.
		settle.publish()
		if n == 2 {
			return 0, beyondForecastHorizonErr()
		}
		return 0, deferredErr
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xb5))

	require.EqualValues(t, maxDeferredVerificationWaits+2, attempts.Load(),
		"one wait before re-admission leaves the rest of the budget after it")
}

// A roll-forward that waited out a deferral and then returns from re-admission
// releases its wait, so no goroutine watching the connection outlives the
// callback. Not t.Parallel: goleak needs no concurrent tests starting
// goroutines.
func TestChainsyncClientRollForwardReleasesDeferredWaitOnReadmissionReturn(
	t *testing.T,
) {
	f := newForecastHorizonFixture(t, "10.0.0.45:3001")
	deferredErr := deferredVerificationErr(t)
	settle := newSettleSignal()
	f.o.config.ChainsyncHeaderVerificationSettled = settle.current
	// The first admission precedes verification; the second, after the
	// header falls past the horizon, refuses it and ends the callback.
	var admissions atomic.Int32
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		return admissions.Add(1) == 1, nil
	}
	var attempts atomic.Int32
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		if attempts.Add(1) == 1 {
			settle.publish()
			return 0, deferredErr
		}
		return 0, beyondForecastHorizonErr()
	}
	connDone := make(chan any)
	t.Cleanup(func() { close(connDone) })
	ignore := goleak.IgnoreCurrent()

	header := newTestBlockHeader(400, 1, 0xb6)
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{
			ConnectionId:       f.conn,
			ConnectionDoneChan: connDone,
		},
		0,
		header,
		ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
	))

	require.EqualValues(t, 2, attempts.Load())
	require.EqualValues(t, 2, admissions.Load())
	goleak.VerifyNone(t, ignore)
}

func TestChainsyncDeferredVerificationEndsOnRealTransportClose(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(ocommon.Point) bool { return true }
	settle := newSettleSignal()
	f.o.config.ChainsyncHeaderVerificationSettled = settle.current
	f.o.catchUpBandWaitTimeout = time.Minute
	deferred := transientContextErr(t)
	var cleanupRelease atomic.Bool
	entered := make(chan struct{}, 1)
	returned := make(chan error, 1)
	f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		if cleanupRelease.Load() {
			return 42, nil
		}
		return 0, deferred
	}
	_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
	t.Cleanup(f.o.stopFutureHeaderResyncs)
	blocks, err := fixtures.GenerateBabbageChainAt(10, 400, 1)
	require.NoError(t, err)
	block := blocks[0]
	require.NotEmpty(t, block.Cbor())
	tip := ochainsync.Tip{Point: ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()), BlockNumber: block.BlockNumber()}
	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() { _ = serverPipe.Close(); _ = clientPipe.Close() })
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	serverConfig := ochainsync.NewConfig(
		ochainsync.WithFindIntersectFunc(func(ochainsync.CallbackContext, []ocommon.Point) (ocommon.Point, ochainsync.Tip, error) {
			return ocommon.NewPointOrigin(), tip, nil
		}),
		ochainsync.WithRequestNextFunc(func(cb ochainsync.CallbackContext) error {
			return cb.Server.RollForward(gledger.BlockTypeBabbage, block.Cbor(), tip)
		}),
	)
	type serverResult struct {
		conn *ouroboros.Connection
		err  error
	}
	serverReady := make(chan serverResult, 1)
	go func() {
		c, err := ouroboros.New(ouroboros.WithConnection(serverPipe), ouroboros.WithServer(true), ouroboros.WithNodeToNode(true), ouroboros.WithNetworkMagic(42), ouroboros.WithDelayProtocolStart(true), ouroboros.WithLogger(logger), ouroboros.WithChainSyncConfig(serverConfig))
		serverReady <- serverResult{c, err}
	}()
	clientConfig := ochainsync.NewConfig(ochainsync.WithPipelineLimit(1), ochainsync.WithRollForwardFunc(func(cb ochainsync.CallbackContext, typ uint, v any, tip ochainsync.Tip) error {
		err := f.o.chainsyncClientRollForward(cb, typ, v, tip)
		returned <- err
		return err
	}))
	client, err := ouroboros.New(ouroboros.WithConnection(clientPipe), ouroboros.WithNodeToNode(true), ouroboros.WithNetworkMagic(42), ouroboros.WithDelayProtocolStart(true), ouroboros.WithLogger(logger), ouroboros.WithChainSyncConfig(clientConfig))
	require.NoError(t, err)
	server := testutil.RequireReceive(t, serverReady, 5*time.Second, "server handshake")
	require.NoError(t, server.err)
	t.Cleanup(func() { cleanupRelease.Store(true); settle.publish(); _ = client.Close(); _ = server.conn.Close() })
	require.True(t, f.state.AddClientConnId(client.Id()))
	f.state.SetClientConnId(client.Id())
	server.conn.ChainSync().Server.Start()
	client.ChainSync().Client.Start()
	require.NoError(t, client.ChainSync().Client.Sync(nil))
	select {
	case <-entered:
	case err := <-client.ErrorChan():
		t.Fatalf("client failed before deferred verification: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("wire-delivered header never entered deferred verification")
	}
	testutil.RequireNoReceive(t, returned, 100*time.Millisecond, "callback must really be parked before disconnect")
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	testutil.RequireReceive(t, returned, 5*time.Second, "connection shutdown must release verification before protocol completion")
	require.NoError(t, testutil.RequireReceive(t, closed, 5*time.Second, "connection close must join the protocol callback"))
	testutil.RequireNoReceive(t, ledgerCh, 100*time.Millisecond, "a disconnected unverified header must not reach the ledger")
}
