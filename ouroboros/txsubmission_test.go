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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// txsubmissionRelayTestTxHex is a real, decodable Conway-era transaction.
// The server-init relay loop parses relayed bodies with
// ledger.NewTransactionFromCbor before admitting them to the mempool, so
// end-to-end relay tests need genuine CBOR rather than the placeholder
// bodies used by the callback-level tests above.
const txsubmissionRelayTestTxHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

const txsubmissionRelayTestEraId = 6 // Conway

const txsubmissionRelayTestNetworkMagic = 42

type txsubmissionTestValidator struct{}

func (txsubmissionTestValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (txsubmissionTestValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[string]struct{},
	map[string]lcommon.Utxo,
) error {
	return nil
}

// txsubmissionRejectingValidator rejects every transaction, so that
// AddTransaction fails the way it would for a real mempool policy
// violation (e.g. an invalid or already-spent UTxO), letting tests observe
// how txsubmissionServerInit handles a mempool error during relay.
type txsubmissionRejectingValidator struct{}

func (txsubmissionRejectingValidator) ValidateTx(gledger.Transaction) error {
	return errors.New("txsubmissionRejectingValidator: rejected")
}

func (txsubmissionRejectingValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[string]struct{},
	map[string]lcommon.Utxo,
) error {
	return errors.New("txsubmissionRejectingValidator: rejected")
}

// TestTxSubmissionClientRequestTxIds verifies empty, partial, and capped
// TxId responses when a peer asks what transactions this node can relay.
func TestTxSubmissionClientRequestTxIds(t *testing.T) {
	tests := []struct {
		name      string
		hashes    []string
		req       uint16
		wantCount int
	}{
		{
			name:      "empty response",
			req:       10,
			wantCount: 0,
		},
		{
			name: "partial response",
			hashes: []string{
				txsubmissionTestHash(1),
				txsubmissionTestHash(2),
			},
			req:       10,
			wantCount: 2,
		},
		{
			name: "full response",
			hashes: []string{
				txsubmissionTestHash(1),
				txsubmissionTestHash(2),
				txsubmissionTestHash(3),
			},
			req:       2,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange a peer consumer with the test's available tx set.
			o, connId := newTxSubmissionTestOuroboros(t)
			o.Mempool.AddConsumer(connId)
			appendTxSubmissionTestTxs(t, o.Mempool, tt.hashes...)

			// Ask the handler for at most the peer-requested number of TxIds.
			ids, err := o.txsubmissionClientRequestTxIds(
				txsubmission.CallbackContext{ConnectionId: connId},
				false,
				0,
				tt.req,
			)

			// Verify the response count and metadata match the offered txs.
			require.NoError(t, err)
			require.Len(t, ids, tt.wantCount)
			for idx, id := range ids {
				require.Equal(t, uint16(6), id.TxId.EraId)
				require.Equal(
					t,
					uint32(len(txsubmissionTestBody(tt.hashes[idx]))),
					id.Size,
				)
				require.Equal(t, tt.hashes[idx], hex.EncodeToString(id.TxId.TxId[:]))
			}
		})
	}
}

// TestTxSubmissionClientRequestTxIdsClearsConsumerCacheOnAck verifies that
// peer acknowledgements discard previously advertised transaction bodies.
func TestTxSubmissionClientRequestTxIdsClearsConsumerCacheOnAck(t *testing.T) {
	// Arrange one cached transaction for a peer consumer.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	hash := txsubmissionTestHash(1)
	appendTxSubmissionTestTxs(t, o.Mempool, hash)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// First advertise the transaction so it is stored in the consumer cache.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	// Send an ack and zero request count to clear the advertised cache.
	ids, err = o.txsubmissionClientRequestTxIds(ctx, false, 1, 0)
	require.NoError(t, err)
	require.Empty(t, ids)

	// Verify the acknowledged transaction body can no longer be served.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		mustTxSubmissionTestTxId(t, hash),
	})
	require.NoError(t, err)
	require.Empty(t, bodies)
}

// TestTxSubmissionClientRequestTxs verifies that known cached TxIds return
// bodies while unknown or already-served TxIds are ignored.
func TestTxSubmissionClientRequestTxs(t *testing.T) {
	// Arrange one known tx and one unknown tx id for the peer request.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	knownHash := txsubmissionTestHash(1)
	unknownHash := txsubmissionTestHash(99)
	appendTxSubmissionTestTxs(t, o.Mempool, knownHash)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// Advertise the known tx first so RequestTxs can find it in cache.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	// Request both unknown and known ids; only the cached known tx is returned.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		mustTxSubmissionTestTxId(t, unknownHash),
		ids[0].TxId,
	})
	require.NoError(t, err)
	require.Equal(t, []txsubmission.TxBody{
		{
			EraId:  6,
			TxBody: txsubmissionTestBody(knownHash),
		},
	}, bodies)

	// Request the known id again to prove served txs are removed from cache.
	bodies, err = o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		ids[0].TxId,
	})
	require.NoError(t, err)
	require.Empty(t, bodies)
}

// TestTxSubmissionClientRequestCallbacksMissingConsumer verifies that both
// client callbacks fail cleanly when no mempool consumer exists.
func TestTxSubmissionClientRequestCallbacksMissingConsumer(t *testing.T) {
	// Arrange a connection id without registering a mempool consumer.
	o, connId := newTxSubmissionTestOuroboros(t)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// RequestTxIds should fail cleanly instead of dereferencing nil state.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.ErrorContains(t, err, "no mempool consumer")
	require.Nil(t, ids)

	// RequestTxs should report the same missing-consumer error.
	bodies, err := o.txsubmissionClientRequestTxs(ctx, []txsubmission.TxId{
		mustTxSubmissionTestTxId(t, txsubmissionTestHash(1)),
	})
	require.ErrorContains(t, err, "no mempool consumer")
	require.Nil(t, bodies)
}

// TestTxSubmissionClientRequestTxIdsMalformedCachedTxHash verifies malformed
// cached transaction hashes return errors instead of panicking.
func TestTxSubmissionClientRequestTxIdsMalformedCachedTxHash(t *testing.T) {
	// Arrange a cached tx with a hash that is valid hex but the wrong length.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	appendTxSubmissionTestTx(t, o.Mempool, &mempool.MempoolTransaction{
		Hash:     strings.Repeat("0", 62),
		Cbor:     []byte("bad-hash-body"),
		Type:     6,
		LastSeen: time.Now(),
	})

	// Verify the malformed hash is returned as an error, not a panic.
	require.NotPanics(t, func() {
		ids, err := o.txsubmissionClientRequestTxIds(
			txsubmission.CallbackContext{ConnectionId: connId},
			false,
			0,
			1,
		)
		require.ErrorContains(t, err, "unexpected tx hash length")
		require.Nil(t, ids)
	})
}

// TestTxSubmissionClientRequestTxIdsZeroRequestDoesNotAdvance verifies a
// zero-count peer request leaves the consumer positioned on the next tx.
func TestTxSubmissionClientRequestTxIdsZeroRequestDoesNotAdvance(t *testing.T) {
	// Arrange one available tx for the peer consumer.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	hash := txsubmissionTestHash(1)
	appendTxSubmissionTestTxs(t, o.Mempool, hash)
	ctx := txsubmission.CallbackContext{ConnectionId: connId}

	// A zero-count request should return nothing.
	ids, err := o.txsubmissionClientRequestTxIds(ctx, false, 0, 0)
	require.NoError(t, err)
	require.Empty(t, ids)

	// A later nonzero request should still see the same first tx.
	ids, err = o.txsubmissionClientRequestTxIds(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, hash, hex.EncodeToString(ids[0].TxId.TxId[:]))
}

// TestTxSubmissionServerInitMissingConnectionReturnsCleanly verifies server
// init exits without error when the connection is already gone.
func TestTxSubmissionServerInitMissingConnectionReturnsCleanly(t *testing.T) {
	// Arrange an Ouroboros instance whose connection manager has no such peer.
	o, connId := newTxSubmissionTestOuroboros(t)

	// Start server init and let its background loop observe the missing peer.
	err := o.txsubmissionServerInit(
		txsubmission.CallbackContext{ConnectionId: connId},
	)

	// Missing connection during init should be treated as a clean exit.
	require.NoError(t, err)
}

// TestTxSubmissionConnectionClosedCleanup verifies connection close handling
// removes txsubmission consumer and rate-limiter state for that peer.
func TestTxSubmissionConnectionClosedCleanup(t *testing.T) {
	// Arrange per-peer mempool and rate-limiter state.
	o, connId := newTxSubmissionTestOuroboros(t)
	o.Mempool.AddConsumer(connId)
	o.txSubmissionRateLimiter = newTxSubmissionRateLimiter(1, 1)
	require.False(t, o.txSubmissionRateLimiter.Allow(connId, 2))

	// Deliver the same connection-close event used by normal node wiring.
	o.HandleConnClosedEvent(event.Event{
		Type: connmanager.ConnectionClosedEventType,
		Data: connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
		},
	})

	// Verify both txsubmission state holders have forgotten the peer.
	require.Nil(t, o.Mempool.Consumer(connId))
	require.True(t, o.txSubmissionRateLimiter.Allow(connId, 1))
}

// newTxSubmissionTestOuroboros builds a lightweight Ouroboros/mempool pair
// for exercising the client-side TxSubmission callbacks directly. Optional
// mutateConfig funcs can override the default MempoolConfig, e.g. to set a
// short TTL for testing expiry behavior.
func newTxSubmissionTestOuroboros(
	t *testing.T,
	mutateConfig ...func(*mempool.MempoolConfig),
) (*Ouroboros, ouroboros.ConnectionId) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       txsubmissionTestValidator{},
		MempoolCapacity: 1024 * 1024,
	}
	for _, mutate := range mutateConfig {
		mutate(&cfg)
	}
	m, err := mempool.NewMempool(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.Stop(t.Context()))
	})

	o := NewOuroboros(OuroborosConfig{Logger: logger})
	o.ConnManager = connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{
		Logger: logger,
	})
	o.Mempool = m
	return o, txsubmissionTestConnId(t)
}

func txsubmissionTestConnId(t *testing.T) ouroboros.ConnectionId {
	t.Helper()
	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3002")
	require.NoError(t, err)
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

func appendTxSubmissionTestTxs(
	t *testing.T,
	m *mempool.Mempool,
	hashes ...string,
) {
	t.Helper()
	for _, hash := range hashes {
		appendTxSubmissionTestTx(t, m, &mempool.MempoolTransaction{
			Hash:     hash,
			Cbor:     txsubmissionTestBody(hash),
			Type:     6,
			LastSeen: time.Now(),
		})
	}
}

func appendTxSubmissionTestTx(
	t *testing.T,
	m *mempool.Mempool,
	tx *mempool.MempoolTransaction,
) {
	t.Helper()
	m.Lock()
	defer m.Unlock()
	value := reflect.ValueOf(m).Elem().FieldByName("transactions")
	transactions := reflect.NewAt(
		value.Type(),
		unsafe.Pointer(value.UnsafeAddr()),
	).Elem()
	transactions.Set(reflect.Append(transactions, reflect.ValueOf(tx)))
}

func txsubmissionTestHash(idx int) string {
	return fmt.Sprintf("%064x", idx)
}

func txsubmissionTestBody(hash string) []byte {
	return []byte("tx-body-" + hash)
}

func mustTxSubmissionTestTxId(t *testing.T, hash string) txsubmission.TxId {
	t.Helper()
	bytes, err := hex.DecodeString(hash)
	require.NoError(t, err)
	require.Len(t, bytes, 32)
	var txId [32]byte
	copy(txId[:], bytes)
	return txsubmission.TxId{
		EraId: 6,
		TxId:  txId,
	}
}

// txSubmissionRelayHarness wires two real Ouroboros nodes together over a
// net.Pipe with the full NtN handshake and TxSubmission mini-protocol, so
// txsubmissionServerInit's background goroutine runs for real: node A's
// TxSubmission server pulls TxIds/Txs from node B's TxSubmission client and
// decodes/admits them into node A's own mempool. This exercises the relay
// loop itself, which the callback-level tests above cannot reach since
// ctx.Server is a concrete network-backed type.
type txSubmissionRelayHarness struct {
	nodeA *Ouroboros
	nodeB *Ouroboros
	connA *ouroboros.Connection
	connB *ouroboros.Connection
	cmA   *connmanager.ConnectionManager
	cmB   *connmanager.ConnectionManager
	mA    *mempool.Mempool
	mB    *mempool.Mempool
}

// newTxSubmissionRelayHarness intentionally does not register any
// t.Cleanup teardown: callers must close the harness themselves so tests
// that compare goroutine counts around the harness's lifetime observe a
// deterministic teardown point rather than one deferred until after the
// test function returns.
func newTxSubmissionRelayHarness(t *testing.T) *txSubmissionRelayHarness {
	return newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{})
}

// txSubmissionRelayHarnessOpts overrides the harness's defaults. Every
// field is optional; the zero value reproduces newTxSubmissionRelayHarness's
// original behavior (a shared discard logger and permissive validators on
// both nodes).
type txSubmissionRelayHarnessOpts struct {
	logger     *slog.Logger
	validatorA mempool.TxValidator
	validatorB mempool.TxValidator
}

func newTxSubmissionRelayHarnessWithOpts(
	t *testing.T,
	opts txSubmissionRelayHarnessOpts,
) *txSubmissionRelayHarness {
	t.Helper()
	logger := opts.logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	validatorA := opts.validatorA
	if validatorA == nil {
		validatorA = txsubmissionTestValidator{}
	}
	validatorB := opts.validatorB
	if validatorB == nil {
		validatorB = txsubmissionTestValidator{}
	}

	mA, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validatorA,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	mB, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validatorB,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)

	cmA := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	cmB := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)

	nodeA := NewOuroboros(OuroborosConfig{ConnManager: cmA, Logger: logger})
	nodeA.Mempool = mA
	nodeB := NewOuroboros(OuroborosConfig{ConnManager: cmB, Logger: logger})
	nodeB.Mempool = mB

	serverPipe, clientPipe := net.Pipe()

	connACh := make(chan *ouroboros.Connection, 1)
	errACh := make(chan error, 1)
	go func() {
		conn, err := ouroboros.New(
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(txsubmissionRelayTestNetworkMagic),
			ouroboros.WithNodeToNode(true),
			ouroboros.WithFullDuplex(true),
			ouroboros.WithLogger(logger),
			ouroboros.WithTxSubmissionConfig(
				txsubmission.NewConfig(
					slices.Concat(
						nodeA.txsubmissionClientConnOpts(),
						nodeA.txsubmissionServerConnOpts(),
					)...,
				),
			),
		)
		if err != nil {
			errACh <- err
			return
		}
		connACh <- conn
	}()

	connB, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNetworkMagic(txsubmissionRelayTestNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithFullDuplex(true),
		ouroboros.WithLogger(logger),
		ouroboros.WithTxSubmissionConfig(
			txsubmission.NewConfig(
				slices.Concat(
					nodeB.txsubmissionClientConnOpts(),
					nodeB.txsubmissionServerConnOpts(),
				)...,
			),
		),
	)
	require.NoError(t, err)

	var connA *ouroboros.Connection
	select {
	case err := <-errACh:
		t.Fatalf("node A connection setup failed: %s", err)
	case connA = <-connACh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for node A connection setup")
	}

	require.True(
		t,
		cmA.AddConnection(connA, false, connA.Id().RemoteAddr.String()),
	)
	require.True(
		t,
		cmB.AddConnection(connB, true, connB.Id().RemoteAddr.String()),
	)

	return &txSubmissionRelayHarness{
		nodeA: nodeA,
		nodeB: nodeB,
		connA: connA,
		connB: connB,
		cmA:   cmA,
		cmB:   cmB,
		mA:    mA,
		mB:    mB,
	}
}

// close tears down both connections and their owning nodes synchronously,
// so callers can reliably observe goroutine counts settling afterward.
func (h *txSubmissionRelayHarness) close(t *testing.T) {
	t.Helper()
	_ = h.connA.Close()
	_ = h.connB.Close()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.cmA.Stop(stopCtx)
	_ = h.cmB.Stop(stopCtx)
	_ = h.mA.Stop(context.Background())
	_ = h.mB.Stop(context.Background())
}

// TestTxSubmissionServerInitRelaysMempoolTransactionEndToEnd drives the real
// txsubmissionServerInit goroutine over an actual TxSubmission session: node
// B offers a real transaction from its mempool, node A's server pulls the
// TxIds then the TxBody, decodes the CBOR, and admits it to its own
// mempool. This is the happy-path relay loop that the direct callback tests
// cannot reach.
func TestTxSubmissionServerInitRelaysMempoolTransactionEndToEnd(t *testing.T) {
	h := newTxSubmissionRelayHarness(t)
	defer h.close(t)

	txBytes, err := hex.DecodeString(txsubmissionRelayTestTxHex)
	require.NoError(t, err)
	require.NoError(t, h.mB.AddTransaction(txsubmissionRelayTestEraId, txBytes))
	wantTx, err := gledger.NewTransactionFromCbor(
		txsubmissionRelayTestEraId,
		txBytes,
	)
	require.NoError(t, err)

	// Mirrors txsubmissionClientStart's role in the real outbound-connection
	// flow: register a mempool consumer for the peer and tell it to start
	// asking us for our mempool contents, which triggers node A's Init
	// callback (txsubmissionServerInit) on the other end of the wire.
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			return len(h.mA.Transactions()) == 1
		},
		5*time.Second,
		10*time.Millisecond,
		"expected node B's transaction to be relayed into node A's mempool",
	)

	relayed := h.mA.Transactions()[0]
	require.Equal(t, wantTx.Hash().String(), relayed.Hash)
	require.Equal(t, txBytes, relayed.Cbor)
}

// TestTxSubmissionServerInitExitsCleanlyOnPeerDisconnect verifies the
// server-init relay goroutine does not leak when the peer connection closes
// while it is parked in a blocking RequestTxIds call. The mempool is seeded
// with exactly one transaction so the loop completes one real round trip
// (proving the goroutine actually reached the blocking call again) before
// the connection is torn down.
//
// Goroutine counts are compared against a baseline captured before the
// harness is built, rather than using goleak, since goleak inspects the
// whole process and would also trip on unrelated pre-existing leaks
// elsewhere in this package's test suite.
func TestTxSubmissionServerInitExitsCleanlyOnPeerDisconnect(t *testing.T) {
	baseline := runtime.NumGoroutine()

	h := newTxSubmissionRelayHarness(t)

	txBytes, err := hex.DecodeString(txsubmissionRelayTestTxHex)
	require.NoError(t, err)
	require.NoError(t, h.mB.AddTransaction(txsubmissionRelayTestEraId, txBytes))

	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			return len(h.mA.Transactions()) == 1
		},
		5*time.Second,
		10*time.Millisecond,
		"expected node B's transaction to be relayed before disconnect",
	)

	// Node B's mempool is now empty, so node A's relay goroutine is parked
	// in a blocking RequestTxIds call awaiting the next offer. Closing here
	// must unblock and exit that goroutine, along with every other
	// goroutine the harness spawned, rather than leaking any of them.
	h.close(t)

	require.Eventually(
		t,
		func() bool {
			return runtime.NumGoroutine() <= baseline+2
		},
		5*time.Second,
		20*time.Millisecond,
		"expected relay and connection goroutines to exit after peer disconnect",
	)
}

// TestTxSubmissionClientRequestTxsExpiredTransactionNotServed verifies that
// a transaction the mempool's own TTL has already expired -- before this
// peer ever advertised it via RequestTxIds -- is handled the same as an
// unknown TxId: an empty reply, not an error. The consumer cache only ever
// learns about a transaction when it is advertised, so a TxId that expired
// from the mempool beforehand must fall straight through to "not found"
// rather than erroring or panicking.
func TestTxSubmissionClientRequestTxsExpiredTransactionNotServed(t *testing.T) {
	o, connId := newTxSubmissionTestOuroboros(t, func(cfg *mempool.MempoolConfig) {
		cfg.TransactionTTL = 10 * time.Millisecond
		cfg.CleanupInterval = 10 * time.Millisecond
	})
	o.Mempool.AddConsumer(connId)

	txBytes, err := hex.DecodeString(txsubmissionRelayTestTxHex)
	require.NoError(t, err)
	require.NoError(t, o.Mempool.AddTransaction(txsubmissionRelayTestEraId, txBytes))
	wantTx, err := gledger.NewTransactionFromCbor(
		txsubmissionRelayTestEraId,
		txBytes,
	)
	require.NoError(t, err)

	// Wait for the mempool's own TTL sweep to remove the transaction. It is
	// never requested via RequestTxIds first, so the consumer cache never
	// learns about it either -- exactly the "expired before offer" case.
	require.Eventually(
		t,
		func() bool {
			return len(o.Mempool.Transactions()) == 0
		},
		5*time.Second,
		10*time.Millisecond,
		"expected transaction to expire from the mempool",
	)

	bodies, err := o.txsubmissionClientRequestTxs(
		txsubmission.CallbackContext{ConnectionId: connId},
		[]txsubmission.TxId{
			mustTxSubmissionTestTxId(t, wantTx.Hash().String()),
		},
	)
	require.NoError(t, err)
	require.Empty(t, bodies)
}

// TestTxSubmissionServerInitMempoolRejectionLogsAndStopsCleanly verifies
// that a mempool error while admitting a relayed transaction -- e.g. a
// validator rejection -- is logged and the relay goroutine returns cleanly
// instead of panicking or wedging the connection. Node A's mempool rejects
// every transaction so a real, well-formed relay reaches
// Mempool.AddTransaction's error path inside txsubmissionServerInit.
func TestTxSubmissionServerInitMempoolRejectionLogsAndStopsCleanly(t *testing.T) {
	logBuf := &lockedBuffer{}
	logger := slog.New(
		slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)

	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		logger:     logger,
		validatorA: txsubmissionRejectingValidator{},
	})
	defer h.close(t)

	txBytes, err := hex.DecodeString(txsubmissionRelayTestTxHex)
	require.NoError(t, err)
	require.NoError(t, h.mB.AddTransaction(txsubmissionRelayTestEraId, txBytes))

	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			return strings.Contains(logBuf.String(), "failed to add tx")
		},
		5*time.Second,
		10*time.Millisecond,
		"expected the mempool rejection to be logged",
	)

	require.Empty(
		t,
		h.mA.Transactions(),
		"rejected transaction must not be admitted to the mempool",
	)
}
