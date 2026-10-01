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

// gRPC reflection tests: both reflection wire versions must advertise every
// service this listener serves, in both API versions. The v1alpha reflection
// service is an older reflection wire protocol, not an older API surface, so a
// v1alpha client must still discover the v1beta services.
// Connect RPC integration tests: in-memory preview ledger + httptest/h2c server,
// using the same generated Connect clients as production callers.
package utxorpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
	betacardano "github.com/utxorpc/go-codegen/utxorpc/v1beta/cardano"
	betaquery "github.com/utxorpc/go-codegen/utxorpc/v1beta/query"
	betaqueryconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/query/queryconnect"
	betasubmit "github.com/utxorpc/go-codegen/utxorpc/v1beta/submit"
	betasubmitconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/submit/submitconnect"
	betasync "github.com/utxorpc/go-codegen/utxorpc/v1beta/sync"
	betasyncconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/sync/syncconnect"
	betawatchconnect "github.com/utxorpc/go-codegen/utxorpc/v1beta/watch/watchconnect"
	"golang.org/x/net/http2"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/encoding/protowire"
)

// followTipTimestampLedger stubs the SlotToTime/GetBlock/Tip boundary that
// followTipResponse calls. Everything else panics if reached, so a call that
// slips past a returned error is visible immediately.
type followTipTimestampLedger struct {
	UtxorpcLedgerState

	tip ochainsync.Tip

	block    models.Block
	blockErr error

	// slotToTimeErrOnSlot fails SlotToTime for exactly this slot; every
	// other slot succeeds with a fixed time built from the slot number.
	slotToTimeErrOnSlot uint64
	slotToTimeErr       error
}

func (l *followTipTimestampLedger) Tip() ochainsync.Tip {
	return l.tip
}

func (l *followTipTimestampLedger) GetBlock(
	ocommon.Point,
) (models.Block, error) {
	return l.block, l.blockErr
}

func (l *followTipTimestampLedger) SlotToTime(
	slot uint64,
) (time.Time, error) {
	if slot == l.slotToTimeErrOnSlot {
		return time.Time{}, l.slotToTimeErr
	}
	return time.UnixMilli(int64(slot) * 1000), nil
}

func newFollowTipTimestampServer(
	ls UtxorpcLedgerState,
) *syncServiceServer {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		LedgerState: ls,
	})
	return &syncServiceServer{utxorpc: u}
}

// TestFollowTipResponse_RollbackSlotToTimeErrorPropagates: a SlotToTime
// failure for a non-origin rollback point must make FollowTip return an
// error, not a Reset action carrying Timestamp 0. Before the fix, the error
// from SlotToTime was discarded (`err == nil` guard) and timestamp stayed at
// its zero value.
func TestFollowTipResponse_RollbackSlotToTimeErrorPropagates(t *testing.T) {
	t.Parallel()
	rollbackPoint := ocommon.NewPoint(100, []byte{0x01, 0x02, 0x03})
	wantErr := errors.New("slot to time stub failure")
	stub := &followTipTimestampLedger{
		block:               models.Block{Number: 42},
		slotToTimeErrOnSlot: rollbackPoint.Slot,
		slotToTimeErr:       wantErr,
	}
	srv := newFollowTipTimestampServer(stub)

	resp, err := srv.followTipResponse(&chain.ChainIteratorResult{
		Point:    rollbackPoint,
		Rollback: true,
	})

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resp, "no response should be built when SlotToTime fails")
}

// TestFollowTipResponse_RollbackSucceedsWithTimestamp is the positive
// counterpart: a successful SlotToTime call still produces a non-zero
// timestamp on the Reset action.
func TestFollowTipResponse_RollbackSucceedsWithTimestamp(t *testing.T) {
	t.Parallel()
	rollbackPoint := ocommon.NewPoint(100, []byte{0x01, 0x02, 0x03})
	stub := &followTipTimestampLedger{
		block: models.Block{Number: 42},
		tip:   ochainsync.Tip{Point: ocommon.NewPoint(200, []byte{0xaa})},
	}
	srv := newFollowTipTimestampServer(stub)

	resp, err := srv.followTipResponse(&chain.ChainIteratorResult{
		Point:    rollbackPoint,
		Rollback: true,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	reset, ok := resp.Action.(*sync.FollowTipResponse_Reset_)
	require.True(t, ok)
	require.Equal(t, uint64(42), reset.Reset_.Height)
	require.Equal(
		t,
		uint64(100_000),
		reset.Reset_.Timestamp,
		"timestamp should reflect the stubbed slot time",
	)
}

// TestFollowTipResponse_TipSlotToTimeErrorPropagates covers the second
// SlotToTime call site: the current-tip timestamp populated on every
// response, reset or apply. Using a rollback-to-origin result reaches this
// call without needing a GetBlock stub or a decodable block CBOR fixture.
func TestFollowTipResponse_TipSlotToTimeErrorPropagates(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("tip slot to time stub failure")
	tipPoint := ocommon.NewPoint(555, []byte{0xbe, 0xef})
	stub := &followTipTimestampLedger{
		tip:                 ochainsync.Tip{Point: tipPoint, BlockNumber: 9},
		slotToTimeErrOnSlot: tipPoint.Slot,
		slotToTimeErr:       wantErr,
	}
	srv := newFollowTipTimestampServer(stub)

	// Rollback to origin: slot 0, empty hash. This skips the rollback-block
	// lookup and its own SlotToTime call, so only the tip's SlotToTime call
	// is exercised.
	resp, err := srv.followTipResponse(&chain.ChainIteratorResult{
		Point:    ocommon.NewPoint(0, nil),
		Rollback: true,
	})

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resp)
}

// slotToTimeFailingLedger is a real ledger whose SlotToTime always fails, so
// FollowTip runs its real chain iterator and only the conversion fails.
type slotToTimeFailingLedger struct {
	*ledger.LedgerState

	err error
}

func (l slotToTimeFailingLedger) SlotToTime(uint64) (time.Time, error) {
	return time.Time{}, l.err
}

// TestConnect_FollowTip_SlotToTimeErrorEndsStream drives the FollowTip handler
// end to end: a SlotToTime failure must reach the client as a stream error,
// not as an Apply frame whose Tip carries Timestamp 0.
func TestConnect_FollowTip_SlotToTimeErrorEndsStream(t *testing.T) {
	t.Parallel()
	const n = 8
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: n})
	blocks := loadTestChainBlocks(t, n)
	require.Len(t, blocks, n)
	inter := blocks[5]

	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: h.EB,
		LedgerState: slotToTimeFailingLedger{
			LedgerState: h.LS,
			err:         errors.New("slot to time stub failure"),
		},
		Mempool: h.MP,
	})
	srv := httptest.NewUnstartedServer(testUtxorpcHTTPHandler(u))
	srv.Config.Protocols = unencryptedHTTP2Protocols()
	srv.Start()
	t.Cleanup(srv.Close)

	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		srv.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	stream, err := cli.FollowTip(ctx, connect.NewRequest(&sync.FollowTipRequest{
		Intersect: []*sync.BlockRef{
			{
				Slot:   inter.Slot,
				Hash:   append([]byte(nil), inter.Hash...),
				Height: inter.Number,
			},
		},
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	require.False(
		t,
		stream.Receive(),
		"expected no frame when SlotToTime fails, got %T",
		stream.Msg().GetAction(),
	)
	require.ErrorContains(t, stream.Err(), "slot to time stub failure")
}

func startOnFreePort(
	t *testing.T,
	ctx context.Context,
	tlsCfg apiconfig.EffectiveTLS,
	opts ...func(*UtxorpcConfig),
) (*Utxorpc, string) {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		addr := testutil.FreePort(t)
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatal(err)
		}
		portNum, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			t.Fatal(err)
		}
		bus := event.NewEventBus(nil, nil)
		t.Cleanup(bus.Close)
		cfg := UtxorpcConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			Host:     host,
			Port:     uint(portNum),
			TLS:      tlsCfg,
		}
		for _, opt := range opts {
			opt(&cfg)
		}
		u := NewUtxorpc(cfg)
		attemptCtx, cancel := context.WithCancel(ctx)
		lastErr = u.Start(attemptCtx)
		if lastErr == nil {
			t.Cleanup(cancel)
			return u, addr
		}
		cancel()
	}
	t.Fatalf("could not start on a free loopback port: %v", lastErr)
	return nil, ""
}

func stopUtxorpc(t *testing.T, u *Utxorpc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := u.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

// The shutdown protocol this test exercises is covered in depth, with the
// windows constructed rather than raced, in internal/apilistener. What is
// checked here is that this package is wired to it -- that a utxorpc server
// keeps the promise its Stop makes, including on the force-close escalation
// paths that are this listener's own.

// TestServerRebindsAfterStop is the production path this fix exists for: a
// live database restore or truncate quiesces the API capabilities and
// reinitializeAPIServers brings them back up on the same configured port (see
// node_lifecycle.go). A Stop that returned while the socket was still bound
// left that restart failing with EADDRINUSE. The constructed tests in
// internal/apilistener assert closure on the original listener object; dialing
// a released ephemeral address here could instead reach another package's
// listener when the suite runs concurrently.
func TestServerRebindsAfterStop(t *testing.T) {
	u, addr := startOnFreePort(
		t, t.Context(),
		apiconfig.EffectiveTLS{},
	)
	stopUtxorpc(t, u)

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	portNum, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	restarted := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
		Host:     host,
		Port:     uint(portNum),
	})
	require.NoError(
		t, restarted.Start(t.Context()),
		"a capability restart must rebind the port Stop released",
	)
	stopUtxorpc(t, restarted)
}

// TestStartIsRefusedWhileAnotherStartHoldsTheGate pins the start gate this
// package is wired to. Nothing else in the package observes it -- the
// already-published rejection comes from Publish -- so without this test the
// BeginStart/EndStart pair can be removed from Start with the suite green.
func TestStartIsRefusedWhileAnotherStartHoldsTheGate(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: bus,
		Host:     "127.0.0.1",
	})
	u.config.Port = 0

	held, err := u.listener.BeginStart()
	require.NoError(t, err)

	err = u.Start(t.Context())
	require.ErrorContains(
		t, err, "start already in progress",
		"Start must take the listener's start gate before publishing",
	)
	require.Nil(
		t, u.listener.Server(),
		"a refused Start must not publish a server",
	)

	u.listener.EndStart(held)
	require.NoError(
		t, u.Start(t.Context()),
		"the gate must be available again once the holder releases it",
	)
	stopUtxorpc(t, u)
}

// TestServerShutdownOnContextCancel asserts cancelling the context passed to
// Start releases the port, which is how the node stops this API during its own
// shutdown. TestUtxorpc_StopObservesCtxCancellation covers the Stop context,
// not this one, so without this test the listener.Watch call can be removed
// from Start with the suite green.
func TestServerShutdownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	_, addr := startOnFreePort(t, ctx, apiconfig.EffectiveTLS{})

	cancel()

	testutil.WaitForCondition(
		t,
		func() bool { return !portAccepts(addr) },
		5*time.Second,
		"listener still accepting after context cancel",
	)
}

// portAccepts reports whether a TCP connection to addr succeeds.
func portAccepts(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// newReflectionTestServer serves the production routing table over h2c.
func newReflectionTestServer(t *testing.T) (*httptest.Server, *http.Client) {
	t.Helper()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
	})
	srv := httptest.NewUnstartedServer(u.newServeMux())
	srv.Config.Protocols = unencryptedHTTP2Protocols()
	srv.Start()
	httpClient := newConnectH2CClient()
	t.Cleanup(func() {
		// Shut down under a deadline rather than calling Close directly:
		// httptest.Server.Close waits without one, so a held-open HTTP/2
		// connection could hang the test instead of failing it. Dropping the
		// client's keep-alive connection first lets the drain finish promptly.
		httpClient.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, srv.Config.Shutdown(ctx))
		srv.Close()
	})
	return srv, httpClient
}

// listServices drives one ServerReflectionInfo bidi stream and returns the
// advertised service names. Req/Res are the reflection message types for the
// wire version under test.
func listServices[Req, Res any](
	t *testing.T,
	httpClient *http.Client,
	baseURL, procedure string,
	newRequest func() *Req,
	names func(*Res) []string,
) []string {
	t.Helper()
	client := connect.NewClient[Req, Res](
		httpClient,
		baseURL+procedure,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream := client.CallBidiStream(ctx)
	require.NoError(t, stream.Send(newRequest()))
	require.NoError(t, stream.CloseRequest())
	res, err := stream.Receive()
	require.NoError(t, err)
	require.NoError(t, stream.CloseResponse())
	return names(res)
}

// TestReflection_V1ListsBothApiVersions asserts the grpc.reflection.v1
// ServerReflection service advertises every served service.
func TestReflection_V1ListsBothApiVersions(t *testing.T) {
	srv, httpClient := newReflectionTestServer(t)
	got := listServices(
		t,
		httpClient,
		srv.URL,
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
		func() *reflectionv1.ServerReflectionRequest {
			return &reflectionv1.ServerReflectionRequest{
				MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{},
			}
		},
		func(res *reflectionv1.ServerReflectionResponse) []string {
			out := []string{}
			for _, svc := range res.GetListServicesResponse().GetService() {
				out = append(out, svc.GetName())
			}
			return out
		},
	)
	assert.ElementsMatch(t, servedServiceNames(), got)
}

// TestReflection_V1AlphaListsBothApiVersions is the regression test for the
// v1alpha reflector being registered with only the v1alpha service names: a
// client speaking the older reflection wire protocol could not discover any
// v1beta service.
func TestReflection_V1AlphaListsBothApiVersions(t *testing.T) {
	srv, httpClient := newReflectionTestServer(t)
	got := listServices(
		t,
		httpClient,
		srv.URL,
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
		func() *reflectionv1alpha.ServerReflectionRequest {
			return &reflectionv1alpha.ServerReflectionRequest{
				MessageRequest: &reflectionv1alpha.ServerReflectionRequest_ListServices{},
			}
		},
		func(res *reflectionv1alpha.ServerReflectionResponse) []string {
			out := []string{}
			for _, svc := range res.GetListServicesResponse().GetService() {
				out = append(out, svc.GetName())
			}
			return out
		},
	)
	assert.ElementsMatch(t, servedServiceNames(), got)
	// Spell out the beta half of the contract so a future edit that drops the
	// beta names from the reflector fails here with a clear reason.
	for _, want := range []string{
		"utxorpc.v1beta.query.QueryService",
		"utxorpc.v1beta.submit.SubmitService",
		"utxorpc.v1beta.sync.SyncService",
		"utxorpc.v1beta.watch.WatchService",
	} {
		assert.Containsf(t, got, want,
			"v1alpha reflection must advertise %s", want)
	}
}

// TestServedServiceNames_CoversBothApiVersions pins the served set itself: both
// API versions of all four services, and nothing else.
func TestServedServiceNames_CoversBothApiVersions(t *testing.T) {
	assert.ElementsMatch(t, []string{
		"utxorpc.v1alpha.query.QueryService",
		"utxorpc.v1beta.query.QueryService",
		"utxorpc.v1alpha.submit.SubmitService",
		"utxorpc.v1beta.submit.SubmitService",
		"utxorpc.v1alpha.sync.SyncService",
		"utxorpc.v1beta.sync.SyncService",
		"utxorpc.v1alpha.watch.WatchService",
		"utxorpc.v1beta.watch.WatchService",
	}, servedServiceNames())
}

func gzipRequestBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return compressed.Bytes()
}

func sendHealthRequest(
	t *testing.T,
	handler http.Handler,
	body []byte,
	compressed bool,
) *httptest.ResponseRecorder {
	t.Helper()
	contentEncoding := ""
	if compressed {
		body = gzipRequestBody(t, body)
		contentEncoding = "gzip"
	}
	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://example.test/grpc.health.v1.Health/Check",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func TestConnectRequestBodyLimitPreservesValidMessages(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{})
	handler := u.newServeMux()

	for _, compressed := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "uncompressed", true: "compressed"}[compressed],
			func(t *testing.T) {
				resp := sendHealthRequest(
					t,
					handler,
					[]byte("{}"),
					compressed,
				)
				require.Equal(t, http.StatusOK, resp.Code)
			},
		)
	}
}

func TestConnectRequestBodyLimitRejectsOversizedCompressedMessage(
	t *testing.T,
) {
	u := NewUtxorpc(UtxorpcConfig{})
	handler := u.newServeMux()

	// Keep the wire body small while making the decoded protobuf message exceed
	// the limit. The Connect handler must bound the buffered compressed and
	// decompressed bytes before the request reaches an interceptor or service
	// method.
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(
		body,
		bytes.Repeat([]byte{'a'}, DefaultMaxRequestBody),
	)
	compressed := gzipRequestBody(t, body)
	require.Less(t, len(compressed), DefaultMaxRequestBody)

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://example.test/grpc.health.v1.Health/Check",
		bytes.NewReader(compressed),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Connect-Protocol-Version", "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	require.Equal(t, http.StatusTooManyRequests, response.Code)
}

func TestConnectRequestBodyLimitRejectsOversizedCompressedWireBody(
	t *testing.T,
) {
	u := NewUtxorpc(UtxorpcConfig{})
	handler := u.newServeMux()

	// Connect bounds the raw request bytes before it decompresses them, which
	// is a separate limit from the decompressed-size check above. Random
	// payload bytes do not compress, so gzip expands the message: the decoded
	// body stays within DefaultMaxRequestBody while the compressed wire body
	// exceeds it. Only the pre-decompression limit can reject this request.
	// The tag and length prefix for a bytes field this size occupy five bytes.
	payload := make([]byte, DefaultMaxRequestBody-5)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(body, payload)
	compressed := gzipRequestBody(t, body)
	require.LessOrEqual(t, len(body), DefaultMaxRequestBody)
	require.Greater(t, len(compressed), DefaultMaxRequestBody)

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://example.test/grpc.health.v1.Health/Check",
		bytes.NewReader(compressed),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Connect-Protocol-Version", "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	require.Equal(t, http.StatusTooManyRequests, response.Code)
}

// --- harness (preview fixture + h2c server) --------------------------------

type noopTxValidator struct{}

func (noopTxValidator) ValidateTx(gledger.Transaction) error { return nil }

func (noopTxValidator) ValidateTxWithOverlay(
	_ gledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
) error {
	return nil
}

// utxorpcConnectHarness wires preview genesis + immutable fixture blocks into an
// in-memory database, starts LedgerState (manual block processing), and serves
// the utxorpc Connect handlers over HTTP/2 cleartext (h2c) like production.
type utxorpcConnectHarness struct {
	EB     *event.EventBus
	DB     *database.Database
	LS     *ledger.LedgerState
	MP     *mempool.Mempool
	U      *Utxorpc
	Server *httptest.Server
	Client *http.Client
	// Tx hashes that were successfully indexed into metadata during harness setup.
	IndexedTxHashes [][]byte
}

type utxorpcHarnessOptions struct {
	numBlocks       int
	blocks          []models.Block
	maxHistoryItems int
	serverTimeout   time.Duration
	skipIndexTxHash []byte
}

func newConnectH2CClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
}

func testUtxorpcHTTPHandler(u *Utxorpc) http.Handler {
	mux := http.NewServeMux()
	compress1KB := connect.WithCompressMinBytes(1024)
	qp, qh := queryconnect.NewQueryServiceHandler(
		&queryServiceServer{utxorpc: u},
		compress1KB,
	)
	sp, sh := submitconnect.NewSubmitServiceHandler(
		&submitServiceServer{utxorpc: u},
		compress1KB,
	)
	yp, yh := syncconnect.NewSyncServiceHandler(
		&syncServiceServer{utxorpc: u},
		compress1KB,
	)
	wp, wh := watchconnect.NewWatchServiceHandler(
		&watchServiceServer{utxorpc: u},
		compress1KB,
	)
	mux.Handle(qp, qh)
	mux.Handle(sp, sh)
	mux.Handle(yp, yh)
	mux.Handle(wp, wh)
	// v1beta routes mirror production wiring in Start: the beta services reuse
	// the alpha handlers via path rewriting, and the query service additionally
	// serves the beta-only ReadState method.
	betaQueryPath := "/" + betaqueryconnect.QueryServiceName + "/"
	mux.Handle(
		betaQueryPath,
		betaVersionedQueryHandler(u, qp, qh, betaQueryPath, compress1KB),
	)
	betaSubmitPath := "/" + betasubmitconnect.SubmitServiceName + "/"
	mux.Handle(betaSubmitPath, rewriteVersionHandler(sh, betaSubmitPath, sp))
	betaSyncPath := "/" + betasyncconnect.SyncServiceName + "/"
	mux.Handle(betaSyncPath, rewriteVersionHandler(yh, betaSyncPath, yp))
	betaWatchPath := "/" + betawatchconnect.WatchServiceName + "/"
	mux.Handle(betaWatchPath, rewriteVersionHandler(wh, betaWatchPath, wp))
	return mux
}

func newUtxorpcConnectHarness(
	t *testing.T,
	opts utxorpcHarnessOptions,
) *utxorpcConnectHarness {
	t.Helper()
	if opts.numBlocks < 2 {
		opts.numBlocks = 2
	}

	nodeCfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preview/config.json",
	)
	require.NoError(t, err)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)

	blocks := opts.blocks
	if len(blocks) == 0 {
		blocks = loadTestChainBlocks(t, opts.numBlocks)
	}
	require.NotEmpty(t, blocks)
	for i := range blocks {
		require.NoError(t, db.BlockCreate(blocks[i], nil))
	}
	// These blocks are inserted directly (db.BlockCreate) rather than run
	// through LedgerState's normal block-application path, so no
	// block_nonce row exists for any of them -- unlike a really-synced
	// chain, which writes one for every applied block including a
	// per-epoch checkpoint. Without at least one checkpoint here,
	// ls.Start below hits healTruncateGapBlockNonces with an empty tip
	// nonce and nothing to reconstruct from -- correctly refused, but for
	// this harness gap rather than a genuine unreconstructable truncate.
	// The nonce value is a fixed placeholder, not folded from real VRF
	// output: these tests assert Connect RPC behavior, not nonce
	// correctness.
	require.NoError(t, db.SetBlockNonce(
		blocks[0].Hash,
		blocks[0].Slot,
		bytes.Repeat([]byte{0x5c}, 32),
		true, // isCheckpoint
		nil,
	))
	indexedTxHashes := indexFixtureTransactionsForReadTx(
		t,
		db,
		blocks,
		opts.skipIndexTxHash,
	)
	tip := blocks[len(blocks)-1]
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(tip.Slot, tip.Hash),
				BlockNumber: tip.Number,
			},
			nil,
		),
	)

	// API-only bus: LedgerState must not subscribe here, otherwise its
	// Blockfetch handler can run before WaitForTx and stall Publish delivery.
	apiBus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { apiBus.Stop() })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		EventBus:              nil,
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		CardanoNodeConfig:     nodeCfg,
		ManualBlockProcessing: true,
		DatabaseWorkerPoolConfig: ledger.DatabaseWorkerPoolConfig{
			Disabled: true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(ls))

	ledgerCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, ls.Start(ledgerCtx))
	t.Cleanup(func() {
		cancel()
		_ = ls.Close()
	})

	mp, err := mempool.NewMempool(mempool.MempoolConfig{
		Validator:       noopTxValidator{},
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        apiBus,
		MempoolCapacity: 1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mp.Stop(context.Background()) })

	maxHist := opts.maxHistoryItems
	if maxHist <= 0 {
		maxHist = DefaultMaxHistoryItems
	}
	u := NewUtxorpc(UtxorpcConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        apiBus,
		LedgerState:     ls,
		Mempool:         mp,
		MaxHistoryItems: maxHist,
		ServerTimeout:   opts.serverTimeout,
	})

	srv := httptest.NewUnstartedServer(testUtxorpcHTTPHandler(u))
	srv.Config.Protocols = unencryptedHTTP2Protocols()
	srv.Start()
	t.Cleanup(srv.Close)

	return &utxorpcConnectHarness{
		EB:              apiBus,
		DB:              db,
		LS:              ls,
		MP:              mp,
		U:               u,
		Server:          srv,
		Client:          newConnectH2CClient(),
		IndexedTxHashes: indexedTxHashes,
	}
}

func indexFixtureTransactionsForReadTx(
	t *testing.T,
	db *database.Database,
	blocks []models.Block,
	skipTxHash []byte,
) [][]byte {
	t.Helper()
	indexed := make([][]byte, 0, 64)
	for i := range blocks {
		mb := blocks[i]
		blk, err := gledger.NewBlockFromCbor(mb.Type, mb.Cbor)
		if err != nil {
			continue
		}
		txs := blk.Transactions()
		if len(txs) == 0 {
			continue
		}
		indexer := database.NewBlockIndexer(mb.Slot, mb.Hash)
		offsets, err := indexer.ComputeOffsets(mb.Cbor, blk)
		if err != nil {
			continue
		}
		point := ocommon.NewPoint(mb.Slot, mb.Hash)
		for j, tx := range txs {
			if bytes.Equal(tx.Hash().Bytes(), skipTxHash) {
				continue
			}
			err := db.SetTransaction(
				tx,
				point,
				uint32(j),
				0,
				nil,
				nil,
				offsets,
				nil,
			)
			if err != nil {
				continue
			}
			indexed = append(indexed, append([]byte(nil), tx.Hash().Bytes()...))
		}
	}
	return indexed
}

// --- fixture helpers --------------------------------------------------------

func firstTxInFixtureBlocks(
	t *testing.T,
	numBlocks int,
) ([]byte, []byte, models.Block) {
	t.Helper()
	blocks := loadTestChainBlocks(t, numBlocks)
	for _, mb := range blocks {
		blk, err := gledger.NewBlockFromCbor(mb.Type, mb.Cbor)
		require.NoError(t, err)
		txs := blk.Transactions()
		if len(txs) == 0 {
			continue
		}
		tx := txs[0]
		return tx.Hash().Bytes(), tx.Cbor(), mb
	}
	t.Fatal("no transaction found in fixture blocks")
	return nil, nil, models.Block{}
}

func firstTwoTxsInFixtureBlocks(
	t *testing.T,
	numBlocks int,
) (gledger.Transaction, models.Block, gledger.Transaction, models.Block) {
	t.Helper()
	blocks := loadTestChainBlocks(t, numBlocks)
	var firstTx gledger.Transaction
	var firstBlock models.Block
	for _, mb := range blocks {
		blk, err := gledger.NewBlockFromCbor(mb.Type, mb.Cbor)
		require.NoError(t, err)
		for _, tx := range blk.Transactions() {
			if firstTx == nil {
				firstTx = tx
				firstBlock = mb
				continue
			}
			if tx.Hash() != firstTx.Hash() {
				return firstTx, firstBlock, tx, mb
			}
		}
	}
	t.Fatal("fewer than two distinct transactions found in fixture blocks")
	return nil, models.Block{}, nil, models.Block{}
}

// --- tests ------------------------------------------------------------------

func TestConnect_ReadParams(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadParams(
		ctx,
		connect.NewRequest(&query.ReadParamsRequest{}),
	)
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetValues())
	require.NotNil(t, out.Msg.GetValues().GetCardano())
	require.NotNil(t, out.Msg.GetLedgerTip())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetLedgerTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetLedgerTip().GetHash())
	require.Equal(t, tip.BlockNumber, out.Msg.GetLedgerTip().GetHeight())
}

func TestConnect_ReadEraSummary(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadEraSummary(
		ctx,
		connect.NewRequest(&query.ReadEraSummaryRequest{}),
	)
	require.NoError(t, err)
	s := out.Msg.GetCardano()
	require.NotNil(t, s)
	require.NotEmpty(t, s.GetSummaries())
	require.NotEmpty(t, s.GetSummaries()[0].GetName())
	require.NotNil(t, s.GetSummaries()[0].GetStart())
}

func TestConnect_ReadGenesis(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadGenesis(
		ctx,
		connect.NewRequest(&query.ReadGenesisRequest{}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, out.Msg.GetCaip2())
	require.Equal(t, "cardano:preview", out.Msg.GetCaip2())
	require.NotNil(t, out.Msg.GetCardano())
}

func TestConnect_ReadTip(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 15})
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadTip(ctx, connect.NewRequest(&sync.ReadTipRequest{}))
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetTip())
	require.NotEmpty(t, out.Msg.GetTip().GetHash())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetTip().GetHash())
	require.Equal(t, tip.BlockNumber, out.Msg.GetTip().GetHeight())
}

func TestConnect_FetchBlock(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 12})
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tip := h.LS.Tip()
	out, err := cli.FetchBlock(
		ctx,
		connect.NewRequest(&sync.FetchBlockRequest{
			Ref: []*sync.BlockRef{
				{Slot: tip.Point.Slot, Hash: tip.Point.Hash},
			},
		}),
	)
	require.NoError(t, err)
	require.Len(t, out.Msg.GetBlock(), 1)
	require.NotEmpty(t, out.Msg.GetBlock()[0].GetNativeBytes())
	blk, err := h.LS.GetBlock(tip.Point)
	require.NoError(t, err)
	require.Equal(t, blk.Cbor, out.Msg.GetBlock()[0].GetNativeBytes())
}

func TestConnect_DumpHistory(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 25})
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, 25)
	require.NotEmpty(t, blocks)
	out, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			StartToken: &sync.BlockRef{
				Slot:   blocks[0].Slot,
				Hash:   blocks[0].Hash,
				Height: blocks[0].Number,
			},
			MaxItems: 3,
		}),
	)
	require.NoError(t, err)
	require.Len(t, out.Msg.GetBlock(), 3)
	// start_token is exclusive in the server implementation, so the first page
	// begins at the block after the token.
	require.Equal(t, blocks[1].Cbor, out.Msg.GetBlock()[0].GetNativeBytes())
	require.Equal(t, blocks[2].Cbor, out.Msg.GetBlock()[1].GetNativeBytes())
	require.Equal(t, blocks[3].Cbor, out.Msg.GetBlock()[2].GetNativeBytes())
	require.NotNil(
		t,
		out.Msg.GetNextToken(),
		"history longer than maxItems must expose next_token for pagination",
	)
	require.Equal(t, blocks[3].Slot, out.Msg.GetNextToken().GetSlot())
	require.Equal(t, blocks[3].Hash, out.Msg.GetNextToken().GetHash())
	require.Equal(t, blocks[3].Number, out.Msg.GetNextToken().GetHeight())
	out2, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			StartToken: out.Msg.GetNextToken(),
			MaxItems:   10,
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, out2.Msg.GetBlock())
	require.Equal(t, blocks[4].Cbor, out2.Msg.GetBlock()[0].GetNativeBytes())
}

func TestConnect_DumpHistory_StartTokenNotOnChain(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 10})
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			StartToken: &sync.BlockRef{
				Slot:   999_999,
				Hash:   []byte{0xde, 0xad, 0xbe, 0xef},
				Height: 999_999,
			},
			MaxItems: 5,
		}),
	)
	require.Error(t, err)
	connErr, ok := err.(*connect.Error)
	require.True(t, ok, "expected connect.Error, got %T", err)
	// Depending on where validation happens (iterator lookup vs handler-level),
	// this can surface as InvalidArgument or Unknown wrapping ErrBlockNotFound.
	require.Contains(
		t,
		[]connect.Code{connect.CodeInvalidArgument, connect.CodeUnknown},
		connErr.Code(),
	)
}

func TestConnect_DumpHistory_MaxItemsExceeded(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks:       10,
		maxHistoryItems: 50,
	})
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			MaxItems: 100,
		}),
	)
	require.Error(t, err)
	connErr, ok := err.(*connect.Error)
	require.True(t, ok, "expected connect.Error, got %T", err)
	require.Equal(t, connect.CodeInvalidArgument, connErr.Code())
	require.Contains(t, connErr.Message(), "maxItems 100 exceeds maximum of 50")
}

func TestConnect_SearchUtxos(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{
			MaxItems: 5,
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetLedgerTip())
	require.NotEmpty(t, out.Msg.GetItems(), "fixture should expose live UTxOs")
	require.LessOrEqual(t, len(out.Msg.GetItems()), 5)
	require.NotNil(t, out.Msg.GetItems()[0].GetTxoRef())
	require.NotEmpty(
		t,
		out.Msg.GetNextToken(),
		"pagination token expected for maxItems=5",
	)

	out2, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{
			MaxItems:   5,
			StartToken: out.Msg.GetNextToken(),
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, out2.Msg.GetItems())
	first1 := out.Msg.GetItems()[0].GetTxoRef()
	first2 := out2.Msg.GetItems()[0].GetTxoRef()
	require.False(
		t,
		bytes.Equal(first1.GetHash(), first2.GetHash()) &&
			first1.GetIndex() == first2.GetIndex(),
		"second page should advance past the first page",
	)
}

func TestConnect_ReadUtxos(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	searchOut, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		searchOut.Msg.GetItems(),
		"fixture should expose at least one live UTxO",
	)
	ref := searchOut.Msg.GetItems()[0].GetTxoRef()
	out, err := cli.ReadUtxos(
		ctx,
		connect.NewRequest(&query.ReadUtxosRequest{
			Keys: []*query.TxoRef{ref},
		}),
	)
	require.NoError(t, err)
	require.Len(t, out.Msg.GetItems(), 1)
	gotRef := out.Msg.GetItems()[0].GetTxoRef()
	require.NotNil(t, gotRef)
	require.Equal(t, ref.GetHash(), gotRef.GetHash())
	require.Equal(t, ref.GetIndex(), gotRef.GetIndex())
	require.NotNil(t, out.Msg.GetLedgerTip())
}

// TestConnect_ReadUtxos_MultipleKeys proves ReadUtxos resolves several keys
// in a single request via the batched UTxO lookup (#392), returning exactly
// one item per requested key.
func TestConnect_ReadUtxos_MultipleKeys(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	searchOut, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 5}),
	)
	require.NoError(t, err)
	require.GreaterOrEqual(
		t,
		len(searchOut.Msg.GetItems()),
		2,
		"fixture should expose at least two live UTxOs",
	)
	keys := make([]*query.TxoRef, 0, len(searchOut.Msg.GetItems()))
	wantNativeBytes := make([][]byte, 0, len(searchOut.Msg.GetItems()))
	for _, item := range searchOut.Msg.GetItems() {
		ref := item.GetTxoRef()
		require.NotNil(t, ref)
		nativeBytes := item.GetNativeBytes()
		require.NotNil(t, nativeBytes)
		keys = append(keys, ref)
		wantNativeBytes = append(wantNativeBytes, nativeBytes)
	}
	out, err := cli.ReadUtxos(
		ctx,
		connect.NewRequest(&query.ReadUtxosRequest{Keys: keys}),
	)
	require.NoError(t, err)
	require.Len(t, out.Msg.GetItems(), len(keys))
	for i, item := range out.Msg.GetItems() {
		gotRef := item.GetTxoRef()
		require.NotNil(t, gotRef)
		require.Equal(t, keys[i].GetHash(), gotRef.GetHash())
		require.Equal(t, keys[i].GetIndex(), gotRef.GetIndex())
		// ReadUtxos echoes the requested TxoRef regardless of which UTxO
		// it resolved, so also compare the resolved content itself
		// (NativeBytes) against what SearchUtxos independently found for
		// this same key: a mis-correlated batch lookup would still pass
		// the ref-only checks above but fail this one.
		require.NotEmpty(t, item.GetNativeBytes())
		require.Equal(
			t,
			wantNativeBytes[i],
			item.GetNativeBytes(),
			"ReadUtxos should return the same UTxO content SearchUtxos found for this key",
		)
	}
}

// TestConnect_ReadUtxos_MissingKey proves a request that includes a ref
// with no matching live UTxO still errors the whole call, preserving the
// pre-existing ReadUtxos error-on-miss contract (unlike the ledger-level
// GetUTxOByTxIn query, which silently omits misses for batch lookups).
func TestConnect_ReadUtxos_MissingKey(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	searchOut, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		searchOut.Msg.GetItems(),
		"fixture should expose at least one live UTxO",
	)
	ref := searchOut.Msg.GetItems()[0].GetTxoRef()
	bogusRef := &query.TxoRef{
		Hash:  bytes.Repeat([]byte{0}, 32),
		Index: 9999,
	}
	_, err = cli.ReadUtxos(
		ctx,
		connect.NewRequest(&query.ReadUtxosRequest{
			Keys: []*query.TxoRef{ref, bogusRef},
		}),
	)
	require.Error(t, err)
}

func TestConnect_ReadData_EmptyKeys(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadData(ctx, connect.NewRequest(&query.ReadDataRequest{}))
	require.NoError(t, err)
	require.Empty(t, out.Msg.GetValues())
	require.NotNil(t, out.Msg.GetLedgerTip())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetLedgerTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetLedgerTip().GetHash())
}

func TestConnect_ReadTx(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NotEmpty(
		t,
		h.IndexedTxHashes,
		"harness must index fixture transactions",
	)
	txHash := h.IndexedTxHashes[len(h.IndexedTxHashes)-1]
	out, err := cli.ReadTx(
		ctx,
		connect.NewRequest(&query.ReadTxRequest{Hash: txHash}),
	)
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetTx())
	require.NotNil(t, out.Msg.GetTx().GetCardano())
	require.NotEmpty(t, out.Msg.GetTx().GetNativeBytes())
	txType, err := gledger.DetermineTransactionType(
		out.Msg.GetTx().GetNativeBytes(),
	)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(
		txType,
		out.Msg.GetTx().GetNativeBytes(),
	)
	require.NoError(t, err)
	require.Equal(t, txHash, tx.Hash().Bytes())
	rec, err := h.LS.TransactionByHash(txHash)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.NotNil(t, out.Msg.GetTx().GetBlockRef())
	require.Equal(t, rec.BlockHash, out.Msg.GetTx().GetBlockRef().GetHash())
	require.Equal(t, rec.Slot, out.Msg.GetTx().GetBlockRef().GetSlot())
	require.NotNil(t, out.Msg.GetLedgerTip())
}

func TestConnect_SubmitTx(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	_, txCbor, _ := firstTxInFixtureBlocks(t, 40)
	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.SubmitTx(
		ctx,
		connect.NewRequest(&submit.SubmitTxRequest{
			Tx: &submit.AnyChainTx{
				Type: &submit.AnyChainTx_Raw{Raw: txCbor},
			},
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, out.Msg.GetRef())
	txType, err := gledger.DetermineTransactionType(txCbor)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(txType, txCbor)
	require.NoError(t, err)
	require.Equal(t, tx.Hash().Bytes(), out.Msg.GetRef())
	memTxs := h.MP.Transactions()
	found := false
	for _, mtx := range memTxs {
		if bytes.Equal(mtx.Cbor, txCbor) {
			found = true
			break
		}
	}
	require.True(t, found, "submitted tx must be present in mempool")
}

func TestConnect_ReadMempool(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadMempool(
		ctx,
		connect.NewRequest(&submit.ReadMempoolRequest{}),
	)
	require.NoError(t, err)
	require.Empty(t, out.Msg.GetItems())
}

func TestConnect_WaitForTx_EmptyRefsClosesStream(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := cli.WaitForTx(
		ctx,
		connect.NewRequest(&submit.WaitForTxRequest{}),
	)
	require.NoError(t, err)
	require.False(
		t,
		stream.Receive(),
		"no refs means the handler returns without frames",
	)
	require.NoError(t, stream.Err())
	cancel()
}

// Wait until WaitForTx is listening on the fake event bus before the test
// publishes events.
func waitForEventSubscriber(
	t *testing.T,
	ctx context.Context,
	eb *event.EventBus,
	eventType event.EventType,
) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if eb.HasSubscribers(eventType) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("event subscriber was not registered: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

type waitForTxResult struct {
	resp *submit.WaitForTxResponse
	err  error
}

type waitForTxStreamEvent struct {
	resp     *submit.WaitForTxResponse
	err      error
	terminal bool
}

// receiveWaitForTxStream exposes every response and the terminal stream result
// in wire order.
func receiveWaitForTxStream(
	ctx context.Context,
	cli submitconnect.SubmitServiceClient,
	req *submit.WaitForTxRequest,
) <-chan waitForTxStreamEvent {
	eventCh := make(chan waitForTxStreamEvent, len(req.GetRef())+1)
	go func() {
		defer close(eventCh)
		stream, err := cli.WaitForTx(ctx, connect.NewRequest(req))
		if err != nil {
			eventCh <- waitForTxStreamEvent{err: err, terminal: true}
			return
		}
		for stream.Receive() {
			eventCh <- waitForTxStreamEvent{resp: stream.Msg()}
		}
		eventCh <- waitForTxStreamEvent{
			err:      stream.Err(),
			terminal: true,
		}
	}()
	return eventCh
}

// Run WaitForTx in the background so the test can cancel the request while
// the stream is still open.
func receiveWaitForTx(
	ctx context.Context,
	cli submitconnect.SubmitServiceClient,
	req *submit.WaitForTxRequest,
) <-chan waitForTxResult {
	resultCh := make(chan waitForTxResult, 1)
	go func() {
		stream, err := cli.WaitForTx(ctx, connect.NewRequest(req))
		if err != nil {
			resultCh <- waitForTxResult{err: err}
			return
		}
		if !stream.Receive() {
			resultCh <- waitForTxResult{err: stream.Err()}
			return
		}
		resp := stream.Msg()
		if stream.Receive() {
			resultCh <- waitForTxResult{
				err: fmt.Errorf("unexpected extra WaitForTx frame"),
			}
			return
		}
		resultCh <- waitForTxResult{resp: resp, err: stream.Err()}
	}()
	return resultCh
}

// Test that WaitForTx times out when the transaction is never seen.
func TestConnect_WaitForTx_ServerTimeout(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks:     5,
		serverTimeout: 25 * time.Millisecond,
	})
	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultCh := receiveWaitForTx(
		ctx,
		cli,
		&submit.WaitForTxRequest{
			Ref: [][]byte{bytes.Repeat([]byte{0xaa}, 32)},
		},
	)
	select {
	case result := <-resultCh:
		require.Nil(t, result.resp)
		require.Equal(
			t,
			connect.CodeDeadlineExceeded,
			connect.CodeOf(result.err),
		)
		require.ErrorContains(t, result.err, "wait for tx timed out")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// Test that WaitForTx stops when the client cancels first.
func TestConnect_WaitForTx_ClientCancellation(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks:     5,
		serverTimeout: time.Hour,
	})
	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := receiveWaitForTx(
		ctx,
		cli,
		&submit.WaitForTxRequest{
			Ref: [][]byte{bytes.Repeat([]byte{0xbb}, 32)},
		},
	)
	waitCtx, waitCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer waitCancel()
	waitForEventSubscriber(t, waitCtx, h.EB, ledger.TransactionEventType)

	cancel()

	select {
	case result := <-resultCh:
		require.Nil(t, result.resp)
		require.Equal(t, connect.CodeCanceled, connect.CodeOf(result.err))
	case <-waitCtx.Done():
		t.Fatal(waitCtx.Err())
	}
}

func TestConnect_EvalTx(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	_, txCbor, _ := firstTxInFixtureBlocks(t, 40)
	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.EvalTx(
		ctx,
		connect.NewRequest(&submit.EvalTxRequest{
			Tx: &submit.AnyChainTx{
				Type: &submit.AnyChainTx_Raw{Raw: txCbor},
			},
		}),
	)
	require.NoError(t, err)
	report := out.Msg.GetReport().GetCardano()
	require.NotNil(t, report)
	if len(report.GetErrors()) > 0 {
		require.NotEmpty(t, report.GetErrors()[0].GetMsg())
	} else {
		require.NotNil(t, report.GetExUnits())
	}
}

func followTipStreamErr(
	t *testing.T,
	stream *connect.ServerStreamForClient[sync.FollowTipResponse],
) string {
	t.Helper()
	if err := stream.Err(); err != nil {
		return err.Error()
	}
	return "stream closed without error"
}

func TestConnect_FollowTip_RollbackEmitsReset(t *testing.T) {
	const n = 8
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: n})
	blocks := loadTestChainBlocks(t, n)
	require.Len(t, blocks, n)
	inter := blocks[5]
	roll := ocommon.NewPoint(inter.Slot, inter.Hash)
	require.NoError(t, h.LS.Chain().ValidateRollback(roll))

	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	stream, err := cli.FollowTip(ctx, connect.NewRequest(&sync.FollowTipRequest{
		Intersect: []*sync.BlockRef{
			{
				Slot:   inter.Slot,
				Hash:   append([]byte(nil), inter.Hash...),
				Height: inter.Number,
			},
		},
	}))
	require.NoError(t, err)

	for i := range 2 {
		require.True(
			t,
			stream.Receive(),
			"expected two Apply frames before rollback: %s",
			followTipStreamErr(t, stream),
		)
		apply, ok := stream.Msg().Action.(*sync.FollowTipResponse_Apply)
		require.True(t, ok, "expected Apply, got %T", stream.Msg().Action)
		require.NotNil(t, apply.Apply)
		require.NotEmpty(t, apply.Apply.GetNativeBytes())
		require.Equal(t, blocks[6+i].Cbor, apply.Apply.GetNativeBytes())
		require.NotNil(t, stream.Msg().GetTip())
	}

	require.NoError(t, h.LS.Chain().Rollback(roll))

	require.True(
		t,
		stream.Receive(),
		"expected Reset after chain rollback: %s",
		followTipStreamErr(t, stream),
	)
	reset, ok := stream.Msg().Action.(*sync.FollowTipResponse_Reset_)
	require.True(t, ok, "expected Reset action, got %T", stream.Msg().Action)
	require.Equal(t, roll.Slot, reset.Reset_.GetSlot())
	require.Equal(t, roll.Hash, reset.Reset_.GetHash())
	rollbackBlock, err := h.LS.GetBlock(roll)
	require.NoError(t, err)
	require.Equal(t, rollbackBlock.Number, reset.Reset_.GetHeight())
	rollbackTime, err := h.LS.SlotToTime(roll.Slot)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(rollbackTime.UnixMilli()),
		reset.Reset_.GetTimestamp(),
	)
	cancel()
}

func TestConnect_WatchTx_IdleEmptyForwardBlock(t *testing.T) {
	// Load a long prefix to locate an empty block, then trim the harness chain
	// so that empty block is the tip. Otherwise WatchTx keeps iterating forward
	// and may hit transactions that panic in gouroboros Utxorpc().
	scan := loadTestChainBlocksWithPeriodicTransactions(t, 80)
	var cut int
	found := false
	for j := 6; j < len(scan); j++ {
		blk, err := gledger.NewBlockFromCbor(scan[j].Type, scan[j].Cbor)
		require.NoError(t, err)
		if len(blk.Transactions()) != 0 {
			continue
		}
		cut = j + 1
		found = true
		break
	}
	require.True(t, found, "no suitable empty block in fixture scan")
	if cut < 2 {
		t.Fatalf("empty block cut must include a parent, got %d", cut)
	}
	blocks := scan[:cut]
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{blocks: blocks})
	parent := blocks[cut-2]
	emptyChild := blocks[cut-1]
	require.Equal(t, emptyChild.Slot, h.LS.Tip().Point.Slot)

	cli := watchconnect.NewWatchServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := cli.WatchTx(
		ctx,
		connect.NewRequest(&watch.WatchTxRequest{
			Intersect: []*watch.BlockRef{
				{
					Slot:   parent.Slot,
					Hash:   append([]byte(nil), parent.Hash...),
					Height: parent.Number,
				},
			},
		}),
	)
	require.NoError(t, err)
	if !stream.Receive() {
		t.Fatalf("WatchTx first frame: stream.Err()=%v", stream.Err())
	}
	idle, ok := stream.Msg().Action.(*watch.WatchTxResponse_Idle)
	require.True(t, ok, "expected Idle, got %T", stream.Msg().Action)
	require.NotNil(t, idle.Idle)
	require.Equal(t, emptyChild.Slot, idle.Idle.GetSlot())
	require.Equal(t, emptyChild.Number, idle.Idle.GetHeight())
	require.Equal(t, emptyChild.Hash, idle.Idle.GetHash())
	require.Equal(t, h.LS.Tip().Point.Hash, idle.Idle.GetHash())
	cancel()
}

func TestConnect_WaitForTx_ConfirmsOnlyCommittedApply(t *testing.T) {
	committedTx, _, pendingTx, eventBlock := firstTwoTxsInFixtureBlocks(t, 40)
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks:       40,
		skipIndexTxHash: pendingTx.Hash().Bytes(),
	})
	committedHash := committedTx.Hash().Bytes()
	committedRecord, err := h.LS.TransactionByHash(committedHash)
	require.NoError(t, err)
	require.NotNil(
		t,
		committedRecord,
		"committed fixture transaction must be indexed",
	)
	pendingRecord, err := h.LS.TransactionByHash(pendingTx.Hash().Bytes())
	require.NoError(t, err)
	require.Nil(
		t,
		pendingRecord,
		"pending fixture transaction must not be indexed",
	)
	blk, err := gledger.NewBlockFromCbor(eventBlock.Type, eventBlock.Cbor)
	require.NoError(t, err)
	requestedInBlock := false
	for _, tx := range blk.Transactions() {
		if tx.Hash() == pendingTx.Hash() {
			requestedInBlock = true
			break
		}
	}
	require.True(
		t,
		requestedInBlock,
		"raw blockfetch must contain the pending transaction",
	)

	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	eventCh := receiveWaitForTxStream(
		ctx,
		cli,
		&submit.WaitForTxRequest{
			Ref: [][]byte{
				append([]byte(nil), committedHash...),
				append([]byte(nil), pendingTx.Hash().Bytes()...),
			},
		},
	)
	waitCtx, waitCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer waitCancel()
	waitForEventSubscriber(t, waitCtx, h.EB, ledger.TransactionEventType)
	require.False(
		t,
		h.EB.HasSubscribers(ledger.BlockfetchEventType),
		"WaitForTx must not consume pre-validation blockfetch events",
	)
	committed := testutil.RequireReceive(
		t,
		eventCh,
		5*time.Second,
		"persisted WaitForTx stream event",
	)
	require.False(t, committed.terminal)
	require.NoError(t, committed.err)
	require.NotNil(t, committed.resp)
	require.Equal(t, submit.Stage_STAGE_CONFIRMED, committed.resp.GetStage())
	require.Equal(t, committedHash, committed.resp.GetRef())

	// Raw blockfetch precedes validation, while a rollback removes the
	// transaction from the active chain. Neither is a confirmation source.
	h.EB.Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(
			ledger.BlockfetchEventType,
			ledger.BlockfetchEvent{
				Block: blk,
				Point: ocommon.NewPoint(eventBlock.Slot, eventBlock.Hash),
				Type:  uint(eventBlock.Type),
			},
		),
	)
	h.EB.Publish(
		ledger.TransactionEventType,
		event.NewEvent(
			ledger.TransactionEventType,
			ledger.TransactionEvent{
				Transaction: pendingTx,
				Point: ocommon.NewPoint(
					eventBlock.Slot,
					eventBlock.Hash,
				),
				Rollback: true,
			},
		),
	)
	testutil.RequireNoReceive(
		t,
		eventCh,
		100*time.Millisecond,
		"raw blockfetch and rollback must not confirm a pending transaction",
	)

	// Ledger emits the forward transaction event only after the active-chain
	// database transaction commits.
	h.EB.Publish(
		ledger.TransactionEventType,
		event.NewEvent(
			ledger.TransactionEventType,
			ledger.TransactionEvent{
				Transaction: pendingTx,
				Point: ocommon.NewPoint(
					eventBlock.Slot,
					eventBlock.Hash,
				),
			},
		),
	)

	confirmed := testutil.RequireReceive(
		t,
		eventCh,
		5*time.Second,
		"post-commit WaitForTx stream event",
	)
	require.False(t, confirmed.terminal)
	require.NoError(t, confirmed.err)
	require.NotNil(t, confirmed.resp)
	require.Equal(t, submit.Stage_STAGE_CONFIRMED, confirmed.resp.GetStage())
	require.Equal(t, pendingTx.Hash().Bytes(), confirmed.resp.GetRef())

	terminal := testutil.RequireReceive(
		t,
		eventCh,
		5*time.Second,
		"terminal WaitForTx stream event",
	)
	require.True(t, terminal.terminal)
	require.NoError(t, terminal.err)
	_, open := <-eventCh
	require.False(t, open)
}

func TestConnect_WaitForTx_AlreadyCommittedTransaction(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks:     40,
		serverTimeout: time.Second,
	})
	require.NotEmpty(t, h.IndexedTxHashes)
	txHash := h.IndexedTxHashes[0]

	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := cli.WaitForTx(
		ctx,
		connect.NewRequest(&submit.WaitForTxRequest{
			Ref: [][]byte{
				append([]byte(nil), txHash...),
				append([]byte(nil), txHash...),
			},
		}),
	)
	require.NoError(t, err)
	require.True(
		t,
		stream.Receive(),
		"already-committed transaction should be confirmed: %v",
		stream.Err(),
	)
	resp := stream.Msg()
	require.NotNil(t, resp)
	require.Equal(t, submit.Stage_STAGE_CONFIRMED, resp.GetStage())
	require.Equal(t, txHash, resp.GetRef())
	require.False(
		t,
		stream.Receive(),
		"handler returns after confirming all refs",
	)
	require.NoError(t, stream.Err())
}

func TestConnect_WatchMempool_StreamsOnAddTransactionEvent(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	txHash, txCbor, _ := firstTxInFixtureBlocks(t, 40)
	txType, err := gledger.DetermineTransactionType(txCbor)
	require.NoError(t, err)

	cli := submitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	stopPublish := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPublish:
				return
			case <-ticker.C:
				h.EB.Publish(
					mempool.AddTransactionEventType,
					event.NewEvent(
						mempool.AddTransactionEventType,
						mempool.AddTransactionEvent{
							Hash: hex.EncodeToString(txHash),
							Type: txType,
							Body: append([]byte(nil), txCbor...),
						},
					),
				)
			}
		}
	}()
	defer close(stopPublish)

	stream, err := cli.WatchMempool(
		ctx,
		connect.NewRequest(&submit.WatchMempoolRequest{}),
	)
	require.NoError(t, err)
	if !stream.Receive() {
		require.NoError(t, stream.Err())
		t.Fatal("WatchMempool stream closed without event frame")
	}
	resp := stream.Msg()
	require.NotNil(t, resp.GetTx())
	require.Equal(t, submit.Stage_STAGE_MEMPOOL, resp.GetTx().GetStage())
	require.True(t, bytes.Equal(txCbor, resp.GetTx().GetNativeBytes()))
	outTxType, err := gledger.DetermineTransactionType(
		resp.GetTx().GetNativeBytes(),
	)
	require.NoError(t, err)
	outTx, err := gledger.NewTransactionFromCbor(
		outTxType,
		resp.GetTx().GetNativeBytes(),
	)
	require.NoError(t, err)
	require.Equal(t, txHash, outTx.Hash().Bytes())
	cancel()
}

// --- v1beta serving/routing tests ------------------------------------------

// TestConnect_Beta_ReadParams drives a real v1beta QueryService call through
// betaVersionedQueryHandler and asserts it is rewritten onto the shared v1alpha
// handler, returning the same ledger-backed response as the v1alpha service.
func TestConnect_Beta_ReadParams(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := betaqueryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadParams(
		ctx,
		connect.NewRequest(&betaquery.ReadParamsRequest{}),
	)
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetValues())
	require.NotNil(t, out.Msg.GetValues().GetCardano())
	require.NotNil(t, out.Msg.GetLedgerTip())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetLedgerTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetLedgerTip().GetHash())
	require.Equal(t, tip.BlockNumber, out.Msg.GetLedgerTip().GetHeight())
}

// seedBetaReadStatePool gives a pool a registration and snapshot stake in the
// harness database, so ReadState has a real distribution to report rather than
// an empty one.
func seedBetaReadStatePool(
	t *testing.T,
	h *utxorpcConnectHarness,
	poolKeyHash []byte,
	vrfKeyHash []byte,
	stake uint64,
	snapshotEpoch uint64,
) lcommon.PoolKeyHash {
	t.Helper()
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))
	require.NoError(t, h.DB.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: vrfKeyHash},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  vrfKeyHash,
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	require.NoError(t, h.DB.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        snapshotEpoch,
			SnapshotType: "mark",
			PoolKeyHash:  pkh.Bytes(),
			TotalStake:   dbtypes.Uint64(stake),
			CapturedSlot: 1,
		},
		nil,
	))
	return pkh
}

// TestConnect_Beta_ReadState_StakePoolDistribution drives the beta-only
// ReadState method over a real Connect/gRPC client. It covers the routing
// branch inside betaVersionedQueryHandler -- which must not be rewritten onto
// the alpha handler, since alpha has no such method -- and the answer it now
// serves, over the same ledger the node-to-client GetPoolDistr2 query reads.
func TestConnect_Beta_ReadState_StakePoolDistribution(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})

	// The harness chain sits in epoch 0, and leader election reads the
	// snapshot for the preceding epoch, which at epoch 0 is epoch 0.
	vrf := bytes.Repeat([]byte{0xA1}, 32)
	pkh := seedBetaReadStatePool(
		t, h, bytes.Repeat([]byte{0x5A}, 28), vrf, 3_000_000, 0,
	)
	seedBetaReadStatePool(
		t, h,
		bytes.Repeat([]byte{0x7B}, 28), bytes.Repeat([]byte{0xB2}, 32),
		1_000_000, 0,
	)

	cli := betaqueryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out, err := cli.ReadState(
		ctx,
		connect.NewRequest(&betaquery.ReadStateRequest{
			Query: &betaquery.AnyChainStateQuery{
				Query: &betaquery.AnyChainStateQuery_Cardano{
					Cardano: &betacardano.StateQuery{
						Query: &betacardano.StateQuery_StakePoolDistribution{
							StakePoolDistribution: &betacardano.GetStakePoolDistribution{},
						},
					},
				},
			},
		}),
	)
	require.NoError(t, err)

	pools := out.Msg.GetResult().GetCardano().
		GetStakePoolDistribution().GetPools()
	require.Len(t, pools, 2)
	// Ordered by pool key hash, so the reply is a function of the snapshot
	// rather than of map iteration order.
	require.Equal(t, pkh.Bytes(), pools[0].GetPoolKeyhash())
	require.Equal(t, vrf, pools[0].GetVrfKeyhash())
	require.Equal(t, int32(3), pools[0].GetStakeFraction().GetNumerator())
	require.Equal(t, uint32(4), pools[0].GetStakeFraction().GetDenominator())

	tip := h.LS.Tip()
	require.NotNil(t, out.Msg.GetLedgerTip())
	require.Equal(t, tip.Point.Slot, out.Msg.GetLedgerTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetLedgerTip().GetHash())
	require.Equal(t, tip.BlockNumber, out.Msg.GetLedgerTip().GetHeight())
}

// TestConnect_Beta_ReadState_PoolFilter covers the bounded request form over
// the wire, and the rejection of a filter entry that is not a pool key hash.
func TestConnect_Beta_ReadState_PoolFilter(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	pkh := seedBetaReadStatePool(
		t, h,
		bytes.Repeat([]byte{0x5A}, 28), bytes.Repeat([]byte{0xA1}, 32),
		3_000_000, 0,
	)
	seedBetaReadStatePool(
		t, h,
		bytes.Repeat([]byte{0x7B}, 28), bytes.Repeat([]byte{0xB2}, 32),
		1_000_000, 0,
	)

	cli := betaqueryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	readState := func(
		poolKeyHashes ...[]byte,
	) (*connect.Response[betaquery.ReadStateResponse], error) {
		return cli.ReadState(
			ctx,
			connect.NewRequest(&betaquery.ReadStateRequest{
				Query: &betaquery.AnyChainStateQuery{
					Query: &betaquery.AnyChainStateQuery_Cardano{
						Cardano: &betacardano.StateQuery{
							Query: &betacardano.StateQuery_StakePoolDistribution{
								StakePoolDistribution: &betacardano.GetStakePoolDistribution{
									PoolKeyhashes: poolKeyHashes,
								},
							},
						},
					},
				},
			}),
		)
	}

	out, err := readState(pkh.Bytes())
	require.NoError(t, err)
	pools := out.Msg.GetResult().GetCardano().
		GetStakePoolDistribution().GetPools()
	require.Len(t, pools, 1, "only the requested pool is reported")
	require.Equal(t, pkh.Bytes(), pools[0].GetPoolKeyhash())
	// The filter selects what is reported, not what it is a share of, so the
	// fraction is still this pool's share of the whole snapshot.
	require.Equal(t, int32(3), pools[0].GetStakeFraction().GetNumerator())
	require.Equal(t, uint32(4), pools[0].GetStakeFraction().GetDenominator())

	_, err = readState([]byte{0x01, 0x02})
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// TestConnect_Beta_ReadState_RejectsEmptyRequest covers a ReadState carrying no
// query. It still has to reach the beta handler rather than the alpha one,
// which has no ReadState method at all.
func TestConnect_Beta_ReadState_RejectsEmptyRequest(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := betaqueryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := cli.ReadState(
		ctx,
		connect.NewRequest(&betaquery.ReadStateRequest{}),
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// TestConnect_Beta_ReadTip drives a real v1beta SyncService call to confirm a
// non-query service is served through rewriteVersionHandler onto v1alpha.
func TestConnect_Beta_ReadTip(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 15})
	cli := betasyncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadTip(ctx, connect.NewRequest(&betasync.ReadTipRequest{}))
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetTip())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetTip().GetHash())
	require.Equal(t, tip.BlockNumber, out.Msg.GetTip().GetHeight())
}

// TestConnect_Beta_WaitForTx_EmptyRefsClosesStream exercises a versioned
// streaming handler served through rewriteVersionHandler: an empty-ref
// beta WaitForTx must open and cleanly close without frames, like v1alpha.
func TestConnect_Beta_WaitForTx_EmptyRefsClosesStream(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := betasubmitconnect.NewSubmitServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := cli.WaitForTx(
		ctx,
		connect.NewRequest(&betasubmit.WaitForTxRequest{}),
	)
	require.NoError(t, err)
	require.False(
		t,
		stream.Receive(),
		"no refs means the handler returns without frames",
	)
	require.NoError(t, stream.Err())
	cancel()
}

// tipHeightLedgerStub reports a tip whose height the ledger already knows and
// fails every block lookup. That is the shape of the inconsistency this
// contract has to survive: the tip itself is known, its stored block row is
// not readable.
//
// The interface is embedded rather than implemented in full so an unexpected
// call panics instead of silently returning a zero value.
type tipHeightLedgerStub struct {
	UtxorpcLedgerState

	tip ochainsync.Tip
	// blockLookups counts reads of the stored block. The tip carries its own
	// height, so building a chain point must not need one at all.
	blockLookups int
}

func (s *tipHeightLedgerStub) Tip() ochainsync.Tip {
	return s.tip
}

func (s *tipHeightLedgerStub) GetBlock(
	ocommon.Point,
) (models.Block, error) {
	s.blockLookups++
	return models.Block{}, errors.New("no such block")
}

func (s *tipHeightLedgerStub) BlockByHash(
	[]byte,
) (models.Block, error) {
	s.blockLookups++
	return models.Block{}, errors.New("no such block")
}

func newTipHeightServers(
	t *testing.T,
	ls UtxorpcLedgerState,
) (*syncServiceServer, *queryServiceServer) {
	t.Helper()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: ls,
	})
	return &syncServiceServer{utxorpc: u}, &queryServiceServer{utxorpc: u}
}

// TestReadTip_HeightComesFromTheLedgerTip covers ReadTip when the tip's block
// row cannot be read.
//
// height is a plain proto3 uint64, so a client cannot tell an unknown height
// from a real one: reporting 0 beside a non-origin slot and hash asserts that
// the tip is the origin block, which is a different claim from "unknown". The
// ledger tip already carries its block number, so the height never has to be
// re-derived from storage and this state cannot arise.
func TestReadTip_HeightComesFromTheLedgerTip(t *testing.T) {
	stub := &tipHeightLedgerStub{
		tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(4321, []byte{0xBE, 0xEF}),
			BlockNumber: 7,
		},
	}
	syncSrv, _ := newTipHeightServers(t, stub)

	out, err := syncSrv.ReadTip(
		context.Background(),
		connect.NewRequest(&sync.ReadTipRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(4321), tip.GetSlot())
	assert.Equal(t, []byte{0xBE, 0xEF}, tip.GetHash())
	assert.Equal(t, uint64(7), tip.GetHeight(),
		"the height must be the one the ledger tip reports")
	assert.Zero(t, stub.blockLookups,
		"the tip carries its height; no block read should be needed")
}

// TestReadTip_OriginTipStaysZero is the negative case. Height 0 is the correct
// answer at the origin, so the fix must not turn a genuine zero into an error
// or a fabricated value.
func TestReadTip_OriginTipStaysZero(t *testing.T) {
	stub := &tipHeightLedgerStub{tip: ochainsync.Tip{}}
	syncSrv, _ := newTipHeightServers(t, stub)

	out, err := syncSrv.ReadTip(
		context.Background(),
		connect.NewRequest(&sync.ReadTipRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetTip()
	require.NotNil(t, tip)
	assert.Zero(t, tip.GetSlot())
	assert.Zero(t, tip.GetHeight())
}

// TestReadParams_LedgerTipHeightComesFromTheLedgerTip covers the same contract
// on the query service, which reports the tip alongside the parameter set.
func TestReadParams_LedgerTipHeightComesFromTheLedgerTip(t *testing.T) {
	stub := &shelleyLedgerStub{
		byronLedgerStub: byronLedgerStub{
			tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(42, []byte{0xab, 0xcd}),
				BlockNumber: 9,
			},
		},
		pparams: testShelleyPParams(),
	}
	srv := newByronQueryServer(t, stub)

	out, err := srv.ReadParams(
		context.Background(),
		connect.NewRequest(&query.ReadParamsRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetLedgerTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(42), tip.GetSlot())
	assert.Equal(t, uint64(9), tip.GetHeight(),
		"the height must be the one the ledger tip reports")
}

// TestReadData_LedgerTipHeightComesFromTheLedgerTip covers ReadData, which
// never looked the height up at all: it built its chain point from the tip's
// point alone and left Height at the zero value, so every response claimed the
// tip was the origin block.
func TestReadData_LedgerTipHeightComesFromTheLedgerTip(t *testing.T) {
	stub := &tipHeightLedgerStub{
		tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(555, []byte{0x11, 0x22}),
			BlockNumber: 12,
		},
	}
	_, querySrv := newTipHeightServers(t, stub)

	out, err := querySrv.ReadData(
		context.Background(),
		connect.NewRequest(&query.ReadDataRequest{}),
	)
	require.NoError(t, err)
	tip := out.Msg.GetLedgerTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(555), tip.GetSlot())
	assert.Equal(t, uint64(12), tip.GetHeight(),
		"the height must be the one the ledger tip reports")
}

func TestUtxorpcAnonymousPlaintextTLSAndCORS(t *testing.T) {
	const origin = "https://wallet.example"
	cert, key := testutil.GenerateTestTLSCertKey(t)
	for _, tc := range []struct {
		name string
		tls  apiconfig.EffectiveTLS
		url  string
		cli  *http.Client
	}{
		{"plaintext", apiconfig.EffectiveTLS{}, "http://", http.DefaultClient},
		{"tls", apiconfig.EffectiveTLS{Enabled: true, CertFilePath: cert, KeyFilePath: key}, "https://", testutil.InsecureHTTPClient()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			client := *tc.cli
			client.Timeout = 5 * time.Second
			u, addr := startOnFreePort(
				t,
				ctx,
				tc.tls,
				func(cfg *UtxorpcConfig) {
					cfg.CORSAllowedOrigins = []string{origin}
				},
			)
			t.Cleanup(func() { stopUtxorpc(t, u) })
			resp := healthCheckAnonymous(t, ctx, &client, tc.url+addr, origin)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(
				t,
				origin,
				resp.Header.Get("Access-Control-Allow-Origin"),
			)
			preflight, err := http.NewRequestWithContext(
				ctx,
				http.MethodOptions,
				tc.url+addr+"/grpc.health.v1.Health/Check",
				nil,
			)
			require.NoError(t, err)
			preflight.Header.Set("Origin", origin)
			preflight.Header.Set(
				"Access-Control-Request-Method",
				http.MethodPost,
			)
			preflight.Header.Set(
				"Access-Control-Request-Headers",
				"Content-Type, Connect-Protocol-Version",
			)
			corsResp, err := client.Do(preflight)
			require.NoError(t, err)
			defer corsResp.Body.Close()
			require.Equal(t, http.StatusNoContent, corsResp.StatusCode)
			require.Equal(
				t,
				origin,
				corsResp.Header.Get("Access-Control-Allow-Origin"),
			)
		})
	}
}

func healthCheckAnonymous(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	origin string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/grpc.health.v1.Health/Check",
		strings.NewReader("{}"),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	req.Header.Set("Origin", origin)
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

func TestWaitForTxRejectsMalformedReferencesBeforeWork(t *testing.T) {
	for _, size := range []int{0, 31, 33} {
		for _, index := range []int{0, 1} {
			t.Run(
				fmt.Sprintf("size_%d/index_%d", size, index),
				func(t *testing.T) {
					eventBus := newControlledWaitForTxEventBus()
					var logs bytes.Buffer
					lookups := 0
					u := NewUtxorpc(UtxorpcConfig{
						Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
						EventBus: eventBus,
						LedgerState: &waitForTxLedgerStub{
							transactionByHash: func([]byte) (*models.Transaction, error) {
								lookups++
								return nil, errors.New("unexpected lookup")
							},
						},
					})
					refs := make([][]byte, 0, index+1)
					if index == 1 {
						refs = append(refs, bytes.Repeat([]byte{0x42}, 32))
					}
					refs = append(refs, make([]byte, size))
					server := &submitServiceServer{utxorpc: u}
					err := server.WaitForTx(
						context.Background(),
						connect.NewRequest(&submit.WaitForTxRequest{Ref: refs}),
						nil,
					)
					select {
					case <-eventBus.subscribed:
						t.Fatal(
							"malformed reference must be rejected before subscription",
						)
					default:
					}
					require.Zero(t, lookups)
					require.Empty(
						t,
						logs.String(),
						"malformed references must be rejected before logging",
					)
					require.Equal(
						t,
						connect.CodeInvalidArgument,
						connect.CodeOf(err),
					)
					require.ErrorContains(
						t,
						err,
						fmt.Sprintf(
							"transaction reference at index %d must be 32 bytes, got %d",
							index,
							size,
						),
					)
				},
			)
		}
	}
}

func TestWaitForTxReferenceAdmissionControls(t *testing.T) {
	for _, refs := range [][][]byte{nil, {}, {bytes.Repeat([]byte{0x42}, 32)}} {
		t.Run(
			fmt.Sprintf("references_%d_nil_%t", len(refs), refs == nil),
			func(t *testing.T) {
				eventBus := newControlledWaitForTxEventBus()
				lookupErr := errors.New("ledger lookup reached")
				lookups := 0
				u := NewUtxorpc(UtxorpcConfig{
					EventBus: eventBus,
					LedgerState: &waitForTxLedgerStub{
						transactionByHash: func(hash []byte) (*models.Transaction, error) {
							lookups++
							require.Equal(t, refs[0], hash)
							return nil, lookupErr
						},
					},
				})
				server := &submitServiceServer{utxorpc: u}
				err := server.WaitForTx(
					context.Background(),
					connect.NewRequest(&submit.WaitForTxRequest{Ref: refs}),
					nil,
				)
				if len(refs) == 0 {
					require.NoError(t, err)
					require.Zero(t, lookups)
					select {
					case <-eventBus.subscribed:
						t.Fatal("empty reference list must not subscribe")
					default:
					}
				} else {
					require.ErrorIs(t, err, lookupErr)
					require.Equal(t, 1, lookups, "valid reference must reach the existing lookup")
				}
			},
		)
	}
}
