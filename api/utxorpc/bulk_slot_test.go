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

package utxorpc

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	gosync "sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	betaquery "github.com/utxorpc/go-codegen/utxorpc/v1beta/query"
)

// stalledResponseWriter blocks the first body write until released, standing
// in for a client that stops reading its response.
type stalledResponseWriter struct {
	header  http.Header
	status  int
	writing chan struct{}
	release chan struct{}
	once    gosync.Once
	body    bytes.Buffer
}

func newStalledResponseWriter() *stalledResponseWriter {
	return &stalledResponseWriter{
		header:  http.Header{},
		writing: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *stalledResponseWriter) Header() http.Header { return w.header }

func (w *stalledResponseWriter) WriteHeader(status int) { w.status = status }

func (w *stalledResponseWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.writing) })
	<-w.release
	return w.body.Write(p)
}

func (w *stalledResponseWriter) Flush() {}

// TestBulkSlot_HeldUntilResponseWritten serves ReadMempool through the real
// routing table to a reader that stalls mid-response, and requires the
// request's slot to stay held until the write finishes.
func TestBulkSlot_HeldUntilResponseWritten(t *testing.T) {
	t.Parallel()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:                    discardLogger(),
		Mempool:                   newBoundsRecordingMempool(3),
		MaxConcurrentBulkRequests: 1,
	})
	handler := u.newServeMux()
	submitSrv := &submitServiceServer{utxorpc: u}

	w := newStalledResponseWriter()
	req := httptest.NewRequest(
		http.MethodPost,
		"/utxorpc.v1alpha.submit.SubmitService/ReadMempool",
		bytes.NewReader(nil),
	)
	req.Header.Set("Content-Type", "application/proto")
	served := make(chan struct{})
	go func() {
		defer close(served)
		handler.ServeHTTP(w, req)
	}()
	testutil.RequireReceive(
		t, w.writing, 5*time.Second, "response write started",
	)

	_, err := submitSrv.ReadMempool(
		context.Background(),
		connect.NewRequest(&submit.ReadMempoolRequest{}),
	)
	require.Error(t, err,
		"the slot must stay held while its response is being written")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	close(w.release)
	testutil.RequireReceive(t, served, 5*time.Second, "response written")
	require.Contains(t, []int{0, http.StatusOK}, w.status)
	require.NotZero(t, w.body.Len())

	_, err = submitSrv.ReadMempool(
		context.Background(),
		connect.NewRequest(&submit.ReadMempoolRequest{}),
	)
	require.NoError(t, err, "the slot is returned once the write completes")
}

// invalidBulkRequests sends one request per gated handler that fails its
// own parameter validation.
func invalidBulkRequests(u *Utxorpc) map[string]error {
	ctx := context.Background()
	querySrv := &queryServiceServer{utxorpc: u}
	syncSrv := &syncServiceServer{utxorpc: u}
	betaSrv := &betaQueryServiceServer{utxorpc: u}
	errs := map[string]error{}
	_, errs["ReadUtxos"] = querySrv.ReadUtxos(ctx, connect.NewRequest(
		&query.ReadUtxosRequest{
			Keys: make([]*query.TxoRef, u.config.MaxUtxoKeys+1),
		}))
	_, errs["ReadData"] = querySrv.ReadData(ctx, connect.NewRequest(
		&query.ReadDataRequest{Keys: [][]byte{{0x01}}}))
	_, errs["SearchUtxos"] = querySrv.SearchUtxos(ctx, connect.NewRequest(
		&query.SearchUtxosRequest{StartToken: "not-a-token"}))
	_, errs["FetchBlock"] = syncSrv.FetchBlock(ctx, connect.NewRequest(
		&sync.FetchBlockRequest{
			Ref: make([]*sync.BlockRef, u.config.MaxBlockRefs+1),
		}))
	_, errs["DumpHistory"] = syncSrv.DumpHistory(ctx, connect.NewRequest(
		&sync.DumpHistoryRequest{
			MaxItems: uint32(u.config.MaxHistoryItems) + 1, //nolint:gosec
		}))
	_, errs["ReadState"] = betaSrv.ReadState(ctx, connect.NewRequest(
		&betaquery.ReadStateRequest{}))
	return errs
}

// TestBulkSlot_InvalidRequestsDoNotContend requires a request that fails
// validation to be rejected as such even when every bulk slot is held: it
// does no bulk work, so it neither needs a slot nor takes one from a valid
// request.
func TestBulkSlot_InvalidRequestsDoNotContend(t *testing.T) {
	t.Parallel()
	mp := newBoundsRecordingMempool(1)
	mp.entered = make(chan struct{}, 1)
	mp.release = make(chan struct{})
	var releaseOnce gosync.Once
	release := func() { releaseOnce.Do(func() { close(mp.release) }) }
	defer release()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:                    discardLogger(),
		Mempool:                   mp,
		MaxConcurrentBulkRequests: 1,
	})
	held := make(chan error, 1)
	go func() {
		_, err := (&submitServiceServer{utxorpc: u}).ReadMempool(
			context.Background(),
			connect.NewRequest(&submit.ReadMempoolRequest{}),
		)
		held <- err
	}()
	testutil.RequireReceive(t, mp.entered, 5*time.Second, "slot held")

	for name, err := range invalidBulkRequests(u) {
		require.Error(t, err, name)
		require.Equal(t,
			connect.CodeInvalidArgument, connect.CodeOf(err), name)
	}

	release()
	require.NoError(t,
		testutil.RequireReceive(t, held, 5*time.Second, "held request"))
}

// TestBulkRequests_LogAfterValidation requires a request that fails
// validation to write no request log.
func TestBulkRequests_LogAfterValidation(t *testing.T) {
	t.Parallel()
	var logBuf bytes.Buffer
	var logMu gosync.Mutex
	u := NewUtxorpc(UtxorpcConfig{
		Logger: slog.New(slog.NewJSONHandler(
			writerFunc(func(p []byte) (int, error) {
				logMu.Lock()
				defer logMu.Unlock()
				return logBuf.Write(p)
			}), nil,
		)),
	})

	for name, err := range invalidBulkRequests(u) {
		require.Equal(t,
			connect.CodeInvalidArgument, connect.CodeOf(err), name)
	}

	logMu.Lock()
	defer logMu.Unlock()
	require.Empty(t, logBuf.String())
}
