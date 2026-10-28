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

package blockfrost

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

var chainSummaryRoutes = []string{
	"/api/v0/network",
	"/api/v0/epochs/latest",
	"/api/v0/epochs/5",
	"/api/v0/epochs/5/next",
	"/api/v0/epochs/5/previous",
}

// blockingChainNode parks the chain and epoch summary routes until released
// or until the request context ends.
type blockingChainNode struct {
	*blockingAddressNode
}

func (n *blockingChainNode) Network(
	ctx context.Context,
) (NetworkInfo, error) {
	return NetworkInfo{}, n.wait(ctx)
}

func (n *blockingChainNode) CurrentEpoch(
	ctx context.Context,
) (EpochInfo, error) {
	return EpochInfo{}, n.wait(ctx)
}

func (n *blockingChainNode) Epoch(
	ctx context.Context,
	_ uint64,
) (EpochInfo, error) {
	return EpochInfo{}, n.wait(ctx)
}

func (n *blockingChainNode) EpochsAfter(
	ctx context.Context,
	_ uint64,
	_ PaginationParams,
) ([]EpochInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

func (n *blockingChainNode) EpochsBefore(
	ctx context.Context,
	_ uint64,
	_ PaginationParams,
) ([]EpochInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

func newBlockingChainNode() *blockingChainNode {
	return &blockingChainNode{newBlockingAddressNode()}
}

func TestHandleChainSummaryRoutesShareAdmission(t *testing.T) {
	t.Parallel()
	node := newBlockingChainNode()
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	// Two different routes fill the per-client cap, so the cap is shared.
	for _, path := range chainSummaryRoutes[:2] {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(context.Background(), path),
			)
		})
	}
	for range 2 {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}
	for _, path := range chainSummaryRoutes {
		// A rejected request returns at once; the deadline only keeps a
		// wrongly admitted one from parking the test.
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(w, addressSummaryRequest(ctx, path))
		cancel()
		require.Equal(t, http.StatusTooManyRequests, w.Code, path)
		require.Equal(t, "1", w.Header().Get("Retry-After"), path)
	}

	// A different client is not affected by the first one's cap.
	other := addressSummaryRequest(context.Background(), chainSummaryRoutes[0])
	other.RemoteAddr = "192.0.2.8:4000"
	wg.Go(func() {
		b.handler().ServeHTTP(httptest.NewRecorder(), other)
	})
	testutil.RequireReceive(
		t, node.entered, 5*time.Second, "other client admitted",
	)

	release()
	wg.Wait()

	// The slots are free again once the requests finish.
	for _, path := range chainSummaryRoutes {
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(
			w, addressSummaryRequest(context.Background(), path),
		)
		require.Equal(t, http.StatusOK, w.Code, path)
	}
}

func TestHandleChainSummaryPropagatesRequestContext(t *testing.T) {
	t.Parallel()
	for _, path := range chainSummaryRoutes {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			node := newBlockingChainNode()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() { close(node.release) })
			}
			defer release()
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				newTestBlockfrost(node).handler().ServeHTTP(
					w, addressSummaryRequest(ctx, path),
				)
				done <- w
			}()
			testutil.RequireReceive(
				t, node.entered, 5*time.Second, "request reached node",
			)
			cancel()
			w := testutil.RequireReceive(
				t, done, 5*time.Second,
				"handler returns once the client cancels",
			)
			// A cancelled request has no reader, so nothing is written.
			require.Equal(t, 0, w.Body.Len())
		})
	}
}

func TestNodeAdapterChainSummariesStopOnCancelledContext(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	page := PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc}

	_, err := fx.adapter.CurrentEpoch(ctx)
	require.ErrorIs(t, err, context.Canceled, "CurrentEpoch")
	_, err = fx.adapter.Network(ctx)
	require.ErrorIs(t, err, context.Canceled, "Network")
	_, err = fx.adapter.Epoch(ctx, 1)
	require.ErrorIs(t, err, context.Canceled, "Epoch")
	_, _, err = fx.adapter.EpochsAfter(ctx, 0, page)
	require.ErrorIs(t, err, context.Canceled, "EpochsAfter")
	_, _, err = fx.adapter.EpochsBefore(ctx, 2, page)
	require.ErrorIs(t, err, context.Canceled, "EpochsBefore")

	// The same calls succeed on a live context.
	live := context.Background()
	_, err = fx.adapter.CurrentEpoch(live)
	require.NoError(t, err)
	_, err = fx.adapter.Network(live)
	require.NoError(t, err)
}
