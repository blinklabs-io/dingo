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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

const (
	poolAggregateDetailPath   = "/api/v0/pools/" + "00000000000000000000000000000000000000000000000000000001"
	poolAggregateExtendedPath = "/api/v0/pools/extended"
)

type blockingPoolNode struct {
	*mockNode
	entered chan struct{}
	release chan struct{}
}

func newBlockingPoolNode() *blockingPoolNode {
	return &blockingPoolNode{
		mockNode: &mockNode{},
		entered:  make(chan struct{}, 8),
		release:  make(chan struct{}),
	}
}

func (n *blockingPoolNode) wait(ctx context.Context) error {
	n.entered <- struct{}{}
	select {
	case <-n.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *blockingPoolNode) PoolDetail(
	ctx context.Context,
	poolID string,
) (PoolDetailInfo, error) {
	if _, err := parsePoolID(poolID); err != nil {
		return PoolDetailInfo{}, err
	}
	return PoolDetailInfo{}, n.wait(ctx)
}

func (n *blockingPoolNode) PoolsExtended(
	ctx context.Context,
	_ PaginationParams,
) ([]PoolExtendedInfo, int, error) {
	return []PoolExtendedInfo{}, 0, n.wait(ctx)
}

// saturatePoolAggregate admits the per-client limit of requests for path and
// leaves them blocked in the node until the returned release is called.
func saturatePoolAggregate(
	t *testing.T,
	b *Blockfrost,
	node *blockingPoolNode,
	paths ...string,
) func() {
	t.Helper()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	t.Cleanup(release)
	var wg sync.WaitGroup
	for _, path := range paths {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(context.Background(), path),
			)
		})
	}
	for range paths {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}
	return func() {
		release()
		wg.Wait()
	}
}

func TestHandlePoolAggregateAdmissionCap(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		poolAggregateDetailPath,
		poolAggregateExtendedPath,
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			node := newBlockingPoolNode()
			b := newTestBlockfrost(node)
			done := saturatePoolAggregate(t, b, node, path, path)

			ctx, cancel := context.WithTimeout(
				context.Background(), 2*time.Second,
			)
			defer cancel()
			w := httptest.NewRecorder()
			b.handler().ServeHTTP(w, addressSummaryRequest(ctx, path))
			require.Equal(t, http.StatusTooManyRequests, w.Code)
			require.Equal(t, "1", w.Header().Get("Retry-After"))
			done()
		})
	}
}

// TestHandlePoolAggregateRoutesShareAdmission proves the pool detail and the
// extended pool list draw on one pool, so mixing the routes does not double a
// client's share.
func TestHandlePoolAggregateRoutesShareAdmission(t *testing.T) {
	t.Parallel()
	node := newBlockingPoolNode()
	b := newTestBlockfrost(node)
	done := saturatePoolAggregate(
		t, b, node, poolAggregateDetailPath, poolAggregateDetailPath,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w, addressSummaryRequest(ctx, poolAggregateExtendedPath),
	)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	done()
}

// TestHandlePoolAggregateValidatesBeforeAdmission proves a malformed pool id
// or page is answered 400 while the pool is full, so rejected input never
// reads as capacity and never takes a slot.
func TestHandlePoolAggregateValidatesBeforeAdmission(t *testing.T) {
	t.Parallel()
	node := newBlockingPoolNode()
	b := newTestBlockfrost(node)
	done := saturatePoolAggregate(
		t, b, node, poolAggregateDetailPath, poolAggregateDetailPath,
	)
	for _, path := range []string{
		"/api/v0/pools/not-a-pool-id",
		poolAggregateExtendedPath + "?count=0",
		poolAggregateExtendedPath + "?page=0",
	} {
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(w, addressSummaryRequest(ctx, path))
		cancel()
		require.Equal(t, http.StatusBadRequest, w.Code, path)
	}
	done()
}

func TestHandlePoolAggregateCancelledRequestWritesNothing(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		poolAggregateDetailPath,
		poolAggregateExtendedPath,
	} {
		t.Run(strings.TrimPrefix(path, "/api/v0/pools/"), func(t *testing.T) {
			t.Parallel()
			node := newBlockingPoolNode()
			t.Cleanup(func() { close(node.release) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
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
				t,
				done,
				5*time.Second,
				"handler returns once the client cancels",
			)
			require.Equal(t, 0, w.Body.Len())
		})
	}
}
