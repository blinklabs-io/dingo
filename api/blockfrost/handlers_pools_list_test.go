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

var poolListTestPaths = []string{
	"/api/v0/pools",
	"/api/v0/pools/retiring",
}

type blockingPoolListNode struct {
	*blockingAddressNode
}

func (n *blockingPoolListNode) PoolsList(
	ctx context.Context, _ PaginationParams,
) ([]string, int, error) {
	return nil, 0, n.wait(ctx)
}

func (n *blockingPoolListNode) PoolsRetiring(
	ctx context.Context, _ PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

// TestHandlePoolListDeadlineIs503 covers a request that outlasts its
// deadline: it is retryable on both routes and each overrun releases its
// slot, since more overruns than one client may hold would otherwise turn
// into 429.
func TestHandlePoolListDeadlineIs503(t *testing.T) {
	t.Parallel()
	node := &poolListDeadlineNode{mockNode: &mockNode{}}
	b := newTestBlockfrost(node)

	for _, path := range poolListTestPaths {
		for range defaultMaxPoolListsPerClient + 1 {
			w := httptest.NewRecorder()
			b.handler().ServeHTTP(
				w, addressSummaryRequest(context.Background(), path),
			)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, path)
			require.Equal(t, "1", w.Header().Get("Retry-After"), path)
		}
	}
}

type poolListDeadlineNode struct {
	*mockNode
}

func (poolListDeadlineNode) PoolsList(
	context.Context, PaginationParams,
) ([]string, int, error) {
	return nil, 0, context.DeadlineExceeded
}

func (poolListDeadlineNode) PoolsRetiring(
	context.Context, PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	return nil, 0, context.DeadlineExceeded
}

// TestHandlePoolListAdmission covers a client holding its share of the pool
// across both routes: its next request is refused at once with a retryable
// 429, and another client is still admitted.
func TestHandlePoolListAdmission(t *testing.T) {
	t.Parallel()
	node := &blockingPoolListNode{newBlockingAddressNode()}
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	for i := range defaultMaxPoolListsPerClient {
		path := poolListTestPaths[i%len(poolListTestPaths)]
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(context.Background(), path),
			)
		})
	}
	for range defaultMaxPoolListsPerClient {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, addressSummaryRequest(ctx, poolListTestPaths[0]))
	cancel()
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))

	other := addressSummaryRequest(context.Background(), poolListTestPaths[1])
	other.RemoteAddr = "192.0.2.8:4000"
	wg.Go(func() { b.handler().ServeHTTP(httptest.NewRecorder(), other) })
	testutil.RequireReceive(
		t, node.entered, 5*time.Second, "other client admitted",
	)

	release()
	wg.Wait()
}
