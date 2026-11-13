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

// runBlockedPoolListRequests starts one request per path and waits until
// every one is inside the node.
func runBlockedPoolListRequests(
	t *testing.T,
	b *Blockfrost,
	node *blockingPoolListNode,
	wg *sync.WaitGroup,
	paths []string,
) {
	t.Helper()
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
}

// TestHandlePoolListUnlimitedByDefault covers a server with no pool list
// limit configured: any number of requests from one client run at once.
func TestHandlePoolListUnlimitedByDefault(t *testing.T) {
	t.Parallel()
	node := &blockingPoolListNode{newBlockingAddressNode()}
	b := newTestBlockfrost(node)
	require.Nil(t, b.poolListAdmission)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	paths := make([]string, 0, 8)
	for i := range 8 {
		paths = append(paths, poolListTestPaths[i%len(poolListTestPaths)])
	}
	runBlockedPoolListRequests(t, b, node, &wg, paths)
	release()
	wg.Wait()
}

// TestHandlePoolListAdmissionWhenConfigured covers a configured limit shared
// by both routes: a client at its share is refused at once with a retryable
// 429, and another client is still admitted.
func TestHandlePoolListAdmissionWhenConfigured(t *testing.T) {
	t.Parallel()
	node := &blockingPoolListNode{newBlockingAddressNode()}
	b := New(BlockfrostConfig{
		ListenAddress:                   "127.0.0.1:0",
		MaxConcurrentPoolLists:          2,
		MaxConcurrentPoolListsPerClient: 1,
	}, node, nil)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	runBlockedPoolListRequests(
		t, b, node, &wg, poolListTestPaths[:1],
	)

	for _, path := range poolListTestPaths {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(w, addressSummaryRequest(ctx, path))
		cancel()
		require.Equal(t, http.StatusTooManyRequests, w.Code, path)
		require.Equal(t, "1", w.Header().Get("Retry-After"), path)
	}

	other := addressSummaryRequest(context.Background(), poolListTestPaths[1])
	other.RemoteAddr = "192.0.2.8:4000"
	wg.Go(func() { b.handler().ServeHTTP(httptest.NewRecorder(), other) })
	testutil.RequireReceive(
		t, node.entered, 5*time.Second, "other client admitted",
	)

	third := addressSummaryRequest(context.Background(), poolListTestPaths[0])
	third.RemoteAddr = "192.0.2.9:4000"
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, third)
	require.Equal(t, http.StatusTooManyRequests, w.Code)

	release()
	wg.Wait()
}

// TestHandlePoolListClientDisconnectWritesNoError covers a client that leaves
// mid-query on either route: the handler logs and answers nothing, not a 500.
func TestHandlePoolListClientDisconnectWritesNoError(t *testing.T) {
	t.Parallel()
	node := &poolListCanceledNode{mockNode: &mockNode{}}
	b := newTestBlockfrost(node)
	for _, path := range poolListTestPaths {
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(
			w, addressSummaryRequest(context.Background(), path),
		)
		require.NotEqual(t, http.StatusInternalServerError, w.Code, path)
		require.Empty(t, w.Body.String(), path)
	}
}

type poolListCanceledNode struct {
	*mockNode
}

func (poolListCanceledNode) PoolsList(
	context.Context, PaginationParams,
) ([]string, int, error) {
	return nil, 0, context.Canceled
}

func (poolListCanceledNode) PoolsRetiring(
	context.Context, PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	return nil, 0, context.Canceled
}
