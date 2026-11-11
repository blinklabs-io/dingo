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

const blockListTestPool = "00000000000000000000000000000000000000000000000000000000"

var blockListTestPaths = []string{
	"/api/v0/pools/" + blockListTestPool + "/blocks",
	"/api/v0/epochs/12/blocks",
	"/api/v0/epochs/12/blocks/" + blockListTestPool,
}

type blockingBlockListNode struct {
	*blockingAddressNode
}

func (n *blockingBlockListNode) PoolBlocks(
	ctx context.Context, _ string, _ PaginationParams,
) ([]string, int, error) {
	return nil, 0, n.wait(ctx)
}

func (n *blockingBlockListNode) EpochBlocks(
	ctx context.Context, _ uint64, _ PaginationParams,
) ([]string, int, error) {
	return nil, 0, n.wait(ctx)
}

func (n *blockingBlockListNode) EpochPoolBlocks(
	ctx context.Context, _ uint64, _ string, _ PaginationParams,
) ([]string, int, error) {
	return nil, 0, n.wait(ctx)
}

// TestHandleBlockListDeadlineIs503 covers a page that outlasts its deadline:
// it is retryable on every route and each overrun releases its slot, since
// more overruns than one client may hold would otherwise turn into 429.
func TestHandleBlockListDeadlineIs503(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.poolBlocks.err = context.DeadlineExceeded
	node.epochBlocks.err = context.DeadlineExceeded
	node.epochPoolBlocks.err = context.DeadlineExceeded
	b := newTestBlockfrost(node)

	for _, path := range blockListTestPaths {
		for range defaultMaxBlockListsPerClient + 1 {
			w := httptest.NewRecorder()
			b.handler().ServeHTTP(
				w, addressSummaryRequest(context.Background(), path),
			)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, path)
			require.Equal(t, "1", w.Header().Get("Retry-After"), path)
		}
	}
}

// TestHandleBlockListAdmission covers a client holding its share of the pool
// across the three routes: its next page is refused at once with a retryable
// 429, and another client is still admitted.
func TestHandleBlockListAdmission(t *testing.T) {
	t.Parallel()
	node := &blockingBlockListNode{newBlockingAddressNode()}
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	for i := range defaultMaxBlockListsPerClient {
		path := blockListTestPaths[i]
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(context.Background(), path),
			)
		})
	}
	for range defaultMaxBlockListsPerClient {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "page admitted",
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w, addressSummaryRequest(ctx, blockListTestPaths[2]),
	)
	cancel()
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))

	other := addressSummaryRequest(context.Background(), blockListTestPaths[0])
	other.RemoteAddr = "192.0.2.8:4000"
	wg.Go(func() { b.handler().ServeHTTP(httptest.NewRecorder(), other) })
	testutil.RequireReceive(
		t, node.entered, 5*time.Second, "other client admitted",
	)

	release()
	wg.Wait()
}
