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

var metadataLabelTestPaths = []string{
	"/api/v0/metadata/txs/labels/721",
	"/api/v0/metadata/txs/labels/721/cbor",
}

// blockingMetadataNode parks each page until released or its context ends.
type blockingMetadataNode struct {
	*blockingAddressNode
}

func (n *blockingMetadataNode) MetadataTransactions(
	ctx context.Context,
	_ uint64,
	_ PaginationParams,
) ([]MetadataTransactionJSONInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

// TestHandleMetadataLabelDeadlineIs503 covers a page that outlasts its
// deadline: it is retryable on both routes, and each overrun releases its
// admission slot, since more overruns than one client may hold at once would
// otherwise turn into 429.
func TestHandleMetadataLabelDeadlineIs503(t *testing.T) {
	t.Parallel()
	node := &mockNode{
		metadataJSONErr: context.DeadlineExceeded,
		metadataCBORErr: context.DeadlineExceeded,
	}
	b := newTestBlockfrost(node)

	for _, path := range metadataLabelTestPaths {
		for range defaultMaxMetadataLabelPagesPerClient + 1 {
			w := httptest.NewRecorder()
			b.handler().ServeHTTP(
				w,
				addressSummaryRequest(context.Background(), path),
			)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, path)
			require.Equal(t, "1", w.Header().Get("Retry-After"), path)
		}
	}
}

// TestHandleMetadataLabelAdmission covers a client holding its share of the
// metadata label pool: its next page is refused at once with a retryable 429,
// and another client is still admitted.
func TestHandleMetadataLabelAdmission(t *testing.T) {
	t.Parallel()
	node := &blockingMetadataNode{newBlockingAddressNode()}
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	for range defaultMaxMetadataLabelPagesPerClient {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(
					context.Background(), metadataLabelTestPaths[0],
				),
			)
		})
	}
	for range defaultMaxMetadataLabelPagesPerClient {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "page admitted",
		)
	}

	// A rejected request returns at once; the deadline only keeps a wrongly
	// admitted one from parking the test.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w, addressSummaryRequest(ctx, metadataLabelTestPaths[0]),
	)
	cancel()
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))

	other := addressSummaryRequest(
		context.Background(), metadataLabelTestPaths[0],
	)
	other.RemoteAddr = "192.0.2.8:4000"
	wg.Go(func() { b.handler().ServeHTTP(httptest.NewRecorder(), other) })
	testutil.RequireReceive(
		t, node.entered, 5*time.Second, "other client admitted",
	)

	release()
	wg.Wait()
}
