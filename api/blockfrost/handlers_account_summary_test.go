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
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

type blockingAccountNode struct {
	*mockNode
	entered chan struct{}
	release chan struct{}
}

func (n *blockingAccountNode) Account(
	ctx context.Context,
	_ string,
) (AccountInfo, error) {
	n.entered <- struct{}{}
	select {
	case <-n.release:
		return AccountInfo{StakeAddress: "stake_test1"}, nil
	case <-ctx.Done():
		return AccountInfo{}, ctx.Err()
	}
}

func newBlockingAccountNode() *blockingAccountNode {
	return &blockingAccountNode{
		mockNode: &mockNode{},
		entered:  make(chan struct{}, 8),
		release:  make(chan struct{}),
	}
}

func TestHandleAccountCancelledRequestWritesNothing(t *testing.T) {
	t.Parallel()
	node := newBlockingAccountNode()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		newTestBlockfrost(node).handler().ServeHTTP(
			w, addressSummaryRequest(ctx, "/api/v0/accounts/stake_test1"),
		)
		done <- w
	}()
	testutil.RequireReceive(
		t, node.entered, 5*time.Second, "request reached node",
	)
	cancel()
	w := testutil.RequireReceive(
		t, done, 5*time.Second, "handler returns once the client cancels",
	)
	require.Equal(t, 0, w.Body.Len())
}

func TestHandleAccountAdmissionCap(t *testing.T) {
	t.Parallel()
	node := newBlockingAccountNode()
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(
					context.Background(), "/api/v0/accounts/stake_test1",
				),
			)
		})
	}
	for range 2 {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w, addressSummaryRequest(ctx, "/api/v0/accounts/stake_test1"),
	)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))

	release()
	wg.Wait()
}

// TestHandleAccountSharesAddressSummaryAdmission proves the account summary
// draws on the same admission pool as the address summaries, so a client
// cannot double its share of whole-history scans by mixing the routes.
func TestHandleAccountSharesAddressSummaryAdmission(t *testing.T) {
	t.Parallel()
	node := newBlockingAccountNode()
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(
					context.Background(), "/api/v0/accounts/stake_test1",
				),
			)
		})
	}
	for range 2 {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}

	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w,
		addressSummaryRequest(context.Background(), "/api/v0/addresses/addr1"),
	)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))

	release()
	wg.Wait()
}

func TestNodeAdapterAccountHonorsContext(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	stakingKey := bytes.Repeat([]byte{0x09}, 28)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: stakingKey,
		}),
	)
	address := newRewardHistoryStakeAddress(t, stakingKey)

	_, err := adapter.Account(context.Background(), address)
	require.NoError(t, err, "control: live context succeeds")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = adapter.Account(ctx, address)
	require.ErrorIs(t, err, context.Canceled)
}
