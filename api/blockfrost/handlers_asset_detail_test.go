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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

const assetDetailTestPath = "/api/v0/assets/" +
	"00112233445566778899aabbccddeeff00112233445566778899aabb"

// assetCtxNode records the context an asset lookup receives and answers with
// a fixed error, or parks until released or the context ends when block is
// set.
type assetCtxNode struct {
	*blockingAddressNode
	mu    sync.Mutex
	ctx   context.Context
	err   error
	block bool
}

func (n *assetCtxNode) Asset(
	ctx context.Context,
	_ string,
	_ []byte,
) (AssetInfo, error) {
	n.mu.Lock()
	n.ctx = ctx
	n.mu.Unlock()
	if n.block {
		return AssetInfo{}, n.wait(ctx)
	}
	return AssetInfo{}, n.err
}

func newAssetCtxNode() *assetCtxNode {
	return &assetCtxNode{blockingAddressNode: newBlockingAddressNode()}
}

// TestHandleAssetStopsWhenTheClientDisconnects covers a disconnected client:
// its request context ends, the lookup returns that error, and nothing is
// written to a connection nobody reads.
func TestHandleAssetStopsWhenTheClientDisconnects(t *testing.T) {
	t.Parallel()
	node := newAssetCtxNode()
	node.block = true
	b := newTestBlockfrost(node)

	ctx, cancel := context.WithCancel(context.Background())
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handler().ServeHTTP(w, addressSummaryRequest(ctx, assetDetailTestPath))
	}()
	testutil.RequireReceive(t, node.entered, 5*time.Second, "lookup started")
	cancel()
	testutil.RequireReceive(t, done, 5*time.Second, "lookup stopped")
	require.Zero(t, w.Body.Len(), "nothing is written to a gone client")
}

// TestHandleAssetOverConfiguredHoldingLimitIs400 covers an asset held by more UTxOs
// than the configured bound: it exceeds the service limit, as the other UTxO
// queries do, rather than being reported as an internal error.
func TestHandleAssetOverConfiguredHoldingLimitIs400(t *testing.T) {
	t.Parallel()
	node := newAssetCtxNode()
	node.err = fmt.Errorf("get quantity: %w", models.ErrTooManyUtxoResults)
	b := newTestBlockfrost(node)

	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w, addressSummaryRequest(context.Background(), assetDetailTestPath),
	)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "service limit")
}

// assetListRoutes are the listings that total the live holdings of every
// asset they return.
var assetListRoutes = []string{
	"/api/v0/assets",
	"/api/v0/assets/policy/" +
		"00112233445566778899aabbccddeeff00112233445566778899aabb",
}

// blockingAssetListNode parks the asset listings until released or until the
// request context ends.
type blockingAssetListNode struct {
	*blockingAddressNode
}

func (n *blockingAssetListNode) Assets(
	ctx context.Context,
	_ PaginationParams,
) ([]AssetListItemInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

func (n *blockingAssetListNode) AssetsByPolicy(
	ctx context.Context,
	_ string,
	_ PaginationParams,
) ([]AssetListItemInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

// TestHandleAssetListingStopsWhenTheClientDisconnects covers a listing whose
// client goes away: the lookup returns the context error and nothing is
// written.
func TestHandleAssetListingStopsWhenTheClientDisconnects(t *testing.T) {
	t.Parallel()
	node := &blockingAssetListNode{newBlockingAddressNode()}
	b := newTestBlockfrost(node)

	ctx, cancel := context.WithCancel(context.Background())
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handler().ServeHTTP(w, addressSummaryRequest(ctx, assetListRoutes[0]))
	}()
	testutil.RequireReceive(t, node.entered, 5*time.Second, "listing started")
	cancel()
	testutil.RequireReceive(t, done, 5*time.Second, "listing stopped")
	require.Zero(t, w.Body.Len())
}
