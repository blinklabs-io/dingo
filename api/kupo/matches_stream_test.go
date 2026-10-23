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

package kupo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestMatchesStreamTheCompleteResult pins the upstream Kupo contract: an
// ordinary request streams every match as one 200 array, however large.
func TestMatchesStreamTheCompleteResult(t *testing.T) {
	t.Parallel()

	const total = 10_002
	httpServer := httptest.NewServer(
		newTestServer(&pagedNode{total: total}).handler(),
	)
	t.Cleanup(httpServer.Close)

	resp, err := http.Get(httpServer.URL + "/matches") // #nosec G107 G704 -- local test server
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var rows []struct {
		OutputIndex uint32 `json:"output_index"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rows))
	require.Len(t, rows, total)
	for i, row := range rows {
		require.Equal(t, uint32(i), row.OutputIndex) // #nosec G115 -- test index is small
	}
	require.Empty(t, resp.Header.Get("X-Kupo-Next-Cursor"))
	require.Empty(t, resp.Trailer.Get("X-Kupo-Next-Cursor"))
}

// TestMatchesAcceptOnlySpecQueryParameters checks every query parameter the
// Kupo v2.12 OpenAPI document defines for GET /matches, and that the former
// pagination parameters, which it does not define, are rejected.
func TestMatchesAcceptOnlySpecQueryParameters(t *testing.T) {
	t.Parallel()

	server := newTestServer(&mockNode{})
	for _, query := range []string{
		"resolve_hashes",
		"spent",
		"unspent",
		"order=oldest_first",
		"order=most_recent_first",
		"created_after=1",
		"spent_after=1",
		"created_before=10",
		"spent_before=10",
		"policy_id=" + strings.Repeat("11", 28),
		"policy_id=" + strings.Repeat("11", 28) + "&asset_name=abcd",
		"transaction_id=" + strings.Repeat("22", 32),
		"transaction_id=" + strings.Repeat("22", 32) + "&output_index=0",
	} {
		resp := serve(t, server, http.MethodGet, "/matches?"+query, nil)
		require.Equal(t, http.StatusOK, resp.Code, query)
	}
	for _, query := range []string{"limit=1", "cursor=1:0:0:" + strings.Repeat("00", 32)} {
		resp := serve(t, server, http.MethodGet, "/matches?"+query, nil)
		require.Equal(t, http.StatusBadRequest, resp.Code, query)
	}
}

// TestNodeMatchesHoldNoSnapshotWhileTheCallerConsumesAPage proves a match
// stream does not pin a read snapshot while its client reads: with room for a
// single admitted snapshot, another reader opens one between two rows.
func TestNodeMatchesHoldNoSnapshotWhileTheCallerConsumesAPage(t *testing.T) {
	t.Parallel()

	enterprise := historyFixtureEnterprise(t)
	adapter, db, keys := newHistoryFixture(
		t,
		func(int) lcommon.Address { return enterprise },
		2,
	)
	iter, err := adapter.Matches(
		context.Background(),
		MatchQuery{Pattern: "*", OldestFirst: true},
	)
	require.NoError(t, err)
	defer iter.Close()
	got := 0
	for {
		_, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		got++
		if got == 1 || got == matchPageSize+1 {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				2*time.Second,
			)
			txn, _, err := database.NewReadSnapshotContext(ctx, db)
			cancel()
			require.NoError(
				t,
				err,
				"another reader must get a snapshot after row %d",
				got,
			)
			txn.Release()
		}
	}
	require.Equal(t, len(keys), got)
}

// TestStoredMatchesStreamTheCompleteResultInBothOrders runs the handler over
// the storage-backed iterator across several storage pages.
func TestStoredMatchesStreamTheCompleteResultInBothOrders(t *testing.T) {
	t.Parallel()

	enterprise := historyFixtureEnterprise(t)
	adapter, keys := newHistoryFixtureAdapter(
		t,
		func(int) lcommon.Address { return enterprise },
	)
	httpServer := httptest.NewServer(
		New(Config{ListenAddress: ":0"}, adapter, nil).handler(),
	)
	t.Cleanup(httpServer.Close)

	for _, order := range []string{"oldest_first", "most_recent_first"} {
		want := slices.Clone(keys)
		if order == "most_recent_first" {
			slices.Reverse(want)
		}
		resp, err := http.Get(httpServer.URL + "/matches?order=" + order) // #nosec G107 G704 -- local test server
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var rows []struct {
			TransactionID string `json:"transaction_id"`
			OutputIndex   uint32 `json:"output_index"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&rows))
		require.NoError(t, resp.Body.Close())
		got := make([]string, len(rows))
		for i, row := range rows {
			got[i] = fmt.Sprintf("%s#%d", row.TransactionID, row.OutputIndex)
		}
		require.Equal(t, want, got, order)
	}
}

// pagedNode serves total matches, the i-th with output index i.
type pagedNode struct {
	mockNode
	total uint64
}

func (n *pagedNode) Matches(
	context.Context,
	MatchQuery,
) (MatchIterator, error) {
	return &pagedIterator{total: n.total}, nil
}

type pagedIterator struct {
	total uint64
	next  uint64
}

func (*pagedIterator) Tip() Point { return Point{SlotNo: 1} }

func (i *pagedIterator) Next() (Match, bool, error) {
	if i.next >= i.total {
		return Match{}, false, nil
	}
	match := Match{
		TransactionID: strings.Repeat("ab", 32),
		OutputIndex:   uint32(i.next), // #nosec G115 -- test index is small
		Address:       "addr_test1" + strings.Repeat("q", 98),
		Value:         Value{Coins: 1},
		CreatedAt:     Point{SlotNo: i.next},
	}
	i.next++
	return match, true, nil
}

func (*pagedIterator) Close() {}

// holdingNode blocks its first two streams until release closes and answers
// every later stream immediately.
type holdingNode struct {
	mockNode
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (n *holdingNode) Matches(
	_ context.Context,
	_ MatchQuery,
) (MatchIterator, error) {
	if n.calls.Add(1) <= 2 {
		n.started <- struct{}{}
		return &blockedIterator{release: n.release}, nil
	}
	return &pagedIterator{}, nil
}

type blockedIterator struct {
	pagedIterator
	release chan struct{}
}

func (i *blockedIterator) Next() (Match, bool, error) {
	<-i.release
	return Match{}, false, nil
}

func TestMatchesTwoLongStreamsLeaveCapacityForAThirdClient(t *testing.T) {
	t.Parallel()

	node := &holdingNode{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	server := newTestServer(node)
	handler := server.handler()
	done := make(chan struct{}, 2)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer func() {
		release()
		for range 2 {
			testutil.RequireReceive(t, done, testutil.AsyncWait, "stream end")
		}
	}()
	for _, addr := range []string{"192.0.2.1:1000", "192.0.2.2:1000"} {
		req := httptest.NewRequest(http.MethodGet, "/matches", nil)
		req.RemoteAddr = addr
		go func() {
			handler.ServeHTTP(httptest.NewRecorder(), req)
			done <- struct{}{}
		}()
	}
	for range 2 {
		testutil.RequireReceive(t, node.started, testutil.AsyncWait, "stream start")
	}

	req := httptest.NewRequest(http.MethodGet, "/matches", nil)
	req.RemoteAddr = "192.0.2.3:1000"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	require.Equal(
		t,
		http.StatusOK,
		recorder.Code,
		"a third client must not be refused while two streams run",
	)
}
