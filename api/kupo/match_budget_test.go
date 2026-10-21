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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func budgetMatch(index uint64) Match {
	return Match{
		TransactionID: strings.Repeat("ab", 32),
		OutputIndex:   uint32(index), // #nosec G115 -- test index is small
		Address:       "addr_test1" + strings.Repeat("q", 98),
		Value:         Value{Coins: 1},
		CreatedAt:     Point{SlotNo: index},
	}
}

func budgetCursor(index uint64) *MatchCursor {
	return &MatchCursor{
		Slot:          index,
		TransactionID: make([]byte, 32),
	}
}

// pagedNode serves matches[i] at slot i and resumes strictly after the
// cursor's slot. scanLimit, when positive, ends iteration on the scan budget.
type pagedNode struct {
	mockNode
	total     uint64
	scanLimit uint64
}

func (n *pagedNode) Matches(
	_ context.Context,
	query MatchQuery,
) (MatchIterator, error) {
	n.lastQuery = query
	next := uint64(0)
	if query.Cursor != nil {
		next = query.Cursor.Slot + 1
	}
	return &pagedIterator{total: n.total, next: next, scanLimit: n.scanLimit}, nil
}

type pagedIterator struct {
	total     uint64
	next      uint64
	scanLimit uint64
	scanned   uint64
	last      *MatchCursor
	truncated bool
}

func (*pagedIterator) Tip() Point { return Point{SlotNo: 1} }

func (i *pagedIterator) Next() (Match, bool, error) {
	if i.next >= i.total {
		return Match{}, false, nil
	}
	if i.scanLimit > 0 && i.scanned >= i.scanLimit {
		i.truncated = true
		return Match{}, false, nil
	}
	match := budgetMatch(i.next)
	i.last = budgetCursor(i.next)
	i.next++
	i.scanned++
	return match, true, nil
}

func (i *pagedIterator) Resume() (*MatchCursor, bool) { return i.last, i.truncated }

func (*pagedIterator) Close() {}

type budgetPage struct {
	outputs []uint32
	cursor  string
}

func fetchBudgetPage(
	t *testing.T,
	baseURL string,
	cursor string,
) budgetPage {
	t.Helper()
	target := baseURL + "/matches"
	if cursor != "" {
		target += "?cursor=" + url.QueryEscape(cursor)
	}
	resp, err := http.Get(target) // #nosec G107 G704 -- local test server
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var rows []struct {
		OutputIndex uint32 `json:"output_index"`
	}
	require.NoError(t, json.Unmarshal(body, &rows))
	page := budgetPage{cursor: resp.Trailer.Get(nextCursorTrailer)}
	for _, row := range rows {
		page.outputs = append(page.outputs, row.OutputIndex)
	}
	return page
}

func TestMatchesRowBudgetTruncatesAndResumesWithoutGapsOrDuplicates(
	t *testing.T,
) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 25})
	server.maxMatchRows = 10
	httpServer := httptest.NewServer(server.handler())
	t.Cleanup(httpServer.Close)

	var all []uint32
	var sizes []int
	cursor := ""
	for range 10 {
		page := fetchBudgetPage(t, httpServer.URL, cursor)
		all = append(all, page.outputs...)
		sizes = append(sizes, len(page.outputs))
		if page.cursor == "" {
			break
		}
		cursor = page.cursor
	}
	require.Equal(t, []int{10, 10, 5}, sizes)
	want := make([]uint32, 25)
	for i := range want {
		want[i] = uint32(i) // #nosec G115 -- test index is small
	}
	require.Equal(t, want, all)
}

func TestMatchesByteBudgetTruncatesWithCursor(t *testing.T) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 100})
	server.maxMatchBytes = 1500
	httpServer := httptest.NewServer(server.handler())
	t.Cleanup(httpServer.Close)

	page := fetchBudgetPage(t, httpServer.URL, "")
	require.NotEmpty(t, page.outputs)
	require.Less(t, len(page.outputs), 100)
	require.NotEmpty(t, page.cursor)
	next := fetchBudgetPage(t, httpServer.URL, page.cursor)
	require.NotEmpty(t, next.outputs)
	require.Equal(t, page.outputs[len(page.outputs)-1]+1, next.outputs[0])
}

func TestMatchesScanBudgetReturnsCursorEvenWithoutRows(t *testing.T) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 100, scanLimit: 7})
	httpServer := httptest.NewServer(server.handler())
	t.Cleanup(httpServer.Close)

	page := fetchBudgetPage(t, httpServer.URL, "")
	require.Len(t, page.outputs, 7)
	require.Equal(t, budgetCursor(6).String(), page.cursor)
}

func TestMatchesCompleteResultCarriesNoCursor(t *testing.T) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 3})
	httpServer := httptest.NewServer(server.handler())
	t.Cleanup(httpServer.Close)

	page := fetchBudgetPage(t, httpServer.URL, "")
	require.Len(t, page.outputs, 3)
	require.Empty(t, page.cursor)
}

func TestMatchesRequestsHonourScanBudgetAndCursorParameter(t *testing.T) {
	t.Parallel()

	node := &pagedNode{total: 5}
	server := newTestServer(node)
	server.maxMatchScan = 123
	cursor := budgetCursor(2)

	resp := serve(t, server, http.MethodGet,
		"/matches?cursor="+url.QueryEscape(cursor.String()), nil)
	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, 123, node.lastQuery.MaxScan)
	require.Equal(t, cursor, node.lastQuery.Cursor)

	for _, bad := range []string{"1", "a:b:c:d", "1:2:3:zz", "1:2:3:abcd"} {
		resp := serve(t, server, http.MethodGet,
			"/matches?cursor="+url.QueryEscape(bad), nil)
		require.Equal(t, http.StatusBadRequest, resp.Code, bad)
	}
}

func TestMatchCursorRoundTrip(t *testing.T) {
	t.Parallel()

	in := &MatchCursor{
		Slot:             123,
		TransactionIndex: 4,
		OutputIndex:      5,
		TransactionID:    []byte(strings.Repeat("\x01", 32)),
	}
	out, err := ParseMatchCursor(in.String())
	require.NoError(t, err)
	require.Equal(t, in, out)
}

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
