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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// fetchBudgetPage requests one opted-in page of at most limit rows.
func fetchBudgetPage(
	t *testing.T,
	baseURL string,
	limit int,
	cursor string,
) budgetPage {
	t.Helper()
	target := baseURL + "/matches?limit=" + strconv.Itoa(limit)
	if cursor != "" {
		target += "&cursor=" + url.QueryEscape(cursor)
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
	page := budgetPage{cursor: resp.Header.Get(nextCursorHeader)}
	for _, row := range rows {
		page.outputs = append(page.outputs, row.OutputIndex)
	}
	return page
}

func fetchAllPages(
	t *testing.T,
	baseURL string,
	limit int,
) ([]uint32, []int) {
	t.Helper()
	var all []uint32
	var sizes []int
	cursor := ""
	for range 200 {
		page := fetchBudgetPage(t, baseURL, limit, cursor)
		all = append(all, page.outputs...)
		sizes = append(sizes, len(page.outputs))
		if page.cursor == "" {
			break
		}
		cursor = page.cursor
	}
	return all, sizes
}

func sequence(n int) []uint32 {
	ret := make([]uint32, n)
	for i := range ret {
		ret[i] = uint32(i) // #nosec G115 -- test index is small
	}
	return ret
}

func requireIncompleteResultRefused(t *testing.T, resp *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusUnprocessableEntity, resp.Code)
	var rows []json.RawMessage
	require.Error(
		t,
		json.Unmarshal(resp.Body.Bytes(), &rows),
		"a refused result must not decode as a match array",
	)
	var hint struct {
		Hint string `json:"hint"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &hint))
	require.Contains(t, hint.Hint, "limit")
	require.Empty(t, resp.Header().Get(nextCursorHeader))
}

func TestMatchesWithoutLimitRefusesMoreThanTheRowBudget(t *testing.T) {
	t.Parallel()

	resp := serve(t, newTestServer(&pagedNode{total: maxMatchRows + 1}),
		http.MethodGet, "/matches", nil)
	requireIncompleteResultRefused(t, resp)
}

func TestMatchesWithoutLimitReturnsAResultThatFillsTheRowBudget(
	t *testing.T,
) {
	t.Parallel()

	resp := serve(t, newTestServer(&pagedNode{total: maxMatchRows}),
		http.MethodGet, "/matches", nil)
	require.Equal(t, http.StatusOK, resp.Code)
	var rows []json.RawMessage
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &rows))
	require.Len(t, rows, maxMatchRows)
	require.Empty(t, resp.Header().Get(nextCursorHeader))
}

func TestMatchesWithoutLimitRefusesMoreThanTheByteBudget(t *testing.T) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 100})
	server.maxMatchBytes = 1500
	requireIncompleteResultRefused(
		t,
		serve(t, server, http.MethodGet, "/matches", nil),
	)
}

func TestMatchesWithoutLimitRefusesAScanLimitedResult(t *testing.T) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 100, scanLimit: 7})
	requireIncompleteResultRefused(
		t,
		serve(t, server, http.MethodGet, "/matches", nil),
	)
}

func TestMatchesLimitPagesWithoutGapsOrDuplicates(t *testing.T) {
	t.Parallel()

	for _, total := range []int{25, 20} {
		httpServer := httptest.NewServer(
			newTestServer(&pagedNode{total: uint64(total)}).handler(),
		)
		all, sizes := fetchAllPages(t, httpServer.URL, 10)
		httpServer.Close()
		require.Equal(t, sequence(total), all)
		// A page that ends exactly at the last row carries no cursor.
		want := []int{10, 10}
		if total == 25 {
			want = append(want, 5)
		}
		require.Equal(t, want, sizes)
	}
}

func TestMatchesLimitByteBudgetEndsThePageWithACursor(t *testing.T) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 100})
	server.maxMatchBytes = 1500
	httpServer := httptest.NewServer(server.handler())
	t.Cleanup(httpServer.Close)

	first := fetchBudgetPage(t, httpServer.URL, maxMatchRows, "")
	require.NotEmpty(t, first.outputs)
	require.Less(t, len(first.outputs), 100)
	require.NotEmpty(t, first.cursor)
	all, _ := fetchAllPages(t, httpServer.URL, maxMatchRows)
	require.Equal(t, sequence(100), all)
}

func TestMatchesLimitScanBudgetEndsThePageWithACursor(t *testing.T) {
	t.Parallel()

	server := newTestServer(&pagedNode{total: 100, scanLimit: 7})
	httpServer := httptest.NewServer(server.handler())
	t.Cleanup(httpServer.Close)

	page := fetchBudgetPage(t, httpServer.URL, 50, "")
	require.Len(t, page.outputs, 7)
	require.Equal(t, budgetCursor(6).String(), page.cursor)
}

func TestMatchesRequestsHonourScanBudgetLimitAndCursor(t *testing.T) {
	t.Parallel()

	node := &pagedNode{total: 5}
	server := newTestServer(node)
	server.maxMatchScan = 123
	cursor := budgetCursor(2)

	resp := serve(t, server, http.MethodGet,
		"/matches?limit=7&cursor="+url.QueryEscape(cursor.String()), nil)
	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, 123, node.lastQuery.MaxScan)
	require.Equal(t, 7, node.lastQuery.Limit)
	require.Equal(t, cursor, node.lastQuery.Cursor)

	for _, bad := range []string{
		"limit=1&cursor=1",
		"limit=1&cursor=a:b:c:d",
		"limit=1&cursor=1:2:3:zz",
		"limit=1&cursor=1:2:3:abcd",
		"cursor=" + url.QueryEscape(cursor.String()),
		"limit=0",
		"limit=-1",
		"limit=abc",
		"limit=" + strconv.Itoa(maxMatchRows+1),
	} {
		resp := serve(t, server, http.MethodGet, "/matches?"+bad, nil)
		require.Equal(t, http.StatusBadRequest, resp.Code, bad)
	}
}

// closeOrderIterator records whether the iterator, which stands for the
// read snapshot, was closed before the response body was first written.
type closeOrderIterator struct {
	pagedIterator
	closed *atomic.Bool
}

func (i *closeOrderIterator) Close() { i.closed.Store(true) }

type closeOrderNode struct {
	mockNode
	closed *atomic.Bool
}

func (n *closeOrderNode) Matches(
	context.Context,
	MatchQuery,
) (MatchIterator, error) {
	return &closeOrderIterator{
		pagedIterator: pagedIterator{total: 3},
		closed:        n.closed,
	}, nil
}

type firstWriteRecorder struct {
	*httptest.ResponseRecorder
	closed        *atomic.Bool
	closedAtFirst bool
	sawWrite      bool
}

func (w *firstWriteRecorder) Write(p []byte) (int, error) {
	if !w.sawWrite {
		w.sawWrite = true
		w.closedAtFirst = w.closed.Load()
	}
	return w.ResponseRecorder.Write(p)
}

func TestMatchesReleaseTheSnapshotBeforeWritingTheBody(t *testing.T) {
	t.Parallel()

	closed := &atomic.Bool{}
	server := newTestServer(&closeOrderNode{closed: closed})
	w := &firstWriteRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		closed:           closed,
	}
	server.handler().ServeHTTP(
		w,
		httptest.NewRequest(http.MethodGet, "/matches", nil),
	)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, w.sawWrite)
	require.True(
		t,
		w.closedAtFirst,
		"a client-paced body write must not hold the read snapshot",
	)
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

// stallingNode returns a few matches and then blocks until the request
// lifetime ends, standing in for a slow storage scan.
type stallingNode struct{ mockNode }

func (*stallingNode) Matches(
	ctx context.Context,
	_ MatchQuery,
) (MatchIterator, error) {
	return &stallingIterator{pagedIterator: pagedIterator{total: 10}, ctx: ctx}, nil
}

type stallingIterator struct {
	pagedIterator
	ctx context.Context
}

func (i *stallingIterator) Next() (Match, bool, error) {
	if i.next < 3 {
		return i.pagedIterator.Next()
	}
	<-i.ctx.Done()
	return Match{}, false, i.ctx.Err()
}

func TestMatchesPastTheRequestLifetimeFailInsteadOfReturningAPrefix(
	t *testing.T,
) {
	t.Parallel()

	server := newTestServer(&stallingNode{})
	server.matchStreamTimeout = 50 * time.Millisecond
	resp := serve(t, server, http.MethodGet, "/matches", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	var rows []json.RawMessage
	require.Error(t, json.Unmarshal(resp.Body.Bytes(), &rows))
}
