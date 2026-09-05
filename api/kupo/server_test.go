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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type mockNode struct {
	tip         Point
	tipCalls    int
	snapshotTip Point
	matches     []Match
	lastQuery   MatchQuery
	datum       *Datum
	script      *Script
	checkpoints []Point
	checkpoint  *Point
	metadata    []Metadata
	blockHash   string
	health      Health
	healthTip   Point
	healthCode  int
	err         error
}

func (m *mockNode) Tip() (Point, error) {
	m.tipCalls++
	return m.tip, m.err
}

func (m *mockNode) Matches(
	_ context.Context,
	query MatchQuery,
) (MatchIterator, error) {
	m.lastQuery = query
	if m.err != nil {
		return nil, m.err
	}
	return &sliceMatchIterator{tip: m.tip, matches: m.matches}, nil
}

type sliceMatchIterator struct {
	tip     Point
	matches []Match
	next    int
}

func (i *sliceMatchIterator) Tip() Point { return i.tip }

func (i *sliceMatchIterator) Next() (Match, bool, error) {
	if i.next >= len(i.matches) {
		return Match{}, false, nil
	}
	match := i.matches[i.next]
	i.next++
	return match, true, nil
}

func (*sliceMatchIterator) Close() {}

func (m *mockNode) Datum(
	context.Context,
	[]byte,
) (*Datum, Point, error) {
	return m.datum, m.snapshotTip, m.err
}

func (m *mockNode) Script(
	context.Context,
	[]byte,
) (*Script, Point, error) {
	return m.script, m.snapshotTip, m.err
}

func (m *mockNode) Checkpoints(context.Context) ([]Point, Point, error) {
	return m.checkpoints, m.snapshotTip, m.err
}

func (m *mockNode) Checkpoint(
	context.Context,
	uint64,
	bool,
) (*Point, Point, error) {
	return m.checkpoint, m.snapshotTip, m.err
}

func (m *mockNode) Metadata(
	context.Context,
	uint64,
	[]byte,
) ([]Metadata, string, Point, error) {
	return m.metadata, m.blockHash, m.snapshotTip, m.err
}

func (m *mockNode) Health() (Health, Point, int, error) {
	return m.health, m.healthTip, m.healthCode, m.err
}

func newTestServer(node KupoNode) *Server {
	return New(Config{ListenAddress: ":0"}, node, nil)
}

func serve(
	t *testing.T,
	server *Server,
	method, target string,
	body io.Reader,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, req)
	return recorder
}

func TestImmutablePatterns(t *testing.T) {
	node := &mockNode{
		tip: Point{SlotNo: 42, HeaderHash: strings.Repeat("ab", 32)},
	}
	server := newTestServer(node)

	response := serve(t, server, http.MethodGet, "/patterns", nil)
	if response.Code != http.StatusOK || response.Body.String() != "[\"*\"]\n" {
		t.Fatalf("GET /patterns = %d %q", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Most-Recent-Checkpoint") != "42" {
		t.Fatalf("missing checkpoint header: %v", response.Header())
	}

	response = serve(
		t,
		server,
		http.MethodPut,
		"/patterns",
		bytes.NewBufferString(
			`{"patterns":["*/*","`+strings.Repeat(
				"ab",
				28,
			)+`.*"],"rollback_to":"origin"}`,
		),
	)
	if response.Code != http.StatusOK || response.Body.String() != "[\"*\"]\n" {
		t.Fatalf("PUT /patterns = %d %q", response.Code, response.Body.String())
	}

	response = serve(t, server, http.MethodDelete, "/patterns/*", nil)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("DELETE /patterns/* status = %d", response.Code)
	}
	response = serve(t, server, http.MethodDelete, "/matches/*", nil)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("DELETE /matches/* status = %d", response.Code)
	}
	response = serve(t, server, http.MethodGet, "/patterns/not-a-pattern", nil)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid pattern status = %d", response.Code)
	}
}

func TestMatchesQueryAndQuantityEncoding(t *testing.T) {
	datum, scriptHash := strings.Repeat("11", 32), strings.Repeat("22", 28)
	resolvedDatum := "d87980"
	node := &mockNode{
		tip: Point{SlotNo: 99, HeaderHash: strings.Repeat("aa", 32)},
		matches: []Match{{
			TransactionIndex: 2,
			TransactionID:    strings.Repeat("33", 32),
			OutputIndex:      1,
			Address:          "addr_test1test",
			Value: Value{
				Coins:  123,
				Assets: map[string]uint64{strings.Repeat("44", 28) + ".01": 7},
			},
			DatumHash:  &datum,
			DatumType:  "inline",
			ScriptHash: &scriptHash,
			CreatedAt:  Point{SlotNo: 98, HeaderHash: strings.Repeat("55", 32)},
			Datum:      &resolvedDatum,
			Script: &Script{
				Language: "plutus:v2",
				Script:   "4d01",
			},
		}},
	}
	server := newTestServer(node)
	req := httptest.NewRequest(
		http.MethodGet,
		"/matches/*/*?resolve_hashes&unspent&order=oldest_first&"+
			"transaction_id="+strings.Repeat("33", 32)+"&output_index=1",
		nil,
	)
	req.Header.Set("Accept", "application/json;asset-quantity=string")
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"matches status = %d: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(
		got,
		"asset-quantity=string",
	) {
		t.Fatalf("Content-Type = %q", got)
	}
	if node.lastQuery.Pattern != "*/*" || !node.lastQuery.ResolveHashes ||
		node.lastQuery.Status != MatchStatusUnspent ||
		!node.lastQuery.OldestFirst || node.lastQuery.OutputIndex == nil {
		t.Fatalf("parsed query = %#v", node.lastQuery)
	}
	var payload []map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	value := payload[0]["value"].(map[string]any)
	if value["coins"] != "123" {
		t.Fatalf("string coins = %#v", value["coins"])
	}
	resolvedScript := payload[0]["script"].(map[string]any)
	if resolvedScript["language"] != "plutus:v2" {
		t.Fatalf("resolved script = %#v", resolvedScript)
	}
}

func TestMatchQueryValidation(t *testing.T) {
	node := &mockNode{tip: Point{HeaderHash: strings.Repeat("aa", 32)}}
	server := newTestServer(node)
	invalid := []string{
		"/matches?spent&unspent",
		"/matches?spent=false",
		"/matches?resolve_hashes=1",
		"/matches?order=oldest_first&order=most_recent_first",
		"/matches?created_after=1&spent_after=2",
		"/matches?created_before=1&spent_before=2",
		"/matches?asset_name=01",
		"/matches?transaction_id=" + strings.Repeat(
			"11",
			32,
		) + "&output_index=x",
		"/matches?output_index=1",
		"/matches?unknown=1",
		"/matches?policy_id=" + strings.Repeat("11", 28) +
			"&transaction_id=" + strings.Repeat("22", 32),
	}
	for _, target := range invalid {
		t.Run(target, func(t *testing.T) {
			response := serve(t, server, http.MethodGet, target, nil)
			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"status = %d: %s",
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}

func TestConditionalRequestAndMethodMismatch(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	node := &mockNode{snapshotTip: Point{SlotNo: 42, HeaderHash: hash}}
	server := newTestServer(node)
	req := httptest.NewRequest(http.MethodGet, "/checkpoints", nil)
	req.Header.Set("If-None-Match", hash)
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNotModified || recorder.Body.Len() != 0 {
		t.Fatalf(
			"conditional response = %d %q",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	if node.tipCalls != 0 {
		t.Fatalf("conditional request made %d separate tip reads", node.tipCalls)
	}
	response := serve(t, server, http.MethodPost, "/health", nil)
	if response.Code != http.StatusNotAcceptable {
		t.Fatalf("POST /health status = %d", response.Code)
	}
	response = serve(
		t,
		server,
		http.MethodGet,
		"/checkpoints/42?unknown=1",
		nil,
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown query status = %d", response.Code)
	}
	response = serve(
		t,
		server,
		http.MethodGet,
		"/checkpoints/42?strict=false",
		nil,
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("valued strict flag status = %d", response.Code)
	}
}

func TestDataResponsesUseOperationSnapshotTip(t *testing.T) {
	snapshotHash := strings.Repeat("bb", 32)
	for _, test := range []struct {
		name   string
		target string
	}{
		{name: "datum", target: "/datums/" + strings.Repeat("11", 32)},
		{name: "script", target: "/scripts/" + strings.Repeat("22", 28)},
		{name: "checkpoints", target: "/checkpoints"},
		{name: "checkpoint", target: "/checkpoints/42"},
		{name: "metadata", target: "/metadata/42"},
	} {
		t.Run(test.name, func(t *testing.T) {
			liveHash := strings.Repeat("aa", 32)
			node := &mockNode{
				tip: Point{
					SlotNo:     43,
					HeaderHash: liveHash,
				},
				snapshotTip: Point{SlotNo: 42, HeaderHash: snapshotHash},
				datum:       &Datum{Datum: "d87980"},
				script:      &Script{Language: "native", Script: "8200"},
				checkpoints: []Point{{SlotNo: 42, HeaderHash: snapshotHash}},
				checkpoint:  &Point{SlotNo: 42, HeaderHash: snapshotHash},
				metadata:    []Metadata{{Hash: strings.Repeat("33", 32)}},
			}
			req := httptest.NewRequest(http.MethodGet, test.target, nil)
			req.Header.Set("If-None-Match", liveHash)
			recorder := httptest.NewRecorder()
			newTestServer(node).handler().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusOK || recorder.Body.Len() == 0 {
				t.Fatalf(
					"live-tip conditional response = %d %q",
					recorder.Code,
					recorder.Body.String(),
				)
			}
			if got := recorder.Header().Get("ETag"); got != snapshotHash {
				t.Fatalf("snapshot ETag = %q, want %q", got, snapshotHash)
			}

			req = httptest.NewRequest(http.MethodGet, test.target, nil)
			req.Header.Set("If-None-Match", snapshotHash)
			recorder = httptest.NewRecorder()
			newTestServer(node).handler().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusNotModified || recorder.Body.Len() != 0 {
				t.Fatalf(
					"snapshot conditional response = %d %q",
					recorder.Code,
					recorder.Body.String(),
				)
			}
			if node.tipCalls != 0 {
				t.Fatalf("response made %d separate tip reads", node.tipCalls)
			}
			if got := recorder.Header().Get("X-Most-Recent-Checkpoint"); got != "42" {
				t.Fatalf("snapshot checkpoint header = %q, want 42", got)
			}
		})
	}
}

func TestV1RouteAliases(t *testing.T) {
	node := &mockNode{
		snapshotTip: Point{SlotNo: 42, HeaderHash: strings.Repeat("aa", 32)},
		healthCode:  http.StatusOK,
		datum:       &Datum{Datum: "d87980"},
		script:      &Script{Language: "native", Script: "00"},
	}
	server := newTestServer(node)
	for _, test := range []struct {
		method string
		target string
		body   string
		status int
	}{
		{http.MethodGet, "/v1/matches", "", http.StatusOK},
		{http.MethodGet, "/v1/matches/*", "", http.StatusOK},
		{http.MethodGet, "/v1/matches/*/*", "", http.StatusOK},
		{http.MethodDelete, "/v1/matches/*", "", http.StatusBadRequest},
		{http.MethodDelete, "/v1/matches/*/*", "", http.StatusBadRequest},
		{
			http.MethodGet,
			"/v1/datums/" + strings.Repeat("11", 32),
			"",
			http.StatusOK,
		},
		{
			http.MethodGet,
			"/v1/scripts/" + strings.Repeat("22", 28),
			"",
			http.StatusOK,
		},
		{http.MethodGet, "/v1/patterns", "", http.StatusOK},
		{http.MethodPut, "/v1/patterns", `{"patterns":["*"]}`, http.StatusOK},
		{http.MethodGet, "/v1/patterns/*", "", http.StatusOK},
		{http.MethodGet, "/v1/patterns/*/*", "", http.StatusOK},
		{http.MethodPut, "/v1/patterns/*", "", http.StatusOK},
		{http.MethodPut, "/v1/patterns/*/*", "", http.StatusOK},
		{http.MethodDelete, "/v1/patterns/*", "", http.StatusBadRequest},
		{http.MethodDelete, "/v1/patterns/*/*", "", http.StatusBadRequest},
		{http.MethodGet, "/v1/checkpoints", "", http.StatusOK},
		{http.MethodGet, "/v1/checkpoints/42", "", http.StatusOK},
		{http.MethodGet, "/v1/metadata/0", "", http.StatusOK},
		{http.MethodGet, "/v1/health", "", http.StatusOK},
		{http.MethodGet, "/v1/metrics", "", http.StatusOK},
	} {
		t.Run(test.method+" "+test.target, func(t *testing.T) {
			response := serve(
				t,
				server,
				test.method,
				test.target,
				strings.NewReader(test.body),
			)
			if response.Code != test.status {
				t.Fatalf(
					"status = %d, want %d: %s",
					response.Code,
					test.status,
					response.Body.String(),
				)
			}
		})
	}

	response := serve(t, server, http.MethodPost, "/v1/health", nil)
	if response.Code != http.StatusNotAcceptable {
		t.Fatalf("POST /v1/health status = %d", response.Code)
	}
}

func TestDatumAndScriptNotFound(t *testing.T) {
	server := newTestServer(&mockNode{
		snapshotTip: Point{SlotNo: 42, HeaderHash: strings.Repeat("aa", 32)},
	})
	for _, target := range []string{
		"/datums/" + strings.Repeat("11", 32),
		"/scripts/" + strings.Repeat("22", 28),
	} {
		t.Run(target, func(t *testing.T) {
			response := serve(t, server, http.MethodGet, target, nil)
			if response.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", response.Code)
			}
		})
	}
}

func TestHealthAndMetricsNegotiation(t *testing.T) {
	checkpoint, nodeTip, seconds, sync := uint64(40), uint64(50), uint64(2), 0.8
	health := Health{
		ConnectionStatus:       "connected",
		MostRecentCheckpoint:   &checkpoint,
		MostRecentNodeTip:      &nodeTip,
		SecondsSinceLastBlock:  &seconds,
		NetworkSynchronization: &sync,
		Version:                "test",
	}
	health.Configuration.Indexes = "installed"
	node := &mockNode{
		// A concurrent tip read would observe 41. Health headers must use the
		// checkpoint sampled with the response body instead.
		tip:        Point{SlotNo: 41, HeaderHash: strings.Repeat("aa", 32)},
		health:     health,
		healthTip:  Point{SlotNo: 40, HeaderHash: strings.Repeat("bb", 32)},
		healthCode: http.StatusAccepted,
	}
	server := newTestServer(node)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Accept", "text/plain")
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusAccepted ||
		!strings.Contains(
			recorder.Body.String(),
			"# TYPE kupo_network_synchronization gauge",
		) {
		t.Fatalf(
			"health prometheus = %d %q",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	if recorder.Header().Get("ETag") == "" {
		t.Fatal("health response omitted ETag")
	}
	if got := recorder.Header().Get("X-Most-Recent-Checkpoint"); got != "40" {
		t.Fatalf("health checkpoint header = %q, want 40", got)
	}

	response := serve(t, server, http.MethodGet, "/metrics", nil)
	if response.Code != http.StatusOK ||
		!strings.HasPrefix(
			response.Header().Get("Content-Type"),
			"application/json",
		) {
		t.Fatalf(
			"metrics = %d %q",
			response.Code,
			response.Header().Get("Content-Type"),
		)
	}

	for _, target := range []string{"/health", "/metrics"} {
		req = httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("Accept", "*/*")
		recorder = httptest.NewRecorder()
		server.handler().ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK &&
			recorder.Code != http.StatusAccepted {
			t.Fatalf("wildcard Accept for %s = %d", target, recorder.Code)
		}
		if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(
			got,
			"text/plain",
		) {
			t.Fatalf("wildcard Accept for %s returned %q", target, got)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "text/plain")
	recorder = httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK ||
		!strings.HasPrefix(
			recorder.Header().Get("Content-Type"),
			"text/plain",
		) {
		t.Fatalf(
			"Prometheus metrics = %d %q",
			recorder.Code,
			recorder.Header().Get("Content-Type"),
		)
	}

	for _, target := range []string{"/health", "/metrics"} {
		req = httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("Accept", "image/png")
		recorder = httptest.NewRecorder()
		server.handler().ServeHTTP(recorder, req)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("unsupported Accept for %s = %d", target, recorder.Code)
		}
	}
}

func TestScriptFromLedgerPlutusV4(t *testing.T) {
	script := scriptFromLedger(lcommon.PlutusV4Script{0x4d, 0x01})
	if script == nil || script.Language != "plutus:v4" ||
		script.Script != "4d01" {
		t.Fatalf("Plutus V4 script = %#v", script)
	}
}

func TestStartStop(t *testing.T) {
	server := newTestServer(&mockNode{})
	if err := server.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	stopCtx, stopCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer stopCancel()
	if err := server.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}
