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

package ouroboros

import (
	"context"
	"errors"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// metricValue returns the value of the single series of the named counter or
// gauge, or the series carrying the given label value when label is not "".
func metricValue(
	t *testing.T,
	reg *prometheus.Registry,
	name string,
	label string,
) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			if label != "" {
				found := false
				for _, pair := range m.GetLabel() {
					if pair.GetValue() == label {
						found = true
					}
				}
				if !found {
					continue
				}
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue(), true
			}
			return m.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

type failingRangeRequester struct{}

func (failingRangeRequester) RequestRange(
	context.Context,
	blockfetch.RangeRequest,
) (uint64, error) {
	return 0, errors.New("request refused")
}

func TestBlockfetchRequestMetricsCountIssuedRequests(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	var requester blockfetchRangeRequester = &fakeBlockfetchRangeRequester{}
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return requester, nil
	}
	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	issued, ok := metricValue(
		t, reg, "dingo_blockfetch_requests_issued_total", "",
	)
	require.True(t, ok, "counter must be exported before the first request")
	require.Zero(t, issued)
	last, ok := metricValue(
		t, reg, "dingo_blockfetch_last_request_timestamp_seconds", "",
	)
	require.True(t, ok)
	require.Zero(t, last, "no request issued yet")

	before := time.Now().Add(-time.Second)
	_, err := o.BlockfetchClientRequestRange(testConnId(), start, end)
	require.NoError(t, err)
	_, err = o.BlockfetchClientRequestRange(testConnId(), start, end)
	require.NoError(t, err)

	issued, _ = metricValue(
		t, reg, "dingo_blockfetch_requests_issued_total", "",
	)
	require.Equal(t, float64(2), issued)
	last, _ = metricValue(
		t, reg, "dingo_blockfetch_last_request_timestamp_seconds", "",
	)
	require.Greater(t, last, float64(before.Unix()))

	// A request the client refuses was never issued.
	requester = failingRangeRequester{}
	_, err = o.BlockfetchClientRequestRange(testConnId(), start, end)
	require.Error(t, err)
	issued, _ = metricValue(
		t, reg, "dingo_blockfetch_requests_issued_total", "",
	)
	require.Equal(t, float64(2), issued)
}

func completedTotals(
	t *testing.T,
	reg *prometheus.Registry,
) (float64, float64) {
	t.Helper()
	ok, _ := metricValue(
		t, reg, "dingo_blockfetch_requests_completed_total", "ok",
	)
	failed, _ := metricValue(
		t, reg, "dingo_blockfetch_requests_completed_total", "error",
	)
	return ok, failed
}

func TestBlockfetchRequestMetricsCountCompletedRequests(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	connId := testConnId()
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return &fakeBlockfetchRangeRequester{}, nil
	}
	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	for _, result := range []string{"ok", "error"} {
		v, ok := metricValue(
			t, reg, "dingo_blockfetch_requests_completed_total", result,
		)
		require.True(t, ok, "result %q must be materialized", result)
		require.Zero(t, v)
	}

	// Each request reaches RangeDoneFunc exactly once, under its own ID.
	results := []error{nil, nil, errors.New("no blocks")}
	for _, rangeErr := range results {
		requestId, err := o.BlockfetchClientRequestRange(
			connId, start, end,
		)
		require.NoError(t, err)
		require.NoError(t, o.blockfetchClientRangeDone(
			blockfetch.CallbackContext{
				ConnectionId: connId,
				RequestId:    requestId,
			},
			rangeErr,
		))
	}

	ok, failed := completedTotals(t, reg)
	require.Equal(t, float64(2), ok)
	require.Equal(t, float64(1), failed)
}

// terminalThenFailRequester reports the request terminal through
// RangeDoneFunc and then fails the call that queued it, as gouroboros does
// when protocol shutdown drains a request whose send is still failing.
type terminalThenFailRequester struct {
	o      *Ouroboros
	connId ouroboros.ConnectionId
}

func (f *terminalThenFailRequester) RequestRange(
	context.Context,
	blockfetch.RangeRequest,
) (uint64, error) {
	if err := f.o.blockfetchClientRangeDone(
		blockfetch.CallbackContext{ConnectionId: f.connId, RequestId: 1},
		errors.New("protocol shutting down"),
	); err != nil {
		return 0, err
	}
	return 0, errors.New("protocol shutting down")
}

// A request whose RequestRange fails is not counted as issued, so its
// terminal callback must not be counted as completed either, or completed
// can exceed issued.
func TestBlockfetchRequestMetricsSkipCompletionOfFailedRequest(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	connId := testConnId()
	requester := &terminalThenFailRequester{o: o, connId: connId}
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return requester, nil
	}
	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	_, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.Error(t, err)

	issued, _ := metricValue(
		t, reg, "dingo_blockfetch_requests_issued_total", "",
	)
	ok, failed := completedTotals(t, reg)
	require.Zero(t, issued)
	require.Zero(t, ok)
	require.Zero(t, failed, "a request never counted as issued completed")
}

// A peer that answers before RequestRange returns still has its request
// counted once on each side, with the result its terminal callback reported.
func TestBlockfetchRequestMetricsCountEarlyCompletion(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	connId := testConnId()
	requester := &terminalBeforeIdRequester{o: o, connId: connId}
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return requester, nil
	}
	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	_, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.NoError(t, err)

	issued, _ := metricValue(
		t, reg, "dingo_blockfetch_requests_issued_total", "",
	)
	ok, failed := completedTotals(t, reg)
	require.Equal(t, float64(1), issued)
	require.Equal(t, float64(1), ok)
	require.Zero(t, failed)
}
