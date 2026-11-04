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
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// onlySpan returns the attributes of the single ended span, after checking
// its name and that it did not record a failure.
func onlySpan(
	t *testing.T,
	recorder *tracetest.SpanRecorder,
	name string,
) map[attribute.Key]attribute.Value {
	t.Helper()
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, name, spans[0].Name())
	require.Equal(t, codes.Unset, spans[0].Status().Code)
	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range spans[0].Attributes() {
		attrs[kv.Key] = kv.Value
	}
	return attrs
}

func newTracingTestOuroboros(t *testing.T) *Ouroboros {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, _ = bus.Subscribe(ledger.ChainsyncEventType)
	return newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestChainsyncClientRollForwardRecordsSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	o := newTracingTestOuroboros(t)
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID},
		0,
		header,
		tip,
	))

	attrs := onlySpan(t, recorder, "block.header_received")
	require.Equal(t, connID.String(), attrs["peer.connection_id"].AsString())
	require.Equal(t, int64(100), attrs["block.slot"].AsInt64())
	require.Equal(t, int64(1), attrs["block.number"].AsInt64())
	require.Equal(
		t,
		tracing.BlockTraceID(header.Hash().Bytes()),
		recorder.Ended()[0].SpanContext().TraceID(),
	)
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestHeaderFromTwoPeersTwoSpansOneTrace(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	header := newTestBlockHeader(100, 1, 0xbb)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}
	for _, remote := range []string{"1.1.1.1:3001", "2.2.2.2:3001"} {
		o := newTracingTestOuroboros(t)
		require.NoError(t, o.chainsyncClientRollForward(
			ochainsync.CallbackContext{
				ConnectionId: newTestConnId("127.0.0.1:6000", remote),
			},
			0,
			header,
			tip,
		))
	}

	var peers []string
	for _, span := range recorder.Ended() {
		if span.Name() != "block.header_received" {
			continue
		}
		require.Equal(
			t,
			tracing.BlockTraceID(header.Hash().Bytes()),
			span.SpanContext().TraceID(),
		)
		for _, kv := range span.Attributes() {
			if kv.Key == "peer.connection_id" {
				peers = append(peers, kv.Value.AsString())
			}
		}
	}
	require.Len(t, peers, 2)
	require.NotEqual(t, peers[0], peers[1])
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestBlockfetchClientBlockRawRecordsSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	o := newTracingTestOuroboros(t)
	connID := testConnId()
	raw := testutil.BuildDecodableConwayBlockBytes(t, 42, 7)
	block, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, raw)
	require.NoError(t, err)

	require.NoError(t, o.blockfetchClientBlockRaw(
		blockfetch.CallbackContext{ConnectionId: connID},
		gledger.BlockTypeConway,
		raw,
	))

	attrs := onlySpan(t, recorder, "block.body_received")
	require.Equal(t, connID.String(), attrs["peer.connection_id"].AsString())
	require.Equal(t, int64(42), attrs["block.slot"].AsInt64())
	require.Equal(
		t,
		tracing.BlockTraceID(block.Hash().Bytes()),
		recorder.Ended()[0].SpanContext().TraceID(),
	)
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestBlockfetchDecodeFailureRecordsNoBlockSpan(t *testing.T) {
	recorder := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	o := newTracingTestOuroboros(t)

	require.Error(t, o.blockfetchClientBlockRaw(
		blockfetch.CallbackContext{ConnectionId: testConnId()},
		gledger.BlockTypeConway,
		[]byte{0xff},
	))

	require.Empty(t, recorder.Ended())
}
